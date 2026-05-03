/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.S2Geography.GeographyWKBSerializer
import org.apache.sedona.common.geography.{Functions => GeographyFunctions}
import org.apache.sedona.core.spatialOperator.{SpatialPredicate, SpatialPredicateEvaluators}
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators.SpatialPredicateEvaluator
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}

import scala.collection.JavaConverters._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, GenericInternalRow, JoinedRow, Predicate, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.locationtech.jts.index.SpatialIndex

import java.util.Collections
import scala.collection.mutable

case class BroadcastIndexJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    streamShape: Expression,
    indexBuildSide: JoinSide,
    windowJoinSide: JoinSide,
    joinType: JoinType,
    spatialPredicate: SpatialPredicate,
    extraCondition: Option[Expression] = None,
    distance: Option[Expression] = None,
    geographyShape: Boolean = false)
    extends SedonaBinaryExecNode
    with TraitJoinQueryBase
    with Logging {

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x: Any =>
        throw new IllegalArgumentException(
          s"BroadcastIndexJoinExec should not take $x as the JoinType")
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val (streamed, broadcast) = indexBuildSide match {
    case LeftSide => (right, left.asInstanceOf[SpatialIndexExec])
    case RightSide => (left, right.asInstanceOf[SpatialIndexExec])
  }

  // Using lazy val to avoid serialization
  @transient private lazy val boundCondition: (InternalRow => Boolean) = extraCondition match {
    case Some(condition) =>
      Predicate.create(condition, streamed.output ++ broadcast.output).eval _ // SPARK3 anchor
    //      newPredicate(condition, broadcast.output ++ streamed.output).eval _ // SPARK2 anchor
    case None =>
      (r: InternalRow) => true
  }

  protected def createResultProjection(): InternalRow => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output,
        (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  private val (windowExpression, objectExpression) = if (indexBuildSide == windowJoinSide) {
    (broadcast.shape, streamShape)
  } else {
    (streamShape, broadcast.shape)
  }

  private val isRasterPredicate = windowExpression.dataType
    .isInstanceOf[RasterUDT] || objectExpression.dataType.isInstanceOf[RasterUDT]

  private val spatialExpression = (distance, spatialPredicate, isRasterPredicate) match {
    case (Some(r), SpatialPredicate.INTERSECTS, false) =>
      s"ST_Distance($windowExpression, $objectExpression) <= $r"
    case (Some(r), _, false) => s"ST_Distance($windowExpression, $objectExpression) < $r"
    case (None, _, false) => s"ST_$spatialPredicate($windowExpression, $objectExpression)"
    case (None, _, true) => s"RS_$spatialPredicate($windowExpression, $objectExpression)"
    case (Some(r), _, true) =>
      throw new UnsupportedOperationException(
        "Distance joins are not supported for raster predicates")
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + s" $spatialExpression" // SPARK3 anchor
//  override def simpleString: String = super.simpleString + s" $spatialExpression" // SPARK2 anchor

  private lazy val evaluator: SpatialPredicateEvaluator = if (indexBuildSide == windowJoinSide) {
    SpatialPredicateEvaluators.create(spatialPredicate)
  } else {
    SpatialPredicateEvaluators.create(SpatialPredicate.inverse(spatialPredicate))
  }

  // True when the build (window) side is also the side that originally appeared as the
  // left-hand argument of the spatial predicate. Mirrors the inverse logic used by
  // `evaluator` for the JTS path; required for Geography because Functions.contains is
  // asymmetric and we cannot rely on a SpatialPredicateEvaluator.
  private lazy val refinerSwap: Boolean = indexBuildSide != windowJoinSide

  private def newRefiner(): JoinRefiner = {
    if (geographyShape) {
      if (distance.isDefined) new GeographyDistanceRefiner(refinerSwap)
      else new GeographyRelationRefiner(spatialPredicate, refinerSwap)
    } else new JtsRefiner(evaluator)
  }

  private def innerJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      index: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val refiner = newRefiner()
    val joinedRow = new JoinedRow
    streamIter.flatMap { case (geom, row) =>
      if (geom == null) {
        Iterator.empty
      } else {
        joinedRow.withLeft(row)
        index.value
          .query(geom.getEnvelopeInternal)
          .iterator
          .asScala
          .asInstanceOf[Iterator[Geometry]]
          .filter(candidate => refiner.matches(candidate, geom))
          .map(candidate => joinedRow.withRight(refiner.unpackRow(candidate)))
          .filter(boundCondition)
      }
    }
  }

  private def semiJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      index: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val refiner = newRefiner()
    val joinedRow = new JoinedRow
    streamIter.flatMap { case (geom, row) =>
      val left = row
      if (geom == null) {
        Iterator.empty
      } else {
        joinedRow.withLeft(left)
        val anyMatches = index.value
          .query(geom.getEnvelopeInternal)
          .iterator
          .asScala
          .asInstanceOf[Iterator[Geometry]]
          .filter(candidate => refiner.matches(candidate, geom))
          .map(candidate => joinedRow.withRight(refiner.unpackRow(candidate)))
          .exists(boundCondition)

        if (anyMatches) Iterator.single(left) else Iterator.empty
      }
    }
  }

  private def antiJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      index: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val refiner = newRefiner()
    val joinedRow = new JoinedRow
    streamIter.flatMap { case (geom, row) =>
      val left = row
      joinedRow.withLeft(row)
      val anyMatches = (if (geom == null) Collections.EMPTY_LIST
                        else index.value.query(geom.getEnvelopeInternal)).iterator.asScala
        .asInstanceOf[Iterator[Geometry]]
        .filter(candidate => refiner.matches(candidate, geom))
        .map(candidate => joinedRow.withRight(refiner.unpackRow(candidate)))
        .exists(boundCondition)

      if (anyMatches) {
        Iterator.empty
      } else {
        Iterator.single(left)
      }
    }
  }

  private def outerJoin(
      streamIter: Iterator[(Geometry, UnsafeRow)],
      index: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val refiner = newRefiner()
    val joinedRow = new JoinedRow
    val nullRow = new GenericInternalRow(broadcast.output.length)

    streamIter.flatMap { case (geom, row) =>
      joinedRow.withLeft(row)
      val candidates = (if (geom == null) Collections.EMPTY_LIST
                        else index.value.query(geom.getEnvelopeInternal)).iterator.asScala
        .asInstanceOf[Iterator[Geometry]]
        .filter(candidate => refiner.matches(candidate, geom))

      new RowIterator {
        private var found = false
        override def advanceNext(): Boolean = {
          while (candidates.hasNext) {
            val candidateRow = refiner.unpackRow(candidates.next())
            if (boundCondition(joinedRow.withRight(candidateRow))) {
              found = true
              return true
            }
          }
          if (!found) {
            joinedRow.withRight(nullRow)
            found = true
            return true
          }
          false
        }
        override def getRow: InternalRow = joinedRow
      }.toScala
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val boundStreamShape = BindReferences.bindReference(streamShape, streamed.output)
    val streamResultsRaw = streamed.execute().asInstanceOf[RDD[UnsafeRow]]

    val broadcastIndex = broadcast.executeBroadcast[SpatialIndex]()

    val streamShapes = createStreamShapes(streamResultsRaw, boundStreamShape)

    streamShapes.mapPartitions { streamedIter =>
      val joinedIter = joinType match {
        case _: InnerLike =>
          innerJoin(streamedIter, broadcastIndex)
        case LeftSemi =>
          semiJoin(streamedIter, broadcastIndex)
        case LeftAnti =>
          antiJoin(streamedIter, broadcastIndex)
        case LeftOuter | RightOuter =>
          outerJoin(streamedIter, broadcastIndex)
        case x: Any =>
          throw new IllegalArgumentException(
            s"BroadcastIndexJoinExec should not take $x as the JoinType")
      }

      val resultProj = createResultProjection()
      joinedIter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  private def createStreamShapes(
      streamResultsRaw: RDD[UnsafeRow],
      boundStreamShape: Expression) = {
    distance match {
      case Some(distanceExpression) if geographyShape =>
        val boundDistance =
          BindReferences.bindReference(distanceExpression, streamed.output)
        // When the broadcast side already expanded its envelope by `d` (the
        // literal-radius case, where the planner forwards the same distance to
        // both sides so the per-row radius is also available here for the
        // refiner), keep the stream envelope unexpanded. The coarse filter then
        // matches the geometry path: `expand(build, d) ∩ stream`, not the wider
        // `expand(build, d) ∩ expand(stream, d)`. When only the stream side
        // received the distance (per-row radius bound to the stream side), the
        // build side is unexpanded and we still need to expand the stream
        // envelope to ensure the index returns all candidates within `d`.
        val streamSideExpands = broadcast.distance.isEmpty
        streamResultsRaw.map(row => {
          val serialized = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (serialized == null) {
            (null, row)
          } else {
            val geog = GeographyWKBSerializer.deserialize(serialized)
            val radius = boundDistance.eval(row).asInstanceOf[Double]
            val baseEnvelope = JoinedGeometry.geographyToEnvelopeGeometry(geog)
            val shape = if (streamSideExpands) {
              JoinedGeometry.geometryToExpandedEnvelope(baseEnvelope, radius, isGeography = true)
            } else {
              baseEnvelope
            }
            shape.setUserData(GeographyJoinShape(geog, row, radius))
            (shape, row)
          }
        })
      case Some(distanceExpression) =>
        streamResultsRaw.map(row => {
          val geom = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (geom == null) {
            (null, row)
          } else {
            val geometry = GeometrySerializer.deserialize(geom)
            val radius = BindReferences
              .bindReference(distanceExpression, streamed.output)
              .eval(row)
              .asInstanceOf[Double]
            val envelope = geometry.getEnvelopeInternal
            envelope.expandBy(radius)
            (geometry.getFactory.toGeometry(envelope), row)
          }
        })
      case _ if geographyShape =>
        streamResultsRaw.map(row => {
          val serialized = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (serialized == null) {
            (null, row)
          } else {
            val geog = GeographyWKBSerializer.deserialize(serialized)
            val shape = JoinedGeometry.geographyToEnvelopeGeometry(geog)
            shape.setUserData(GeographyJoinShape(geog, row))
            (shape, row)
          }
        })
      case _ =>
        streamResultsRaw.map(row => {
          val serializedObject = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (serializedObject == null) {
            (null, row)
          } else {
            val shape = if (isRasterPredicate) {
              if (boundStreamShape.dataType.isInstanceOf[RasterUDT]) {
                val raster = RasterSerializer.deserialize(serializedObject)
                JoinedGeometryRaster.rasterToWGS84Envelope(raster)
              } else {
                val geom = GeometrySerializer.deserialize(serializedObject)
                JoinedGeometryRaster.geometryToWGS84Envelope(geom)
              }
            } else {
              GeometrySerializer.deserialize(serializedObject)
            }
            (shape, row)
          }
        })
    }
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}

/**
 * Per-iter helper that decides whether a candidate from the broadcast index actually satisfies
 * the spatial predicate, and unpacks the candidate's `userData` into the output row.
 *
 * Three implementations: `JtsRefiner` for the planar JTS path, `GeographyRelationRefiner` for
 * non-distance Geography predicates (CONTAINS, WITHIN, INTERSECTS, EQUALS), and
 * `GeographyDistanceRefiner` for ST_DWithin on Geography.
 */
private sealed trait JoinRefiner {
  def matches(candidate: Geometry, streamShape: Geometry): Boolean
  def unpackRow(candidate: Geometry): UnsafeRow
}

private final class JtsRefiner(evaluator: SpatialPredicateEvaluator) extends JoinRefiner {
  private val factory = new PreparedGeometryFactory()
  private val preparedGeometries = new mutable.HashMap[Geometry, PreparedGeometry]
  override def matches(candidate: Geometry, streamShape: Geometry): Boolean =
    evaluator.eval(
      preparedGeometries.getOrElseUpdate(candidate, factory.create(candidate)),
      streamShape)
  override def unpackRow(candidate: Geometry): UnsafeRow =
    candidate.getUserData.asInstanceOf[UnsafeRow]
}

/**
 * Refines candidates with the appropriate `org.apache.sedona.common.geography.Functions`
 * predicate (CONTAINS / WITHIN / INTERSECTS / EQUALS). Caching of the per-Geography S2 ShapeIndex
 * happens inside `WKBGeography.getShapeIndexGeography()`, so we do not need a JTS-style
 * PreparedGeometry cache here — the build side keeps the same Geography JVM instances for the
 * lifetime of the broadcast. `swap` flips operand order when the build side does not correspond
 * to the predicate's left-hand argument (handles the `RIGHT JOIN` / right-broadcast case for
 * asymmetric predicates).
 */
private final class GeographyRelationRefiner(predicate: SpatialPredicate, swap: Boolean)
    extends JoinRefiner {
  override def matches(candidate: Geometry, streamShape: Geometry): Boolean = {
    val buildShape = candidate.getUserData.asInstanceOf[GeographyJoinShape]
    val streamShapeData = streamShape.getUserData.asInstanceOf[GeographyJoinShape]
    val (a, b) =
      if (swap) (streamShapeData.geog, buildShape.geog)
      else (buildShape.geog, streamShapeData.geog)
    predicate match {
      case SpatialPredicate.CONTAINS => GeographyFunctions.contains(a, b)
      case SpatialPredicate.WITHIN => GeographyFunctions.within(a, b)
      case SpatialPredicate.INTERSECTS => GeographyFunctions.intersects(a, b)
      case SpatialPredicate.EQUALS => GeographyFunctions.equals(a, b)
      case other =>
        throw new UnsupportedOperationException(
          s"Geography broadcast spatial join does not support predicate $other")
    }
  }

  override def unpackRow(candidate: Geometry): UnsafeRow =
    candidate.getUserData.asInstanceOf[GeographyJoinShape].row
}

/**
 * Refines candidates for ST_DWithin on Geography. The per-row distance threshold is carried on
 * the stream-side `GeographyJoinShape.radius`, populated when the stream shape is built.
 */
private final class GeographyDistanceRefiner(swap: Boolean) extends JoinRefiner {
  override def matches(candidate: Geometry, streamShape: Geometry): Boolean = {
    val buildShape = candidate.getUserData.asInstanceOf[GeographyJoinShape]
    val streamShapeData = streamShape.getUserData.asInstanceOf[GeographyJoinShape]
    val (a, b) =
      if (swap) (streamShapeData.geog, buildShape.geog)
      else (buildShape.geog, streamShapeData.geog)
    GeographyFunctions.dWithin(a, b, streamShapeData.radius)
  }

  override def unpackRow(candidate: Geometry): UnsafeRow =
    candidate.getUserData.asInstanceOf[GeographyJoinShape].row
}
