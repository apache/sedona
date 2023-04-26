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

import org.apache.sedona.core.spatialOperator.{SpatialPredicate, SpatialPredicateEvaluators}
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators.SpatialPredicateEvaluator
import org.apache.sedona.sql.utils.GeometrySerializer

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
  distance: Option[Expression] = None)
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
      case x =>
        throw new IllegalArgumentException(s"BroadcastIndexJoinExec should not take $x as the JoinType")
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
        output, (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  private val (windowExpression, objectExpression) = if (indexBuildSide == windowJoinSide) {
    (broadcast.shape, streamShape)
  } else {
    (streamShape, broadcast.shape)
  }

  private val spatialExpression = (distance, spatialPredicate) match {
    case (Some(r), SpatialPredicate.INTERSECTS) => s"ST_Distance($windowExpression, $objectExpression) <= $r"
    case (Some(r), _) => s"ST_Distance($windowExpression, $objectExpression) < $r"
    case (None, _) => s"ST_$spatialPredicate($windowExpression, $objectExpression)"
  }

  override def simpleString(maxFields: Int): String = super.simpleString(maxFields) + s" $spatialExpression" // SPARK3 anchor
//  override def simpleString: String = super.simpleString + s" $spatialExpression" // SPARK2 anchor

  private lazy val evaluator: SpatialPredicateEvaluator = if (indexBuildSide == windowJoinSide) {
    SpatialPredicateEvaluators.create(spatialPredicate)
  } else {
    SpatialPredicateEvaluators.create(SpatialPredicate.inverse(spatialPredicate))
  }

  private def innerJoin(streamIter: Iterator[(Geometry, UnsafeRow)], index: Broadcast[SpatialIndex]): Iterator[InternalRow] = {
    val factory = new PreparedGeometryFactory()
    val preparedGeometries = new mutable.HashMap[Geometry, PreparedGeometry]
    val joinedRow = new JoinedRow
    streamIter.flatMap { case (geom, row) =>
      joinedRow.withLeft(row)
      index.value.query(geom.getEnvelopeInternal)
        .iterator.asScala.asInstanceOf[Iterator[Geometry]]
        .filter(candidate => evaluator.eval(preparedGeometries.getOrElseUpdate(candidate, { factory.create(candidate) }), geom))
        .map(candidate => joinedRow.withRight(candidate.getUserData.asInstanceOf[UnsafeRow]))
        .filter(boundCondition)
    }
  }

  private def semiJoin(
    streamIter: Iterator[(Geometry, UnsafeRow)], index: Broadcast[SpatialIndex]
  ): Iterator[InternalRow] = {
    val factory = new PreparedGeometryFactory()
    val preparedGeometries = new mutable.HashMap[Geometry, PreparedGeometry]
    val joinedRow = new JoinedRow
    streamIter.flatMap { case (geom, row) =>
      val left = row
      joinedRow.withLeft(left)
      val anyMatches = index.value.query(geom.getEnvelopeInternal)
        .iterator.asScala.asInstanceOf[Iterator[Geometry]]
        .filter(candidate => evaluator.eval(preparedGeometries.getOrElseUpdate(candidate, {
          factory.create(candidate)
        }), geom))
        .map(candidate => joinedRow.withRight(candidate.getUserData.asInstanceOf[UnsafeRow]))
        .exists(boundCondition)

      if (anyMatches) {
        Iterator.single(left)
      } else {
        Iterator.empty
      }
    }
  }

  private def antiJoin(
    streamIter: Iterator[(Geometry, UnsafeRow)], index: Broadcast[SpatialIndex]
  ): Iterator[InternalRow] = {
    val factory = new PreparedGeometryFactory()
    val preparedGeometries = new mutable.HashMap[Geometry, PreparedGeometry]
    val joinedRow = new JoinedRow
    streamIter.flatMap { case (geom, row) =>
      val left = row
      joinedRow.withLeft(row)
      val anyMatches = (if (geom == null) Collections.EMPTY_LIST else index.value.query(geom.getEnvelopeInternal))
        .iterator.asScala.asInstanceOf[Iterator[Geometry]]
        .filter(candidate => evaluator.eval(preparedGeometries.getOrElseUpdate(candidate, {
          factory.create(candidate)
        }), geom))
        .map(candidate => joinedRow.withRight(candidate.getUserData.asInstanceOf[UnsafeRow]))
        .exists(boundCondition)

      if (anyMatches) {
        Iterator.empty
      } else {
        Iterator.single(left)
      }
    }
  }

  private def outerJoin(
    streamIter: Iterator[(Geometry, UnsafeRow)], index: Broadcast[SpatialIndex]
  ): Iterator[InternalRow] = {
    val factory = new PreparedGeometryFactory()
    val preparedGeometries = new mutable.HashMap[Geometry, PreparedGeometry]
    val joinedRow = new JoinedRow
    val nullRow = new GenericInternalRow(broadcast.output.length)

    streamIter.flatMap { case (geom, row) =>
      joinedRow.withLeft(row)
      val candidates = (if (geom == null) Collections.EMPTY_LIST else index.value.query(geom.getEnvelopeInternal))
        .iterator.asScala.asInstanceOf[Iterator[Geometry]]
        .filter(candidate => evaluator.eval(preparedGeometries.getOrElseUpdate(candidate, {
          factory.create(candidate)
        }), geom))

      new RowIterator {
        private var found = false
        override def advanceNext(): Boolean = {
          while (candidates.hasNext) {
            val candidateRow = candidates.next().getUserData.asInstanceOf[UnsafeRow]
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
        case x =>
          throw new IllegalArgumentException(s"BroadcastIndexJoinExec should not take $x as the JoinType")

      }

      val resultProj = createResultProjection()
      joinedIter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  private def createStreamShapes(streamResultsRaw: RDD[UnsafeRow], boundStreamShape: Expression) = {
    distance match {
      case Some(distanceExpression) =>
        streamResultsRaw.map(row => {
          val geom = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (geom == null) {
            (null, row)
          } else {
            val geometry = GeometrySerializer.deserialize(geom)
            val radius = BindReferences.bindReference(distanceExpression, streamed.output).eval(row).asInstanceOf[Double]
            val envelope = geometry.getEnvelopeInternal
            envelope.expandBy(radius)
            (geometry.getFactory.toGeometry(envelope), row)
          }
        })
      case _ =>
        streamResultsRaw.map(row => {
          val geom = boundStreamShape.eval(row).asInstanceOf[Array[Byte]]
          if (geom == null) {
            (null, row)
          } else {
            (GeometrySerializer.deserialize(geom), row)
          }
        })
    }
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
