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

import collection.JavaConverters._

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.SpatialIndex

case class BroadcastIndexJoinExec(left: SparkPlan,
                                  right: SparkPlan,
                                  streamShape: Expression,
                                  indexBuildSide: JoinSide,
                                  windowJoinSide: JoinSide,
                                  intersects: Boolean,
                                  extraCondition: Option[Expression] = None,
                                  radius: Option[Expression] = None)
  extends SedonaBinaryExecNode
    with TraitJoinQueryBase
    with Logging {

  override def output: Seq[Attribute] = left.output ++ right.output

  // Using lazy val to avoid serialization
  @transient private lazy val boundCondition: (InternalRow => Boolean) = extraCondition match {
    case Some(condition) =>
      Predicate.create(condition, output).eval _ // SPARK3 anchor
//      newPredicate(condition, output).eval _ // SPARK2 anchor
    case None =>
      (r: InternalRow) => true
  }

  private val (streamed, broadcast) = indexBuildSide match {
    case LeftSide => (right, left.asInstanceOf[SpatialIndexExec])
    case RightSide => (left, right.asInstanceOf[SpatialIndexExec])
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  private val (windowExpression, objectExpression) = if (indexBuildSide == windowJoinSide) {
    (broadcast.shape, streamShape)
  } else {
    (streamShape, broadcast.shape)
  }

  private val spatialExpression = radius match {
      case Some(r) if intersects => s"ST_Distance($windowExpression, $objectExpression) <= $r"
      case Some(r) if !intersects => s"ST_Distance($windowExpression, $objectExpression) < $r"
      case None if intersects => s"ST_Intersects($windowExpression, $objectExpression)"
      case None if !intersects => s"ST_Contains($windowExpression, $objectExpression)"
    }

  override def simpleString(maxFields: Int): String = super.simpleString(maxFields) + s" $spatialExpression" // SPARK3 anchor
//  override def simpleString: String = super.simpleString + s" $spatialExpression" // SPARK2 anchor

  private def windowBroadcastJoin(index: Broadcast[SpatialIndex], spatialRdd: SpatialRDD[Geometry]): RDD[(Geometry, Geometry)] = {
    spatialRdd.getRawSpatialRDD.rdd.flatMap { row =>
      val candidates = index.value.query(row.getEnvelopeInternal).iterator.asScala.asInstanceOf[Iterator[Geometry]]
      candidates
        .filter(candidate => if (intersects) candidate.intersects(row) else candidate.covers(row))
        .map(candidate => (candidate, row))
    }
  }

  private def objectBroadcastJoin(index: Broadcast[SpatialIndex], spatialRdd: SpatialRDD[Geometry]): RDD[(Geometry, Geometry)] = {
    spatialRdd.getRawSpatialRDD.rdd.flatMap { row =>
      val candidates = index.value.query(row.getEnvelopeInternal).iterator.asScala.asInstanceOf[Iterator[Geometry]]
      candidates
        .filter(candidate => if (intersects) row.intersects(candidate) else row.covers(candidate))
        .map(candidate => (row, candidate))
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val boundStreamShape = BindReferences.bindReference(streamShape, streamed.output)
    val streamResultsRaw = streamed.execute().asInstanceOf[RDD[UnsafeRow]]

    // If there's a radius and the objects are being broadcast, we need to build the CircleRDD on the window stream side
    val streamShapes = radius match {
      case Some(radiusExpression) if indexBuildSide != windowJoinSide =>
        toCircleRDD(streamResultsRaw, boundStreamShape, BindReferences.bindReference(radiusExpression, streamed.output))
      case _ =>
        toSpatialRDD(streamResultsRaw, boundStreamShape)
    }

    val broadcastIndex = broadcast.executeBroadcast[SpatialIndex]()

    val pairs = (indexBuildSide, windowJoinSide) match {
      case (LeftSide, LeftSide) => windowBroadcastJoin(broadcastIndex, streamShapes)
      case (LeftSide, RightSide) => objectBroadcastJoin(broadcastIndex, streamShapes).map { case (left, right) => (right, left) }
      case (RightSide, LeftSide) => objectBroadcastJoin(broadcastIndex, streamShapes)
      case (RightSide, RightSide) => windowBroadcastJoin(broadcastIndex, streamShapes).map { case (left, right) => (right, left) }
    }

    pairs.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.map {
        case (l, r) =>
          val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
          val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
          joiner.join(leftRow, rightRow)
      }.filter(boundCondition(_))
    }
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
