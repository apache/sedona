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

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.SpatialIndex

import collection.JavaConverters._


case class BroadcastIndexJoinExec(left: SparkPlan,
                                  right: SparkPlan,
                                  streamShape: Expression,
                                  indexBuildSide: BuildSide,
                                  windowJoinSide: BuildSide,
                                  intersects: Boolean,
                                  extraCondition: Option[Expression] = None)
  extends BinaryExecNode
    with TraitJoinQueryBase
    with Logging {

  override def output: Seq[Attribute] = left.output ++ right.output

  private val (streamed, broadcast) = indexBuildSide match {
    case BuildLeft => (right, left.asInstanceOf[SpatialIndexExec])
    case BuildRight => (left, right.asInstanceOf[SpatialIndexExec])
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

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
    val streamShapes = toSpatialRdd(streamResultsRaw, boundStreamShape)

    val broadcastIndex = broadcast.executeBroadcast[SpatialIndex]()

    val pairs = (indexBuildSide, windowJoinSide) match {
      case (BuildLeft, BuildLeft) => windowBroadcastJoin(broadcastIndex, streamShapes)
      case (BuildLeft, BuildRight) => objectBroadcastJoin(broadcastIndex, streamShapes).map { case (left, right) => (right, left) }
      case (BuildRight, BuildLeft) => objectBroadcastJoin(broadcastIndex, streamShapes)
      case (BuildRight, BuildRight) => windowBroadcastJoin(broadcastIndex, streamShapes).map { case (left, right) => (right, left) }
    }

    pairs.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.map {
        case (l, r) =>
          val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
          val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
          joiner.join(leftRow, rightRow)
      }
    }
  }
}
