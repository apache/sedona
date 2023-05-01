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

import org.apache.sedona.core.enums.JoinSparitionDominantSide
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.SparkPlan
import org.locationtech.jts.geom.Geometry

trait TraitJoinQueryExec extends TraitJoinQueryBase {
  self: SparkPlan =>

  val left: SparkPlan
  val right: SparkPlan
  val leftShape: Expression
  val rightShape: Expression
  val spatialPredicate: SpatialPredicate
  val extraCondition: Option[Expression]

  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def doExecute(): RDD[InternalRow] = {
    val boundLeftShape = BindReferences.bindReference(leftShape, left.output)
    val boundRightShape = BindReferences.bindReference(rightShape, right.output)

    val leftResultsRaw = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResultsRaw = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val sedonaConf = SedonaConf.fromActiveSession
    val (leftShapes, rightShapes) =
      toSpatialRddPair(leftResultsRaw, boundLeftShape, rightResultsRaw, boundRightShape)

    // Only do SpatialRDD analyze when the user doesn't know approximate total count of the spatial partitioning
    // dominant side rdd
    if (sedonaConf.getJoinApproximateTotalCount == -1) {
      if (sedonaConf.getJoinSparitionDominantSide == JoinSparitionDominantSide.LEFT) {
        leftShapes.analyze()
      }
      else {
        rightShapes.analyze()
      }
    }
    log.info("[SedonaSQL] Number of partitions on the left: " + leftResultsRaw.partitions.size)
    log.info("[SedonaSQL] Number of partitions on the right: " + rightResultsRaw.partitions.size)

    var numPartitions = -1
    try {
      if (sedonaConf.getJoinSparitionDominantSide == JoinSparitionDominantSide.LEFT) {
        if (sedonaConf.getFallbackPartitionNum != -1) {
          numPartitions = sedonaConf.getFallbackPartitionNum
        }
        else {
          numPartitions = joinPartitionNumOptimizer(leftShapes.rawSpatialRDD.partitions.size(), rightShapes.rawSpatialRDD.partitions.size(),
            leftShapes.approximateTotalCount)
        }
        doSpatialPartitioning(leftShapes, rightShapes, numPartitions, sedonaConf)
      }
      else {
        if (sedonaConf.getFallbackPartitionNum != -1) {
          numPartitions = sedonaConf.getFallbackPartitionNum
        }
        else {
          numPartitions = rightShapes.rawSpatialRDD.partitions.size()
          numPartitions = joinPartitionNumOptimizer(rightShapes.rawSpatialRDD.partitions.size(), leftShapes.rawSpatialRDD.partitions.size(),
            rightShapes.approximateTotalCount)
        }
        doSpatialPartitioning(rightShapes, leftShapes, numPartitions, sedonaConf)
      }
    }
    catch {
      case e: IllegalArgumentException => {
        print(e.getMessage)
        // Partition number are not qualified
        // Use fallback num partitions specified in SedonaConf
        if (sedonaConf.getJoinSparitionDominantSide == JoinSparitionDominantSide.LEFT) {
          numPartitions = sedonaConf.getFallbackPartitionNum
          doSpatialPartitioning(leftShapes, rightShapes, numPartitions, sedonaConf)
        }
        else {
          numPartitions = sedonaConf.getFallbackPartitionNum
          doSpatialPartitioning(rightShapes, leftShapes, numPartitions, sedonaConf)
        }
      }
    }


    val joinParams = new JoinParams(sedonaConf.getUseIndex, spatialPredicate, sedonaConf.getIndexType, sedonaConf.getJoinBuildSide)

    //logInfo(s"leftShape count ${leftShapes.spatialPartitionedRDD.count()}")
    //logInfo(s"rightShape count ${rightShapes.spatialPartitionedRDD.count()}")

    val matchesRDD: RDD[(Geometry, Geometry)] = (leftShapes.spatialPartitionedRDD, rightShapes.spatialPartitionedRDD) match {
      case (null, null) =>
        // Dominant side is empty, skipped creating partitioned RDDs. Result of join should also be empty.
        sparkContext.parallelize(Seq[(Geometry, Geometry)]())
      case _ => JoinQuery.spatialJoin(leftShapes, rightShapes, joinParams).rdd
    }

    logDebug(s"Join result has ${matchesRDD.count()} rows")

    matchesRDD.mapPartitions { iter =>
      val joinRow = {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        (l: UnsafeRow, r: UnsafeRow) => joiner.join(l, r)
      }

      val joined = iter.map { case (l, r) =>
        val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
        val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
        joinRow(leftRow, rightRow)
      }

      extraCondition match {
        case Some(condition) =>
          val boundCondition = Predicate.create(condition, output)
          joined.filter(row => boundCondition.eval(row))
        case None => joined
      }
    }
  }

  def joinPartitionNumOptimizer(dominantSidePartNum: Int, followerSidePartNum: Int, dominantSideCount: Long): Int = {
    log.info("[SedonaSQL] Dominant side count: " + dominantSideCount)
    var numPartition = -1
    var candidatePartitionNum = (dominantSideCount / 2).intValue()
    if (dominantSidePartNum * 2 > dominantSideCount) {
      log.warn(s"[SedonaSQL] Join dominant side partition number $dominantSidePartNum is larger than 1/2 of the dominant side count $dominantSideCount")
      log.warn(s"[SedonaSQL] Try to use follower side partition number $followerSidePartNum")
      if (followerSidePartNum * 2 > dominantSideCount) {
        log.warn(s"[SedonaSQL] Join follower side partition number is also larger than 1/2 of the dominant side count $dominantSideCount")
        log.warn(s"[SedonaSQL] Try to use 1/2 of the dominant side count $candidatePartitionNum as the partition number of both sides")
        if (candidatePartitionNum == 0) {
          log.warn(s"[SedonaSQL] 1/2 of $candidatePartitionNum is equal to 0. Use 1 as the partition number of both sides instead.")
          numPartition = 1
        }
        else numPartition = candidatePartitionNum
      }
      else numPartition = followerSidePartNum
    }
    else numPartition = dominantSidePartNum
    return numPartition
  }
}
