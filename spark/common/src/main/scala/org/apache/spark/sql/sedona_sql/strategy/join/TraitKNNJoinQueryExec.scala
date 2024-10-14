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

import org.apache.commons.lang3.Range
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialPartitioning.{QuadTreeRTPartitioner, SpatialPartitioner}
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, Predicate, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.locationtech.jts.geom.{Envelope, Geometry}

import java.io.PrintWriter
import java.nio.file.Paths
import java.util

/**
 * TraitKNNJoinQueryExec is a trait that extends the TraitJoinQueryExec trait and provides the
 * necessary functionality to execute a KNN join operation.
 *
 * It is used by the KNNJoinExec class to execute a KNN join operation. The KNN join operation is
 * a k-nearest neighbors join that finds the k-nearest neighbors of each object in the right
 * dataset for each query in the left dataset.
 */
trait TraitKNNJoinQueryExec extends TraitJoinQueryExec {
  self: SparkPlan =>

  protected var broadcastJoin: Boolean = false
  protected var querySide: JoinSide = null

  private lazy val sedonaConf = SedonaConf.fromActiveSession
  override lazy val metrics: Map[String, SQLMetric] = Map.empty

  override protected def doExecute(): RDD[InternalRow] = {
    // Execute the join
    executeKNNJoin(sedonaConf)
  }

  /**
   * Executes a KNN (k-nearest neighbors) join operation using the Sedona spatial library.
   *
   * This method binds the left and right shape references to their respective outputs, executes
   * the left and right datasets as RDDs, converts them to spatial RDDs, and performs spatial
   * partitioning based on Sedona configuration.
   *
   * The number of partitions is determined either by a predefined fallback value or optimized
   * based on the sizes of the object and query shapes. If the partitioning fails, it uses the
   * fallback value.
   *
   * It saves the spatial partitioner to a file if specified, gets the KNN join parameters,
   * performs the KNN join, and finally converts the matched RDD to RowRDD.
   *
   * @param sedonaConf
   *   The Sedona configuration settings.
   * @return
   *   RDD[InternalRow] The result of the KNN join as an RDD of InternalRows.
   */
  private def executeKNNJoin(sedonaConf: SedonaConf): RDD[InternalRow] = {
    val (querySparkPlan: SparkPlan, objectSparkPlan: SparkPlan, swapped: Boolean) =
      getQueryAndObjectPlans(leftShape)

    val boundQueryShape = BindReferences.bindReference(leftShape, querySparkPlan.output)
    val boundObjectShape = BindReferences.bindReference(rightShape, objectSparkPlan.output)

    val queryResultsRaw = querySparkPlan.execute().asInstanceOf[RDD[UnsafeRow]]
    val objectResultsRaw = objectSparkPlan.execute().asInstanceOf[RDD[UnsafeRow]]

    val sedonaConf = SedonaConf.fromActiveSession

    val (queryShapes, objectShapes) =
      toSpatialRddPair(queryResultsRaw, boundQueryShape, objectResultsRaw, boundObjectShape)

    objectShapes.analyze()
    log.info(
      "[SedonaSQL] Number of partitions on the objectShapes (right): " + objectResultsRaw.partitions.size)

    val joinParams: JoinParams = getKNNJoinParams

    // calculate the optimized or predefined number of partitions
    // and do spatial partitioning
    var numPartitions = -1
    try {
      if (sedonaConf.getFallbackPartitionNum != -1) {
        numPartitions = sedonaConf.getFallbackPartitionNum
      } else {
        // object shapes are the dominant side
        numPartitions = knnJoinPartitionNumOptimizer(
          objectShapes.rawSpatialRDD.partitions.size(),
          queryShapes.rawSpatialRDD.partitions.size(),
          objectShapes.approximateTotalCount,
          joinParams.k)
      }
      // object shapes are the dominant side
      doSpatialPartitioning(objectShapes, queryShapes, numPartitions, sedonaConf)
    } catch {
      case e: IllegalArgumentException => {
        log.error(e.getMessage)
        // Partition number are not qualified
        // Use fallback num partitions specified in SedonaConf
        numPartitions = sedonaConf.getFallbackPartitionNum
        doSpatialPartitioning(queryShapes, objectShapes, numPartitions, sedonaConf)
      }
    }

    val matchesRDD: RDD[(Geometry, Geometry)] =
      (queryShapes.spatialPartitionedRDD, objectShapes.spatialPartitionedRDD) match {
        case (null, null) =>
          if (broadcastJoin) {
            JoinQuery
              .knnJoin(
                queryShapes,
                objectShapes,
                joinParams,
                sedonaConf.isIncludeTieBreakersInKNNJoins,
                broadcastJoin)
              .rdd
          } else {
            sparkContext.parallelize(Seq[(Geometry, Geometry)]())
          }
        case _ =>
          JoinQuery
            .knnJoin(
              queryShapes,
              objectShapes,
              joinParams,
              sedonaConf.isIncludeTieBreakersInKNNJoins,
              broadcastJoin)
            .rdd
      }

    // Convert the matchesRDD to RowRDD
    joinedRddToRowRdd(matchesRDD, swapped)
  }

  def knnJoinPartitionNumOptimizer(
      objectSidePartNum: Int,
      querySidePartNum: Int,
      objectSideCount: Long,
      numNeighbor: Int): Int = {
    log.info("[SedonaSQL] object side count: " + objectSideCount)
    var numPartition = -1
    val candidatePartitionNum = (objectSideCount / (numNeighbor * 2)).intValue()
    if (objectSidePartNum * 2 > objectSideCount) {
      log.warn(
        s"[SedonaSQL] KNN join object side partition number $objectSidePartNum is larger than 1/2 of the object side count $objectSideCount")
      log.warn(
        s"[SedonaSQL] Try to use object (follower) side partition number $querySidePartNum")
      if (querySidePartNum * 2 > objectSideCount) {
        log.warn(
          s"[SedonaSQL] KNN join object (follower) side partition number is also larger than 1/2 of the object side count $objectSideCount")
        log.warn(
          s"[SedonaSQL] Try to use 1/2 of the object side count $candidatePartitionNum as the partition number of both sides")
        if (candidatePartitionNum == 0) {
          log.warn(
            s"[SedonaSQL] 1/2 of $candidatePartitionNum is equal to 0. Use 1 as the partition number of both sides instead.")
          numPartition = 1
        } else numPartition = candidatePartitionNum
      } else numPartition = querySidePartNum
    } else numPartition = objectSidePartNum
    numPartition
  }

  /**
   * Gets the query and object plans based on the left shape.
   *
   * This method checks if the left shape is part of the left or right plan and returns the query
   * and object plans accordingly.
   *
   * @param leftShape
   *   The left shape expression.
   * @return
   *   (SparkPlan, SparkPlan) The query and object plans.
   */
  private def getQueryAndObjectPlans(leftShape: Expression) = {
    val isLeftQuerySide =
      left.toString().toLowerCase().contains(leftShape.toString().toLowerCase())
    if (isLeftQuerySide) {
      querySide = LeftSide
      (left, right, false)
    } else {
      querySide = RightSide
      (right, left, true)
    }
  }

  /**
   * Converts the joined RDD of geometries to an RDD of InternalRows.
   *
   * This method maps over the partitions of the joined RDD, creating an UnsafeRow joiner that
   * combines the left and right rows based on the given schemas.
   *
   * Each geometry's user data is expected to be an UnsafeRow, and the joiner is used to produce
   * joined rows from the left and right geometry pairs.
   *
   * @param joinedRdd
   *   The RDD containing pairs of joined geometries.
   * @return
   *   RDD[InternalRow] The resulting RDD of joined InternalRows.
   */
  protected def joinedRddToRowRdd(
      joinedRdd: RDD[(Geometry, Geometry)],
      swapped: Boolean): RDD[InternalRow] = {
    joinedRdd.mapPartitions { iter =>
      val joinRow = {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        (l: UnsafeRow, r: UnsafeRow) => joiner.join(l, r)
      }

      val joined = iter.map { case (l, r) =>
        val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
        val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
        if (swapped)
          joinRow(rightRow, leftRow)
        else
          joinRow(leftRow, rightRow)
      }

      // Apply the extra join conditions if it exists (e.g., S.ID < Q.ID)
      extraCondition match {
        case Some(condition) =>
          val boundCondition = Predicate.create(condition, output)
          joined.filter(row => boundCondition.eval(row))
        case None => joined
      }
    }
  }

  private def saveKNNPartitionerToFile(
      partitioner: SpatialPartitioner,
      savePath: String): Unit = {
    partitioner match {
      case null =>
        log.warn("[SedonaSQL] Spatial partitioner is null. Skip saving to file.")

      case qt: QuadTreeRTPartitioner =>
        val filePath = createFilePath(savePath, "quadtree-rt")
        log.info(s"[SedonaSQL] Saving QuadTreeRT partitioner to file: $filePath")
        writeGridsToFile(filePath, qt.getOverlappedGrids)

      case _ =>
        log.info("[SedonaSQL] Spatial partitioner type is not supported for saving to file.")
    }
  }

  private def createFilePath(savePath: String, partitionerType: String): String = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Paths.get(savePath).toFile.mkdirs()
    Paths
      .get(savePath, s"partitioner-$partitionerType-$executionId-${System.currentTimeMillis()}")
      .toString
  }

  private def writeGridsToFile(
      filePath: String,
      grids: java.util.Map[Integer, java.util.List[Envelope]]): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      grids.forEach { case (key, envelopes) =>
        envelopes.forEach { envelope =>
          writer.write(
            s"$key,${envelope.getMinX},${envelope.getMinY},${envelope.getMaxX},${envelope.getMaxY}\n")
        }
      }
    } finally {
      writer.close()
    }
  }

  private def writeGridsToFile(
      filePath: String,
      ranges: util.List[Range[java.lang.Long]]): Unit = {
    val writer = new PrintWriter(filePath)
    try {
      var rangeId = 1
      ranges.forEach { range =>
        writer.write(s"$rangeId,${range.getMinimum},${range.getMaximum}\n")
        rangeId += 1
      }
    } finally {
      writer.close()
    }
  }

  // The following methods are abstract and must be implemented by the concrete class
  // that extends this trait.
  // Override these methods to provide the necessary functionality for the KNN join.
  def getKNNJoinParams: JoinParams
}

object TraitKNNJoinQueryExec {
  val counter = new java.util.concurrent.atomic.AtomicLong(0)
}
