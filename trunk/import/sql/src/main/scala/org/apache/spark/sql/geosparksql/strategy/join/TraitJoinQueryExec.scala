/*
 * FILE: TraitJoinQueryExec.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.geosparksql.strategy.join

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeRow, Predicate}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.datasyslab.geospark.enums.JoinSparitionDominantSide
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.GeometrySerializer

trait TraitJoinQueryExec {
  self: SparkPlan =>

  val left: SparkPlan
  val right: SparkPlan
  val leftShape: Expression
  val rightShape: Expression
  val intersects: Boolean
  val extraCondition: Option[Expression]

  // Using lazy val to avoid serialization
  @transient private lazy val boundCondition: (InternalRow => Boolean) = {
    if (extraCondition.isDefined) {
      Predicate.create(extraCondition.get, left.output ++ right.output).eval _
    } else { (r: InternalRow) =>
      true
    }
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def doExecute(): RDD[InternalRow] = {
    val boundLeftShape = BindReferences.bindReference(leftShape, left.output)
    val boundRightShape = BindReferences.bindReference(rightShape, right.output)

    val leftResultsRaw = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResultsRaw = right.execute().asInstanceOf[RDD[UnsafeRow]]

    var geosparkConf = new GeoSparkConf(sparkContext.conf)
    val (leftShapes, rightShapes) =
      toSpatialRddPair(leftResultsRaw, boundLeftShape, rightResultsRaw, boundRightShape)

    // Only do SpatialRDD analyze when the user doesn't know approximate total count of the spatial partitioning
    // dominant side rdd
    if (geosparkConf.getJoinApproximateTotalCount == -1) {
      if (geosparkConf.getJoinSparitionDominantSide == JoinSparitionDominantSide.LEFT) {
        leftShapes.analyze()
        geosparkConf.setJoinApproximateTotalCount(leftShapes.approximateTotalCount)
        geosparkConf.setDatasetBoundary(leftShapes.boundaryEnvelope)
      }
      else {
        rightShapes.analyze()
        geosparkConf.setJoinApproximateTotalCount(rightShapes.approximateTotalCount)
        geosparkConf.setDatasetBoundary(rightShapes.boundaryEnvelope)
      }
    }
    log.info("[GeoSparkSQL] Number of partitions on the left: " + leftResultsRaw.partitions.size)
    log.info("[GeoSparkSQL] Number of partitions on the right: " + rightResultsRaw.partitions.size)

    var numPartitions = -1
    try {
      if (geosparkConf.getJoinSparitionDominantSide == JoinSparitionDominantSide.LEFT) {
        if (geosparkConf.getFallbackPartitionNum != -1) {
          numPartitions = geosparkConf.getFallbackPartitionNum
        }
        else {
          numPartitions = joinPartitionNumOptimizer(leftShapes.rawSpatialRDD.partitions.size(), rightShapes.rawSpatialRDD.partitions.size(),
            leftShapes.approximateTotalCount)
        }
        doSpatialPartitioning(leftShapes, rightShapes, numPartitions, geosparkConf)
      }
      else {
        if (geosparkConf.getFallbackPartitionNum != -1) {
          numPartitions = geosparkConf.getFallbackPartitionNum
        }
        else {
          numPartitions = rightShapes.rawSpatialRDD.partitions.size()
          numPartitions = joinPartitionNumOptimizer(rightShapes.rawSpatialRDD.partitions.size(), leftShapes.rawSpatialRDD.partitions.size(),
            rightShapes.approximateTotalCount)
        }
        doSpatialPartitioning(rightShapes, leftShapes, numPartitions, geosparkConf)
      }
    }
    catch {
      case e: IllegalArgumentException => {
        print(e.getMessage)
        // Partition number are not qualified
        // Use fallback num partitions specified in GeoSparkConf
        if (geosparkConf.getJoinSparitionDominantSide == JoinSparitionDominantSide.LEFT) {
          numPartitions = geosparkConf.getFallbackPartitionNum
          doSpatialPartitioning(leftShapes, rightShapes, numPartitions, geosparkConf)
        }
        else {
          numPartitions = geosparkConf.getFallbackPartitionNum
          doSpatialPartitioning(rightShapes, leftShapes, numPartitions, geosparkConf)
        }
      }
    }


    val joinParams = new JoinParams(intersects, geosparkConf.getIndexType, geosparkConf.getJoinBuildSide)

    //logInfo(s"leftShape count ${leftShapes.spatialPartitionedRDD.count()}")
    //logInfo(s"rightShape count ${rightShapes.spatialPartitionedRDD.count()}")

    val matches = JoinQuery.spatialJoin(leftShapes, rightShapes, joinParams)

    logDebug(s"Join result has ${matches.count()} rows")

    matches.rdd.mapPartitions { iter =>
      val filtered =
        if (extraCondition.isDefined) {
          val boundCondition = Predicate.create(extraCondition.get, left.output ++ right.output)
          iter.filter {
            case (l, r) =>
              val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
              val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
              var joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
              boundCondition.eval(joiner.join(leftRow, rightRow))
          }
        } else {
          iter
        }

      filtered.map {
        case (l, r) =>
          val leftRow = l.getUserData.asInstanceOf[UnsafeRow]
          val rightRow = r.getUserData.asInstanceOf[UnsafeRow]
          var joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          joiner.join(leftRow, rightRow)
      }
    }
  }

  protected def toSpatialRdd(rdd: RDD[UnsafeRow],
                             shapeExpression: Expression): SpatialRDD[Geometry] = {

    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x => {
          val shape = GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[ArrayData])
          //logInfo(shape.toString)
          shape.setUserData(x.copy)
          shape
        }
        }
        .toJavaRDD())
    spatialRdd
  }

  def toSpatialRddPair(buildRdd: RDD[UnsafeRow],
                       buildExpr: Expression,
                       streamedRdd: RDD[UnsafeRow],
                       streamedExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    (toSpatialRdd(buildRdd, buildExpr), toSpatialRdd(streamedRdd, streamedExpr))

  def doSpatialPartitioning(dominantShapes: SpatialRDD[Geometry], followerShapes: SpatialRDD[Geometry],
                            numPartitions: Integer, geosparkConf: GeoSparkConf): Unit = {
    dominantShapes.spatialPartitioning(geosparkConf.getJoinGridType, numPartitions)
    followerShapes.spatialPartitioning(dominantShapes.getPartitioner)
  }

  def joinPartitionNumOptimizer(dominantSidePartNum:Int, followerSidePartNum:Int, dominantSideCount:Long): Int =
  {
    log.info("[GeoSparkSQL] Dominant side count: " + dominantSideCount)
    var numPartition = -1
    var candidatePartitionNum = (dominantSideCount/2).intValue()
    if (dominantSidePartNum*2 > dominantSideCount)
    {
      log.warn(s"[GeoSparkSQL] Join dominant side partition number $dominantSidePartNum is larger than 1/2 of the dominant side count $dominantSideCount")
      log.warn(s"[GeoSparkSQL] Try to use follower side partition number $followerSidePartNum")
      if (followerSidePartNum*2 > dominantSideCount){
        log.warn(s"[GeoSparkSQL] Join follower side partition number is also larger than 1/2 of the dominant side count $dominantSideCount")
        log.warn(s"[GeoSparkSQL] Try to use 1/2 of the dominant side count $candidatePartitionNum as the partition number of both sides")
        if (candidatePartitionNum==0)
        {
          log.warn(s"[GeoSparkSQL] 1/2 of $candidatePartitionNum is equal to 0. Use 1 as the partition number of both sides instead.")
          numPartition = 1
        }
        else numPartition = candidatePartitionNum
      }
      else numPartition = followerSidePartNum
    }
    else numPartition = dominantSidePartNum
    return  numPartition
  }
}
