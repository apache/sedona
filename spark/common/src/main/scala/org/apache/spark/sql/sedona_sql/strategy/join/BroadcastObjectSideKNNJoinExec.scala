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

import org.apache.sedona.core.enums.{DistanceMetric, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

case class BroadcastObjectSideKNNJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    joinSide: JoinSide,
    joinType: JoinType,
    k: Expression,
    useApproximate: Boolean,
    spatialPredicate: SpatialPredicate,
    isGeography: Boolean,
    condition: Expression,
    extraCondition: Option[Expression] = None)
    extends SedonaBinaryExecNode
    with TraitKNNJoinQueryExec
    with Logging {

  /**
   * Convert the both RDDs to SpatialRDDs
   * @param leftRdd
   *   the left RDD
   * @param leftShapeExpr
   *   the shape expression
   * @param rightRdd
   *   the right RDD
   * @param rightShapeExpr
   *   the shape expression
   * @return
   */
  override def toSpatialRddPair(
      leftRdd: RDD[UnsafeRow],
      leftShapeExpr: Expression,
      rightRdd: RDD[UnsafeRow],
      rightShapeExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) = {
    (leftToSpatialRDD(leftRdd, leftShapeExpr), rightToSpatialRDD(rightRdd, rightShapeExpr))
  }

  /**
   * Convert the left RDD (queries) to SpatialRDD
   * @param rdd
   *   the left RDD
   * @param shapeExpression
   *   the shape expression
   * @return
   */
  def leftToSpatialRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression): SpatialRDD[Geometry] = {
    toSpatialRDD(rdd, shapeExpression)
  }

  /**
   * Convert the right RDD (queries) to SpatialRDD
   * @param rdd
   *   the right RDD
   * @param shapeExpression
   *   the shape expression
   * @param projection
   *   the projection
   * @return
   */
  def rightToSpatialRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression): SpatialRDD[Geometry] = {
    toSpatialRDD(rdd, shapeExpression)
  }

  /**
   * Broadcast the dominant shapes (objects) to all the partitions
   *
   * This type of the join does not need to do spatial partition.
   *
   * For left side (queries) broadcast: the join needs to be reduced after the join. For right
   * side (objects) broadcast: the join does not need to be reduced after the join.
   *
   * @param objectsShapes
   *   the dominant shapes (objects)
   * @param queryShapes
   *   the follower shapes (queries)
   * @param numPartitions
   *   the number of partitions
   * @param sedonaConf
   *   the Sedona configuration
   */
  override def doSpatialPartitioning(
      objectsShapes: SpatialRDD[Geometry],
      queryShapes: SpatialRDD[Geometry],
      numPartitions: Integer,
      sedonaConf: SedonaConf): Unit = {
    require(numPartitions > 0, "The number of partitions must be greater than 0.")
    val kValue: Int = this.k.eval().asInstanceOf[Int]
    require(kValue > 0, "The number of neighbors must be greater than 0.")
    objectsShapes.setNeighborSampleNumber(kValue)
    broadcastJoin = true
  }

  /**
   * Get the KNN join parameters This is required to determine the join strategy to support
   * different KNN join strategies. This function needs to be updated when new join strategies are
   * supported.
   *
   * @return
   *   the KNN join parameters
   */
  override def getKNNJoinParams: JoinParams = {
    // Please update this function when new join strategies are added
    // Number of neighbors to find
    val kValue: Int = this.k.eval().asInstanceOf[Int]
    // Metric to use in the join to calculate the distance, only Euclidean and Spheroid are supported
    val distanceMetric = if (isGeography) DistanceMetric.SPHEROID else DistanceMetric.EUCLIDEAN
    val joinParams =
      new JoinParams(true, null, IndexType.RTREE, null, kValue, distanceMetric, null)
    joinParams
  }

  /**
   * Copy the plan with new children
   * @param newLeft
   * @param newRight
   * @return
   */
  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
