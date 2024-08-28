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

import org.apache.sedona.core.enums.{DistanceMetric, GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialPartitioning.QuadTreeRTPartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

/**
 * KNN / AKNN joins requires target geometries (objects) to be in the same partition as the query
 * geometries. To create an overlap and guarantee matching geometries end up in the same
 * partition, the target geometry is expanded during partitioning.
 *
 * E.g., SELECT * FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, $numNeighbors,
 * true) SELECT * FROM QUERIES JOIN OBJECTS ON ST_AKNN(QUERIES.GEOM, OBJECTS.GEOM, $numNeighbors,
 * true)
 *
 * @param left
 *   left side of the join
 * @param right
 *   right side of the join
 * @param leftShape
 *   shape expression for the left side
 * @param rightShape
 *   shape expression for the right side
 * @param k
 *   \- number of neighbors to find
 * @param useApproximate
 *   whether to use approximate distance for the join
 * @param spatialPredicate
 *   spatial predicate as join condition
 * @param condition
 *   full join condition
 * @param extraCondition
 *   extra join condition other than spatialPredicate
 */
case class KNNJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    joinType: JoinType,
    k: Expression,
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
   * @param projection
   *   the projection
   * @return
   */
  def leftToSpatialRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      projection: Option[Seq[Expression]] = None): SpatialRDD[Geometry] = {
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
      shapeExpression: Expression,
      projection: Option[Seq[Expression]] = None): SpatialRDD[Geometry] = {
    toSpatialRDD(rdd, shapeExpression)
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

  /**
   * Execute the spatial partitioning for KNN join This is required to ensure that the target
   * geometries (objects) are in the same partition as the query geometries.
   *
   * Different KNN algorithms require different partitioning strategies. E.g., approximate KNN
   * join requires a different partitioning strategy than exact KNN join.
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

    exactSpatialPartitioning(objectsShapes, queryShapes, numPartitions)
  }

  /**
   * Exact spatial partitioning for KNN join
   * @param dominantShapes
   *   the dominant (objects) shapes
   * @param followerShapes
   *   the follower (queries) shapes
   */
  private def exactSpatialPartitioning(
      dominantShapes: SpatialRDD[Geometry],
      followerShapes: SpatialRDD[Geometry],
      numPartitions: Integer): Unit = {
    // analyze the both RDDs to get the statistics (e.g., boundary)
    dominantShapes.analyze()
    followerShapes.analyze()

    // expand the boundary for partition to include both RDDs
    dominantShapes.boundaryEnvelope.expandToInclude(followerShapes.boundaryEnvelope)

    // use modified quadtree partitioning, as it is an exact algorithm
    dominantShapes.spatialPartitioning(GridType.QUADTREE_RTREE, numPartitions)
    followerShapes.spatialPartitioning(
      dominantShapes.getPartitioner
        .asInstanceOf[QuadTreeRTPartitioner]
        .nonOverlappedPartitioner())

    dominantShapes.buildIndex(IndexType.RTREE, true)
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
}
