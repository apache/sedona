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
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.locationtech.jts.geom.Geometry

trait TraitJoinQueryBase {
  self: SparkPlan =>

  def toSpatialRddPair(leftRdd: RDD[UnsafeRow],
                       leftShapeExpr: Expression,
                       rightRdd: RDD[UnsafeRow],
                       rightShapeExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    (toSpatialRDD(leftRdd, leftShapeExpr), toSpatialRDD(rightRdd, rightShapeExpr))

  def toSpatialRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          val shape = GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[Array[Byte]])
          shape.setUserData(x.copy)
          shape
        }
        .toJavaRDD())
    spatialRdd
  }

  def toExpandedEnvelopeRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression, boundRadius: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          val shape = GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[Array[Byte]])
          val envelope = shape.getEnvelopeInternal.copy()
          envelope.expandBy(boundRadius.eval(x).asInstanceOf[Double])

          val expandedEnvelope = shape.getFactory.toGeometry(envelope)
          expandedEnvelope.setUserData(x.copy)
          expandedEnvelope
        }
        .toJavaRDD())
    spatialRdd
  }

  def doSpatialPartitioning(dominantShapes: SpatialRDD[Geometry], followerShapes: SpatialRDD[Geometry],
                            numPartitions: Integer, sedonaConf: SedonaConf): Unit = {
    if (dominantShapes.approximateTotalCount > 0) {
      dominantShapes.spatialPartitioning(sedonaConf.getJoinGridType, numPartitions)
      followerShapes.spatialPartitioning(dominantShapes.getPartitioner)
    }
  }
}
