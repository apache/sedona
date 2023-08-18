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

import org.apache.sedona.common.raster.GeometryFunctions
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.locationtech.jts.geom.{Envelope, Geometry}

trait TraitJoinQueryBase {
  self: SparkPlan =>

  def toSpatialRddPair(leftRdd: RDD[UnsafeRow],
                       leftShapeExpr: Expression,
                       rightRdd: RDD[UnsafeRow],
                       rightShapeExpr: Expression, isLeftRaster: Boolean, isRightRaster: Boolean): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    (if (isLeftRaster) toSpatialRDDRaster(leftRdd, leftShapeExpr) else toSpatialRDD(leftRdd, leftShapeExpr),
      if (isRightRaster) toSpatialRDDRaster(rightRdd, rightShapeExpr) else toSpatialRDD(rightRdd, rightShapeExpr))

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

  def toSpatialRDDRaster(rdd: RDD[UnsafeRow], shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          val shape = GeometryFunctions.convexHull(RasterSerializer.deserialize(shapeExpression.eval(x).asInstanceOf[Array[Byte]]))
          shape.setUserData(x.copy)
          shape
        }
        .toJavaRDD())
    spatialRdd
  }

  def toExpandedEnvelopeRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression, boundRadius: Expression, isGeography: Boolean): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          val shape = GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[Array[Byte]])
          val envelope = shape.getEnvelopeInternal.copy()
          expandEnvelope(envelope, boundRadius.eval(x).asInstanceOf[Double], 6357000.0, isGeography)

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

  /**
    * Expand the given envelope by the given distance in meter.
    * For geography, we expand the envelope by the given distance in both longitude and latitude.
    * @param envelope
    * @param distance in meter
    * @param radius in meter
    * @param isGeography
    */
  private def expandEnvelope(envelope:Envelope, distance:Double, radius:Double, isGeography:Boolean):Unit = {
    if (isGeography) {
      val scaleFactor = 1.1 // 10% buffer to get rid of false negatives
      val latRadian = Math.toRadians((envelope.getMinX + envelope.getMaxX) / 2.0)
      val latDeltaRadian = distance / radius;
      val latDeltaDegree = Math.toDegrees(latDeltaRadian)
      val lonDeltaRadian = Math.max(Math.abs(distance / (radius * Math.cos(latRadian + latDeltaRadian))),
        Math.abs(distance / (radius * Math.cos(latRadian - latDeltaRadian))))
      val lonDeltaDegree = Math.toDegrees(lonDeltaRadian)
      envelope.expandBy(latDeltaDegree * scaleFactor, lonDeltaDegree * scaleFactor)
    } else {
      envelope.expandBy(distance)
    }
  }
}