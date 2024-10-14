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
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.locationtech.jts.geom.Geometry

trait TraitJoinQueryBase {
  self: SparkPlan =>

  def toSpatialRddPair(
      leftRdd: RDD[UnsafeRow],
      leftShapeExpr: Expression,
      rightRdd: RDD[UnsafeRow],
      rightShapeExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) = {
    if (leftShapeExpr.dataType.isInstanceOf[RasterUDT] || rightShapeExpr.dataType
        .isInstanceOf[RasterUDT]) {
      (toWGS84EnvelopeRDD(leftRdd, leftShapeExpr), toWGS84EnvelopeRDD(rightRdd, rightShapeExpr))
    } else {
      (toSpatialRDD(leftRdd, leftShapeExpr), toSpatialRDD(rightRdd, rightShapeExpr))
    }
  }

  def toSpatialRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          val shape =
            GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[Array[Byte]])
          shape.setUserData(x.copy)
          shape
        }
        .toJavaRDD())
    spatialRdd
  }

  def toWGS84EnvelopeRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression): SpatialRDD[Geometry] = {
    // This RDD is for performing raster-geometry or raster-raster join, where we need to perform implicit CRS
    // transformation for both sides. We use expanded WGS84 envelope as the joined geometries and perform a
    // coarse-grained spatial join.
    val spatialRdd = new SpatialRDD[Geometry]
    val wgs84EnvelopeRdd = if (shapeExpression.dataType.isInstanceOf[RasterUDT]) {
      rdd.map { row =>
        val raster =
          RasterSerializer.deserialize(shapeExpression.eval(row).asInstanceOf[Array[Byte]])
        val shape = JoinedGeometryRaster.rasterToWGS84Envelope(raster)
        shape.setUserData(row.copy)
        shape
      }
    } else {
      rdd.map { row =>
        val geom =
          GeometrySerializer.deserialize(shapeExpression.eval(row).asInstanceOf[Array[Byte]])
        val shape = JoinedGeometryRaster.geometryToWGS84Envelope(geom)
        shape.setUserData(row.copy)
        shape
      }
    }
    spatialRdd.setRawSpatialRDD(wgs84EnvelopeRdd)
    spatialRdd
  }

  def toExpandedEnvelopeRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      boundRadius: Expression,
      isGeography: Boolean): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          val shape =
            GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[Array[Byte]])
          val distance = boundRadius.eval(x).asInstanceOf[Double]
          val expandedEnvelope =
            JoinedGeometry.geometryToExpandedEnvelope(shape, distance, isGeography)
          expandedEnvelope.setUserData(x.copy)
          expandedEnvelope
        }
        .toJavaRDD())
    spatialRdd
  }

  def doSpatialPartitioning(
      dominantShapes: SpatialRDD[Geometry],
      followerShapes: SpatialRDD[Geometry],
      numPartitions: Integer,
      sedonaConf: SedonaConf): Unit = {
    if (dominantShapes.approximateTotalCount > 0 && numPartitions > 0) {
      dominantShapes.spatialPartitioning(sedonaConf.getJoinGridType, numPartitions)
      followerShapes.spatialPartitioning(dominantShapes.getPartitioner)
    }
  }
}
