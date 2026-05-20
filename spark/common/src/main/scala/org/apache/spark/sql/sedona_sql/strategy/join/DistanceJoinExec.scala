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

import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, Literal, UnsafeRow}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

/**
 * Distance joins requires matching geometries to be in the same partition, despite not
 * necessarily overlapping. To create an overlap and guarantee matching geometries end up in the
 * same partition, the left geometry is expanded before partitioning. It's the logical equivalent
 * of:
 *
 * select * from a join b on ST_Distance(a.geom, b.geom) <= 1
 *
 * becomes
 *
 * select * from a join b on ST_Intersects(ST_Envelope(ST_Buffer(a.geom, 1)), b.geom) and
 * ST_Distance(a.geom, b.geom) <= 1
 *
 * @param left
 *   left side of the join
 * @param right
 *   right side of the join
 * @param leftShape
 *   expression for the first argument of spatialPredicate
 * @param rightShape
 *   expression for the second argument of spatialPredicate
 * @param distance
 *   \- ST_Distance(left, right) <= distance. Distance can be literal or a computation over 'left'
 *   or 'right'.
 * @param distanceBoundToLeft
 *   whether distance expression references attributes from left relation or right relation
 * @param spatialPredicate
 *   spatial predicate as join condition
 * @param extraCondition
 *   extra join condition other than spatialPredicate
 */
case class DistanceJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    distance: Expression,
    distanceBoundToLeft: Boolean,
    spatialPredicate: SpatialPredicate,
    isGeography: Boolean,
    extraCondition: Option[Expression] = None)
    extends SedonaBinaryExecNode
    with TraitJoinQueryExec
    with Logging {

  private lazy val boundRadius = if (distanceBoundToLeft) {
    BindReferences.bindReference(distance, left.output)
  } else {
    BindReferences.bindReference(distance, right.output)
  }

  override def toSpatialRddPair(
      leftRdd: RDD[UnsafeRow],
      leftShapeExpr: Expression,
      rightRdd: RDD[UnsafeRow],
      rightShapeExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) = {
    // Raster predicates project both sides to a WGS84 envelope before partitioning so the coarse
    // R-tree filter applies regardless of the input CRS — mirroring the non-distance raster join
    // (toWGS84EnvelopeRDD) and the broadcast-index raster distance path.
    val isRasterPredicate =
      leftShapeExpr.dataType.isInstanceOf[RasterUDT] ||
        rightShapeExpr.dataType.isInstanceOf[RasterUDT]
    if (isRasterPredicate) {
      // Route both sides through `toExpandedWGS84EnvelopeRDD` so the polar / antimeridian
      // world-envelope fallback in `expandRasterFilterEnvelope` applies symmetrically. The side
      // that doesn't carry the user-supplied distance still goes through the same path with a
      // zero radius — the Haversine expansion collapses to a no-op for ordinary footprints, and
      // the fallback ensures problematic ones get a global filter envelope.
      val zeroRadius = Literal(0.0, DoubleType)
      if (distanceBoundToLeft) {
        (
          toExpandedWGS84EnvelopeRDD(leftRdd, leftShapeExpr, boundRadius),
          toExpandedWGS84EnvelopeRDD(rightRdd, rightShapeExpr, zeroRadius))
      } else {
        (
          toExpandedWGS84EnvelopeRDD(leftRdd, leftShapeExpr, zeroRadius),
          toExpandedWGS84EnvelopeRDD(rightRdd, rightShapeExpr, boundRadius))
      }
    } else if (distanceBoundToLeft) {
      (
        toExpandedEnvelopeRDD(leftRdd, leftShapeExpr, boundRadius, isGeography),
        toSpatialRDD(rightRdd, rightShapeExpr))
    } else {
      (
        toSpatialRDD(leftRdd, leftShapeExpr),
        toExpandedEnvelopeRDD(rightRdd, rightShapeExpr, boundRadius, isGeography))
    }
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }

}
