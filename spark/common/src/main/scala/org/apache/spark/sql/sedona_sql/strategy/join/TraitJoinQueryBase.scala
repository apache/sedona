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

import org.apache.sedona.common.Constructors
import org.apache.sedona.common.S2Geography.GeographyWKBSerializer
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.UDT.{Box2DUDT, Box3DUDT, RasterUDT}
import org.locationtech.jts.geom.{Geometry, GeometryFactory}

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
          // Null shape rows materialise as an empty geometry collection so they carry the row
          // payload through the partitioner / index without participating in any spatial match
          // — mirrors the pre-existing `GeometrySerializer.deserialize(null)` fallback.
          val shape = TraitJoinQueryBase.shapeToGeometryOrEmpty(shapeExpression, x)
          shape.setUserData(x.copy)
          shape
        }
        .toJavaRDD())
    spatialRdd
  }

  /**
   * Builds a SpatialRDD from a column of GeographyUDT bytes. Each row becomes a JTS geometry
   * whose envelope is the Geography's lat/lng bounding rectangle (full-longitude when the
   * rectangle wraps the antimeridian). The Geography object is carried alongside the original row
   * in `userData` via [[GeographyJoinShape]] so the join executor can perform S2-based predicate
   * refinement and emit the row.
   */
  def toGeographySpatialRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .flatMap { x =>
          val geogBytes = shapeExpression.eval(x).asInstanceOf[Array[Byte]]
          if (geogBytes == null) {
            None
          } else {
            val geog = GeographyWKBSerializer.deserialize(geogBytes)
            val shape = JoinedGeometry.geographyToEnvelopeGeometry(geog)
            shape.setUserData(GeographyJoinShape(geog, x.copy))
            Some(shape)
          }
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
          val shape = TraitJoinQueryBase.shapeToGeometryOrEmpty(shapeExpression, x)
          val distance = boundRadius.eval(x).asInstanceOf[Double]
          val expandedEnvelope =
            JoinedGeometry.geometryToExpandedEnvelope(shape, distance, isGeography)
          expandedEnvelope.setUserData(x.copy)
          expandedEnvelope
        }
        .toJavaRDD())
    spatialRdd
  }

  /**
   * Raster variant of [[toExpandedEnvelopeRDD]] for distance joins with raster predicates. Each
   * row's shape (raster or geometry) is first projected to a WGS84 envelope (matching the
   * non-distance raster path in [[toWGS84EnvelopeRDD]]) and then expanded by `boundRadius` meters
   * using the Haversine polar-radius approximation — the same envelope expansion the
   * `ST_DistanceSphere` distance-join uses. Treating `boundRadius` as meters everywhere (rather
   * than the input CRS's native unit) keeps the coarse R-tree filter and the per-row `RS_DWithin`
   * predicate aligned on a single unit.
   *
   * Polar and antimeridian-crossing rasters already span half or more of the globe in their
   * planar WGS84 envelope; for those rows the helper substitutes a global envelope so the R-tree
   * filter pairs them with every counterpart and the per-row S2 predicate produces the final
   * answer. Mid-latitude rasters retain the tight Haversine bound.
   */
  def toExpandedWGS84EnvelopeRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      boundRadius: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    val expandedRdd = rdd.map { row =>
      val serialized = shapeExpression.eval(row).asInstanceOf[Array[Byte]]
      val baseShape =
        if (serialized == null) {
          new GeometryFactory().createGeometryCollection()
        } else if (shapeExpression.dataType.isInstanceOf[RasterUDT]) {
          val raster = RasterSerializer.deserialize(serialized)
          JoinedGeometryRaster.rasterToWGS84Envelope(raster)
        } else {
          val geom = GeometrySerializer.deserialize(serialized)
          JoinedGeometryRaster.geometryToWGS84Envelope(geom)
        }
      val distance = boundRadius.eval(row).asInstanceOf[Double]
      val expanded = expandRasterFilterEnvelope(baseShape, distance)
      expanded.setUserData(row.copy)
      expanded
    }
    spatialRdd.setRawSpatialRDD(expandedRdd)
    spatialRdd
  }

  /**
   * Compute the R-tree filter envelope for a single side of a raster distance join. Mid-latitude
   * / single-hemisphere rasters get a Haversine-expanded envelope (tight, optimization-friendly).
   * Footprints whose planar WGS84 envelope already spans more than half the globe in longitude or
   * grazes a pole fall back to a world envelope so the coarse filter never excludes a true match
   * — the per-row S2 predicate decides correctness for those rows.
   */
  private[join] def expandRasterFilterEnvelope(
      baseShape: Geometry,
      distance: Double): Geometry = {
    val envelope = baseShape.getEnvelopeInternal
    val touchesPole = envelope.getMaxY >= 90.0 || envelope.getMinY <= -90.0
    val tooWide = envelope.getWidth > 180.0
    if (touchesPole || tooWide) {
      baseShape.getFactory.toGeometry(new org.locationtech.jts.geom.Envelope(-180, 180, -90, 90))
    } else {
      JoinedGeometry.geometryToExpandedEnvelope(baseShape, distance, isGeography = true)
    }
  }

  /**
   * Geography variant of [[toExpandedEnvelopeRDD]]. Each row becomes a JTS geometry whose
   * envelope is the Geography's lat/lng bounding rectangle expanded by `boundRadius` meters using
   * the Haversine-based polar-radius approximation in
   * [[JoinedGeometry.geometryToExpandedEnvelope]] (with `isGeography=true`). The Geography object
   * and per-row radius are carried alongside the original row in `userData` via
   * [[GeographyJoinShape]] so the join executor can perform S2-based ST_DWithin refinement.
   */
  def toExpandedGeographyEnvelopeRDD(
      rdd: RDD[UnsafeRow],
      shapeExpression: Expression,
      boundRadius: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(rdd
      .flatMap { x =>
        val geogBytes = shapeExpression.eval(x).asInstanceOf[Array[Byte]]
        if (geogBytes == null) {
          None
        } else {
          val geog = GeographyWKBSerializer.deserialize(geogBytes)
          val distance = boundRadius.eval(x).asInstanceOf[Double]
          val baseEnvelope = JoinedGeometry.geographyToEnvelopeGeometry(geog)
          val expandedEnvelope =
            JoinedGeometry.geometryToExpandedEnvelope(baseEnvelope, distance, isGeography = true)
          expandedEnvelope.setUserData(GeographyJoinShape(geog, x.copy, distance))
          Some(expandedEnvelope)
        }
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

object TraitJoinQueryBase {

  /**
   * Materialise a shape column value as a JTS [[Geometry]]. Box2D-typed columns are turned into
   * the closed rectangular polygon implied by their `(xmin, ymin, xmax, ymax)` bounds; Box3D
   * columns are projected to their XY footprint (`xmin, ymin, xmax, ymax`) and the Z axis is
   * re-checked per candidate pair by the scalar Box3D predicate that `JoinQueryDetector` folds
   * into the join's `extraCondition`. All other shape columns are deserialised from the Sedona
   * geometry binary form.
   *
   * Producing a JTS rectangle here lets the rest of the join machinery — partitioner, R-tree
   * `IndexBuilder`, refine evaluator — stay shape-agnostic. JTS already short-circuits
   * rectangle-rectangle predicates (`Polygon.isRectangle` triggers `RectangleIntersects` /
   * `RectangleContains`), so a Box2D-on-Box2D `ST_Intersects` join naturally pays only the
   * four-double envelope comparison at refine time.
   *
   * Inverted Box2D bounds (`xmin > xmax` / `ymin > ymax`) are rejected with the same
   * `IllegalArgumentException` raised by `Predicates.boxIntersects` / `boxContains`. Inverted
   * bounds have no defined planar meaning today (they are reserved for future
   * antimeridian-wraparound semantics on Geography bboxes) and would silently mis-prune the
   * R-tree if accepted here. Box3D applies the same check on all three axes — Z has no wraparound
   * convention.
   *
   * Returns `null` when the shape column evaluates to NULL; the caller is expected to either skip
   * the row or substitute an empty geometry.
   */
  def shapeToGeometry(shapeExpression: Expression, row: InternalRow): Geometry = {
    val evaluated = shapeExpression.eval(row)
    if (evaluated == null) {
      null
    } else
      shapeExpression.dataType match {
        case _: Box2DUDT =>
          val box = evaluated.asInstanceOf[InternalRow]
          val xmin = box.getDouble(0)
          val ymin = box.getDouble(1)
          val xmax = box.getDouble(2)
          val ymax = box.getDouble(3)
          if (xmin > xmax || ymin > ymax) {
            throw new IllegalArgumentException(
              "Box2D join input has inverted bounds (xmin > xmax or ymin > ymax). " +
                "Planar Box2D predicates require ordered intervals; inverted bounds are " +
                "reserved for future antimeridian wraparound semantics.")
          }
          Constructors.polygonFromEnvelope(xmin, ymin, xmax, ymax)
        case _: Box3DUDT =>
          val box = evaluated.asInstanceOf[InternalRow]
          val xmin = box.getDouble(0)
          val ymin = box.getDouble(1)
          val zmin = box.getDouble(2)
          val xmax = box.getDouble(3)
          val ymax = box.getDouble(4)
          val zmax = box.getDouble(5)
          // Eager validation: every row in the dataset is inspected during index build, so any
          // inverted-bound row throws here even if it would not have matched anything. This is
          // a slightly broader failure surface than the prior row-by-row fallback (which only
          // threw on rows the join actually evaluated), but it matches the Box2D contract on
          // line above and the scalar Box3D predicate contract — those reject inverted bounds
          // outright on the grounds that Z has no wraparound convention.
          if (xmin > xmax || ymin > ymax || zmin > zmax) {
            throw new IllegalArgumentException(
              "Box3D join input has inverted bounds (xmin > xmax, ymin > ymax, or " +
                "zmin > zmax). Box3D predicates require ordered intervals on all three axes.")
          }
          // XY footprint only — the Z axis is rechecked per candidate by the scalar Box3D
          // predicate folded into `extraCondition`.
          Constructors.polygonFromEnvelope(xmin, ymin, xmax, ymax)
        case _ =>
          GeometrySerializer.deserialize(evaluated.asInstanceOf[Array[Byte]])
      }
  }

  /**
   * Convenience wrapper that substitutes an empty geometry collection for NULL shapes. Used by
   * the partitioned-RDD path where each row must carry a non-null geometry so the original
   * `UnsafeRow` survives to outer-join output; spatial predicates against the empty geometry
   * produce no matches, matching the legacy `GeometrySerializer.deserialize(null)` behaviour.
   */
  def shapeToGeometryOrEmpty(shapeExpression: Expression, row: InternalRow): Geometry = {
    val shape = shapeToGeometry(shapeExpression, row)
    if (shape == null) new GeometryFactory().createGeometryCollection() else shape
  }
}
