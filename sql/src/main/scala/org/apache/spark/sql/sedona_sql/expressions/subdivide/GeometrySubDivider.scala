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

package org.apache.spark.sql.sedona_sql.expressions.subdivide

import org.apache.spark.sql.sedona_sql.expressions.simplify.GeometrySimplifier
import org.geotools.geometry.jts.JTS
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryCollection, GeometryFactory, Point}
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer

object GeometrySubDivider {

  val geometryFactory = new GeometryFactory()

  import GeometrySubDividerConfiguration._

  def equalValue(value: Double, pivot: Double): Boolean = {
    math.abs(value - pivot) > FP_TOLERANCE
  }

  def subDivideRecursive(
                          geom: Geometry,
                          dimension: Int,
                          maxVertices: Int,
                          depth: Int,
                          geometries: Seq[Geometry]
                        ): Seq[Geometry] = {

    val numberOfVertices = geom.getNumPoints
    val geometryBbox = geom.getEnvelope.getEnvelopeInternal
    val width = geometryBbox.getWidth
    val height = geometryBbox.getHeight
    val geometryBboxAligned = if (width == 0) new Envelope(
      geometryBbox.getMinX - FP_TOLERANCE,
      geometryBbox.getMaxX + FP_TOLERANCE,
      geometryBbox.getMinY,
      geometryBbox.getMaxY
    ) else if (height == 0) new Envelope(
      geometryBbox.getMinX,
      geometryBbox.getMaxX,
      geometryBbox.getMinY - FP_TOLERANCE,
      geometryBbox.getMaxY + FP_TOLERANCE
    ) else geometryBbox
    if (
      geom == null || geom.getDimension < dimension || numberOfVertices == 0 || geometryBboxAligned == null
    ) geometries
    else if (width == 0.0 && height == 0.0) geom match {
      case point: Point => if (dimension == 0) geom +: geometries else geometries
      case polygon: Geometry => geometries
    }
    else if (geom.isInstanceOf[GeometryCollection] && !geom.getGeometryType.contains("Point")) {
      val collection = geom.asInstanceOf[GeometryCollection]
      val numberOfGeometries = collection.getNumGeometries
      (0 until numberOfGeometries).map(
        i => subDivideRecursive(collection.getGeometryN(i), dimension, maxVertices, depth, geometries)
      ).reduce(_ ++ _)
    }
    else {
      if (numberOfVertices <= maxVertices || depth > maxDepth) {
        geom +: geometries
      }
      else {
        val split_ordinate = if (width > height) 0 else 1

        val center = if (split_ordinate == 0) (geometryBboxAligned.getMinX + geometryBboxAligned.getMaxX) / 2
        else (geometryBboxAligned.getMinY + geometryBboxAligned.getMaxY) / 2

        val pivot = PivotFinder.findPivot(geom, split_ordinate, center, numberOfVertices)

        val subBoxes = getSubBoxes(split_ordinate, SubdivideExtent.fromClip(geometryBboxAligned), pivot, center)

        val intersectedSimplified = getIntersectionGeometries(subBoxes.subBox, geom)
        val intersectedIter2Simplfied = getIntersectionGeometries(subBoxes.subBox2, geom)

        if (intersectedSimplified.isNonEmpty && intersectedIter2Simplfied.isNonEmpty) {
          subDivideRecursive(intersectedSimplified, dimension, maxVertices, depth + 1, geometries) ++
            subDivideRecursive(intersectedIter2Simplfied, dimension, maxVertices, depth + 1, geometries)
        } else if (intersectedSimplified.isNonEmpty) {
          subDivideRecursive(intersectedSimplified, dimension, maxVertices, depth + 1, geometries)
        } else if (intersectedIter2Simplfied.isNonEmpty) {
          subDivideRecursive(intersectedIter2Simplfied, dimension, maxVertices, depth + 1, geometries)
        }
        else {
          geometries
        }

      }
    }
  }

  private def getSubBoxes(split_ordinate: Int, subbox: SubdivideExtent, pivot: Double, center: Double): SubBoxes = {
    if (split_ordinate == 0) {
      if (equalValue(subbox.xmax, pivot) && equalValue(subbox.xmin, pivot))
        SubBoxes(subbox.copy(xmax = pivot), subbox.copy(xmin = pivot))
      else
        SubBoxes(subbox.copy(xmax = center), subbox.copy(xmin = center))
    }
    else {
      if (equalValue(subbox.ymax, pivot) && equalValue(subbox.ymin, pivot))
        SubBoxes(subbox.copy(ymax = pivot), subbox.copy(ymin = pivot))
      else
        SubBoxes(subbox.copy(ymax = center), subbox.copy(ymin = center))
    }
  }

  private def getIntersectionGeometries(extent: SubdivideExtent, geom: Geometry): Geometry = {
    val subBox = new Envelope(extent.xmin, extent.xmax, extent.ymin, extent.ymax)
    val intersected = geom.intersection(JTS.toGeometry(subBox))
    GeometrySimplifier.simplify(intersected, true, 0.0)
  }

  def subDividePrecise(geom: Geometry, maxVertices: Int): Seq[Geometry] = {
    if (geom == null) {
      Seq()
    }
    else {
      if (maxVertices < minMaxVertices) {
        Seq()
      }
      else {
        subDivideRecursive(geom, geom.getDimension, maxVertices, startDepth, Seq())
      }
    }

  }

  def subDivide(geom: Geometry, maxVertices: Int): Seq[Geometry] = {
    subDividePrecise(geom, maxVertices)
  }

}

