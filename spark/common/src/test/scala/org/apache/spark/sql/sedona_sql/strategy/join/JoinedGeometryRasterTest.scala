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

import org.apache.sedona.common.raster.RasterConstructors
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

class JoinedGeometryRasterTest extends AnyFunSuite {

  private val globalWGS84Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0)

  test("operands requiring scalar validation use the global coarse envelope") {
    val raster =
      RasterConstructors.makeEmptyRaster(1, 2, 2, 1000.0, 2000.0, 1.0)
    val geometry = new GeometryFactory().createPoint(new Coordinate(1000.0, 2000.0))
    val emptyGeometry = new GeometryFactory().createGeometryCollection()
    emptyGeometry.setSRID(4326)
    val outOfWorldGeometry =
      new GeometryFactory().createPoint(new Coordinate(1000.0, 2000.0))
    outOfWorldGeometry.setSRID(4326)
    val unknownSridGeometry =
      new GeometryFactory().createPoint(new Coordinate(0.0, 0.0))
    unknownSridGeometry.setSRID(999999)
    val outOfWorldRaster =
      RasterConstructors.makeEmptyRaster(1, "B", 2, 2, 1000.0, 2000.0, 1.0, -1.0, 0.0, 0.0, 4326)

    assertGlobalEnvelope(JoinedGeometryRaster.rasterToWGS84EnvelopeForRefinement(raster))
    assertGlobalEnvelope(JoinedGeometryRaster.geometryToWGS84EnvelopeForRefinement(geometry))
    assertGlobalEnvelope(JoinedGeometryRaster.geometryToWGS84EnvelopeForRefinement(emptyGeometry))
    assertGlobalEnvelope(
      JoinedGeometryRaster.geometryToWGS84EnvelopeForRefinement(outOfWorldGeometry))
    assertGlobalEnvelope(
      JoinedGeometryRaster.geometryToWGS84EnvelopeForRefinement(unknownSridGeometry))
    assertGlobalEnvelope(
      JoinedGeometryRaster.rasterToWGS84EnvelopeForRefinement(outOfWorldRaster))
  }

  test("ordinary WGS84 envelopes preserve CRS-less coordinates for distance joins") {
    val raster =
      RasterConstructors.makeEmptyRaster(1, 2, 2, 1000.0, 2000.0, 1.0)
    val geometry = new GeometryFactory().createPoint(new Coordinate(1000.0, 2000.0))
    val rasterEnvelope = raster.getEnvelope2D

    assert(
      JoinedGeometryRaster.rasterToWGS84Envelope(raster).getEnvelopeInternal == new Envelope(
        rasterEnvelope.getMinX,
        rasterEnvelope.getMaxX,
        rasterEnvelope.getMinY,
        rasterEnvelope.getMaxY))
    assert(JoinedGeometryRaster.geometryToWGS84Envelope(geometry) eq geometry)
  }

  private def assertGlobalEnvelope(geometry: Geometry): Unit =
    assert(geometry.getEnvelopeInternal == globalWGS84Envelope)
}
