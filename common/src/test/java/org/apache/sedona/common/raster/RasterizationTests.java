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
package org.apache.sedona.common.raster;

import static org.apache.sedona.common.raster.RasterAccessors.metadata;

import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class RasterizationTests extends RasterTestBase {
  private final WKTReader wktReader = new WKTReader();

  @Test
  public void testRasterizeGeomExtent()
      throws ParseException, FactoryException, TransformException {
    GridCoverage2D testRaster = RasterConstructors.makeEmptyRaster(1, "F", 4, 4, 4, 4, 1);
    double[] metadata = metadata(testRaster);

    // Grid aligned polygon completely contained within raster extent
    String wktPolygon = "POLYGON ((5 2, 6 1, 5 3, 7 3, 5 2))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {5.0, 7.0, 1.0, 3.0}, testRaster, metadata, false);

    // Grid aligned polygon in line with raster extent
    wktPolygon = "POLYGON ((4 2, 6 0, 8 2, 6 4, 4 2))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {4.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // Polygon partially outside raster extent
    wktPolygon = "POLYGON ((3 1, 5 -1, 8 2, 6 4, 3 1))";
    validateRasterizeGeomExtent(
        wktPolygon, new double[] {4.0, 8.0, 0.0, 4.0}, testRaster, metadata, false);

    // Polygon completely outside raster extent
    wktPolygon = "POLYGON ((-1 -1, 0 -2, 1 -1, 0 0, -1 -1))";
    validateRasterizeGeomExtent(wktPolygon, null, testRaster, metadata, false);

    //  Partial pixel alignment
    String wktPolygon5 = "POLYGON ((5.5 2.5, 4.5 0.5, 6.5 1.5, 5.5 2.5))";
    validateRasterizeGeomExtent(
        wktPolygon5, new double[] {4.0, 7.0, 0.0, 3.0}, testRaster, metadata, false);

    GridCoverage2D testRaster_frac =
        RasterConstructors.makeEmptyRaster(
            1, "F", 4, 4, 4.3333, 4.6666, 0.3333, -0.3333, 0, 0, 4326);
    double[] metadata_frac = metadata(testRaster_frac);

    // Grid aligned polygon completely contained within raster extent
    wktPolygon = "Polygon((4.6666 4.0, 4.9999 3.7777, 5.3332 4.0,  4.9999 4.3333,  4.6666 4.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.6666, 5.3332, 3.6667, 4.3333},
        testRaster_frac,
        metadata_frac,
        false);

    //  Partial pixel alignment
    wktPolygon = "Polygon((4.7 3.9, 4.9999 3.9, 5.1 4.0,  4.9999 4.2, 4.7 3.9))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.6666, 5.3332, 3.6667, 4.3333},
        testRaster_frac,
        metadata_frac,
        false);

    // polygon larger than raster extent
    wktPolygon = "Polygon((4.0 3.0, 4.0 2.0, 8.0 2.0, 8.0 5.0, 4.0 5.0, 4.0 3.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {4.3333, 5.6665, 3.3334, 4.6666},
        testRaster_frac,
        metadata_frac,
        false);

    GridCoverage2D testRaster_neg =
        RasterConstructors.makeEmptyRaster(
            1, "F", 4, 4, -4.3333, -4.6666, 0.3333, -0.3333, 0, 0, 4326);
    double[] metadata_neg = metadata(testRaster_neg);

    // polygon larger than raster extent
    wktPolygon = "Polygon((-8.0 -2.0, -2.0 -2.0, -2.0 -6.0, -8.0 -6.0, -8.0 -2.0))";
    validateRasterizeGeomExtent(
        wktPolygon,
        new double[] {-4.3333, -3.0001, -5.9998, -4.6666},
        testRaster_neg,
        metadata_neg,
        false);

    // horizontal line
    String wktLine = "LINESTRING (5.0 2.0, 6.0 2.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {5.0, 6.0, 1.0, 3.0}, testRaster, metadata, false);

    // vertical line
    wktLine = "LINESTRING (5.0 2.0, 5.0 3.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {4.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // diagonal line
    wktLine = "LINESTRING (5.0 2.0, 6.0 3.0)";
    validateRasterizeGeomExtent(
        wktLine, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // point tests
    String wktPoint = "POINT (5.0 2.0)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {4.0, 6.0, 1.0, 3.0}, testRaster, metadata, false);

    // intersecting 2 pixels
    wktPoint = "POINT (5.0 2.5)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {4.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);

    // within pixel
    wktPoint = "POINT (5.25 2.25)";
    validateRasterizeGeomExtent(
        wktPoint, new double[] {5.0, 6.0, 2.0, 3.0}, testRaster, metadata, false);
  }

  private void validateRasterizeGeomExtent(
      String wkt,
      double[] expectedEnvelope,
      GridCoverage2D raster,
      double[] metadata,
      boolean allTouched)
      throws ParseException {
    Geometry geom = wktReader.read(wkt);
    ReferencedEnvelope envelope =
        Rasterization.rasterizeGeomExtent(geom, raster, metadata, allTouched);
    if (expectedEnvelope == null) {
      Assert.assertNull(envelope);
    } else {
      Assert.assertEquals(expectedEnvelope[0], envelope.getMinX(), FP_TOLERANCE);
      Assert.assertEquals(expectedEnvelope[1], envelope.getMaxX(), FP_TOLERANCE);
      Assert.assertEquals(expectedEnvelope[2], envelope.getMinY(), FP_TOLERANCE);
      Assert.assertEquals(expectedEnvelope[3], envelope.getMaxY(), FP_TOLERANCE);
    }
  }
}
