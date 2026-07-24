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

import static org.apache.sedona.common.utils.RasterUtils.flipVerticallyPixelSpace;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.coverage.SampleDimensionType;
import org.geotools.api.geometry.Position;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public class RasterConstructorsTest extends RasterTestBase {

  @Test
  public void fromArcInfoAsciiGrid() throws IOException, FactoryException {
    GridCoverage2D gridCoverage2D =
        RasterConstructors.fromArcInfoAsciiGrid(arc.getBytes(StandardCharsets.UTF_8));

    Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
    assertEquals(3600, envelope.getArea(), 0.1);
    assertEquals(378922d + 30, envelope.getCentroid().getX(), 0.1);
    assertEquals(4072345d + 30, envelope.getCentroid().getY(), 0.1);
    assertEquals(2, gridCoverage2D.getRenderedImage().getTileHeight());
    assertEquals(2, gridCoverage2D.getRenderedImage().getTileWidth());
    assertEquals(0d, RasterUtils.getNoDataValue(gridCoverage2D.getSampleDimension(0)), 0.1);
    assertEquals(
        3d, gridCoverage2D.getRenderedImage().getData().getPixel(1, 1, (double[]) null)[0], 0.1);
  }

  @Test
  public void fromGeoTiff() throws IOException, FactoryException {
    GridCoverage2D gridCoverage2D = RasterConstructors.fromGeoTiff(geoTiff);

    Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
    assertEquals(100, envelope.getArea(), 0.1);
    assertEquals(5, envelope.getCentroid().getX(), 0.1);
    assertEquals(5, envelope.getCentroid().getY(), 0.1);
    assertEquals(10, gridCoverage2D.getRenderedImage().getTileHeight());
    assertEquals(10, gridCoverage2D.getRenderedImage().getTileWidth());
    assertEquals(
        10d, gridCoverage2D.getRenderedImage().getData().getPixel(5, 5, (double[]) null)[0], 0.1);
    assertEquals(4, gridCoverage2D.getNumSampleDimensions());
  }

  @Test
  public void testAsRasterWithEmptyRaster() throws FactoryException, ParseException {
    // The empty reference rasters below carry no nodata value. Omitting the noDataValue argument
    // would now inherit the reference band's nodata value and error when there is none, so the
    // calls that exercise the default burn value of 1 with a zero background pass an explicit null
    // noDataValue (the escape hatch for "no nodata value, zero background").
    // Polygon
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -1, 2, -2, 0, 0, 4326);
    GridCoverage2D raster_bottom_up = flipVerticallyPixelSpace(raster);

    Geometry geom =
        Constructors.geomFromWKT("POLYGON((15 -15, 18 -20, 15 -24, 24 -25, 15 -15))", 0);
    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", true, 3093151, 3d);

    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);

    double[] expected =
        new double[] {
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0,
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3093151.0,
          3093151.0, 3093151.0, 3093151.0, 3.0, 3.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0,
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0,
          0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 1d, null);
    expected =
        new double[] {
          1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    assertArrayEquals(expected, actual, 0.1d);

    // Making sure orientation is preserved
    assertEquals(
        RasterAccessors.metadata(raster_bottom_up)[5],
        RasterAccessors.metadata(rasterized)[5],
        0.0);

    // MultiPolygon
    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON ( ((2 -2, 4 -2, 4 -4, 2 -4, 2 -2)), ((4 -4, 6 -4, 6 -6, 5 -7, 4 -6, 4 -4)), ((6 -6, 8 -6, 8 -8, 6 -8, 6 -6)), ((8 -6, 10 -6, 10 -4, 9 -3, 8 -4, 8 -6)) )",
            0);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", true, 3093151, 3d, true);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          3093151.0, 3093151.0, 3.0, 3.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0,
          3093151.0, 3.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3.0, 3093151.0, 3093151.0,
          3093151.0, 3.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0,
          1.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 1d, null);

    // Making sure orientation is preserved
    assertEquals(
        RasterAccessors.metadata(raster_bottom_up)[5],
        RasterAccessors.metadata(rasterized)[5],
        0.0);

    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0,
          0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // MultiLineString
    geom =
        Constructors.geomFromWKT("MULTILINESTRING ((5 -5, 10 -10), (10 -10, 15 -15, 20 -20))", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 3093151, 3d);

    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3093151.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 3093151, 3d);

    // Making sure orientation is preserved
    assertEquals(
        RasterAccessors.metadata(raster_bottom_up)[5],
        RasterAccessors.metadata(rasterized)[5],
        0.0);

    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);

    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 1d, null);

    // Making sure orientation is preserved
    assertEquals(
        RasterAccessors.metadata(raster_bottom_up)[5],
        RasterAccessors.metadata(rasterized)[5],
        0.0);

    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    // LinearRing
    geom = Constructors.geomFromWKT("LINEARRING (10 -10, 18 -20, 15 -24, 24 -25, 10 -10)", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected =
        new double[] {
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0,
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0,
          3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0,
          3093151.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 3093151, 3d);

    // Making sure orientation is preserved
    assertEquals(
        RasterAccessors.metadata(raster_bottom_up)[5],
        RasterAccessors.metadata(rasterized)[5],
        0.0);

    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0,
          3093151.0, 3093151.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0,
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected =
        new double[] {
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 1d, null);

    // Making sure orientation is preserved
    assertEquals(
        RasterAccessors.metadata(raster_bottom_up)[5],
        RasterAccessors.metadata(rasterized)[5],
        0.0);

    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // MultiPoints
    geom = Constructors.geomFromWKT("MULTIPOINT ((5 -5), (10 -10), (15 -15))", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected =
        new double[] {
          3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3093151.0, 3093151.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0,
          3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3.0, 3.0, 3.0,
          3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0,
          3093151.0, 3093151.0, 3.0, 3.0, 3.0, 3.0, 3.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected =
        new double[] {
          1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0
        };

    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Point
    geom = Constructors.geomFromWKT("POINT (5 -5)", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 3093151, 3d);

    actual = MapAlgebra.bandAsArray(rasterized, 1);

    expected = new double[] {3093151.0, 3093151.0, 3093151.0, 3093151.0};
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected = new double[] {1.0, 1.0, 1.0, 1.0};
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 1d, null);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterBackgroundIsNoDataValue() throws FactoryException, ParseException {
    // Axis-aligned rings on square pixels keep the pixel selection trivial;
    // the interesting part is that every pixel NOT covered by the geometry
    // must read back as the noDataValue rather than 0, matching the band's
    // nodata metadata (PostGIS ST_AsRaster fills untouched cells with
    // nodataval; gdal_rasterize -init <nodata> -a_nodata <nodata>).
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -2, 0, 0, 0);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((100.5 499.5, 113.5 499.5, 113.5 488.5, 100.5 488.5, 100.5 499.5), "
                + "(104.5 497.5, 109.5 497.5, 109.5 491.5, 104.5 491.5, 104.5 497.5))",
            0);

    GridCoverage2D rasterized =
        RasterConstructors.asRaster(geom, raster, "d", false, 1d, 9d, false);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.1d);
    assertEquals(9d, RasterUtils.getNoDataValue(rasterized.getSampleDimension(0)), 0.1d);

    // Burning value 0 must stay distinguishable from the nodata background
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 0d, 9d, false);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0, 0, 0, 0, 0, 0, 0,
          0, 0, 9, 9, 9, 0, 0,
          0, 0, 9, 9, 9, 0, 0,
          0, 0, 9, 9, 9, 0, 0,
          0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Geometry-extent output (the default) fills its background the same way;
    // this geometry's snapped envelope is the full grid
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, 9d, true);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Without a noDataValue the background stays 0
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null, false);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 0, 0, 0, 1, 1,
          1, 1, 0, 0, 0, 1, 1,
          1, 1, 0, 0, 0, 1, 1,
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterNegativeZeroNoDataValueIsFilled()
      throws FactoryException, ParseException {
    // -0.0 is a legitimate noDataValue distinct from the allocation's +0.0. A 3x3 grid fully
    // covered except for the centre pixel (a one-pixel hole) must burn 8 pixels and leave the
    // centre as the -0.0 background. RS_Count(band, excludeNoData=true) compares each pixel to
    // the nodata metadata with Double.compare, which distinguishes +0.0 from -0.0: if the hole
    // were left as the zero-initialized +0.0 it would be miscounted as data (9 instead of 8).
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "D", 3, 3, 0, 3, 1, -1, 0, 0, 0);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", 0);

    GridCoverage2D rasterized =
        RasterConstructors.asRaster(geom, raster, "D", false, 1d, -0.0d, false);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          1, 1, 1,
          1, -0.0, 1,
          1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.0d);

    // The hole pixel must be exactly -0.0 (not +0.0), matching the band's nodata metadata.
    // Double.compare is sign-of-zero sensitive; assertArrayEquals with a delta is not.
    assertEquals(0, Double.compare(-0.0d, actual[4]));
    assertEquals(
        0, Double.compare(-0.0d, RasterUtils.getNoDataValue(rasterized.getSampleDimension(0))));

    // 8 data pixels, not 9: the -0.0 hole is excluded as nodata.
    assertEquals(8L, RasterBandAccessors.getCount(rasterized, 1, true));
  }

  @Test
  public void testAsRasterRejectsNonRepresentableNoDataValue()
      throws FactoryException, ParseException {
    // An unsigned 8-bit band ('B') cannot store a negative or >255 nodata. Silently coercing
    // -1.0 -> 255 or 300.0 -> 44 would leave the background reading back as data while the
    // recorded nodata metadata disagreed, so RS_AsRaster rejects such a value up front.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -2, 0, 0, 0);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((100.5 499.5, 113.5 499.5, 113.5 488.5, 100.5 488.5, 100.5 499.5))", 0);

    IllegalArgumentException negative =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> RasterConstructors.asRaster(geom, raster, "B", false, 1d, -1d, false));
    Assert.assertTrue(negative.getMessage(), negative.getMessage().contains("-1.0"));
    Assert.assertTrue(negative.getMessage(), negative.getMessage().contains("'B'"));

    IllegalArgumentException tooLarge =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> RasterConstructors.asRaster(geom, raster, "B", false, 1d, 300d, false));
    Assert.assertTrue(tooLarge.getMessage(), tooLarge.getMessage().contains("300.0"));
    Assert.assertTrue(tooLarge.getMessage(), tooLarge.getMessage().contains("'B'"));

    // A fractional nodata cannot be stored in a signed 32-bit integer band either.
    IllegalArgumentException fractional =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> RasterConstructors.asRaster(geom, raster, "I", false, 1d, 1.5d, false));
    Assert.assertTrue(fractional.getMessage(), fractional.getMessage().contains("1.5"));
    Assert.assertTrue(fractional.getMessage(), fractional.getMessage().contains("'I'"));
  }

  @Test
  public void testAsRasterRepresentableNoDataValueOnIntegerBand()
      throws FactoryException, ParseException {
    // A representable nodata (9 fits an unsigned 8-bit band) still fills the uncovered pixels and
    // is recorded as the band's nodata, mirroring testAsRasterBackgroundIsNoDataValue on a double
    // band. The exact-equality reads confirm no coercion happened on the integer band.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -2, 0, 0, 0);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((100.5 499.5, 113.5 499.5, 113.5 488.5, 100.5 488.5, 100.5 499.5), "
                + "(104.5 497.5, 109.5 497.5, 109.5 491.5, 104.5 491.5, 104.5 497.5))",
            0);

    GridCoverage2D rasterized =
        RasterConstructors.asRaster(geom, raster, "B", false, 1d, 9d, false);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.0d);
    assertEquals(9d, RasterUtils.getNoDataValue(rasterized.getSampleDimension(0)), 0.0d);
  }

  @Test
  public void testAsRasterOmittedNoDataValueInheritsReferenceBandNoData()
      throws FactoryException, ParseException {
    // Omitting the noDataValue argument inherits the reference band's nodata value: it fills every
    // pixel the geometry does not cover (here the polygon's hole) and is recorded as the output
    // band's nodata metadata. This mirrors testAsRasterBackgroundIsNoDataValue, except the value
    // is taken from the reference band rather than passed explicitly. The exact-equality reads pin
    // that the inherited value reaches both the samples and the metadata.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -2, 0, 0, 0);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 9d);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((100.5 499.5, 113.5 499.5, 113.5 488.5, 100.5 488.5, 100.5 499.5), "
                + "(104.5 497.5, 109.5 497.5, 109.5 491.5, 104.5 491.5, 104.5 497.5))",
            0);

    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d");
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 9, 9, 9, 1, 1,
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.0d);
    assertEquals(9d, RasterUtils.getNoDataValue(rasterized.getSampleDimension(0)), 0.0d);
    assertEquals(Double.valueOf(9d), RasterBandAccessors.getBandNoDataValue(rasterized, 1));
  }

  @Test
  public void testAsRasterOmittedNoDataValueErrorsWithoutReferenceNoData()
      throws FactoryException, ParseException {
    // With the noDataValue omitted and the reference band carrying no nodata value there is nothing
    // to inherit, so RS_AsRaster rejects the call rather than silently producing a raster with no
    // nodata value and a zero background.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -2, 0, 0, 0);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((100.5 499.5, 113.5 499.5, 113.5 488.5, 100.5 488.5, 100.5 499.5))", 0);

    IllegalArgumentException error =
        Assert.assertThrows(
            IllegalArgumentException.class, () -> RasterConstructors.asRaster(geom, raster, "d"));
    Assert.assertTrue(error.getMessage(), error.getMessage().contains("noDataValue"));
    Assert.assertTrue(error.getMessage(), error.getMessage().contains("band 1"));
  }

  @Test
  public void testAsRasterExplicitNullNoDataValueLeavesNoNoData()
      throws FactoryException, ParseException {
    // Passing an explicit null noDataValue is the escape hatch for "no nodata value": the output
    // declares no band nodata value and every uncovered pixel stays 0, even when the reference
    // band has a nodata value that the noDataValue-less overloads would otherwise inherit.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -2, 0, 0, 0);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 9d);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((100.5 499.5, 113.5 499.5, 113.5 488.5, 100.5 488.5, 100.5 499.5), "
                + "(104.5 497.5, 109.5 497.5, 109.5 491.5, 104.5 491.5, 104.5 497.5))",
            0);

    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, null);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 0, 0, 0, 1, 1,
          1, 1, 0, 0, 0, 1, 1,
          1, 1, 0, 0, 0, 1, 1,
          1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1
        };
    assertArrayEquals(expected, actual, 0.0d);
    Assert.assertNull(RasterBandAccessors.getBandNoDataValue(rasterized, 1));
  }

  @Test
  public void testAsRasterWithNonSquarePixels() throws FactoryException, ParseException {
    // Pixels are 2 world units wide and 3 tall, so the x-intercept math cannot
    // silently conflate pixel-space and world-space slopes the way square
    // pixels allow. Expected matrices are produced by GDAL
    // (rasterio.features.rasterize) on the same grid; pixels whose centers are
    // inside the geometry must be burned.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 500, 2, -3, 0, 0, 0);
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((102.7 497.4, 112.4 496.9, 104.2 483.7, 102.7 497.4))", 0);

    GridCoverage2D rasterized =
        RasterConstructors.asRaster(geom, raster, "d", false, 1d, 0d, false);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          0, 0, 0, 0, 0, 0, 0,
          0, 1, 1, 1, 1, 1, 0,
          0, 0, 1, 1, 1, 0, 0,
          0, 0, 1, 1, 0, 0, 0,
          0, 0, 1, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // allTouched adds every boundary-touched pixel on the same grid
    rasterized = RasterConstructors.asRaster(geom, raster, "d", true, 1d, 0d, false);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0, 1, 1, 1, 1, 1, 0,
          0, 1, 1, 1, 1, 1, 1,
          0, 1, 1, 1, 1, 1, 0,
          0, 1, 1, 1, 1, 0, 0,
          0, 1, 1, 1, 0, 0, 0,
          0, 0, 1, 0, 0, 0, 0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // An interior ring with diagonal edges exercises hole intercepts too
    geom =
        Constructors.geomFromWKT(
            "POLYGON ((101.1 498.9, 113.5 497.8, 112.9 483.2, 102.3 484.6, 101.1 498.9), "
                + "(104.1 496.9, 110.2 496.9, 106.3 487.8, 104.1 496.9))",
            0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 1d, 0d, false);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0, 1, 1, 0, 0, 0, 0,
          0, 1, 0, 0, 0, 1, 1,
          0, 1, 1, 0, 1, 1, 1,
          0, 1, 1, 0, 1, 1, 1,
          0, 1, 1, 1, 1, 1, 1,
          0, 0, 0, 0, 0, 1, 0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Bottom-up variant of the same grid: the same triangle, vertically
    // flipped in pixel space
    GridCoverage2D rasterBottomUp =
        RasterConstructors.makeEmptyRaster(1, "d", 7, 6, 100, 482, 2, 3, 0, 0, 0);
    geom =
        Constructors.geomFromWKT(
            "POLYGON ((102.7 497.4, 112.4 496.9, 104.2 483.7, 102.7 497.4))", 0);
    rasterized = RasterConstructors.asRaster(geom, rasterBottomUp, "d", false, 1d, 0d, false);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0, 0, 0, 0, 0, 0, 0,
          0, 0, 1, 0, 0, 0, 0,
          0, 0, 1, 1, 0, 0, 0,
          0, 0, 1, 1, 1, 0, 0,
          0, 1, 1, 1, 1, 1, 0,
          0, 0, 0, 0, 0, 0, 0
        };
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterTouchedPixelsExactTraversal() throws FactoryException, ParseException {
    // A boundary edge that clips a pixel over a chord shorter than one pixel
    // must still burn that pixel under allTouched, matching GDAL. Fixed-step
    // sampling of the edge misses such pixels; exact cell traversal does not.
    // Square unit pixels, so this is independent of the non-square-pixel
    // selection path. Expected matrices are from GDAL
    // (rasterio.features.rasterize, all_touched=True).
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 6, 6, 0, 6, 1, -1, 0, 0, 0);

    Geometry polygon =
        Constructors.geomFromWKT("POLYGON ((3.7 1.28, 0.92 5.23, 4.26 4.15, 3.7 1.28))", 0);
    GridCoverage2D rasterized =
        RasterConstructors.asRaster(polygon, raster, "d", true, 1d, 0d, false);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          1, 1, 0, 0, 0, 0,
          0, 1, 1, 1, 1, 0,
          0, 1, 1, 1, 1, 0,
          0, 0, 1, 1, 1, 0,
          0, 0, 0, 1, 0, 0,
          0, 0, 0, 0, 0, 0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // A LineString goes through the same segment code regardless of allTouched
    // (a line has no interior); every pixel it crosses must be burned.
    Geometry line = Constructors.geomFromWKT("LINESTRING (3.97 1.57, 0.31 3.24)", 0);
    rasterized = RasterConstructors.asRaster(line, raster, "d", false, 1d, 0d, false);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0,
          1, 0, 0, 0, 0, 0,
          1, 1, 1, 1, 0, 0,
          0, 0, 0, 1, 0, 0,
          0, 0, 0, 0, 0, 0
        };
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterLingString() throws FactoryException, ParseException {
    // Horizontal LineString
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -1, 2, -2, 0, 0, 4326);
    GridCoverage2D raster_bottom_up =
        RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -511, 2, 2, 0, 0, 4326);

    Geometry geom = Constructors.geomFromEWKT("LINESTRING(1 -1, 2 -1, 10 -1)");
    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 3093151, 0d);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected = new double[] {3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0};
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 3093151, 0d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    assertArrayEquals(expected, actual, 0.1d);

    // Vertical LineString
    geom = Constructors.geomFromEWKT("LINESTRING(1 -1, 1 -2, 1 -10)");
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 3093151, 0d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected = new double[] {3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0};
    assertArrayEquals(expected, actual, 0.1d);

    // Test bottom-up raster case
    rasterized = RasterConstructors.asRaster(geom, raster_bottom_up, "d", false, 3093151, 0d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterWithRaster() throws IOException, ParseException, FactoryException {
    // Polygon
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    Geometry geom =
        Constructors.geomFromWKT("POLYGON((1.5 1.5, 3.8 3.0, 4.5 4.4, 3.4 3.5, 1.5 1.5))", 0);
    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", true, 612028, 5d);
    double[] actual = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).toArray();
    // Matches GDAL/rasterio all_touched=True except where a vertex lands exactly on a grid line:
    // there the geometry touches a cell only at a corner or edge and Sedona burns it (a superset
    // consistent with "all pixels touched"), while GDAL omits it. Tracked separately; see the PR.
    double[] expected = {
      5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 612028.0, 612028.0, 5.0, 5.0, 5.0,
      5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      5.0, 5.0, 5.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      5.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      612028.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      612028.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      612028.0, 612028.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      612028.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      612028.0, 612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0,
      612028.0, 612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 612028.0,
      612028.0, 612028.0, 612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 612028.0,
      612028.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 612028.0, 612028.0, 5.0, 5.0,
      5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0
    };
    assertArrayEquals(expected, actual, 0.1d);

    // Omitting the noDataValue would inherit test5.tiff's band nodata value; pass an explicit null
    // to keep this case's original intent of no nodata value and a zero background.
    rasterized = RasterConstructors.asRaster(geom, raster, "d", false, 5484, null);
    actual = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).toArray();
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0,
          5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterWithRasterExtent() throws IOException, ParseException, FactoryException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON((1.5 1.5, 3.8 3.0, 4.5 4.4, 3.4 3.5, 1.5 1.5))", RasterAccessors.srid(raster));
    GridCoverage2D rasterized =
        RasterConstructors.asRasterWithRasterExtent(geom, raster, "d", false, 612028, 5d);

    int widthActual = RasterAccessors.getWidth(rasterized);
    int widthExpected = RasterAccessors.getWidth(raster);
    assertEquals(widthExpected, widthActual);

    int heightActual = RasterAccessors.getHeight(rasterized);
    int heightExpected = RasterAccessors.getHeight(raster);
    assertEquals(heightExpected, heightActual);
  }

  @Test
  public void testAsRasterWithRasterExtent2() throws FactoryException, ParseException {
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0.5, 0.1, -0.1, 0, 0, 4326);
    GridCoverage2D raster_bottom_up =
        RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0.0, 0.1, 0.1, 0, 0, 4326);

    Geometry geom =
        Constructors.geomFromWKT("POLYGON((0.1 0.1, 0.1 0.4, 0.4 0.4, 0.4 0.1, 0.1 0.1))", 0);
    GridCoverage2D rasterized =
        RasterConstructors.asRasterWithRasterExtent(geom, raster, "d", false, 100d, 0d);
    assertEquals(0, rasterized.getEnvelope2D().getMinX(), 1e-6);
    assertEquals(0, rasterized.getEnvelope2D().getMinY(), 1e-6);
    assertEquals(0.5, rasterized.getEnvelope2D().getWidth(), 1e-6);
    assertEquals(0.5, rasterized.getEnvelope2D().getHeight(), 1e-6);
    assertEquals(5, RasterAccessors.getWidth(rasterized));
    assertEquals(5, RasterAccessors.getHeight(rasterized));
    double sum = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).sum();
    assertEquals(900, sum, 1e-6); // Covers 3x3 grid

    // Test bottom-up raster case
    rasterized =
        RasterConstructors.asRasterWithRasterExtent(geom, raster_bottom_up, "d", false, 100d, 0d);
    assertEquals(0, rasterized.getEnvelope2D().getMinX(), 1e-6);
    assertEquals(0, rasterized.getEnvelope2D().getMinY(), 1e-6);
    assertEquals(0.5, rasterized.getEnvelope2D().getWidth(), 1e-6);
    assertEquals(0.5, rasterized.getEnvelope2D().getHeight(), 1e-6);
    assertEquals(5, RasterAccessors.getWidth(rasterized));
    assertEquals(5, RasterAccessors.getHeight(rasterized));
    sum = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).sum();
    assertEquals(900, sum, 1e-6); // Covers 3x3 grid
  }

  @Test
  public void makeEmptyRaster() throws FactoryException {
    double upperLeftX = 0;
    double upperLeftY = 0;
    int widthInPixel = 1;
    int heightInPixel = 2;
    double pixelSize = 2;
    int numBands = 1;
    String dataType = "I";

    GridCoverage2D gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
    Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
    assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
    assertEquals(
        upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
    assertEquals(
        upperLeftY - heightInPixel * pixelSize, envelope.getEnvelopeInternal().getMinY(), 0.001);
    assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
    assertEquals(
        "REAL_64BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

    gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
    envelope = GeometryFunctions.envelope(gridCoverage2D);
    assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
    assertEquals(
        upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
    assertEquals(
        upperLeftY - heightInPixel * pixelSize, envelope.getEnvelopeInternal().getMinY(), 0.001);
    assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
    assertEquals(
        "SIGNED_32BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

    assertEquals("POLYGON ((0 -4, 0 0, 2 0, 2 -4, 0 -4))", envelope.toString());
    double expectedWidthInDegree = pixelSize * widthInPixel;
    double expectedHeightInDegree = pixelSize * heightInPixel;

    assertEquals(expectedWidthInDegree * expectedHeightInDegree, envelope.getArea(), 0.001);
    assertEquals(heightInPixel, gridCoverage2D.getRenderedImage().getTileHeight());
    assertEquals(widthInPixel, gridCoverage2D.getRenderedImage().getTileWidth());
    assertEquals(
        0d, gridCoverage2D.getRenderedImage().getData().getPixel(0, 0, (double[]) null)[0], 0.001);
    assertEquals(1, gridCoverage2D.getNumSampleDimensions());

    gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands,
            widthInPixel,
            heightInPixel,
            upperLeftX,
            upperLeftY,
            pixelSize,
            -pixelSize - 1,
            0,
            0,
            0);
    envelope = GeometryFunctions.envelope(gridCoverage2D);
    assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
    assertEquals(
        upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
    assertEquals(
        upperLeftY - heightInPixel * (pixelSize + 1),
        envelope.getEnvelopeInternal().getMinY(),
        0.001);
    assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
    assertEquals(
        "REAL_64BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

    gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands,
            dataType,
            widthInPixel,
            heightInPixel,
            upperLeftX,
            upperLeftY,
            pixelSize,
            -pixelSize - 1,
            0,
            0,
            0);
    envelope = GeometryFunctions.envelope(gridCoverage2D);
    assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
    assertEquals(
        upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
    assertEquals(
        upperLeftY - heightInPixel * (pixelSize + 1),
        envelope.getEnvelopeInternal().getMinY(),
        0.001);
    assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
    assertEquals(
        "SIGNED_32BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());
  }

  @Test
  public void testMakeNonEmptyRaster() {
    double[] bandData = new double[10000];
    for (int i = 0; i < bandData.length; i++) {
      bandData[i] = i;
    }
    GridCoverage2D ref =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 100, 100, 100, 80, 10, -10, 0, 0, 4326, new double[][] {bandData});

    // Test with empty band data
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> RasterConstructors.makeNonEmptyRaster(ref, "D", new double[0]));

    // Test with single band
    double[] values = new double[10000];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * i;
    }
    GridCoverage2D raster = RasterConstructors.makeNonEmptyRaster(ref, "D", values);
    assertEquals(100, raster.getRenderedImage().getWidth());
    assertEquals(100, raster.getRenderedImage().getHeight());
    assertEquals(1, raster.getNumSampleDimensions());
    assertEquals(ref.getGridGeometry(), raster.getGridGeometry());
    assertEquals(ref.getCoordinateReferenceSystem(), raster.getCoordinateReferenceSystem());
    Raster r = RasterUtils.getRaster(raster.getRenderedImage());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], r.getSampleDouble(i % 100, i / 100, 0), 0.001);
    }

    // Test with multi band
    values = new double[20000];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * i;
    }
    raster = RasterConstructors.makeNonEmptyRaster(ref, "D", values);
    assertEquals(100, raster.getRenderedImage().getWidth());
    assertEquals(100, raster.getRenderedImage().getHeight());
    assertEquals(2, raster.getNumSampleDimensions());
    assertEquals(
        SampleDimensionType.REAL_64BITS, raster.getSampleDimension(0).getSampleDimensionType());
    assertEquals(ref.getGridGeometry(), raster.getGridGeometry());
    assertEquals(ref.getCoordinateReferenceSystem(), raster.getCoordinateReferenceSystem());
    r = RasterUtils.getRaster(raster.getRenderedImage());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], r.getSampleDouble(i % 100, (i / 100) % 100, i / 10000), 0.001);
    }

    // Test with integer data type
    values = new double[10000];
    for (int i = 0; i < values.length; i++) {
      values[i] = 10.0 + i;
    }
    raster = RasterConstructors.makeNonEmptyRaster(ref, "US", values);
    assertEquals(
        SampleDimensionType.UNSIGNED_16BITS, raster.getSampleDimension(0).getSampleDimensionType());
    r = RasterUtils.getRaster(raster.getRenderedImage());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], r.getSampleDouble(i % 100, i / 100, 0), 0.001);
    }
  }

  @Test
  public void testInDbTileWithoutPadding() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, null, 10, 10, false, Double.NaN));
    assertTilesSameWithGridCoverage(tiles, raster, null, 10, 10, Double.NaN);
  }

  @Test
  public void testInDbTileWithoutPadding2() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, null, 9, 9, false, Double.NaN));
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, Double.NaN);
  }

  @Test
  public void testInDbTileWithPadding() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 2, "EPSG:3857");
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, null, 9, 9, true, 100));
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 100);
  }

  @Test
  public void testInDbTileWithBandSelector() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 2, "EPSG:3857");
    int[] bandIndices = {2};
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, bandIndices, 9, 9, true, 100));
    assertTilesSameWithGridCoverage(tiles, raster, bandIndices, 9, 9, 100);
  }

  @Test
  public void testInDbTileWithBandSelector2() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 4, "EPSG:3857");
    int[] bandIndices = {3, 1};
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, bandIndices, 8, 7, true, 100));
    assertTilesSameWithGridCoverage(tiles, raster, bandIndices, 8, 7, 100);
  }

  @Test
  public void testInDbTileInheritSourceNoDataValue() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    raster = MapAlgebra.addBandFromArray(raster, MapAlgebra.bandAsArray(raster, 1), 1, 13.0);
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, null, 9, 9, true, Double.NaN));
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 13);
  }

  @Test
  public void testInDbTileOverrideSourceNoDataValue() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    raster = MapAlgebra.addBandFromArray(raster, MapAlgebra.bandAsArray(raster, 1), 1, 13.0);
    TileGenerator.Tile[] tiles =
        collectTiles(RasterConstructors.generateTiles(raster, null, 9, 9, true, 42));
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 42);
  }

  private TileGenerator.Tile[] collectTiles(TileGenerator.TileIterator iter) {
    java.util.List<TileGenerator.Tile> list = new java.util.ArrayList<>();
    while (iter.hasNext()) {
      list.add(iter.next());
    }
    return list.toArray(new TileGenerator.Tile[0]);
  }

  private void assertTilesSameWithGridCoverage(
      TileGenerator.Tile[] tiles,
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      double noDataValue) {
    RenderedImage image = gridCoverage2D.getRenderedImage();
    int width = image.getWidth();
    int height = image.getHeight();
    int numTilesX = (int) Math.ceil((double) width / tileWidth);
    int numTilesY = (int) Math.ceil((double) height / tileHeight);
    Assert.assertEquals(numTilesX * numTilesY, tiles.length);

    // For each tile, select a few random points, and do the following checks
    // 1. The pixel at the point is the same as the corresponding pixel in the grid coverage
    // 2. The pixel at the point translates to the same world coordinate as the corresponding pixel
    // in the grid
    //    coverage
    Set<Pair<Integer, Integer>> visitedTiles = new HashSet<>();
    for (TileGenerator.Tile tile : tiles) {
      int tileX = tile.getTileX();
      int tileY = tile.getTileY();
      Pair<Integer, Integer> tilePosition = Pair.of(tileX, tileY);
      Assert.assertFalse(visitedTiles.contains(tilePosition));
      visitedTiles.add(tilePosition);

      int offsetX = tileX * tileWidth;
      int offsetY = tileY * tileHeight;
      for (int i = 0; i < 10; i++) {
        GridCoverage2D tileRaster = tile.getCoverage();
        RenderedImage tileImage = tileRaster.getRenderedImage();
        int currentTileWidth = tileImage.getWidth();
        int currentTileHeight = tileImage.getHeight();
        Assert.assertTrue(currentTileWidth <= tileWidth);
        Assert.assertTrue(currentTileHeight <= tileHeight);
        if (currentTileWidth < tileWidth || currentTileHeight < tileHeight) {
          Assert.assertTrue((tileX == numTilesX - 1) || (tileY == numTilesY - 1));
        }

        int x = (int) (Math.random() * currentTileWidth);
        int y = (int) (Math.random() * currentTileHeight);

        // Check that the pixel at the point is the same as the corresponding pixel in the grid
        // coverage
        GridCoordinates2D tileGridCoord = new GridCoordinates2D(x, y);
        GridCoordinates2D gridCoord = new GridCoordinates2D(offsetX + x, offsetY + y);
        float[] values = tileRaster.evaluate(tileGridCoord, (float[]) null);
        if (offsetX + x < width && offsetY + y < height) {
          float[] expectedValues = gridCoverage2D.evaluate(gridCoord, (float[]) null);
          if (bandIndices == null) {
            assertArrayEquals(expectedValues, values, 1e-6f);
          } else {
            Assert.assertEquals(bandIndices.length, values.length);
            for (int j = 0; j < bandIndices.length; j++) {
              Assert.assertEquals(expectedValues[bandIndices[j] - 1], values[j], 1e-6f);
            }
          }

          // Check that the pixel at the point translates to the same world coordinate as the
          // corresponding
          // pixel in the grid coverage
          try {
            Position actualWorldCoord = tileRaster.getGridGeometry().gridToWorld(tileGridCoord);
            Position expectedWorldCoord = gridCoverage2D.getGridGeometry().gridToWorld(gridCoord);
            Assert.assertEquals(expectedWorldCoord, actualWorldCoord);
          } catch (TransformException e) {
            throw new RuntimeException(e);
          }
        } else {
          // Padded pixel
          for (int k = 0; k < values.length; k++) {
            double tileNoDataValue = RasterUtils.getNoDataValue(tileRaster.getSampleDimension(k));
            Assert.assertEquals(noDataValue, tileNoDataValue, 1e-6f);
            Assert.assertEquals(noDataValue, values[k], 1e-6f);
          }
        }
      }
    }
  }

  @Test
  public void testNetCdfClassic() throws FactoryException, IOException, TransformException {
    GridCoverage2D testRaster = RasterConstructors.fromNetCDF(testNc, "O3");
    double[] expectedMetadata = {4.9375, 50.9375, 80, 48, 0.125, -0.125, 0, 0, 0, 4};
    double[] actualMetadata = RasterAccessors.metadata(testRaster);
    for (int i = 0; i < expectedMetadata.length; i++) {
      assertEquals(expectedMetadata[i], actualMetadata[i], 1e-5);
    }

    double actualFirstGridVal = PixelFunctions.value(testRaster, 0, 0, 1);
    double expectedFirstGridVal = 60.95357131958008;
    assertEquals(expectedFirstGridVal, actualFirstGridVal, 1e-6);
  }

  @Test
  public void testNetCdfClassicLongForm() throws FactoryException, IOException, TransformException {
    GridCoverage2D testRaster = RasterConstructors.fromNetCDF(testNc, "O3", "lon", "lat");
    double[] expectedMetadata = {4.9375, 50.9375, 80, 48, 0.125, -0.125, 0, 0, 0, 4};
    double[] actualMetadata = RasterAccessors.metadata(testRaster);
    for (int i = 0; i < expectedMetadata.length; i++) {
      assertEquals(expectedMetadata[i], actualMetadata[i], 1e-5);
    }

    double actualFirstGridVal = PixelFunctions.value(testRaster, 0, 0, 1);
    double expectedFirstGridVal = 60.95357131958008;
    assertEquals(expectedFirstGridVal, actualFirstGridVal, 1e-6);
  }

  @Test
  public void testNetCdfPackedBandMetadata() throws FactoryException, IOException {
    byte[] bytes =
        Files.readAllBytes(
            new File(resourceFolder + "raster/netcdf_variants/test_reader_permuted.nc").toPath());
    GridCoverage2D raster = RasterConstructors.fromNetCDF(bytes, "temp", "lon", "lat");

    assertEquals(Arrays.asList("time : 100.0"), raster.getProperties().get("BAND_1"));
    assertEquals(Arrays.asList("time : 160.0"), raster.getProperties().get("BAND_2"));
  }

  @Test
  public void testNetCdfPackedMissingValues() throws FactoryException, IOException {
    byte[] bytes =
        Files.readAllBytes(
            new File(resourceFolder + "raster/netcdf_variants/test_packed.nc").toPath());
    GridCoverage2D raster = RasterConstructors.fromNetCDF(bytes, "temp", "lon", "lat");
    double[] values = MapAlgebra.bandAsArray(raster, 1);

    Double noDataValue = RasterBandAccessors.getBandNoDataValue(raster, 1);
    Assert.assertNotNull(noDataValue);
    Assert.assertTrue(Double.isFinite(noDataValue));
    assertEquals(noDataValue, values[1], 0.0);
    assertEquals(noDataValue, values[4], 0.0);
    assertEquals(-999.0, values[7], 1e-6);
    assertEquals(7, RasterBandAccessors.getCount(raster, 1, true));
  }

  @Test
  public void testNetCdfInt64ValidityIsComparedExactly() throws FactoryException, IOException {
    byte[] bytes =
        Files.readAllBytes(
            new File(resourceFolder + "raster/netcdf_variants/test_int64_validity.nc4").toPath());

    for (String variable : Arrays.asList("signed_temp", "unsigned_temp")) {
      GridCoverage2D raster = RasterConstructors.fromNetCDF(bytes, variable, "lon", "lat");
      Double noDataValue = RasterBandAccessors.getBandNoDataValue(raster, 1);
      Assert.assertNotNull(noDataValue);
      Assert.assertTrue(Double.isFinite(noDataValue));
      assertEquals(3, RasterBandAccessors.getCount(raster, 1, true));

      double[] values = MapAlgebra.bandAsArray(raster, 1);
      assertEquals(noDataValue, values[2], 0.0);
      Assert.assertNotEquals(noDataValue, values[0]);
      Assert.assertNotEquals(noDataValue, values[1]);
      Assert.assertNotEquals(noDataValue, values[3]);
    }
  }

  @Test
  public void testRecordInfo() throws IOException {
    String actualRecordInfo = RasterConstructors.getRecordInfo(testNc);
    String expectedRecordInfo =
        "O3(time=2, z=2, lat=48, lon=80)\n" + "\n" + "NO2(time=2, z=2, lat=48, lon=80)";
    assertEquals(expectedRecordInfo, actualRecordInfo);
  }
}
