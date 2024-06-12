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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.coverage.SampleDimensionType;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

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
    // Polygon
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -1, 2, -2, 0, 0, 4326);
    Geometry geom = Constructors.geomFromWKT("POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))", 0);
    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected =
        new double[] {
          3093151.0, 3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0,
          3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d");
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // MultiPolygon
    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((15 15, 1.5 5.5, 3.5 5.5, 3.5 1.5, 15 15)), ((4.4 2.4, 4.4 6.4, 6.4 6.4, 6.4 2.4, 4.4 2.4)))",
            0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 3093151.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d");
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // MultiLineString
    geom = Constructors.geomFromWKT("MULTILINESTRING ((5 5, 10 10), (10 10, 15 15, 20 20))", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d");
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // LinearRing
    geom = Constructors.geomFromWKT("LINEARRING (10 10, 18 20, 15 24, 24 25, 10 10)", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0,
          3093151.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0,
          3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d");
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0,
          1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // MultiPoints
    geom = Constructors.geomFromWKT("MULTIPOINT ((5 5), (10 10), (15 15))", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d");
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0
        };
    assertArrayEquals(expected, actual, 0.1d);

    // Point
    geom = Constructors.geomFromWKT("POINT (5 5)", 0);
    rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected = new double[] {3093151.0};
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d");
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected = new double[] {1.0};
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterLingString() throws FactoryException, ParseException {
    // Horizontal LineString
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -1, 2, -2, 0, 0, 4326);
    Geometry geom = Constructors.geomFromEWKT("LINESTRING(1 1, 2 1, 10 1)");
    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 0d);
    double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
    double[] expected = new double[] {3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0};
    assertArrayEquals(expected, actual, 0.1d);

    // Vertical LineString
    geom = Constructors.geomFromEWKT("LINESTRING(1 1, 1 2, 1 10)");
    rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 0d);
    actual = MapAlgebra.bandAsArray(rasterized, 1);
    expected = new double[] {3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0};
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsRasterWithRaster() throws IOException, ParseException, FactoryException {
    // Polygon
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    Geometry geom =
        Constructors.geomFromWKT("POLYGON((1.5 1.5, 3.8 3.0, 4.5 4.4, 3.4 3.5, 1.5 1.5))", 0);
    GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", 612028, 5d);
    double[] actual = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).toArray();
    double[] expected = {
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 612028.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0,
      612028.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 612028.0,
      612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    };
    assertArrayEquals(expected, actual, 0.1d);

    rasterized = RasterConstructors.asRaster(geom, raster, "d", 5484);
    actual = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).toArray();
    expected =
        new double[] {
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0,
          5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          5484.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0,
          5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
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
        RasterConstructors.asRasterWithRasterExtent(geom, raster, "d", 612028, 5d);

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
    Geometry geom =
        Constructors.geomFromWKT("POLYGON((0.1 0.1, 0.1 0.4, 0.4 0.4, 0.4 0.1, 0.1 0.1))", 0);
    GridCoverage2D rasterized =
        RasterConstructors.asRasterWithRasterExtent(geom, raster, "d", 100d, 0d);
    assertEquals(0, rasterized.getEnvelope2D().x, 1e-6);
    assertEquals(0, rasterized.getEnvelope2D().y, 1e-6);
    assertEquals(0.5, rasterized.getEnvelope2D().width, 1e-6);
    assertEquals(0.5, rasterized.getEnvelope2D().height, 1e-6);
    assertEquals(5, RasterAccessors.getWidth(rasterized));
    assertEquals(5, RasterAccessors.getHeight(rasterized));
    double sum = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).sum();
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
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, null, 10, 10, false, Double.NaN);
    assertTilesSameWithGridCoverage(tiles, raster, null, 10, 10, Double.NaN);
  }

  @Test
  public void testInDbTileWithoutPadding2() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, null, 9, 9, false, Double.NaN);
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, Double.NaN);
  }

  @Test
  public void testInDbTileWithPadding() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 2, "EPSG:3857");
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, null, 9, 9, true, 100);
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 100);
  }

  @Test
  public void testInDbTileWithBandSelector() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 2, "EPSG:3857");
    int[] bandIndices = {2};
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, bandIndices, 9, 9, true, 100);
    assertTilesSameWithGridCoverage(tiles, raster, bandIndices, 9, 9, 100);
  }

  @Test
  public void testInDbTileWithBandSelector2() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 4, "EPSG:3857");
    int[] bandIndices = {3, 1};
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, bandIndices, 8, 7, true, 100);
    assertTilesSameWithGridCoverage(tiles, raster, bandIndices, 8, 7, 100);
  }

  @Test
  public void testInDbTileInheritSourceNoDataValue() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    raster = MapAlgebra.addBandFromArray(raster, MapAlgebra.bandAsArray(raster, 1), 1, 13.0);
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, null, 9, 9, true, Double.NaN);
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 13);
  }

  @Test
  public void testInDbTileOverrideSourceNoDataValue() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 1000, 1010, 10, 1, "EPSG:3857");
    raster = MapAlgebra.addBandFromArray(raster, MapAlgebra.bandAsArray(raster, 1), 1, 13.0);
    RasterConstructors.Tile[] tiles =
        RasterConstructors.generateTiles(raster, null, 9, 9, true, 42);
    assertTilesSameWithGridCoverage(tiles, raster, null, 9, 9, 42);
  }

  private void assertTilesSameWithGridCoverage(
      RasterConstructors.Tile[] tiles,
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
    for (RasterConstructors.Tile tile : tiles) {
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
            Assert.assertArrayEquals(expectedValues, values, 1e-6f);
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
            DirectPosition actualWorldCoord =
                tileRaster.getGridGeometry().gridToWorld(tileGridCoord);
            DirectPosition expectedWorldCoord =
                gridCoverage2D.getGridGeometry().gridToWorld(gridCoord);
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
  public void testRecordInfo() throws IOException {
    String actualRecordInfo = RasterConstructors.getRecordInfo(testNc);
    String expectedRecordInfo =
        "O3(time=2, z=2, lat=48, lon=80)\n" + "\n" + "NO2(time=2, z=2, lat=48, lon=80)";
    assertEquals(expectedRecordInfo, actualRecordInfo);
  }
}
