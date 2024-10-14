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

import static org.junit.Assert.*;

import java.io.IOException;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class RasterAccessorsTest extends RasterTestBase {
  @Test
  public void testNumBands() {
    assertEquals(1, RasterAccessors.numBands(oneBandRaster));
    assertEquals(4, RasterAccessors.numBands(multiBandRaster));
  }

  @Test
  public void testWidthAndHeight() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 10, 20, 0, 0, 8);
    assertEquals(20, RasterAccessors.getHeight(emptyRaster));
    assertEquals(10, RasterAccessors.getWidth(emptyRaster));
  }

  @Test
  public void testWidthAndHeightFromRasterFile() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    assertEquals(512, RasterAccessors.getWidth(raster));
    assertEquals(517, RasterAccessors.getHeight(raster));
  }

  @Test
  public void testSrid() throws FactoryException {
    assertEquals(0, RasterAccessors.srid(oneBandRaster));
    assertEquals(4326, RasterAccessors.srid(multiBandRaster));
  }

  @Test
  public void testRotation() throws IOException, FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, 10, 15, 1, 2, 1, -2, 10, 10, 0);
    double actual = RasterAccessors.getRotation(emptyRaster);
    double expected = -1.4711276743037347;
    assertEquals(expected, actual, 1e-9);
  }

  @Test
  public void testGeoTransform() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 10, 15, 1, 2, 1, -1, 10, 10, 0);
    double[] actual = RasterAccessors.getGeoTransform(emptyRaster);
    double[] expected =
        new double[] {
          10.04987562112089, 10.04987562112089, -1.4711276743037347, -1.5707963267948966, 1.0, 2.0
        };
    assertArrayEquals(expected, actual, 1e-9);
  }

  @Test
  public void testUpperLeftX() throws FactoryException {
    GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(1, 3, 4, 1, 2, 5);
    double upperLeftX = RasterAccessors.getUpperLeftX(gridCoverage2D);
    assertEquals(1, upperLeftX, 0.1d);

    gridCoverage2D = RasterConstructors.makeEmptyRaster(10, 7, 8, 5, 6, 9);
    upperLeftX = RasterAccessors.getUpperLeftX(gridCoverage2D);
    assertEquals(5, upperLeftX, 0.1d);
  }

  @Test
  public void testUpperLeftY() throws FactoryException {
    GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(1, 3, 4, 1, 2, 5);
    double upperLeftY = RasterAccessors.getUpperLeftY(gridCoverage2D);
    assertEquals(2, upperLeftY, 0.1d);

    gridCoverage2D = RasterConstructors.makeEmptyRaster(10, 7, 8, 5, 6, 9);
    upperLeftY = RasterAccessors.getUpperLeftY(gridCoverage2D);
    assertEquals(6, upperLeftY, 0.1d);
  }

  @Test
  public void testScaleX() throws UnsupportedOperationException, FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, 10, 15, 0, 0, 1, -2, 0, 0, 0);
    assertEquals(1, RasterAccessors.getScaleX(emptyRaster), 1e-9);
  }

  @Test
  public void testScaleY() throws UnsupportedOperationException, FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, 10, 15, 0, 0, 1, -2, 0, 0, 0);
    assertEquals(-2, RasterAccessors.getScaleY(emptyRaster), 1e-9);
  }

  @Test
  public void testWorldCoordX() throws FactoryException, TransformException {
    int colX = 1, rowY = 1;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326);
    double actualX = RasterAccessors.getWorldCoordX(emptyRaster, colX, rowY);
    double expectedX = -123;
    assertEquals(expectedX, actualX, 0.1d);
    colX = 2;
    expectedX = -118;
    actualX = RasterAccessors.getWorldCoordX(emptyRaster, colX, rowY);
    assertEquals(expectedX, actualX, 0.1d);
  }

  @Test
  public void testWorldCoordXOutOfBounds() throws FactoryException, TransformException {
    int colX = 6;
    int rowY = 5;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326);
    double actualX = RasterAccessors.getWorldCoordX(emptyRaster, colX, rowY);
    double expectedX = -98;
    assertEquals(expectedX, actualX, 0.1d);
  }

  @Test
  public void testWorldCoordY() throws FactoryException, TransformException {
    int colX = 1, rowY = 1;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326);
    double actualY = RasterAccessors.getWorldCoordY(emptyRaster, colX, rowY);
    double expectedY = 54;
    assertEquals(expectedY, actualY, 0.1d);
    rowY = 2;
    expectedY = 44;
    actualY = RasterAccessors.getWorldCoordY(emptyRaster, colX, rowY);
    assertEquals(expectedY, actualY, 0.1d);
  }

  @Test
  public void testWorldCoordYOutOfBounds() throws FactoryException, TransformException {
    int colX = 4;
    int rowY = 11;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326);
    double actualY = RasterAccessors.getWorldCoordY(emptyRaster, colX, rowY);
    double expectedY = -46;
    assertEquals(expectedY, actualY, 0.1d);
  }

  @Test
  public void testWorldCoord() throws FactoryException, TransformException {
    int colX = 1, rowY = 1;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326);

    Coordinate actual = RasterAccessors.getWorldCoord(emptyRaster, colX, rowY).getCoordinate();
    double expectedX = -123, expectedY = 54;

    assertEquals(expectedX, actual.getX(), 0.1d);
    assertEquals(expectedY, actual.getY(), 0.1d);

    rowY = 2;
    actual = RasterAccessors.getWorldCoord(emptyRaster, colX, rowY).getCoordinate();
    expectedY = 44;

    assertEquals(expectedX, actual.getX(), 0.1d);
    assertEquals(expectedY, actual.getY(), 0.1d);
  }

  @Test
  public void testWorldCoordOutOfBounds() throws FactoryException, TransformException {
    int colX = 4;
    int rowY = 11;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, 4326);

    double expectedX = -108, expectedY = -46;
    Coordinate actual = RasterAccessors.getWorldCoord(emptyRaster, colX, rowY).getCoordinate();

    assertEquals(expectedX, actual.getX(), 0.1d);
    assertEquals(expectedY, actual.getY(), 0.1d);
  }

  @Test
  public void testGridCoordLatLon() throws TransformException, FactoryException {
    double longitude = -123, latitude = 54;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -123, 54, 1, -1, 0, 0, 4326);
    Coordinate coords =
        RasterAccessors.getGridCoord(emptyRaster, longitude, latitude).getCoordinate();
    assertEquals(1, coords.getX(), 1e-9);
    assertEquals(1, coords.getY(), 1e-9);

    longitude = -118;
    latitude = 52;
    coords = RasterAccessors.getGridCoord(emptyRaster, longitude, latitude).getCoordinate();
    assertEquals(6, coords.getX(), 1e-9);
    assertEquals(3, coords.getY(), 1e-9);
  }

  @Test
  public void testGridCoordPointSameSRID() throws TransformException, FactoryException {
    double longitude = -123, latitude = 54;
    int srid = 4326;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -123, 54, 1, -1, 0, 0, srid);
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    Geometry point = geometryFactory.createPoint(new Coordinate(longitude, latitude));
    Coordinate coords = RasterAccessors.getGridCoord(emptyRaster, point).getCoordinate();
    assertEquals(1, coords.getX(), 1e-9);
    assertEquals(1, coords.getY(), 1e-9);
  }

  @Test
  public void testGridCoordPointDifferentSRID() throws FactoryException, TransformException {
    double longitude = -47, latitude = 51;
    int srid = 4326;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, srid);
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    Geometry point = geometryFactory.createPoint(new Coordinate(longitude, latitude));
    point =
        JTS.transform(
            point,
            CRS.findMathTransform(
                emptyRaster.getCoordinateReferenceSystem(), CRS.decode("EPSG:3857", true)));
    point.setSRID(3857);
    Coordinate coords = RasterAccessors.getGridCoord(emptyRaster, point).getCoordinate();
    assertEquals(7, coords.getX(), 1e-9);
    assertEquals(1, coords.getY(), 1e-9);
  }

  @Test
  public void testGridCoordPointIllegalGeom() throws FactoryException {
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry polygon = geometryFactory.toGeometry(new Envelope(5, 10, 5, 10));
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, 4326);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RasterAccessors.getGridCoord(emptyRaster, polygon));
    String expectedMessage = "Only point geometries are expected as real world coordinates";
    assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void testGridCoordXLonLat() throws FactoryException, TransformException {
    double longitude = -47, latitude = 51;
    int srid = 4326;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, srid);
    int expectedX = 7;
    int actualX = RasterAccessors.getGridCoordX(emptyRaster, longitude, latitude);
    assertEquals(expectedX, actualX);
  }

  @Test
  public void testSkewX() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(10, 2, 4, 6, 4, 1, 1, 2, 7, 0);
    assertEquals(2, RasterAccessors.getSkewX(emptyRaster), 0.1d);

    emptyRaster = RasterConstructors.makeEmptyRaster(1, 3, 4, 100.0, 200.0, 2.0, -3.0, 0.1, 0.2, 0);
    assertEquals(0.1, RasterAccessors.getSkewX(emptyRaster), 0.01d);

    emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 5);
    assertEquals(0, RasterAccessors.getSkewX(emptyRaster), 0.1d);
  }

  @Test
  public void testSkewY() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(10, 2, 4, 6, 4, 1, 1, 2, 7, 0);
    assertEquals(7, RasterAccessors.getSkewY(emptyRaster), 0.1d);

    emptyRaster = RasterConstructors.makeEmptyRaster(1, 3, 4, 100.0, 200.0, 2.0, -3.0, 0.1, 0.2, 0);
    assertEquals(0.2, RasterAccessors.getSkewY(emptyRaster), 0.01d);

    emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 5);
    assertEquals(0, RasterAccessors.getSkewY(emptyRaster), 0.1d);
  }

  @Test
  public void testGeoReference() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 100, 100, -53, 51, 2, -2, 4, 5, 4326);
    String actual = RasterAccessors.getGeoReference(emptyRaster);
    String expected = "2.000000 \n5.000000 \n4.000000 \n-2.000000 \n-53.000000 \n51.000000";
    assertEquals(actual, expected);

    emptyRaster = RasterConstructors.makeEmptyRaster(1, 3, 4, 100.0, 200.0, 2.0, -3.0, 0.1, 0.2, 0);
    actual = RasterAccessors.getGeoReference(emptyRaster, "GDAL");
    expected = "2.000000 \n0.200000 \n0.100000 \n-3.000000 \n100.000000 \n200.000000";
    assertEquals(expected, actual);

    emptyRaster = RasterConstructors.makeEmptyRaster(1, 3, 4, 100.0, 200.0, 2.0, -3.0, 0.1, 0.2, 0);
    actual = RasterAccessors.getGeoReference(emptyRaster, "ESRI");
    expected = "2.000000 \n0.200000 \n0.100000 \n-3.000000 \n101.000000 \n198.500000";
    assertEquals(expected, actual);
  }

  @Test
  public void testGridCoordXGeomSameSRID() throws FactoryException, TransformException {
    double longitude = -53, latitude = 51;
    int srid = 4326;
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    Geometry point = geometryFactory.createPoint(new Coordinate(longitude, latitude));
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, srid);
    int expectedX = 1;
    int actualX = RasterAccessors.getGridCoordX(emptyRaster, point);
    assertEquals(expectedX, actualX);
  }

  @Test
  public void testGridCoordSkewedRaster() throws FactoryException, TransformException {
    double longitude = -30, latitude = 69;
    GridCoverage2D skewedRaster =
        RasterConstructors.makeEmptyRaster(1, 100, 100, -53, 51, 2, -2, 4, 5, 4326);
    Coordinate point =
        RasterAccessors.getGridCoord(skewedRaster, longitude, latitude).getCoordinate();
    assertEquals(5, point.getX(), 1e-9);
    assertEquals(4, point.getY(), 1e-9);
  }

  @Test
  public void testGridCoordFromRasterCoord() throws FactoryException, TransformException {
    int x = 1, y = 1;
    GridCoordinates2D gridCoordinates2D = new GridCoordinates2D(x, y);
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 5, -53, 51, 1, -1, 0, 0, 4326);
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    Geometry point =
        geometryFactory.createPoint(
            new Coordinate(
                RasterAccessors.getWorldCoordX(emptyRaster, x, y),
                RasterAccessors.getWorldCoordY(emptyRaster, x, y)));
    Coordinate gridCoords = RasterAccessors.getGridCoord(emptyRaster, point).getCoordinate();
    assertEquals(x, gridCoords.getX(), 1e-9);
    assertEquals(y, gridCoords.getY(), 1e-9);
  }

  @Test
  public void testMetaData() throws FactoryException {
    double upperLeftX = 1;
    double upperLeftY = 2;
    int widthInPixel = 3;
    int heightInPixel = 4;
    double pixelSize = 5;
    int numBands = 1;

    GridCoverage2D gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
    double[] metadata = RasterAccessors.metadata(gridCoverage2D);
    assertEquals(upperLeftX, metadata[0], 1e-9);
    assertEquals(upperLeftY, metadata[1], 1e-9);
    assertEquals(widthInPixel, metadata[2], 1e-9);
    assertEquals(heightInPixel, metadata[3], 1e-9);
    assertEquals(pixelSize, metadata[4], 1e-9);
    assertEquals(-1 * pixelSize, metadata[5], 1e-9);
    assertEquals(0, metadata[6], 1e-9);
    assertEquals(0, metadata[7], 1e-9);
    assertEquals(0, metadata[8], 1e-9);
    assertEquals(numBands, metadata[9], 1e-9);
    assertEquals(widthInPixel, metadata[10], 1e-9);
    assertEquals(heightInPixel, metadata[11], 1e-9);
    assertEquals(12, metadata.length);

    upperLeftX = 5;
    upperLeftY = 6;
    widthInPixel = 7;
    heightInPixel = 8;
    pixelSize = 9;
    numBands = 10;

    gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);

    metadata = RasterAccessors.metadata(gridCoverage2D);

    assertEquals(upperLeftX, metadata[0], 1e-9);
    assertEquals(upperLeftY, metadata[1], 1e-9);
    assertEquals(widthInPixel, metadata[2], 1e-9);
    assertEquals(heightInPixel, metadata[3], 1e-9);
    assertEquals(pixelSize, metadata[4], 1e-9);
    assertEquals(-1 * pixelSize, metadata[5], 1e-9);
    assertEquals(0, metadata[6], 1e-9);
    assertEquals(0, metadata[7], 1e-9);
    assertEquals(0, metadata[8], 1e-9);
    assertEquals(numBands, metadata[9], 1e-9);
    assertEquals(widthInPixel, metadata[10], 1e-9);
    assertEquals(heightInPixel, metadata[11], 1e-9);

    assertEquals(12, metadata.length);
  }

  @Test
  public void testMetadataOfTiledRasters() throws IOException, FactoryException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    double[] metadata = RasterAccessors.metadata(raster);
    assertEquals(-13095817.809, metadata[0], 0.01);
    assertEquals(4021262.749, metadata[1], 0.01);
    assertEquals(512, metadata[2], 1e-9);
    assertEquals(517, metadata[3], 1e-9);
    assertEquals(72.328612721326948, metadata[4], 0.001);
    assertEquals(-72.328612721326948, metadata[5], 0.001);
    assertEquals(0, metadata[6], 1e-9);
    assertEquals(0, metadata[7], 1e-9);
    assertEquals(3857, metadata[8], 1e-9);
    assertEquals(1, metadata[9], 1e-9);
    assertEquals(256, metadata[10], 1e-9);
    assertEquals(256, metadata[11], 1e-9);
    assertEquals(12, metadata.length);
  }

  @Test
  public void testMetaDataUsingSkewedRaster() throws FactoryException {
    int widthInPixel = 3;
    int heightInPixel = 4;
    double upperLeftX = 100.0;
    double upperLeftY = 200.0;
    double scaleX = 2.0;
    double scaleY = -3.0;
    double skewX = 0.1;
    double skewY = 0.2;
    int numBands = 1;

    GridCoverage2D gridCoverage2D =
        RasterConstructors.makeEmptyRaster(
            numBands,
            widthInPixel,
            heightInPixel,
            upperLeftX,
            upperLeftY,
            scaleX,
            scaleY,
            skewX,
            skewY,
            3857);
    double[] metadata = RasterAccessors.metadata(gridCoverage2D);
    assertEquals(upperLeftX, metadata[0], 1e-9);
    assertEquals(upperLeftY, metadata[1], 1e-9);
    assertEquals(widthInPixel, metadata[2], 1e-9);
    assertEquals(heightInPixel, metadata[3], 1e-9);
    assertEquals(scaleX, metadata[4], 1e-9);
    assertEquals(scaleY, metadata[5], 1e-9);
    assertEquals(skewX, metadata[6], 1e-9);
    assertEquals(skewY, metadata[7], 1e-9);
    assertEquals(3857, metadata[8], 1e-9);
    assertEquals(numBands, metadata[9], 1e-9);
    assertEquals(widthInPixel, metadata[10], 1e-9);
    assertEquals(heightInPixel, metadata[11], 1e-9);
    assertEquals(12, metadata.length);
  }
}
