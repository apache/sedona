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
import java.util.Arrays;
import org.apache.sedona.common.Constructors;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class RasterBandAccessorsTest extends RasterTestBase {

  @Test
  public void testBandNoDataValueCustomBand() throws FactoryException {
    int width = 5, height = 10;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, width, height, 53, 51, 1, 1, 0, 0, 4326);
    double[] values = new double[width * height];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 1;
    }
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 2, 1d);
    assertNotNull(RasterBandAccessors.getBandNoDataValue(emptyRaster, 2));
    assertEquals(1, RasterBandAccessors.getBandNoDataValue(emptyRaster, 2), 1e-9);
    assertNull(RasterBandAccessors.getBandNoDataValue(emptyRaster));
  }

  @Test
  public void testBandNoDataValueDefaultBand() throws FactoryException {
    int width = 5, height = 10;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, width, height, 53, 51, 1, 1, 0, 0, 4326);
    double[] values = new double[width * height];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 1;
    }
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 1d);
    assertNotNull(RasterBandAccessors.getBandNoDataValue(emptyRaster));
    assertEquals(1, RasterBandAccessors.getBandNoDataValue(emptyRaster), 1e-9);
  }

  @Test
  public void testBandNoDataValueDefaultNoData() throws FactoryException {
    int width = 5, height = 10;
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, "I", width, height, 53, 51, 1, 1, 0, 0, 0);
    double[] values = new double[width * height];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 1;
    }
    assertNull(RasterBandAccessors.getBandNoDataValue(emptyRaster, 1));
  }

  @Test
  public void testBandNoDataValueIllegalBand() throws FactoryException, IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RasterBandAccessors.getBandNoDataValue(raster, 2));
    assertEquals("Provided band index 2 is not present in the raster", exception.getMessage());
  }

  @Test
  public void testZonalStats() throws FactoryException, ParseException, IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 221170 4197590, 236722 4204770))";
    Geometry geom = Constructors.geomFromWKT(polygon, RasterAccessors.srid(raster));

    double actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", false);
    double expected = 1.0719726E7;
    assertEquals(expected, actual, 0d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 2, "mean", false);
    expected = 220.7527;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "count");
    expected = 184792.0;
    assertEquals(expected, actual, 0.1d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 3, "variance", false);
    expected = 13549.6263;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, "max");
    expected = 255.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "min", false);
    expected = 0.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sd", false);
    expected = 92.1500;
    assertEquals(expected, actual, FP_TOLERANCE);

    geom =
        Constructors.geomFromWKT(
            "POLYGON ((-77.96672569800863073 37.91971182746296876, -77.9688630154902711 37.89620133516485367, -77.93936803424354309 37.90517806858776595, -77.96672569800863073 37.91971182746296876))",
            0);
    Double statValue = RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", false, true);
    assertNotNull(statValue);

    Geometry nonIntersectingGeom =
        Constructors.geomFromWKT(
            "POLYGON ((-78.22106647832458748 37.76411511479908967, -78.20183062098976734 37.72863564460374874, -78.18088490966962922 37.76753482276972562, -78.22106647832458748 37.76411511479908967))",
            0);
    statValue =
        RasterBandAccessors.getZonalStats(raster, nonIntersectingGeom, 1, "sum", false, true);
    assertNull(statValue);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RasterBandAccessors.getZonalStats(raster, nonIntersectingGeom, 1, "sum", false, false));
  }

  @Test
  public void testZonalStatsWithNoData() throws IOException, FactoryException, ParseException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    String polygon =
        "POLYGON((-167.750000 87.750000, -155.250000 87.750000, -155.250000 40.250000, -180.250000 40.250000, -167.750000 87.750000))";
    // Testing implicit CRS transformation
    Geometry geom = Constructors.geomFromWKT(polygon, 0);

    double actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sum", true);
    double expected = 3213526.0;
    assertEquals(expected, actual, 0d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "mean", true);
    expected = 226.5599;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "count");
    expected = 14184.0;
    assertEquals(expected, actual, 0.1d);

    actual = RasterBandAccessors.getZonalStats(raster, geom, "variance");
    expected = 5606.4233;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getZonalStats(raster, geom, "max");
    expected = 255.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "min", true);
    expected = 1.0;
    assertEquals(expected, actual, 1E-1);

    actual = RasterBandAccessors.getZonalStats(raster, geom, 1, "sd", true);
    expected = 74.8760;
    assertEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testZonalStatsAll()
      throws IOException, FactoryException, ParseException, TransformException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((-8673439.6642 4572993.5327, -8673155.5737 4563873.2099, -8701890.3259 4562931.7093, -8682522.8735 4572703.8908, -8673439.6642 4572993.5327))";
    Geometry geom = Constructors.geomFromWKT(polygon, 3857);

    double[] actual = RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false);
    double[] expected =
        new double[] {
          184792.0,
          1.0719726E7,
          58.00968656653401,
          0.0,
          0.0,
          92.15004748703687,
          8491.631251863151,
          0.0,
          255.0
        };
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    geom =
        Constructors.geomFromWKT(
            "POLYGON ((-77.96672569800863073 37.91971182746296876, -77.9688630154902711 37.89620133516485367, -77.93936803424354309 37.90517806858776595, -77.96672569800863073 37.91971182746296876))",
            0);
    actual = RasterBandAccessors.getZonalStatsAll(raster, geom, 1, false);
    assertNotNull(actual);

    Geometry nonIntersectingGeom =
        Constructors.geomFromWKT(
            "POLYGON ((-78.22106647832458748 37.76411511479908967, -78.20183062098976734 37.72863564460374874, -78.18088490966962922 37.76753482276972562, -78.22106647832458748 37.76411511479908967))",
            0);
    actual = RasterBandAccessors.getZonalStatsAll(raster, nonIntersectingGeom, 1, false, true);
    assertNull(actual);
    assertThrows(
        IllegalArgumentException.class,
        () -> RasterBandAccessors.getZonalStatsAll(raster, nonIntersectingGeom, 1, false, false));
  }

  @Test
  public void testZonalStatsAllWithNoData() throws IOException, FactoryException, ParseException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    String polygon =
        "POLYGON((-167.750000 87.750000, -155.250000 87.750000, -155.250000 40.250000, -180.250000 40.250000, -167.750000 87.750000))";
    Geometry geom = Constructors.geomFromWKT(polygon, RasterAccessors.srid(raster));

    double[] actual = RasterBandAccessors.getZonalStatsAll(raster, geom, 1, true);
    double[] expected =
        new double[] {14184.0, 3213526.0, 226.5599, 255.0, 255.0, 74.8760, 5606.4233, 1.0, 255.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testZonalStatsAllWithEmptyRaster() throws FactoryException, ParseException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 6, 6, 1, -1, 1, -1, 0, 0, 4326);
    double[] bandValue =
        new double[] {
          0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 9, 0, 0, 5, 6, 0, 8, 0, 0, 4, 11, 11, 12, 0, 0, 13, 0, 15,
          16, 0, 0, 0, 0, 0, 0, 0
        };
    raster = MapAlgebra.addBandFromArray(raster, bandValue, 1);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 0d);
    // Testing implicit CRS transformation
    Geometry geom = Constructors.geomFromWKT("POLYGON((2 -2, 2 -6, 6 -6, 6 -2, 2 -2))", 0);

    double[] actual = RasterBandAccessors.getZonalStatsAll(raster, geom, 1, true);
    double[] expected = new double[] {13.0, 114.0, 8.7692, 9.0, 11.0, 4.7285, 22.3589, 1.0, 16.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStatsAllWithAllNoData() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values =
        new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0d);
    double[] actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster);
    double[] expected = {0.0, 0.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStats() throws FactoryException, IOException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {
          1, 2, 0, 0, 0, 0, 7, 8, 0, 10, 11, 0, 0, 0, 0, 16, 17, 0, 19, 20, 21, 0, 23, 24, 25
        };
    double[] values2 =
        new double[] {
          0, 0, 28, 29, 0, 0, 0, 33, 34, 35, 36, 37, 38, 0, 0, 0, 0, 43, 44, 45, 46, 47, 48, 49, 50
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);

    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");

    double actual = RasterBandAccessors.getSummaryStats(emptyRaster, "count", 2, true);
    double expected = 16.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "sum", 2, true);
    expected = 642.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "mean", 2, true);
    expected = 40.125;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "stddev", 2, true);
    expected = 6.9988838395847095;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "min", 2, true);
    expected = 28.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(emptyRaster, "max", 2, true);
    expected = 50.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "count", 1, false);
    expected = 1036800.0;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "sum", 1, false);
    expected = 2.06233487E8;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "mean", 1, false);
    expected = 198.91347125792052;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "stddev", 1, false);
    expected = 95.09054096111336;
    assertEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStats(raster, "min", 1, false);
    expected = 0.0;
    assertEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStatsAllWithEmptyRaster() throws FactoryException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {
          1, 2, 0, 0, 0, 0, 7, 8, 0, 10, 11, 0, 0, 0, 0, 16, 17, 0, 19, 20, 21, 0, 23, 24, 25
        };
    double[] values2 =
        new double[] {
          0, 0, 28, 29, 0, 0, 0, 33, 34, 35, 36, 37, 38, 0, 0, 0, 0, 43, 44, 45, 46, 47, 48, 49, 50
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    double[] actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster, 1, false);
    double[] expected = {25.0, 204.0, 8.1600, 9.2765, 0.0, 25.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster, 2);
    expected = new double[] {16.0, 642.0, 40.125, 6.9988838395847095, 28.0, 50.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(emptyRaster);
    expected = new double[] {14.0, 204.0, 14.5714, 7.7617, 1.0, 25.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testSummaryStatsAllWithRaster() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    double[] actual = RasterBandAccessors.getSummaryStatsAll(raster, 1, false);
    double[] expected = {1036800.0, 2.06233487E8, 198.9134, 95.0905, 0.0, 255.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(raster, 1);
    expected = new double[] {928192.0, 2.06233487E8, 222.1883, 70.2055, 1.0, 255.0};
    assertArrayEquals(expected, actual, FP_TOLERANCE);

    actual = RasterBandAccessors.getSummaryStatsAll(raster);
    assertArrayEquals(expected, actual, FP_TOLERANCE);
  }

  @Test
  public void testCountWithEmptyRaster() throws FactoryException {
    // With each parameter and excludeNoDataValue as true
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {0, 0, 0, 5, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0};
    double[] values2 =
        new double[] {0, 0, 0, 6, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    long actual = RasterBandAccessors.getCount(emptyRaster, 1, false);
    long expected = 25;
    assertEquals(expected, actual);

    // with just band parameter
    actual = RasterBandAccessors.getCount(emptyRaster, 2);
    expected = 4;
    assertEquals(expected, actual);

    // with no parameters except raster
    actual = RasterBandAccessors.getCount(emptyRaster);
    expected = 6;
    assertEquals(expected, actual);
  }

  @Test
  public void testCountWithEmptySkewedRaster() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, 5, 5, 23, -25, 1, -1, 2, 2, 0);
    double[] values1 =
        new double[] {0, 0, 0, 3, 4, 6, 0, 3, 2, 0, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 0, 2, 2, 0, 0};
    double[] values2 =
        new double[] {0, 0, 0, 0, 3, 2, 5, 6, 0, 0, 3, 2, 0, 0, 2, 3, 0, 0, 0, 0, 0, 3, 4, 4, 3};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    long actual = RasterBandAccessors.getCount(emptyRaster, 2, false);
    long expected = 25;
    assertEquals(expected, actual);

    // without excludeNoDataValue flag
    actual = RasterBandAccessors.getCount(emptyRaster, 1);
    expected = 10;
    assertEquals(expected, actual);

    // just with raster
    actual = RasterBandAccessors.getCount(emptyRaster);
    expected = 10;
    assertEquals(expected, actual);
  }

  @Test
  public void testCountWithRaster() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    long actual = RasterBandAccessors.getCount(raster, 1, false);
    long expected = 1036800;
    assertEquals(expected, actual);

    actual = RasterBandAccessors.getCount(raster, 1);
    expected = 928192;
    assertEquals(expected, actual);

    actual = RasterBandAccessors.getCount(raster);
    expected = 928192;
    assertEquals(expected, actual);
  }

  @Test
  public void testGetBand() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(4, 5, 5, 3, -215, 2, -2, 2, 2, 0);
    double[] values1 =
        new double[] {
          16, 0, 24, 33, 43, 49, 64, 0, 76, 77, 79, 89, 0, 116, 118, 125, 135, 0, 157, 190, 215,
          229, 241, 248, 249
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 3, 0d);
    GridCoverage2D resultRaster = RasterBandAccessors.getBand(emptyRaster, new int[] {3, 3, 3});
    int actual = RasterAccessors.numBands(resultRaster);
    int expected = 3;
    assertEquals(expected, actual);

    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(resultRaster), 0, 9).toArray();
    double[] expectedMetadata =
        Arrays.stream(RasterAccessors.metadata(emptyRaster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    double[] actualBandValues = MapAlgebra.bandAsArray(resultRaster, 3);
    double[] expectedBandValues = MapAlgebra.bandAsArray(emptyRaster, 3);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);
  }

  @Test
  public void testGetBandWithDataTypes() throws FactoryException, IOException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(4, "d", 5, 5, 3, -215, 2, -2, 2, 2, 0);
    double[] values1 =
        new double[] {
          16, 0, 24, 33, 43, 49, 64, 0, 76, 77, 79, 89, 0, 116, 118, 125, 135, 0, 157, 190, 215,
          229, 241, 248, 249
        };
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    String actual = RasterBandAccessors.getBandType(emptyRaster, 1);
    String expected = "REAL_64BITS";
    assertEquals(expected, actual);

    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    raster = RasterBandAccessors.getBand(raster, new int[] {2, 1, 3});
    for (int i = 1; i <= RasterAccessors.numBands(raster); i++) {
      actual = RasterBandAccessors.getBandType(raster, i);
      expected = "UNSIGNED_8BITS";
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetBandWithRaster() throws IOException, FactoryException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    GridCoverage2D resultRaster = RasterBandAccessors.getBand(raster, new int[] {1, 2, 2, 2, 1});
    int actual = RasterAccessors.numBands(resultRaster);
    int expected = 5;
    assertEquals(actual, expected);

    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(resultRaster), 0, 9).toArray();
    double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    double[] actualBandValues = MapAlgebra.bandAsArray(raster, 2);
    double[] expectedBandValues = MapAlgebra.bandAsArray(resultRaster, 2);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);
  }

  @Test
  public void testBandPixelType() throws FactoryException {
    double[] values = new double[] {1.2, 1.1, 32.2, 43.2};

    // create double raster
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, "D", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("REAL_64BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("REAL_64BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    double[] bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    double[] expectedBandValuesD = new double[] {1.2, 1.1, 32.2, 43.2};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesD[i], bandValues[i], 1e-9);
    }
    // create float raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "F", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("REAL_32BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("REAL_32BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    float[] expectedBandValuesF = new float[] {1.2f, 1.1f, 32.2f, 43.2f};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesF[i], bandValues[i], 1e-9);
    }

    // create integer raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "I", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("SIGNED_32BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("SIGNED_32BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    int[] expectedBandValuesI = new int[] {1, 1, 32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesI[i], bandValues[i], 1e-9);
    }

    // create byte raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "B", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    assertEquals("UNSIGNED_8BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("UNSIGNED_8BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    byte[] expectedBandValuesB = new byte[] {1, 1, 32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesB[i], bandValues[i], 1e-9);
    }

    // create short raster
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "S", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("SIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("SIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);
    short[] expectedBandValuesS = new short[] {1, 1, 32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(expectedBandValuesS[i], bandValues[i], 1e-9);
    }

    // create unsigned short raster
    values = new double[] {-1.2, 1.1, -32.2, 43.2};
    emptyRaster = RasterConstructors.makeEmptyRaster(2, "US", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);
    assertEquals("UNSIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster));
    assertEquals("UNSIGNED_16BITS", RasterBandAccessors.getBandType(emptyRaster, 2));
    bandValues = MapAlgebra.bandAsArray(emptyRaster, 1);

    short[] expectedBandValuesUS = new short[] {-1, 1, -32, 43};
    for (int i = 0; i < bandValues.length; i++) {
      assertEquals(
          Short.toUnsignedInt(expectedBandValuesUS[i]),
          Short.toUnsignedInt((short) bandValues[i]),
          1e-9);
    }
  }

  @Test
  public void testBandPixelTypeIllegalBand() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, "US", 2, 2, 53, 51, 1, 1, 0, 0, 0);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> RasterBandAccessors.getBandType(emptyRaster, 5));
    assertEquals("Provided band index 5 is not present in the raster", exception.getMessage());
  }

  @Test
  public void testBandIsNoData() throws FactoryException {
    String[] dataTypes = new String[] {"B", "S", "US", "I", "F", "D"};
    int width = 3;
    int height = 3;
    double noDataValue = 5.0;
    double[] band1 = new double[width * height];
    double[] band2 = new double[width * height];
    Arrays.fill(band1, noDataValue);
    for (int k = 0; k < band2.length; k++) {
      band2[k] = k;
    }
    for (String dataType : dataTypes) {
      GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, dataType, 3, 3, 0, 0, 1);
      raster = MapAlgebra.addBandFromArray(raster, band1, 1, null);
      raster = MapAlgebra.addBandFromArray(raster, band2, 2, null);

      // Currently raster does not have a nodata value, isBandNoData always returns false
      assertFalse(RasterBandAccessors.bandIsNoData(raster, 1));
      assertFalse(RasterBandAccessors.bandIsNoData(raster, 2));

      // Set nodata value for both bands, now band 1 is filled with nodata values
      raster = RasterBandEditors.setBandNoDataValue(raster, 1, noDataValue);
      raster = RasterBandEditors.setBandNoDataValue(raster, 2, noDataValue);
      assertTrue(RasterBandAccessors.bandIsNoData(raster, 1));
      assertFalse(RasterBandAccessors.bandIsNoData(raster, 2));
    }
  }
}
