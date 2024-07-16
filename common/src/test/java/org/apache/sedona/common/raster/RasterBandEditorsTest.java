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

import static org.apache.sedona.common.raster.RasterBandEditors.rasterUnion;
import static org.junit.Assert.*;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.raster.serde.Serde;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class RasterBandEditorsTest extends RasterTestBase {

  @Test
  public void testSetBandNoDataValueWithRaster() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(raster, 1, 3d);
    double actual = RasterBandAccessors.getBandNoDataValue(grid);
    double expected = 3;
    assertEquals(expected, actual, 0.1d);
    assert (Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(grid, 1)));

    grid = RasterBandEditors.setBandNoDataValue(raster, -999d);
    actual = RasterBandAccessors.getBandNoDataValue(grid);
    expected = -999;
    assertEquals(expected, actual, 0.1d);
    assert (Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(grid, 1)));
  }

  @Test
  public void testSetBandNoDataValueWithNull() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(raster, 1, null);
    String actual = Arrays.toString(grid.getSampleDimensions());
    String expected = "[RenderedSampleDimension[\"PALETTE_INDEX\"]]";
    assertEquals(expected, actual);
  }

  @Test
  public void testGetSummaryStats() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 10.0, true);

    // Test single output
    double resultSummary = RasterBandAccessors.getSummaryStats(raster, "count", 1, false);
    assertEquals(1036800, (int) resultSummary);

    resultSummary = RasterBandAccessors.getSummaryStats(raster, "sum", 1, false);
    assertEquals(207319567, (int) resultSummary);

    resultSummary = RasterBandAccessors.getSummaryStats(raster, "mean", 1, false);
    assertEquals(199, (int) resultSummary);

    resultSummary = RasterBandAccessors.getSummaryStats(raster, "stddev", 1, false);
    assertEquals(92, (int) resultSummary);

    resultSummary = RasterBandAccessors.getSummaryStats(raster, "min", 1, false);
    assertEquals(1, (int) resultSummary);

    resultSummary = RasterBandAccessors.getSummaryStats(raster, "max", 1, false);
    assertEquals(255, (int) resultSummary);
  }

  @Test
  public void testSetBandNoDataValueWithReplaceOptionRaster() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    double[] originalSummary = RasterBandAccessors.getSummaryStatsAll(raster, 1, false);
    int sumOG = (int) originalSummary[1];

    assertEquals(206233487, sumOG);
    GridCoverage2D resultRaster = RasterBandEditors.setBandNoDataValue(raster, 1, 10.0, true);
    double[] resultSummary = RasterBandAccessors.getSummaryStatsAll(resultRaster, 1, false);
    int sumActual = (int) resultSummary[1];

    // 108608 is the total no-data values in the raster
    // 10.0 is the new no-data value
    int sumExpected = sumOG + (10 * 108608);
    assertEquals(sumExpected, sumActual);

    // Not replacing previous no-data value
    resultRaster = RasterBandEditors.setBandNoDataValue(raster, 1, 10.0);
    resultSummary = RasterBandAccessors.getSummaryStatsAll(resultRaster, 1, false);
    sumActual = (int) resultSummary[1];
    assertEquals(sumOG, sumActual);
  }

  @Test
  public void testSetBandNoDataValueWithReplaceOption() throws FactoryException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 10, 20, 10, 20, 1);
    double[] band1 = new double[200];
    DecimalFormat df = new DecimalFormat("0.00");
    for (int i = 0; i < band1.length; i++) {
      if (i % 3 == 0) {
        band1[i] = 15;
        continue;
      }
      band1[i] = Double.parseDouble(df.format(Math.random() * 10));
    }
    raster = MapAlgebra.addBandFromArray(raster, band1, 1);
    // setting the noData property
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 15.0);

    // invoking replace option.
    GridCoverage2D result = RasterBandEditors.setBandNoDataValue(raster, 1, 20.0, true);
    double[] resultBand = MapAlgebra.bandAsArray(result, 1);

    Map<Double, Long> resultMap =
        Arrays.stream(resultBand)
            .boxed()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    Map<Double, Long> actualMap =
        Arrays.stream(band1)
            .boxed()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    assertEquals(actualMap.get(15.0), resultMap.get(20.0));
  }

  @Test
  public void testSetBandNoDataValueWithEmptyRaster() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 4326);
    GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(emptyRaster, 1, 999d);
    double actual = RasterBandAccessors.getBandNoDataValue(grid);
    double expected = 999;
    assertEquals(expected, actual, 0.1d);

    grid = RasterBandEditors.setBandNoDataValue(emptyRaster, -444.444);
    actual = RasterBandAccessors.getBandNoDataValue(grid);
    expected = -444.444;
    assertEquals(expected, actual, 0.0001d);
  }

  @Test
  public void testSetBandNoDataValueWithEmptyRasterMultipleBand() throws FactoryException {
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(2, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 0);
    GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(emptyRaster, -9999d);
    grid = RasterBandEditors.setBandNoDataValue(grid, 2, 444d);
    assertEquals(-9999, (double) RasterBandAccessors.getBandNoDataValue(grid), 0.1d);
    assertEquals(444, (double) RasterBandAccessors.getBandNoDataValue(grid, 2), 0.1d);
  }

  @Test
  public void testClipWithGeometryTransform()
      throws FactoryException, IOException, ParseException, TransformException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((-8682522.873537656 4572703.890837922, -8673439.664183248 4572993.532747675, -8673155.57366801 4563873.2099182755, -8701890.325907696 4562931.7093397, -8682522.873537656 4572703.890837922))";
    Geometry geom = Constructors.geomFromWKT(polygon, 3857);

    GridCoverage2D clippedRaster = RasterBandEditors.clip(raster, 1, geom, 200, false);
    double[] clippedMetadata =
        Arrays.stream(RasterAccessors.metadata(clippedRaster), 0, 9).toArray();
    double[] originalMetadata = Arrays.stream(RasterAccessors.metadata(raster), 0, 9).toArray();
    assertArrayEquals(originalMetadata, clippedMetadata, 0.01d);

    List<Geometry> points = new ArrayList<>();
    points.add(Constructors.geomFromWKT("POINT(223802 4.21769e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(224759 4.20453e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(237201 4.20429e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(237919 4.20357e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(254668 4.21769e+06)", 26918));
    Double[] actualValues = PixelFunctions.values(clippedRaster, points, 1).toArray(new Double[0]);
    Double[] expectedValues = new Double[] {null, null, 0.0, 0.0, null};
    assertTrue(Arrays.equals(expectedValues, actualValues));
  }

  @Test
  public void testClip()
      throws IOException, FactoryException, TransformException, ParseException,
          ClassNotFoundException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    String polygon =
        "POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 221170 4197590, 236722 4204770))";
    Geometry geom = Constructors.geomFromWKT(polygon, RasterAccessors.srid(raster));

    GridCoverage2D clippedRaster = RasterBandEditors.clip(raster, 1, geom, 200, false);
    double[] clippedMetadata =
        Arrays.stream(RasterAccessors.metadata(clippedRaster), 0, 9).toArray();
    double[] originalMetadata = Arrays.stream(RasterAccessors.metadata(raster), 0, 9).toArray();
    assertArrayEquals(originalMetadata, clippedMetadata, 0.01d);

    String actual = String.valueOf(clippedRaster.getSampleDimensions()[0]);
    String expected =
        "RenderedSampleDimension(\"RED_BAND\":[200.0 ... 200.0])\n  ‣ Category(\"No data\":[200...200])\n";
    assertEquals(expected, actual);

    List<Geometry> points = new ArrayList<>();
    points.add(Constructors.geomFromWKT("POINT(223802 4.21769e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(224759 4.20453e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(237201 4.20429e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(237919 4.20357e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(254668 4.21769e+06)", 26918));
    Double[] actualValues = PixelFunctions.values(clippedRaster, points, 1).toArray(new Double[0]);
    Double[] expectedValues = new Double[] {null, null, 0.0, 0.0, null};
    assertTrue(Arrays.equals(expectedValues, actualValues));

    GridCoverage2D croppedRaster = RasterBandEditors.clip(raster, 1, geom, 200, true);
    assertEquals(0, croppedRaster.getRenderedImage().getMinX());
    assertEquals(0, croppedRaster.getRenderedImage().getMinY());
    GridCoverage2D croppedRaster2 = Serde.deserialize(Serde.serialize(croppedRaster));
    assertSameCoverage(croppedRaster, croppedRaster2);
    points = new ArrayList<>();
    points.add(Constructors.geomFromWKT("POINT(236842 4.20465e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(236961 4.20453e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(237201 4.20429e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(237919 4.20357e+06)", 26918));
    points.add(Constructors.geomFromWKT("POINT(223802 4.20465e+06)", 26918));
    actualValues = PixelFunctions.values(croppedRaster, points, 1).toArray(new Double[0]);
    expectedValues = new Double[] {0.0, 0.0, 0.0, 0.0, null};
    assertTrue(Arrays.equals(expectedValues, actualValues));
  }

  @Test
  public void testRasterUnion() throws FactoryException {
    double[][] rasterData1 =
        new double[][] {
          {
            13, 80, 49, 15, 4, 46, 47, 94, 58, 37, 6, 22, 98, 26, 78, 66, 86, 79, 5, 65, 7, 12, 89,
            67
          },
          {
            37, 4, 5, 15, 60, 83, 24, 19, 23, 87, 98, 89, 59, 71, 42, 46, 0, 80, 27, 73, 66, 100,
            78, 64
          },
          {
            73, 39, 50, 13, 45, 21, 87, 38, 63, 22, 44, 6, 8, 24, 19, 10, 89, 3, 48, 28, 0, 71, 59,
            11
          }
        };
    GridCoverage2D raster1 =
        RasterConstructors.makeNonEmptyRaster(3, "i", 4, 6, 1, -1, 1, -1, 0, 0, 0, rasterData1);

    double[][] rasterData2 =
        new double[][] {
          {
            35, 68, 56, 87, 49, 20, 73, 90, 45, 96, 52, 98, 2, 82, 88, 74, 77, 60, 5, 61, 81, 32, 9,
            15
          },
          {
            55, 49, 72, 10, 63, 94, 100, 83, 61, 47, 20, 15, 34, 46, 52, 11, 23, 98, 70, 67, 18, 39,
            53, 91
          }
        };
    GridCoverage2D raster2 =
        RasterConstructors.makeNonEmptyRaster(2, "d", 4, 6, 1, -1, 1, -1, 0, 0, 0, rasterData2);

    GridCoverage2D result = rasterUnion(raster1, raster2);
    int actualNumBands = RasterAccessors.numBands(result);
    int expectedNumBands = 5;
    assertEquals(expectedNumBands, actualNumBands);

    double[] actualBandValues = MapAlgebra.bandAsArray(result, 5);
    double[] expectedBandValues = MapAlgebra.bandAsArray(raster2, 2);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(result), 0, 9).toArray();
    double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster2), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    result = rasterUnion(raster1, raster2, raster1);
    actualNumBands = RasterAccessors.numBands(result);
    expectedNumBands = 8;
    assertEquals(expectedNumBands, actualNumBands);

    actualBandValues = MapAlgebra.bandAsArray(result, 7);
    expectedBandValues = MapAlgebra.bandAsArray(raster1, 2);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    actualMetadata = Arrays.stream(RasterAccessors.metadata(result), 0, 9).toArray();
    expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster2), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    result = rasterUnion(raster1, raster2, raster1, raster2);
    actualNumBands = RasterAccessors.numBands(result);
    expectedNumBands = 10;
    assertEquals(expectedNumBands, actualNumBands);

    actualBandValues = MapAlgebra.bandAsArray(result, 9);
    expectedBandValues = MapAlgebra.bandAsArray(raster2, 1);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    actualMetadata = Arrays.stream(RasterAccessors.metadata(result), 0, 9).toArray();
    expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster2), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    result = rasterUnion(raster1, raster2, raster1, raster2, raster1);
    actualNumBands = RasterAccessors.numBands(result);
    expectedNumBands = 13;
    assertEquals(expectedNumBands, actualNumBands);

    actualBandValues = MapAlgebra.bandAsArray(result, 13);
    expectedBandValues = MapAlgebra.bandAsArray(raster1, 3);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    actualMetadata = Arrays.stream(RasterAccessors.metadata(result), 0, 9).toArray();
    expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster2), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);

    result = rasterUnion(raster1, raster2, raster1, raster2, raster1, raster2);
    actualNumBands = RasterAccessors.numBands(result);
    expectedNumBands = 15;
    assertEquals(expectedNumBands, actualNumBands);

    actualBandValues = MapAlgebra.bandAsArray(result, 14);
    expectedBandValues = MapAlgebra.bandAsArray(raster2, 1);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    actualMetadata = Arrays.stream(RasterAccessors.metadata(result), 0, 9).toArray();
    expectedMetadata = Arrays.stream(RasterAccessors.metadata(raster2), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
  }

  @Test
  public void testAddBandWithEmptyRaster() throws FactoryException {
    double[][] rasterData1 =
        new double[][] {
          {
            13, 80, 49, 15, 4, 46, 47, 94, 58, 37, 6, 22, 98, 26, 78, 66, 86, 79, 5, 65, 7, 12, 89,
            67
          },
          {
            37, 4, 5, 15, 60, 83, 24, 19, 23, 87, 98, 89, 59, 71, 42, 46, 0, 80, 27, 73, 66, 100,
            78, 64
          },
          {
            73, 39, 50, 13, 45, 21, 87, 38, 63, 22, 44, 6, 8, 24, 19, 10, 89, 3, 48, 28, 0, 71, 59,
            11
          }
        };
    GridCoverage2D toRaster =
        RasterConstructors.makeNonEmptyRaster(3, "i", 4, 6, 1, -1, 1, 1, 0, 0, 0, rasterData1);

    // fromRaster's data type is Double to test the preservation of data type
    double[][] rasterData2 =
        new double[][] {
          {
            35, 68, 56, 87, 49, 20, 73, 90, 45, 96, 52, 98, 2, 82, 88, 74, 77, 60, 5, 61, 81, 32, 9,
            15
          },
          {
            55, 49, 72, 10, 63, 94, 100, 83, 61, 47, 20, 15, 34, 46, 52, 11, 23, 98, 70, 67, 18, 39,
            53, 91
          }
        };
    GridCoverage2D fromRaster =
        RasterConstructors.makeNonEmptyRaster(2, "d", 4, 6, 10, -10, 1, -1, 0, 0, 0, rasterData2);

    // test 4 parameter variant
    testAddBand4Param(fromRaster, toRaster);

    // test 3 parameter variant
    testAddBand3Param(fromRaster, toRaster);

    // test 2 parameter variant
    testAddBand2Param(fromRaster, toRaster);
  }

  public static void testAddBand4Param(GridCoverage2D fromRaster, GridCoverage2D toRaster)
      throws FactoryException {
    GridCoverage2D actualRaster = RasterBandEditors.addBand(toRaster, fromRaster, 1, 4);

    // test numBands
    int actualNumBands = RasterAccessors.numBands(actualRaster);
    int expectedNumBands = 4;
    assertEquals(expectedNumBands, actualNumBands);

    // test data type preservation
    String actualDataType = RasterBandAccessors.getBandType(actualRaster);
    String expectedDataType = "SIGNED_32BITS";
    assertEquals(expectedDataType, actualDataType);

    // test new band values in the resultant raster
    double[] actualBandValues = MapAlgebra.bandAsArray(actualRaster, 4);
    double[] expectedBandValues = MapAlgebra.bandAsArray(fromRaster, 1);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    // test preservation of original raster
    // remove last index as that's number of bands, and they wouldn't be equal
    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(actualRaster), 0, 9).toArray();
    double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(toRaster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
  }

  public static void testAddBand3Param(GridCoverage2D fromRaster, GridCoverage2D toRaster)
      throws FactoryException {
    GridCoverage2D actualRaster = RasterBandEditors.addBand(toRaster, fromRaster, 2);

    // test numBands
    int actualNumBands = RasterAccessors.numBands(actualRaster);
    int expectedNumBands = 4;
    assertEquals(expectedNumBands, actualNumBands);

    // test data type preservation
    String actualDataType = RasterBandAccessors.getBandType(actualRaster);
    String expectedDataType = "SIGNED_32BITS";
    assertEquals(expectedDataType, actualDataType);

    // test new band values in the resultant raster
    double[] actualBandValues = MapAlgebra.bandAsArray(actualRaster, 4);
    double[] expectedBandValues = MapAlgebra.bandAsArray(fromRaster, 2);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    // test preservation of original raster
    // remove last index as that's number of bands, and they wouldn't be equal
    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(actualRaster), 0, 9).toArray();
    double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(toRaster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
  }

  public static void testAddBand2Param(GridCoverage2D fromRaster, GridCoverage2D toRaster)
      throws FactoryException {
    GridCoverage2D actualRaster = RasterBandEditors.addBand(toRaster, fromRaster);

    // test numBands
    int actualNumBands = RasterAccessors.numBands(actualRaster);
    int expectedNumBands = 4;
    assertEquals(expectedNumBands, actualNumBands);

    // test data type preservation
    String actualDataType = RasterBandAccessors.getBandType(actualRaster);
    String expectedDataType = "SIGNED_32BITS";
    assertEquals(expectedDataType, actualDataType);

    // test new band values in the resultant raster
    double[] actualBandValues = MapAlgebra.bandAsArray(actualRaster, 4);
    double[] expectedBandValues = MapAlgebra.bandAsArray(fromRaster, 1);
    assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

    // test preservation of original raster
    // remove last index as that's number of bands, and they wouldn't be equal
    double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(actualRaster), 0, 9).toArray();
    double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(toRaster), 0, 9).toArray();
    assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
  }
}
