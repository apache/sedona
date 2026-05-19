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

import java.io.File;
import java.io.IOException;
import java.net.URLConnection;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;

public class RasterOutputTest extends RasterTestBase {

  @Test
  public void testAsBase64() throws IOException {
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
    String resultRaw = RasterOutputs.asBase64(raster);
    assertTrue(
        resultRaw.startsWith(
            "iVBORw0KGgoAAAANSUhEUgAABaAAAALQCAMAAABR+ye1AAADAFBMVEXE9/W48vOq7PGa5u6L3"));
  }

  @Test
  public void testAsBase64Float() throws IOException {
    double[] bandData = {
      202.125, 101.221, 7.468, 27.575, 18.463, 106.103, 80.995, 213.73, 249.73, 147.455, 202.669,
      223.379, 6.898, 64.108, 81.585, 51.162, 198.681, 147.957, 14.233, 14.146, 209.691, 121.825,
      197.658, 235.804, 129.798
    };
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 5, 5, 1, 1, 1, 1, 0, 0, 4326, new double[][] {bandData});

    String resultRaw = RasterOutputs.asBase64(raster);
    assertTrue(resultRaw.startsWith("iVBORw0KGgoAAAANSUhEUgAAAAUAAAA"));
  }

  @Test
  public void testAsPNG() throws IOException, FactoryException {
    String dirPath = System.getProperty("user.dir") + "/target/testAsPNGFunction/";
    new File(dirPath).mkdirs();
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    byte[] pngData = RasterOutputs.asPNG(raster);
    RasterOutputs.writeToDiskFile(pngData, dirPath + "test1.png");
    File f = new File(dirPath + "test1.png");
    String mimeType = URLConnection.guessContentTypeFromName(f.getName());
    assertEquals("image/png", mimeType);
  }

  @Test
  public void testAsPNGWithBand() throws IOException, FactoryException {
    String dirPath = System.getProperty("user.dir") + "/target/testAsPNGFunction/";
    new File(dirPath).mkdirs();
    GridCoverage2D raster =
        rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
    byte[] pngData = RasterOutputs.asPNG(RasterBandAccessors.getBand(raster, new int[] {3, 1, 2}));
    RasterOutputs.writeToDiskFile(pngData, dirPath + "test2.png");
    File f = new File(dirPath + "test2.png");
    String mimeType = URLConnection.guessContentTypeFromName(f.getName());
    assertEquals("image/png", mimeType);
  }

  @Test
  public void testAsGeoTiff() throws IOException {
    GridCoverage2D rasterOg = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    GridCoverage2D rasterTest =
        RasterConstructors.fromGeoTiff(
            RasterOutputs.asGeoTiff(rasterFromGeoTiff(resourceFolder + "raster/test1.tiff")));
    assert (rasterTest != null);
    assertEquals(rasterTest.getEnvelope().toString(), rasterOg.getEnvelope().toString());
  }

  @Test
  public void testWriteToDiskFile() throws IOException {
    new File(System.getProperty("user.dir") + "/target/estToGeoTiffFunction/").mkdirs();
    GridCoverage2D rasterOg = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] bytes = RasterOutputs.asGeoTiff(rasterOg);
    String filePath = System.getProperty("user.dir") + "/target/estToGeoTiffFunction/test1.tiff";
    boolean successful = RasterOutputs.writeToDiskFile(bytes, filePath);
    assertTrue(successful);

    GridCoverage2D rasterConverted = rasterFromGeoTiff(filePath);
    double[] actual = MapAlgebra.bandAsArray(rasterOg, 1);
    double[] expected = MapAlgebra.bandAsArray(rasterConverted, 1);
    assertArrayEquals(expected, actual, 0.1d);
  }

  @Test
  public void testAsGeoTiffWithCompressionTypes() throws IOException {
    GridCoverage2D rasterOg = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] rasterBytes1 = RasterOutputs.asGeoTiff(rasterOg, "LZW", 1.0);
    byte[] rasterBytes2 = RasterOutputs.asGeoTiff(rasterOg, "Deflate", 0.5);
    GridCoverage2D rasterNew = RasterConstructors.fromGeoTiff(rasterBytes1);
    assertEquals(rasterOg.getEnvelope().toString(), rasterNew.getEnvelope().toString());
    assert (rasterBytes1.length > rasterBytes2.length);
  }

  @Test
  public void testAsMatrixBand() {
    double[][] rasterValues = {{1, 3, 4, 0, 2, 9, 10, 11, 3, 4, 5, 6}};
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "s", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue = "| 1   3   4   0|\n" + "| 2   9  10  11|\n" + "| 3   4   5   6|\n";
    String actual = RasterOutputs.asMatrix(raster, 1);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsMatrixBandShort() {
    double[][] rasterValues = {{1, 3, 4, 0, 2, 9, 10, 11, 3, 4, 5, 6}};
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "s", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue = "| 1   3   4   0|\n" + "| 2   9  10  11|\n" + "| 3   4   5   6|\n";
    String actual = RasterOutputs.asMatrix(raster, 1);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsMatrixBandUShort() {
    double[][] rasterValues = {{1, 3, 4, 0, 2, 9, 10, 11, 3, 4, 5, 6}};
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "us", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue = "| 1   3   4   0|\n" + "| 2   9  10  11|\n" + "| 3   4   5   6|\n";
    String actual = RasterOutputs.asMatrix(raster, 1);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsMatrixBandBytePrecision() {
    double[][] rasterValues = {{1, 3, 4, 0, 2, 9, 10, 11, 3, 4, 5, 6}};
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "b", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue = "| 1   3   4   0|\n" + "| 2   9  10  11|\n" + "| 3   4   5   6|\n";
    String actual = RasterOutputs.asMatrix(raster, 1, 2);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsMatrixBandPrecision() {
    double[][] rasterValues = {{1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6}};
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "d", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue =
        "| 1.00000   3.33333   4.00000   0.00010|\n"
            + "| 2.22220   9.00000  10.00000  11.11111|\n"
            + "| 3.00000   4.00000   5.00000   6.00000|\n";
    String actual = RasterOutputs.asMatrix(raster, 1, 5);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsMatrixDefault() {
    double[][] rasterValues = {{1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6}};
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(1, "d", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue =
        "| 1.000000   3.333333   4.000000   0.000100|\n"
            + "| 2.222200   9.000000  10.000000  11.111111|\n"
            + "| 3.000000   4.000000   5.000000   6.000000|\n";
    String actual = RasterOutputs.asMatrix(raster);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsMatrixMultipleBands() {
    double[][] rasterValues = {
      {1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6},
      {3.22, 1, 5.321, 4, 5.33333112334, 10000, 0.0, 1.93, 1190.12121, 9.8, 23, 1}
    };
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(2, "f", 4, 3, 0, 0, 1, -1, 0, 0, 0, rasterValues);
    String expectedValue =
        "|    3.22      1.00      5.32      4.00|\n"
            + "|    5.33  10000.00      0.00      1.93|\n"
            + "| 1190.12      9.80     23.00      1.00|\n";
    String actual = RasterOutputs.asMatrix(raster, 2, 2);
    assertEquals(expectedValue, actual);
  }

  @Test
  public void testAsImage() throws IOException, FactoryException {
    GridCoverage2D testRaster =
        RasterConstructors.makeEmptyRaster(1, "b", 5, 4, 0, 0, 1, -1, 0, 0, 0);
    double[] bandValues = {
      13, 200, 255, 1, 4, 100, 13, 224, 11, 12, 76, 98, 97, 56, 45, 21, 35, 67, 43, 75
    };
    testRaster = MapAlgebra.addBandFromArray(testRaster, bandValues, 1);
    String htmlString = RasterOutputs.createHTMLString(testRaster);
    String expectedStart =
        "<img src=\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAECAAAAABjWKqcAAAAIElEQV";
    String expectedEnd = "width=\"200\" />";
    assertTrue(htmlString.startsWith(expectedStart));
    assertTrue(htmlString.endsWith(expectedEnd));
  }

  @Test
  public void testAsImageCustomWidth() throws IOException, FactoryException {
    GridCoverage2D testRaster =
        RasterConstructors.makeEmptyRaster(1, "b", 5, 4, 0, 0, 1, -1, 0, 0, 0);
    double[] bandValues = {
      13, 200, 255, 1, 4, 100, 13, 224, 11, 12, 76, 98, 97, 56, 45, 21, 35, 67, 43, 75
    };
    testRaster = MapAlgebra.addBandFromArray(testRaster, bandValues, 1);
    String htmlString = RasterOutputs.createHTMLString(testRaster, 500);
    String expectedStart =
        "<img src=\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAECAAAAABjWKqcAAAAIElEQV";
    String expectedEnd = "width=\"500\" />";
    assertTrue(htmlString.startsWith(expectedStart));
    assertTrue(htmlString.endsWith(expectedEnd));
  }

  @Test
  public void testAsImageVariousBandDataType() throws IOException, FactoryException {
    String[] dataTypes = {"b", "d", "f", "i", "s", "us"};
    int width = 100;
    int height = 100;
    for (String dataType : dataTypes) {
      for (int numBands = 1; numBands < 5; numBands++) {
        GridCoverage2D testRaster =
            RasterConstructors.makeEmptyRaster(
                numBands, dataType, width, height, 0, 0, 1, -1, 0, 0, 0);
        double[] bandValues = new double[width * height];
        for (int k = 0; k < numBands; k++) {
          for (int i = 0; i < bandValues.length; i++) {
            bandValues[i] = k + i;
          }
          testRaster = MapAlgebra.addBandFromArray(testRaster, bandValues, k + 1);
        }
        String htmlString = RasterOutputs.createHTMLString(testRaster, 50);
        String expectedStart = "<img src=\"data:image/png;base64,iVBORw0K";
        String expectedEnd = "width=\"50\" />";
        assertTrue(htmlString.startsWith(expectedStart));
        assertTrue(htmlString.endsWith(expectedEnd));
      }
    }
  }

  // ---- RS_AsCOG / asCOG tests ----

  @Test
  public void testAsCOGDefaults() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cogBytes = RasterOutputs.asCOG(raster);
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length >= 2);
    // Verify it is a valid TIFF (starts with II or MM)
    assertTrue(
        (cogBytes[0] == 'I' && cogBytes[1] == 'I') || (cogBytes[0] == 'M' && cogBytes[1] == 'M'));
  }

  @Test
  public void testAsCOGRoundTrip() throws IOException {
    GridCoverage2D original = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cogBytes = RasterOutputs.asCOG(original, "LZW", 256);
    // Read COG bytes back as a raster via GeoTiff reader
    GridCoverage2D roundTripped = RasterConstructors.fromGeoTiff(cogBytes);
    assertNotNull(roundTripped);
    // Verify envelope is preserved
    assertEquals(original.getEnvelope2D().toString(), roundTripped.getEnvelope2D().toString());
    // Verify dimensions are preserved
    assertEquals(
        original.getRenderedImage().getWidth(), roundTripped.getRenderedImage().getWidth());
    assertEquals(
        original.getRenderedImage().getHeight(), roundTripped.getRenderedImage().getHeight());
    // Verify number of bands is preserved
    assertEquals(original.getNumSampleDimensions(), roundTripped.getNumSampleDimensions());
  }

  @Test
  public void testAsCOGWithCompression() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cogLZW = RasterOutputs.asCOG(raster, "LZW");
    byte[] cogDeflate = RasterOutputs.asCOG(raster, "Deflate");
    assertNotNull(cogLZW);
    assertNotNull(cogDeflate);
    assertTrue(cogLZW.length > 0);
    assertTrue(cogDeflate.length > 0);
    // Different compressions should produce different sizes
    assertNotEquals(cogLZW.length, cogDeflate.length);
  }

  @Test
  public void testAsCOGWithCompressionAndTileSize() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cog256 = RasterOutputs.asCOG(raster, "Deflate", 256);
    byte[] cog512 = RasterOutputs.asCOG(raster, "Deflate", 512);
    assertNotNull(cog256);
    assertNotNull(cog512);
    assertTrue(cog256.length > 0);
    assertTrue(cog512.length > 0);
  }

  @Test
  public void testAsCOGWithCompressionTileSizeAndQuality() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cogHighQ = RasterOutputs.asCOG(raster, "Deflate", 256, 1.0);
    byte[] cogLowQ = RasterOutputs.asCOG(raster, "Deflate", 256, 0.1);
    assertNotNull(cogHighQ);
    assertNotNull(cogLowQ);
    assertTrue(cogHighQ.length > 0);
    assertTrue(cogLowQ.length > 0);
  }

  @Test
  public void testAsCOGWithResampling() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cog = RasterOutputs.asCOG(raster, "Deflate", 256, 0.2, "Bilinear");
    assertNotNull(cog);
    assertTrue(cog.length > 0);
  }

  @Test
  public void testAsCOGAllArgs() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    byte[] cog = RasterOutputs.asCOG(raster, "LZW", 256, 0.5, "Nearest", 2);
    assertNotNull(cog);
    assertTrue(cog.length > 0);
  }

  @Test
  public void testAsCOGCaseInsensitive() throws IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
    // compression and resampling should be case-insensitive
    byte[] cog = RasterOutputs.asCOG(raster, "lzw", 256, 0.5, "bilinear", 2);
    assertNotNull(cog);
    assertTrue(cog.length > 0);
    // uppercase
    byte[] cog2 = RasterOutputs.asCOG(raster, "DEFLATE", 256, 0.5, "NEAREST", 2);
    assertNotNull(cog2);
    assertTrue(cog2.length > 0);
    // mixed case: packbits
    byte[] cog3 = RasterOutputs.asCOG(raster, "packbits");
    assertNotNull(cog3);
    assertTrue(cog3.length > 0);
  }
}
