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
package org.apache.sedona.common.raster.cog;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.sedona.common.raster.MapAlgebra;
import org.apache.sedona.common.raster.RasterConstructors;
import org.apache.sedona.common.raster.RasterOutputs;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;

public class CogWriterTest {

  private static final String resourceFolder =
      System.getProperty("user.dir") + "/../spark/common/src/test/resources/";

  private GridCoverage2D rasterFromGeoTiff(String filePath) throws IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(filePath));
    return RasterConstructors.fromGeoTiff(bytes);
  }

  @Test
  public void testComputeOverviewDecimations() {
    // 1000x1000 with blockSize=256: ceil(log2(1000/256)) = ceil(1.97) = 2 levels -> [2, 4]
    List<Integer> decimations = CogWriter.computeOverviewDecimations(1000, 1000, 256);
    assertEquals(2, decimations.size());
    assertEquals(Integer.valueOf(2), decimations.get(0));
    assertEquals(Integer.valueOf(4), decimations.get(1));

    // 10000x10000 with blockSize=256: ceil(log2(10000/256)) = ceil(5.29) = 6 levels
    decimations = CogWriter.computeOverviewDecimations(10000, 10000, 256);
    assertEquals(6, decimations.size());
    assertEquals(Integer.valueOf(2), decimations.get(0));
    assertEquals(Integer.valueOf(4), decimations.get(1));
    assertEquals(Integer.valueOf(8), decimations.get(2));
    assertEquals(Integer.valueOf(16), decimations.get(3));
    assertEquals(Integer.valueOf(32), decimations.get(4));
    assertEquals(Integer.valueOf(64), decimations.get(5));

    // Very small image: 50x50 with blockSize=256 -> no overviews
    decimations = CogWriter.computeOverviewDecimations(50, 50, 256);
    assertEquals(0, decimations.size());

    // Exactly one tile: 256x256 with blockSize=256 -> no overviews
    decimations = CogWriter.computeOverviewDecimations(256, 256, 256);
    assertEquals(0, decimations.size());
  }

  @Test
  public void testGenerateOverview() {
    // Create a 100x100 single-band raster
    double[] bandValues = new double[100 * 100];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = i % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 100, 100, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    // Downsample by factor of 2
    GridCoverage2D overview = CogWriter.generateOverview(raster, 2);
    assertNotNull(overview);
    assertEquals(50, overview.getRenderedImage().getWidth());
    assertEquals(50, overview.getRenderedImage().getHeight());
  }

  @Test
  public void testWriteSmallRasterAsCog() throws IOException {
    // Create a small raster (no overviews expected due to small size)
    double[] bandValues = new double[50 * 50];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = i % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 50, 50, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    byte[] cogBytes = RasterOutputs.asCloudOptimizedGeoTiff(raster, CogOptions.defaults());
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    // Verify it's a valid TIFF
    assertTrue(
        (cogBytes[0] == 'I' && cogBytes[1] == 'I') || (cogBytes[0] == 'M' && cogBytes[1] == 'M'));

    // Verify it can be read back
    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertNotNull(readBack);
    assertEquals(50, readBack.getRenderedImage().getWidth());
    assertEquals(50, readBack.getRenderedImage().getHeight());
  }

  @Test
  public void testWriteMediumRasterAsCog() throws IOException {
    // Create a 512x512 raster (should produce overviews with 256 tile size)
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 7) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    byte[] cogBytes =
        RasterOutputs.asCloudOptimizedGeoTiff(
            raster, CogOptions.builder().compression("Deflate").compressionQuality(0.5).build());
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    // Verify COG structure: IFDs should be at the beginning of the file
    ByteOrder byteOrder = (cogBytes[0] == 'I') ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    ByteBuffer buf = ByteBuffer.wrap(cogBytes).order(byteOrder);

    // First IFD should be at offset 8 (right after header)
    int firstIfdOffset = buf.getInt(4);
    assertEquals(8, firstIfdOffset);

    // Read first IFD tag count
    int tagCount = buf.getShort(firstIfdOffset) & 0xFFFF;
    assertTrue("First IFD should have tags", tagCount > 0);

    // Check that nextIFDOffset points to another IFD (should have at least 1 overview)
    int nextIfdPointerPos = firstIfdOffset + 2 + tagCount * 12;
    int nextIfdOffset = buf.getInt(nextIfdPointerPos);
    // For a 512x512 image with 256 tile size, we expect at least one overview
    assertTrue("Should have at least one overview IFD", nextIfdOffset > 0);
    // The next IFD should be before any image data (COG requirement).
    // Find the minimum TileOffset to use as an upper bound.
    int minTileOffset = Integer.MAX_VALUE;
    for (int i = 0; i < tagCount; i++) {
      int entryOffset = firstIfdOffset + 2 + i * 12;
      int tag = buf.getShort(entryOffset) & 0xFFFF;
      if (tag == 324 || tag == 273) { // TileOffsets or StripOffsets
        int count = buf.getInt(entryOffset + 4);
        if (count == 1) {
          minTileOffset = Math.min(minTileOffset, buf.getInt(entryOffset + 8));
        } else {
          int arrayOffset = buf.getInt(entryOffset + 8);
          for (int j = 0; j < count; j++) {
            minTileOffset = Math.min(minTileOffset, buf.getInt(arrayOffset + j * 4));
          }
        }
      }
    }
    assertTrue("Overview IFD should be before image data", nextIfdOffset < minTileOffset);

    // Verify it can be read back by GeoTools
    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertNotNull(readBack);
    assertEquals(512, readBack.getRenderedImage().getWidth());
    assertEquals(512, readBack.getRenderedImage().getHeight());

    // Verify pixel values are preserved
    double[] originalValues = MapAlgebra.bandAsArray(raster, 1);
    double[] readBackValues = MapAlgebra.bandAsArray(readBack, 1);
    assertArrayEquals(originalValues, readBackValues, 0.01);
  }

  @Test
  public void testWriteMultibandRasterAsCog() throws IOException {
    // Create a 3-band 256x256 raster
    int width = 256;
    int height = 256;
    int numBands = 3;
    double[][] bandData = new double[numBands][width * height];
    for (int b = 0; b < numBands; b++) {
      for (int i = 0; i < width * height; i++) {
        bandData[b][i] = (i * (b + 1)) % 256;
      }
    }

    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            numBands, "b", width, height, 0, 0, 1, -1, 0, 0, 4326, bandData);

    byte[] cogBytes = RasterOutputs.asCloudOptimizedGeoTiff(raster, CogOptions.defaults());
    assertNotNull(cogBytes);

    // Verify it can be read back
    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertNotNull(readBack);
    assertEquals(width, readBack.getRenderedImage().getWidth());
    assertEquals(height, readBack.getRenderedImage().getHeight());
  }

  @Test
  public void testWriteWithLZWCompression() throws IOException {
    double[] bandValues = new double[100 * 100];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = i % 10; // Highly compressible
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 100, 100, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    byte[] cogBytes =
        RasterOutputs.asCloudOptimizedGeoTiff(
            raster, CogOptions.builder().compression("LZW").compressionQuality(0.5).build());
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertNotNull(readBack);
  }

  @Test
  public void testCogFromExistingGeoTiff() throws IOException {
    // Test with a real GeoTIFF file from test resources
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");

    byte[] cogBytes = RasterOutputs.asCloudOptimizedGeoTiff(raster, CogOptions.defaults());
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    // Verify it can be read back
    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertNotNull(readBack);
    assertEquals(raster.getRenderedImage().getWidth(), readBack.getRenderedImage().getWidth());
    assertEquals(raster.getRenderedImage().getHeight(), readBack.getRenderedImage().getHeight());
  }

  @Test
  public void testTiffIfdParser() throws IOException {
    // Write a tiled GeoTIFF and parse it
    double[] bandValues = new double[256 * 256];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = i % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 256, 256, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    byte[] tiffBytes = RasterOutputs.asGeoTiff(raster, "Deflate", 0.5);

    TiffIfdParser.ParsedTiff parsed = TiffIfdParser.parse(tiffBytes);
    assertNotNull(parsed);
    assertTrue(parsed.tagCount > 0);
    assertTrue(parsed.imageDataLength > 0);
    assertTrue(parsed.ifdEntries.length == parsed.tagCount * 12);
  }

  @Test
  public void testOverviewIfdHasNewSubfileType() throws IOException {
    // Create a 512x512 raster that will have at least one overview
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 3) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    byte[] cogBytes = RasterOutputs.asCloudOptimizedGeoTiff(raster, CogOptions.defaults());
    ByteOrder byteOrder = (cogBytes[0] == 'I') ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    ByteBuffer buf = ByteBuffer.wrap(cogBytes).order(byteOrder);

    // Navigate to second IFD (first overview)
    int firstIfdOffset = buf.getInt(4);
    int tagCount0 = buf.getShort(firstIfdOffset) & 0xFFFF;
    int nextIfdPointerPos = firstIfdOffset + 2 + tagCount0 * 12;
    int secondIfdOffset = buf.getInt(nextIfdPointerPos);
    assertTrue("Should have at least one overview IFD", secondIfdOffset > 0);

    // Scan second IFD for NewSubfileType tag (254)
    int tagCount1 = buf.getShort(secondIfdOffset) & 0xFFFF;
    boolean foundNewSubfileType = false;
    int newSubfileTypeValue = -1;
    for (int i = 0; i < tagCount1; i++) {
      int entryOffset = secondIfdOffset + 2 + i * 12;
      int tag = buf.getShort(entryOffset) & 0xFFFF;
      if (tag == 254) {
        foundNewSubfileType = true;
        newSubfileTypeValue = buf.getInt(entryOffset + 8);
        break;
      }
    }
    assertTrue("Overview IFD must contain NewSubfileType tag (254)", foundNewSubfileType);
    assertEquals("NewSubfileType must be 1 (ReducedImage)", 1, newSubfileTypeValue);
  }

  @Test
  public void testInvalidInputParameters() {
    double[] bandValues = new double[50 * 50];
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 50, 50, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    // Invalid compression type
    try {
      CogOptions.builder().compression("ZSTD").build();
      fail("Expected IllegalArgumentException for invalid compression");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("compression must be one of"));
    }

    // compressionQuality > 1
    try {
      CogOptions.builder().compression("Deflate").compressionQuality(1.5).tileSize(256).build();
      fail("Expected IllegalArgumentException for compressionQuality > 1");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("compressionQuality"));
    }

    // tileSize <= 0
    try {
      CogOptions.builder().compression("Deflate").compressionQuality(0.5).tileSize(0).build();
      fail("Expected IllegalArgumentException for tileSize <= 0");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("tileSize"));
    }

    // tileSize not power of 2
    try {
      CogOptions.builder().compression("Deflate").compressionQuality(0.5).tileSize(100).build();
      fail("Expected IllegalArgumentException for non-power-of-2 tileSize");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("power of 2"));
    }
  }

  @Test
  public void testParserRejectsMalformedTiff() {
    // Too short
    try {
      TiffIfdParser.parse(new byte[] {0, 0, 0});
      fail("Expected IllegalArgumentException for short input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("too short"));
    }

    // Invalid byte order marker
    try {
      TiffIfdParser.parse(new byte[] {'X', 'X', 0, 42, 0, 0, 0, 8});
      fail("Expected IllegalArgumentException for invalid byte order");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("byte order"));
    }

    // Valid header but IFD offset points beyond file
    byte[] badOffset = new byte[16];
    badOffset[0] = 'I';
    badOffset[1] = 'I';
    badOffset[2] = 42;
    badOffset[3] = 0;
    // IFD offset = 9999 (way beyond file)
    ByteBuffer b = ByteBuffer.wrap(badOffset).order(ByteOrder.LITTLE_ENDIAN);
    b.putInt(4, 9999);
    try {
      TiffIfdParser.parse(badOffset);
      fail("Expected IllegalArgumentException for out-of-range IFD offset");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("out of range"));
    }
  }

  @Test
  public void testCogTileOffsetsAreForwardPointing() throws IOException {
    // Create a raster with overviews
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 11) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    byte[] cogBytes = RasterOutputs.asCloudOptimizedGeoTiff(raster, CogOptions.defaults());
    ByteOrder byteOrder = (cogBytes[0] == 'I') ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    ByteBuffer buf = ByteBuffer.wrap(cogBytes).order(byteOrder);

    // Walk all IFDs and verify TileOffsets/StripOffsets point within file bounds
    int ifdOffset = buf.getInt(4);
    int ifdIndex = 0;
    int lastIfdEnd = 0;

    while (ifdOffset != 0) {
      int tagCount = buf.getShort(ifdOffset) & 0xFFFF;
      int ifdEnd = ifdOffset + 2 + tagCount * 12 + 4;
      lastIfdEnd = Math.max(lastIfdEnd, ifdEnd);

      for (int i = 0; i < tagCount; i++) {
        int entryOffset = ifdOffset + 2 + i * 12;
        int tag = buf.getShort(entryOffset) & 0xFFFF;
        int fieldType = buf.getShort(entryOffset + 2) & 0xFFFF;
        int count = buf.getInt(entryOffset + 4);

        // Check TileOffsets (324) or StripOffsets (273)
        if (tag == 324 || tag == 273) {
          if (count == 1) {
            int offset = buf.getInt(entryOffset + 8);
            assertTrue(
                "IFD " + ifdIndex + ": TileOffset " + offset + " must be within file",
                offset >= 0 && offset < cogBytes.length);
          } else {
            // Offsets stored in overflow area
            int arrayOffset = buf.getInt(entryOffset + 8);
            for (int j = 0; j < count; j++) {
              int tileOffset = buf.getInt(arrayOffset + j * 4);
              assertTrue(
                  "IFD " + ifdIndex + " tile " + j + ": offset " + tileOffset + " out of range",
                  tileOffset >= 0 && tileOffset < cogBytes.length);
            }
          }
        }
      }

      // Read next IFD offset
      int nextIfdPointerPos = ifdOffset + 2 + tagCount * 12;
      ifdOffset = buf.getInt(nextIfdPointerPos);
      ifdIndex++;
    }

    // Verify we found at least 2 IFDs (full-res + overview)
    assertTrue("Expected at least 2 IFDs, found " + ifdIndex, ifdIndex >= 2);

    // Verify all IFDs are before image data (forward-pointing)
    // Find the minimum tile offset across all IFDs as the image data start
    int minTileOffsetGlobal = Integer.MAX_VALUE;
    int walkOffset = buf.getInt(4);
    while (walkOffset != 0) {
      int tc = buf.getShort(walkOffset) & 0xFFFF;
      for (int i = 0; i < tc; i++) {
        int eo = walkOffset + 2 + i * 12;
        int t = buf.getShort(eo) & 0xFFFF;
        if (t == 324 || t == 273) {
          int c = buf.getInt(eo + 4);
          if (c == 1) {
            minTileOffsetGlobal = Math.min(minTileOffsetGlobal, buf.getInt(eo + 8));
          } else {
            int ao = buf.getInt(eo + 8);
            for (int j = 0; j < c; j++) {
              minTileOffsetGlobal = Math.min(minTileOffsetGlobal, buf.getInt(ao + j * 4));
            }
          }
        }
      }
      walkOffset = buf.getInt(walkOffset + 2 + tc * 12);
    }
    assertTrue("IFD region should end before image data starts", lastIfdEnd <= minTileOffsetGlobal);
  }

  // --- CogOptions tests ---

  @Test
  public void testCogOptionsDefaults() {
    CogOptions opts = CogOptions.defaults();
    assertEquals("Deflate", opts.getCompression());
    assertEquals(0.2, opts.getCompressionQuality(), 0.001);
    assertEquals(256, opts.getTileSize());
    assertEquals("Nearest", opts.getResampling());
    assertEquals(-1, opts.getOverviewCount());
  }

  @Test
  public void testCogOptionsBuilder() {
    CogOptions opts =
        CogOptions.builder()
            .compression("LZW")
            .compressionQuality(0.8)
            .tileSize(512)
            .resampling("Bilinear")
            .overviewCount(3)
            .build();

    assertEquals("LZW", opts.getCompression());
    assertEquals(0.8, opts.getCompressionQuality(), 0.001);
    assertEquals(512, opts.getTileSize());
    assertEquals("Bilinear", opts.getResampling());
    assertEquals(3, opts.getOverviewCount());
  }

  @Test
  public void testCogOptionsResamplingNormalization() {
    // Case-insensitive resampling names
    assertEquals("Nearest", CogOptions.builder().resampling("nearest").build().getResampling());
    assertEquals("Bilinear", CogOptions.builder().resampling("BILINEAR").build().getResampling());
    assertEquals("Bicubic", CogOptions.builder().resampling("bicubic").build().getResampling());
  }

  @Test
  public void testCogOptionsInvalidResampling() {
    try {
      CogOptions.builder().resampling("Lanczos").build();
      fail("Expected IllegalArgumentException for invalid resampling");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("resampling"));
    }
  }

  @Test
  public void testCogOptionsInvalidTileSize() {
    try {
      CogOptions.builder().tileSize(300).build();
      fail("Expected IllegalArgumentException for non-power-of-2 tileSize");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("power of 2"));
    }
  }

  @Test
  public void testCogOptionsInvalidOverviewCount() {
    try {
      CogOptions.builder().overviewCount(-2).build();
      fail("Expected IllegalArgumentException for negative overviewCount");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("overviewCount"));
    }
  }

  @Test
  public void testWriteWithCogOptions() throws IOException {
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 7) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    CogOptions opts =
        CogOptions.builder().compression("LZW").compressionQuality(0.5).tileSize(256).build();

    byte[] cogBytes = CogWriter.write(raster, opts);
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(512, readBack.getRenderedImage().getWidth());
    assertEquals(512, readBack.getRenderedImage().getHeight());

    double[] originalValues = MapAlgebra.bandAsArray(raster, 1);
    double[] readBackValues = MapAlgebra.bandAsArray(readBack, 1);
    assertArrayEquals(originalValues, readBackValues, 0.01);
  }

  @Test
  public void testWriteWithBilinearResampling() throws IOException {
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 3) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    CogOptions opts = CogOptions.builder().resampling("Bilinear").build();
    byte[] cogBytes = CogWriter.write(raster, opts);
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    // Must be valid TIFF and readable
    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(512, readBack.getRenderedImage().getWidth());
  }

  @Test
  public void testWriteWithBicubicResampling() throws IOException {
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 5) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    CogOptions opts = CogOptions.builder().resampling("Bicubic").build();
    byte[] cogBytes = CogWriter.write(raster, opts);
    assertNotNull(cogBytes);

    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(512, readBack.getRenderedImage().getWidth());
  }

  @Test
  public void testWriteWithOverviewCountZero() throws IOException {
    double[] bandValues = new double[512 * 512];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 11) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 512, 512, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    CogOptions opts = CogOptions.builder().overviewCount(0).build();
    byte[] cogBytes = CogWriter.write(raster, opts);
    assertNotNull(cogBytes);

    // With overviewCount=0, there should be exactly 1 IFD (no overviews)
    ByteOrder byteOrder = (cogBytes[0] == 'I') ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    ByteBuffer buf = ByteBuffer.wrap(cogBytes).order(byteOrder);
    int firstIfdOffset = buf.getInt(4);
    int tagCount = buf.getShort(firstIfdOffset) & 0xFFFF;
    int nextIfdOffset = buf.getInt(firstIfdOffset + 2 + tagCount * 12);
    assertEquals("Should have no overview IFD when overviewCount=0", 0, nextIfdOffset);

    // Should still be readable
    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(512, readBack.getRenderedImage().getWidth());
  }

  @Test
  public void testWriteWithSpecificOverviewCount() throws IOException {
    // 1024x1024 with tileSize=256 would normally produce 2 overviews (2, 4)
    double[] bandValues = new double[1024 * 1024];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 13) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 1024, 1024, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    // Request only 1 overview
    CogOptions opts = CogOptions.builder().overviewCount(1).build();
    byte[] cogBytes = CogWriter.write(raster, opts);
    assertNotNull(cogBytes);

    // Count IFDs: should have exactly 2 (full-res + 1 overview)
    ByteOrder byteOrder = (cogBytes[0] == 'I') ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    ByteBuffer buf = ByteBuffer.wrap(cogBytes).order(byteOrder);

    int ifdCount = 0;
    int ifdOffset = buf.getInt(4);
    while (ifdOffset != 0) {
      ifdCount++;
      int tagCount = buf.getShort(ifdOffset) & 0xFFFF;
      ifdOffset = buf.getInt(ifdOffset + 2 + tagCount * 12);
    }
    assertEquals("Should have exactly 2 IFDs (full-res + 1 overview)", 2, ifdCount);

    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(1024, readBack.getRenderedImage().getWidth());
  }

  @Test
  public void testWriteWithTileSize512() throws IOException {
    double[] bandValues = new double[1024 * 1024];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = (i * 17) % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 1024, 1024, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    CogOptions opts = CogOptions.builder().tileSize(512).build();
    byte[] cogBytes = CogWriter.write(raster, opts);
    assertNotNull(cogBytes);
    assertTrue(cogBytes.length > 0);

    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(1024, readBack.getRenderedImage().getWidth());
  }

  @Test
  public void testRasterOutputsWithCogOptions() throws IOException {
    double[] bandValues = new double[256 * 256];
    for (int i = 0; i < bandValues.length; i++) {
      bandValues[i] = i % 256;
    }
    GridCoverage2D raster =
        RasterConstructors.makeNonEmptyRaster(
            1, "d", 256, 256, 0, 0, 1, -1, 0, 0, 4326, new double[][] {bandValues});

    CogOptions opts = CogOptions.builder().compression("LZW").overviewCount(0).build();
    byte[] cogBytes = RasterOutputs.asCloudOptimizedGeoTiff(raster, opts);
    assertNotNull(cogBytes);

    GridCoverage2D readBack = RasterConstructors.fromGeoTiff(cogBytes);
    assertEquals(256, readBack.getRenderedImage().getWidth());
  }
}
