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
package org.apache.sedona.common.raster.serde;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import org.apache.sedona.common.raster.RasterConstructors;
import org.apache.sedona.common.raster.RasterConstructorsForTesting;
import org.apache.sedona.common.raster.RasterTestBase;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.referencing.FactoryException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.junit.Assert;
import org.junit.Test;

public class SerdeTest extends RasterTestBase {

  private static final String[] testFilePaths = {
    resourceFolder + "/raster/test1.tiff",
    resourceFolder + "/raster/test2.tiff",
    resourceFolder + "/raster/test3.tif",
    resourceFolder + "/raster_geotiff_color/FAA_UTM18N_NAD83.tif"
  };

  @Test
  public void testRoundTripSerdeSingleBandRaster() throws IOException, ClassNotFoundException {
    testRoundTrip(oneBandRaster);
  }

  @Test
  public void testRoundTripSerdeMultiBandRaster() throws IOException, ClassNotFoundException {
    testRoundTrip(multiBandRaster);
  }

  @Test
  public void testInDbRaster() throws IOException, ClassNotFoundException {
    for (String testFilePath : testFilePaths) {
      GeoTiffReader reader = new GeoTiffReader(new File(testFilePath));
      GridCoverage2D raster = reader.read(null);
      testRoundTrip(raster);
    }
  }

  @Test
  public void testNorthPoleRaster() throws IOException, ClassNotFoundException, FactoryException {
    // If we are not using non-strict mode to serializing CRS, this will raise an exception:
    // org.geotools.referencing.wkt.UnformattableObjectException: This "AxisDirection" object is too
    // complex for
    // WKT syntax.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(
            1, "B", 256, 256, -345000.000, 345000.000, 2000, -2000, 0, 0, 3996);
    testRoundTrip(raster);
  }

  private GridCoverage2D testRoundTrip(GridCoverage2D raster)
      throws IOException, ClassNotFoundException {
    return testRoundTrip(raster, 10);
  }

  private GridCoverage2D testRoundTrip(GridCoverage2D raster, int density)
      throws IOException, ClassNotFoundException {
    byte[] bytes = Serde.serialize(raster);
    GridCoverage2D roundTripRaster = Serde.deserialize(bytes);
    assertNotNull(roundTripRaster);
    assertSameCoverage(raster, roundTripRaster, density);
    bytes = Serde.serialize(roundTripRaster);
    roundTripRaster = Serde.deserialize(bytes);
    assertSameCoverage(raster, roundTripRaster, density);
    return roundTripRaster;
  }

  @Test
  public void testDeserializeWithMismatchedColorModel() throws Exception {
    // Simulate what Python UDFs produce: a serialized raster where the colorModel blob
    // describes 4 bands but the SampleModel/DataBuffer contain only 1 band. Before the
    // ColorModel reconciliation fix in DeepCopiedRenderedImage.read(), this would fail with:
    // IllegalArgumentException: "The specified ColorModel is incompatible with the image
    // SampleModel"
    // when GridCoverageFactory.create() wraps the image in RenderedImageAdapter → PlanarImage.

    // 1. Create a 4-band raster and a 1-band raster with the same spatial dimensions
    GridCoverage2D raster4band =
        RasterConstructorsForTesting.makeRasterForTesting(
            4, "F", "BandedSampleModel", 4, 3, 100, 100, 10, -10, 0, 0, 3857);
    GridCoverage2D raster1band =
        RasterConstructorsForTesting.makeRasterForTesting(
            1, "F", "BandedSampleModel", 4, 3, 100, 100, 10, -10, 0, 0, 3857);

    // 2. Serialize both
    byte[] bytes4 = Serde.serialize(raster4band);
    byte[] bytes1 = Serde.serialize(raster1band);

    // 3. Locate the colorModel region in both byte arrays using UnsafeInput to navigate
    //    the Kryo-encoded stream (same approach as Serde.extractPixelBanks).
    int[] cmRegion4 = findColorModelRegion(bytes4);
    int cmOffset4 = cmRegion4[0];
    int cmEnd4 = cmRegion4[1];

    int[] cmRegion1 = findColorModelRegion(bytes1);
    int cmOffset1 = cmRegion1[0];
    int cmEnd1 = cmRegion1[1];

    // 4. Build spliced bytes: bytes1[0..cmOffset1) + bytes4[cmOffset4..cmEnd4) + bytes1[cmEnd1..)
    //    This gives us: 1-band metadata, 4-band colorModel blob, 1-band raster data
    byte[] spliced = new byte[cmOffset1 + (cmEnd4 - cmOffset4) + (bytes1.length - cmEnd1)];
    System.arraycopy(bytes1, 0, spliced, 0, cmOffset1);
    System.arraycopy(bytes4, cmOffset4, spliced, cmOffset1, cmEnd4 - cmOffset4);
    System.arraycopy(
        bytes1, cmEnd1, spliced, cmOffset1 + (cmEnd4 - cmOffset4), bytes1.length - cmEnd1);

    // 5. Deserialize — this would throw IllegalArgumentException before the fix
    GridCoverage2D deserialized = Serde.deserialize(spliced);
    assertNotNull(deserialized);

    // 6. Verify the deserialized raster has correct structure
    Assert.assertEquals(1, deserialized.getNumSampleDimensions());
    java.awt.image.Raster deserializedRaster =
        RasterUtils.getRaster(deserialized.getRenderedImage());
    Assert.assertEquals(1, deserializedRaster.getNumBands());
    Assert.assertEquals(4, deserializedRaster.getWidth());
    Assert.assertEquals(3, deserializedRaster.getHeight());

    // Verify pixel values are from the 1-band raster (band=0, pixel[y][x] = y*4+x)
    for (int y = 0; y < 3; y++) {
      for (int x = 0; x < 4; x++) {
        double expected = y * 4.0 + x;
        Assert.assertEquals(
            "Pixel mismatch at x=" + x + " y=" + y,
            expected,
            deserializedRaster.getSampleDouble(x, y, 0),
            1e-6);
      }
    }

    // 7. Verify the raster can be re-serialized (proves the colorModel is now valid)
    byte[] reserialized = Serde.serialize(deserialized);
    GridCoverage2D reDeserialized = Serde.deserialize(reserialized);
    assertNotNull(reDeserialized);
    Assert.assertEquals(1, reDeserialized.getNumSampleDimensions());

    reDeserialized.dispose(true);
    deserialized.dispose(true);
    raster4band.dispose(true);
    raster1band.dispose(true);
  }

  /**
   * Find the byte range [startOffset, endOffset) of the colorModel length-prefixed section in a
   * serialized IN_DB raster. Uses Kryo's UnsafeInput to navigate the stream correctly, mirroring
   * the approach in {@link Serde#extractPixelBanks(byte[])}.
   *
   * @return int[2] where [0]=start offset (at length prefix), [1]=end offset (past data)
   */
  private int[] findColorModelRegion(byte[] bytes) {
    try (com.esotericsoftware.kryo.io.UnsafeInput in =
        new com.esotericsoftware.kryo.io.UnsafeInput(bytes)) {

      // Skip rasterType byte
      in.readByte();

      // Skip name (UTF-8 string: int length + bytes)
      KryoUtil.skipUTF8String(in);

      // Skip gridEnvelope2D (4 ints = 16 bytes)
      in.skip(16);

      // Skip affine transform (6 doubles = 48 bytes)
      in.skip(48);

      // Skip CRS (length-prefixed bytes)
      int crsLength = in.readInt();
      in.skip(crsLength);

      // Read bandCount and skip GridSampleDimensions
      int bandCount = in.readInt();
      for (int i = 0; i < bandCount; i++) {
        GridSampleDimensionSerializer.skip(in);
      }

      // Skip DeepCopiedRenderedImage header (minX, minY, width, height = 4 ints)
      in.skip(16);

      // Skip properties (length-prefixed via writeObjectWithLength)
      int propsLength = in.readInt();
      in.skip(propsLength);

      // Now at the colorModel length prefix
      int cmStart = in.position();
      int cmDataLength = in.readInt();
      in.skip(cmDataLength);
      int cmEnd = in.position();

      return new int[] {cmStart, cmEnd};
    }
  }
}
