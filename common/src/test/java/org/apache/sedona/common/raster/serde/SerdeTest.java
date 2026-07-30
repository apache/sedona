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
import java.util.Arrays;
import org.apache.sedona.common.raster.RasterBandAccessors;
import org.apache.sedona.common.raster.RasterBandEditors;
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

  @Test
  public void testDeclaredNoDataValueOverridesCategories() throws Exception {
    // Simulate what Python UDFs produce: InDbSedonaRaster.with_bands() replays the source
    // raster's Kryo category blobs while declaring a different nodata value in the sample
    // dimension header. Before the reconciliation fix in GridSampleDimensionSerializer.read()
    // the declared value was discarded, so a Python UDF could not set the nodata of the
    // raster it returned.
    GridCoverage2D raster =
        RasterConstructorsForTesting.makeRasterForTesting(
            1, "D", "BandedSampleModel", 4, 3, 100, 100, 10, -10, 0, 0, 3857);
    raster = RasterBandEditors.setBandNoDataValue(raster, 1, 0.0);
    Assert.assertEquals(0.0, RasterBandAccessors.getBandNoDataValue(raster, 1), 1e-9);

    byte[] bytes = Serde.serialize(raster);

    // Sanity check: an untouched round trip keeps the categories' value, so the JVM-to-JVM
    // path is unaffected by the reconciliation.
    GridCoverage2D unpatched = Serde.deserialize(bytes);
    Assert.assertEquals(0.0, RasterBandAccessors.getBandNoDataValue(unpatched, 1), 1e-9);
    unpatched.dispose(true);

    // Overwrite only the declared double for band 0, leaving the categories describing 0.0.
    byte[] patched = bytes.clone();
    int noDataOffset = findNoDataValueOffset(patched);
    // Kryo's UnsafeOutput writes doubles in native byte order, which is what lets Python's
    // struct.pack("=ddd", ...) interoperate with this format.
    java.nio.ByteBuffer.wrap(patched)
        .order(java.nio.ByteOrder.nativeOrder())
        .putDouble(noDataOffset, -9999.0);

    GridCoverage2D deserialized = Serde.deserialize(patched);
    assertNotNull(deserialized);
    Assert.assertEquals(-9999.0, RasterBandAccessors.getBandNoDataValue(deserialized, 1), 1e-9);

    // The reconciled sample dimension must survive another round trip.
    GridCoverage2D reDeserialized = Serde.deserialize(Serde.serialize(deserialized));
    Assert.assertEquals(-9999.0, RasterBandAccessors.getBandNoDataValue(reDeserialized, 1), 1e-9);

    reDeserialized.dispose(true);
    deserialized.dispose(true);
    raster.dispose(true);
  }

  @Test
  public void testJvmRoundTripPreservesRangeNoDataAndScaleOffset() throws Exception {
    GridCoverage2D raster = makeRangeNoDataRaster(2.0, 7.0);

    GridCoverage2D roundTrip = Serde.deserialize(Serde.serialize(raster));
    assertNoDataRangeAndTransform(roundTrip, 0.0, 10.0, 2.0, 7.0);

    GridCoverage2D secondRoundTrip = Serde.deserialize(Serde.serialize(roundTrip));
    assertNoDataRangeAndTransform(secondRoundTrip, 0.0, 10.0, 2.0, 7.0);

    secondRoundTrip.dispose(true);
    roundTrip.dispose(true);
    raster.dispose(true);
  }

  @Test
  public void testMarkedOverrideAtRangeMinimumCreatesSingleton() throws Exception {
    GridCoverage2D raster = makeRangeNoDataRaster(2.0, 7.0);
    byte[] marked =
        appendNoDataOverrideTrailer(
            Serde.serialize(raster), GridSampleDimensionSerializer.NO_DATA_VALUE_OVERRIDE);

    GridCoverage2D deserialized = Serde.deserialize(marked);
    assertNoDataRangeAndTransform(deserialized, 0.0, 0.0, 2.0, 7.0);
    Assert.assertEquals(
        org.geotools.api.coverage.SampleDimensionType.REAL_64BITS,
        deserialized.getSampleDimension(0).getSampleDimensionType());

    deserialized.dispose(true);
    raster.dispose(true);
  }

  @Test
  public void testMarkedNaNClearsRangeNoData() throws Exception {
    GridCoverage2D raster = makeRangeNoDataRaster(2.0, 7.0);
    byte[] bytes = Serde.serialize(raster);
    java.nio.ByteBuffer.wrap(bytes)
        .order(java.nio.ByteOrder.nativeOrder())
        .putDouble(findNoDataValueOffset(bytes), Double.NaN);
    byte[] marked =
        appendNoDataOverrideTrailer(bytes, GridSampleDimensionSerializer.NO_DATA_VALUE_OVERRIDE);

    GridCoverage2D deserialized = Serde.deserialize(marked);
    Assert.assertNull(RasterBandAccessors.getBandNoDataValue(deserialized, 1));
    Assert.assertEquals(2.0, deserialized.getSampleDimension(0).getScale(), 0.0);
    Assert.assertEquals(7.0, deserialized.getSampleDimension(0).getOffset(), 0.0);

    deserialized.dispose(true);
    raster.dispose(true);
  }

  private GridCoverage2D makeRangeNoDataRaster(double scale, double offset) {
    org.geotools.coverage.Category nodataRange =
        new org.geotools.coverage.Category(
            org.geotools.coverage.Category.NODATA.getName(),
            new java.awt.Color(0, 0, 0, 0),
            org.geotools.util.NumberRange.create(0, true, 10, true));
    org.geotools.coverage.Category data =
        new org.geotools.coverage.Category(
            "data",
            new java.awt.Color[] {java.awt.Color.BLACK},
            org.geotools.util.NumberRange.create(11, true, 100, true));
    org.geotools.coverage.GridSampleDimension dim =
        new org.geotools.coverage.GridSampleDimension(
            "band", new org.geotools.coverage.Category[] {nodataRange, data}, scale, offset);
    GridCoverage2D pixels =
        RasterConstructorsForTesting.makeRasterForTesting(
            1, "D", "BandedSampleModel", 4, 3, 100, 100, 10, -10, 0, 0, 3857);
    return RasterUtils.create(
        pixels.getRenderedImage(),
        pixels.getGridGeometry(),
        new org.geotools.coverage.GridSampleDimension[] {dim},
        null);
  }

  private void assertNoDataRangeAndTransform(
      GridCoverage2D raster,
      double expectedMinimum,
      double expectedMaximum,
      double expectedScale,
      double expectedOffset) {
    org.geotools.coverage.GridSampleDimension dimension = raster.getSampleDimension(0);
    Assert.assertEquals(expectedScale, dimension.getScale(), 0.0);
    Assert.assertEquals(expectedOffset, dimension.getOffset(), 0.0);
    for (org.geotools.coverage.Category category : dimension.getCategories()) {
      if (category.getName().equals(org.geotools.coverage.Category.NODATA.getName())) {
        Assert.assertEquals(expectedMinimum, category.getRange().getMinimum(), 0.0);
        Assert.assertEquals(expectedMaximum, category.getRange().getMaximum(), 0.0);
        return;
      }
    }
    Assert.fail("Expected a NODATA category");
  }

  private byte[] appendNoDataOverrideTrailer(byte[] bytes, int... flags) {
    byte[] trailerMagic = {'N', 'D', 'O', '1'};
    byte[] marked = Arrays.copyOf(bytes, bytes.length + trailerMagic.length + flags.length);
    System.arraycopy(trailerMagic, 0, marked, bytes.length, trailerMagic.length);
    for (int i = 0; i < flags.length; i++) {
      marked[bytes.length + trailerMagic.length + i] = (byte) flags[i];
    }
    return marked;
  }

  @Test
  public void testReconcileKeepsExactSingleValuedNoData() {
    // The JVM-to-JVM path: declared value matches a single-valued category, untouched.
    org.geotools.coverage.GridSampleDimension dim =
        RasterUtils.createSampleDimensionWithNoDataValue("band", -9999.0);
    org.geotools.coverage.GridSampleDimension reconciled =
        GridSampleDimensionSerializer.reconcileNoDataValue(
            new GridSampleDimensionSerializer.DeclaredSampleDimension(dim, -9999.0),
            org.geotools.api.coverage.SampleDimensionType.REAL_64BITS);
    Assert.assertSame(dim, reconciled);
  }

  @Test
  public void testNoDataOverridePreservesNarrowDataCategories() {
    org.geotools.coverage.Category data =
        new org.geotools.coverage.Category(
            "source-category",
            (java.awt.Color[]) null,
            org.geotools.util.NumberRange.create(0, true, 15, true),
            true);
    org.geotools.coverage.GridSampleDimension dim =
        new org.geotools.coverage.GridSampleDimension(
            "band", new org.geotools.coverage.Category[] {data}, 1.0, 0.0);

    org.geotools.coverage.GridSampleDimension reconciled =
        GridSampleDimensionSerializer.reconcileNoDataValue(
            new GridSampleDimensionSerializer.DeclaredSampleDimension(
                dim, 20.0, GridSampleDimensionSerializer.NO_DATA_VALUE_OVERRIDE),
            org.geotools.api.coverage.SampleDimensionType.UNSIGNED_8BITS);

    Assert.assertEquals("source-category", reconciled.getCategory(10).getName().toString());
    Assert.assertEquals(
        org.geotools.coverage.Category.NODATA.getName(), reconciled.getCategory(20).getName());
    Assert.assertNull(reconciled.getCategory(200));
  }

  @Test
  public void testSampleTypeOverrideRetypesEvenWhenCategoryTypeMatches() {
    org.geotools.coverage.Category data =
        new org.geotools.coverage.Category(
            "source-category",
            (java.awt.Color[]) null,
            org.geotools.util.NumberRange.create(0, true, 255, true),
            true);
    org.geotools.coverage.GridSampleDimension dim =
        new org.geotools.coverage.GridSampleDimension(
            "band", new org.geotools.coverage.Category[] {data}, 1.0, 0.0);
    Assert.assertEquals(
        org.geotools.api.coverage.SampleDimensionType.UNSIGNED_8BITS, dim.getSampleDimensionType());

    org.geotools.coverage.GridSampleDimension reconciled =
        GridSampleDimensionSerializer.reconcileNoDataValue(
            new GridSampleDimensionSerializer.DeclaredSampleDimension(
                dim, Double.NaN, GridSampleDimensionSerializer.SAMPLE_TYPE_OVERRIDE),
            org.geotools.api.coverage.SampleDimensionType.UNSIGNED_8BITS);

    Assert.assertNotSame(dim, reconciled);
    Assert.assertEquals("band", reconciled.getCategory(200).getName().toString());
  }

  @Test
  public void testRasterWithoutNoDataStillHasNone() throws Exception {
    // A raster with no nodata declares NaN, which must not be turned into a nodata category.
    GridCoverage2D raster =
        RasterConstructorsForTesting.makeRasterForTesting(
            1, "D", "BandedSampleModel", 4, 3, 100, 100, 10, -10, 0, 0, 3857);
    Assert.assertNull(RasterBandAccessors.getBandNoDataValue(raster, 1));

    GridCoverage2D roundTrip = Serde.deserialize(Serde.serialize(raster));
    Assert.assertNull(RasterBandAccessors.getBandNoDataValue(roundTrip, 1));

    roundTrip.dispose(true);
    raster.dispose(true);
  }

  /**
   * Find the byte offset of band 0's declared noDataValue double in a serialized IN_DB raster. The
   * per-band layout written by {@link GridSampleDimensionSerializer#write} is description, offset,
   * scale, noDataValue, categories.
   */
  private int findNoDataValueOffset(byte[] bytes) {
    try (com.esotericsoftware.kryo.io.UnsafeInput in =
        new com.esotericsoftware.kryo.io.UnsafeInput(bytes)) {
      in.readByte(); // rasterType
      KryoUtil.skipUTF8String(in); // name
      in.skip(16); // gridEnvelope2D
      in.skip(48); // affine transform
      in.skip(in.readInt()); // CRS
      in.readInt(); // bandCount
      KryoUtil.skipUTF8String(in); // band 0 description
      in.skip(16); // band 0 offset + scale
      return in.position();
    }
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
