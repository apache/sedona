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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.Arrays;
import javax.media.jai.ImageLayout;
import javax.media.jai.PlanarImage;
import javax.media.jai.TiledImage;
import org.junit.Test;

public class ByteBandRetilingImageTest {

  @Test
  public void alignedWriterSafeTilesAreReturnedByIdentity() {
    SampleModel grayModel =
        new PixelInterleavedSampleModel(DataBuffer.TYPE_BYTE, 32, 32, 1, 32, new int[] {0});
    TiledImage graySource = tiledImage(64, 64, grayModel);
    ByteBandRetilingImage grayView = new ByteBandRetilingImage(graySource, 32);
    assertSame(graySource.getTile(0, 0), grayView.getTile(0, 0));
    assertSame(graySource.getTile(1, 1), grayView.getTile(1, 1));

    SampleModel rgbModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, 32, 32, 3, 32 * 3, new int[] {0, 1, 2});
    TiledImage rgbSource = tiledImage(64, 64, rgbModel);
    ByteBandRetilingImage rgbView = new ByteBandRetilingImage(rgbSource, 32);
    assertSame(rgbSource.getTile(0, 1), rgbView.getTile(0, 1));
  }

  @Test
  public void nonCanonicalAlignedTilesAreMaterialized() {
    int tileSize = 32;
    SampleModel paddedPixelModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, tileSize, tileSize, 4, tileSize * 4, new int[] {0, 1, 2});
    DataBufferByte paddedPixelBuffer =
        new DataBufferByte(new byte[tileSize * tileSize * 4], tileSize * tileSize * 4);
    WritableRaster paddedPixelRaster =
        Raster.createWritableRaster(paddedPixelModel, paddedPixelBuffer, null);
    fillPattern(paddedPixelRaster);
    CountingSingleTileImage paddedPixelSource =
        new CountingSingleTileImage(
            paddedPixelRaster, PlanarImage.createColorModel(paddedPixelModel));

    Raster normalizedPaddedPixel =
        new ByteBandRetilingImage(paddedPixelSource, tileSize).getTile(0, 0);

    assertNotSame(paddedPixelRaster, normalizedPaddedPixel);
    assertPattern(normalizedPaddedPixel);

    SampleModel reorderedBandModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, tileSize, tileSize, 3, tileSize * 3, new int[] {0, 2, 1});
    WritableRaster reorderedBandRaster = Raster.createWritableRaster(reorderedBandModel, null);
    fillPattern(reorderedBandRaster);
    CountingSingleTileImage reorderedBandSource =
        new CountingSingleTileImage(
            reorderedBandRaster, PlanarImage.createColorModel(reorderedBandModel));

    Raster normalizedReorderedBand =
        new ByteBandRetilingImage(reorderedBandSource, tileSize).getTile(0, 0);

    assertNotSame(reorderedBandRaster, normalizedReorderedBand);
    assertPattern(normalizedReorderedBand);
  }

  @Test
  public void unsafeTilesAreMaterializedOnEveryRequest() {
    SampleModel wideModel =
        new PixelInterleavedSampleModel(DataBuffer.TYPE_BYTE, 32, 32, 1, 64, new int[] {0});
    WritableRaster wideRaster = Raster.createWritableRaster(wideModel, null);
    fillPattern(wideRaster);
    CountingSingleTileImage source =
        new CountingSingleTileImage(wideRaster, PlanarImage.createColorModel(wideModel));
    ByteBandRetilingImage view = new ByteBandRetilingImage(source, 32);

    Raster first = view.getTile(0, 0);
    Raster second = view.getTile(0, 0);

    assertNotSame(wideRaster, first);
    assertNotSame(first, second);
    assertEquals(2, source.getTileRequestCount());
    assertEquals(32, ((ComponentSampleModel) first.getSampleModel()).getScanlineStride());
    assertEquals(0, first.getDataBuffer().getOffset());
    assertPattern(first);
    assertPattern(second);
  }

  @Test
  public void offsetBackedRgbTileIsMaterializedWithoutPixelShift() {
    int width = 32;
    int height = 32;
    SampleModel model =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 3, width * 3, new int[] {0, 1, 2});
    int dataSize = width * height * 3;
    DataBufferByte buffer = new DataBufferByte(new byte[17 + dataSize], dataSize, 17);
    WritableRaster offsetRaster = Raster.createWritableRaster(model, buffer, null);
    fillPattern(offsetRaster);
    CountingSingleTileImage source =
        new CountingSingleTileImage(offsetRaster, PlanarImage.createColorModel(model));

    Raster normalized = new ByteBandRetilingImage(source, width).getTile(0, 0);

    assertNotSame(offsetRaster, normalized);
    assertEquals(0, normalized.getDataBuffer().getOffset());
    assertEquals(1, source.getTileRequestCount());
    assertPattern(normalized);
  }

  @Test
  public void interleavedBulkCopyHonorsBufferOffsetsAndPadding() {
    int width = 7;
    int height = 5;
    Point origin = new Point(10, -7);
    PixelInterleavedSampleModel sourceModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 3, 26, new int[] {0, 1, 2});
    PixelInterleavedSampleModel destinationModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 3, 29, new int[] {0, 1, 2});
    DataBufferByte sourceBuffer = offsetBuffer(13, requiredSize(sourceModel));
    DataBufferByte destinationBuffer = offsetBuffer(19, requiredSize(destinationModel));
    Arrays.fill(destinationBuffer.getData(), (byte) 0xff);
    WritableRaster source = Raster.createWritableRaster(sourceModel, sourceBuffer, origin);
    WritableRaster destination =
        Raster.createWritableRaster(destinationModel, destinationBuffer, origin);
    fillPattern(source);
    Rectangle region = new Rectangle(12, -6, 3, 3);

    assertTrue(ByteBandRetilingImage.copyRowsDirectly(source, destination, region));

    assertRegionCopied(source, destination, region, 0xff);
  }

  @Test
  public void perBandBulkCopyHonorsBankMappingsAndOffsets() {
    int width = 7;
    int height = 5;
    Point origin = new Point(10, -7);
    ComponentSampleModel sourceModel =
        new ComponentSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 1, 11, new int[] {2, 0, 1}, new int[] {3, 5, 7});
    ComponentSampleModel destinationModel =
        new ComponentSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 1, 13, new int[] {1, 2, 0}, new int[] {9, 4, 6});
    DataBufferByte sourceBuffer = bankedBuffer(new int[] {11, 13, 17});
    DataBufferByte destinationBuffer = bankedBuffer(new int[] {19, 23, 29});
    for (byte[] bank : destinationBuffer.getBankData()) {
      Arrays.fill(bank, (byte) 0xff);
    }
    WritableRaster source = Raster.createWritableRaster(sourceModel, sourceBuffer, origin);
    WritableRaster destination =
        Raster.createWritableRaster(destinationModel, destinationBuffer, origin);
    fillPattern(source);
    Rectangle region = new Rectangle(11, -5, 4, 2);

    assertTrue(ByteBandRetilingImage.copyRowsDirectly(source, destination, region));

    assertRegionCopied(source, destination, region, 0xff);
  }

  @Test
  public void incompatibleInterleavedPackingIsRejected() {
    int width = 8;
    int height = 4;
    SampleModel sourceModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 3, width * 3, new int[] {0, 1, 2});
    WritableRaster source = Raster.createWritableRaster(sourceModel, null);
    fillPattern(source);
    Rectangle region = new Rectangle(0, 0, width, height);

    SampleModel widerPixelModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 4, width * 4, new int[] {0, 1, 2});
    WritableRaster widerPixelDestination = Raster.createWritableRaster(widerPixelModel, null);
    fillValue(widerPixelDestination, 0xff);
    assertFalse(ByteBandRetilingImage.copyRowsDirectly(source, widerPixelDestination, region));
    assertValue(widerPixelDestination, 0xff);

    SampleModel reorderedModel =
        new PixelInterleavedSampleModel(
            DataBuffer.TYPE_BYTE, width, height, 3, width * 3, new int[] {2, 1, 0});
    WritableRaster reorderedDestination = Raster.createWritableRaster(reorderedModel, null);
    fillValue(reorderedDestination, 0xff);
    assertFalse(ByteBandRetilingImage.copyRowsDirectly(source, reorderedDestination, region));
    assertValue(reorderedDestination, 0xff);
  }

  @Test
  public void packedByteTilesUseTheAccessorFallback() {
    int width = 32;
    int height = 32;
    SampleModel packedModel =
        new MultiPixelPackedSampleModel(DataBuffer.TYPE_BYTE, width, height, 1);
    WritableRaster sourceRaster = Raster.createWritableRaster(packedModel, null);
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        sourceRaster.setSample(x, y, 0, (x + y) & 1);
      }
    }
    CountingSingleTileImage source =
        new CountingSingleTileImage(sourceRaster, PlanarImage.createColorModel(packedModel));

    Raster normalized = new ByteBandRetilingImage(source, width).getTile(0, 0);

    assertNotSame(sourceRaster, normalized);
    assertEquals(1, source.getTileRequestCount());
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        assertEquals(sourceRaster.getSample(x, y, 0), normalized.getSample(x, y, 0));
      }
    }
  }

  @Test
  public void translatedCrossGridTilesCopyPixelsAndZeroPadEdges() {
    int minX = -37;
    int minY = 23;
    int width = 91;
    int height = 73;
    SampleModel sourceModel =
        new PixelInterleavedSampleModel(DataBuffer.TYPE_BYTE, 17, 13, 1, 17, new int[] {0});
    TiledImage source =
        new TiledImage(
            minX,
            minY,
            width,
            height,
            -29,
            30,
            sourceModel,
            PlanarImage.createColorModel(sourceModel));
    WritableRaster sourceValues =
        Raster.createWritableRaster(
            sourceModel.createCompatibleSampleModel(width, height), new Point(minX, minY));
    fillPattern(sourceValues);
    source.setData(sourceValues);
    ByteBandRetilingImage view = new ByteBandRetilingImage(source, 32);
    Rectangle imageBounds = new Rectangle(minX, minY, width, height);

    for (int tileY = view.getMinTileY(); tileY <= view.getMaxTileY(); tileY++) {
      for (int tileX = view.getMinTileX(); tileX <= view.getMaxTileX(); tileX++) {
        Raster tile = view.getTile(tileX, tileY);
        assertEquals(view.tileXToX(tileX), tile.getMinX());
        assertEquals(view.tileYToY(tileY), tile.getMinY());
        assertEquals(32, tile.getWidth());
        assertEquals(32, tile.getHeight());
        Rectangle tileBounds = tile.getBounds();
        for (int y = tileBounds.y; y < tileBounds.y + tileBounds.height; y++) {
          for (int x = tileBounds.x; x < tileBounds.x + tileBounds.width; x++) {
            int expected = imageBounds.contains(x, y) ? expected(x, y, 0) : 0;
            assertEquals(expected, tile.getSample(x, y, 0));
          }
        }
      }
    }
  }

  private static TiledImage tiledImage(int width, int height, SampleModel model) {
    return new TiledImage(0, 0, width, height, 0, 0, model, PlanarImage.createColorModel(model));
  }

  private static DataBufferByte offsetBuffer(int offset, int size) {
    return new DataBufferByte(new byte[offset + size], size, offset);
  }

  private static DataBufferByte bankedBuffer(int[] offsets) {
    byte[][] banks = new byte[offsets.length][256];
    return new DataBufferByte(banks, 200, offsets);
  }

  private static int requiredSize(ComponentSampleModel model) {
    return model.getScanlineStride() * (model.getHeight() - 1)
        + model.getPixelStride() * model.getWidth();
  }

  private static void fillPattern(WritableRaster raster) {
    Rectangle bounds = raster.getBounds();
    for (int y = bounds.y; y < bounds.y + bounds.height; y++) {
      for (int x = bounds.x; x < bounds.x + bounds.width; x++) {
        for (int band = 0; band < raster.getNumBands(); band++) {
          raster.setSample(x, y, band, expected(x, y, band));
        }
      }
    }
  }

  private static void fillValue(WritableRaster raster, int value) {
    Rectangle bounds = raster.getBounds();
    for (int y = bounds.y; y < bounds.y + bounds.height; y++) {
      for (int x = bounds.x; x < bounds.x + bounds.width; x++) {
        for (int band = 0; band < raster.getNumBands(); band++) {
          raster.setSample(x, y, band, value);
        }
      }
    }
  }

  private static void assertPattern(Raster raster) {
    Rectangle bounds = raster.getBounds();
    for (int y = bounds.y; y < bounds.y + bounds.height; y++) {
      for (int x = bounds.x; x < bounds.x + bounds.width; x++) {
        for (int band = 0; band < raster.getNumBands(); band++) {
          assertEquals(expected(x, y, band), raster.getSample(x, y, band));
        }
      }
    }
  }

  private static void assertValue(Raster raster, int expected) {
    Rectangle bounds = raster.getBounds();
    for (int y = bounds.y; y < bounds.y + bounds.height; y++) {
      for (int x = bounds.x; x < bounds.x + bounds.width; x++) {
        for (int band = 0; band < raster.getNumBands(); band++) {
          assertEquals(expected, raster.getSample(x, y, band));
        }
      }
    }
  }

  private static void assertRegionCopied(
      Raster source, Raster destination, Rectangle region, int outsideValue) {
    Rectangle bounds = destination.getBounds();
    for (int y = bounds.y; y < bounds.y + bounds.height; y++) {
      for (int x = bounds.x; x < bounds.x + bounds.width; x++) {
        for (int band = 0; band < destination.getNumBands(); band++) {
          int expected = region.contains(x, y) ? source.getSample(x, y, band) : outsideValue;
          assertEquals(expected, destination.getSample(x, y, band));
        }
      }
    }
  }

  private static int expected(int x, int y, int band) {
    return Math.floorMod(31 * x + 17 * y + 73 * band, 251);
  }

  private static final class CountingSingleTileImage extends PlanarImage {
    private final Raster tile;
    private int tileRequestCount;

    CountingSingleTileImage(Raster tile, ColorModel colorModel) {
      super(
          new ImageLayout(
              tile.getMinX(),
              tile.getMinY(),
              tile.getWidth(),
              tile.getHeight(),
              tile.getMinX(),
              tile.getMinY(),
              tile.getWidth(),
              tile.getHeight(),
              tile.getSampleModel(),
              colorModel),
          null,
          null);
      this.tile = tile;
    }

    @Override
    public Raster getTile(int tileX, int tileY) {
      tileRequestCount++;
      return tile;
    }

    int getTileRequestCount() {
      return tileRequestCount;
    }
  }
}
