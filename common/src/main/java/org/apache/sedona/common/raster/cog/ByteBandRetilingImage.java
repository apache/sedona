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

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.Arrays;
import javax.media.jai.ImageLayout;
import javax.media.jai.PlanarImage;

/**
 * A non-caching retiling view that shields the TIFF writer from unsafe byte-band tile layouts.
 *
 * <p>When the source tile grid already matches the output grid, concrete tiles that are safe for
 * imageio-ext's direct-buffer path are returned by identity. All other tiles are materialized on
 * demand into fresh rasters; the view never retains them, so it cannot accumulate a second
 * uncompressed copy of the image as {@link javax.media.jai.TiledImage} would.
 *
 * <p>The source is read directly with {@link RenderedImage#getTile(int, int)} and copied with
 * explicit {@link DataBuffer} offsets. This view never reads the source through {@link
 * PlanarImage#getData(Rectangle)} or copies it with {@link WritableRaster#setRect(Raster)} because
 * those bulk paths can drop a positive {@link DataBufferByte} offset. Standard byte component
 * layouts use row-wise array copies; other layouts fall back to offset-aware {@link SampleModel}
 * accessors.
 */
final class ByteBandRetilingImage extends PlanarImage {
  private final RenderedImage source;
  private final boolean sameTileGrid;

  ByteBandRetilingImage(RenderedImage source, int tileSize) {
    super(
        new ImageLayout(
            source.getMinX(),
            source.getMinY(),
            source.getWidth(),
            source.getHeight(),
            source.getMinX(),
            source.getMinY(),
            tileSize,
            tileSize,
            source.getSampleModel().createCompatibleSampleModel(tileSize, tileSize),
            source.getColorModel()),
        null,
        null);
    this.source = source;
    this.sameTileGrid =
        source.getTileWidth() == tileSize
            && source.getTileHeight() == tileSize
            && source.getTileGridXOffset() == source.getMinX()
            && source.getTileGridYOffset() == source.getMinY();
  }

  @Override
  public Raster getTile(int tileX, int tileY) {
    Raster alignedSourceTile = null;
    if (sameTileGrid) {
      alignedSourceTile = source.getTile(tileX, tileY);
      if (isWriterSafe(alignedSourceTile)) {
        return alignedSourceTile;
      }
    }

    WritableRaster tile =
        Raster.createWritableRaster(getSampleModel(), new Point(tileXToX(tileX), tileYToY(tileY)));
    Rectangle bounds = tile.getBounds().intersection(getBounds());
    if (bounds.isEmpty()) {
      return tile;
    }

    if (alignedSourceTile != null) {
      Rectangle region = bounds.intersection(alignedSourceTile.getBounds());
      if (!region.isEmpty()) {
        copyPixels(alignedSourceTile, tile, region);
      }
      return tile;
    }

    int sourceTileWidth = source.getTileWidth();
    int sourceTileHeight = source.getTileHeight();
    int minTileX = Math.floorDiv(bounds.x - source.getTileGridXOffset(), sourceTileWidth);
    int maxTileX =
        Math.floorDiv(bounds.x + bounds.width - 1 - source.getTileGridXOffset(), sourceTileWidth);
    int minTileY = Math.floorDiv(bounds.y - source.getTileGridYOffset(), sourceTileHeight);
    int maxTileY =
        Math.floorDiv(bounds.y + bounds.height - 1 - source.getTileGridYOffset(), sourceTileHeight);

    for (int sourceTileY = minTileY; sourceTileY <= maxTileY; sourceTileY++) {
      for (int sourceTileX = minTileX; sourceTileX <= maxTileX; sourceTileX++) {
        Raster sourceTile = source.getTile(sourceTileX, sourceTileY);
        Rectangle region = bounds.intersection(sourceTile.getBounds());
        if (!region.isEmpty()) {
          copyPixels(sourceTile, tile, region);
        }
      }
    }
    return tile;
  }

  /**
   * Whether imageio-ext can consume this concrete tile through its direct-buffer path. The check is
   * per tile because translations and buffer offsets are properties of the returned raster, not the
   * image's declared sample model.
   */
  private boolean isWriterSafe(Raster tile) {
    int tileSize = getTileWidth();
    if (tile.getWidth() != tileSize
        || tile.getHeight() != tileSize
        || tile.getSampleModelTranslateX() != tile.getMinX()
        || tile.getSampleModelTranslateY() != tile.getMinY()) {
      return false;
    }

    SampleModel sampleModel = tile.getSampleModel();
    if (!(sampleModel instanceof ComponentSampleModel)
        || sampleModel.getWidth() != tileSize
        || sampleModel.getHeight() != tileSize) {
      return false;
    }

    ComponentSampleModel componentModel = (ComponentSampleModel) sampleModel;
    int pixelStride = componentModel.getPixelStride();
    int numBands = componentModel.getNumBands();
    if (pixelStride != numBands || componentModel.getScanlineStride() != tileSize * pixelStride) {
      return false;
    }

    int[] bandOffsets = componentModel.getBandOffsets();
    int[] bankIndices = componentModel.getBankIndices();
    for (int band = 0; band < numBands; band++) {
      if (bandOffsets[band] != band || bankIndices[band] != 0) {
        return false;
      }
    }

    DataBuffer dataBuffer = tile.getDataBuffer();
    if (!(dataBuffer instanceof DataBufferByte)
        || dataBuffer.getNumBanks() != 1
        || dataBuffer.getOffset() != 0) {
      return false;
    }
    long requiredBytes = (long) tileSize * tileSize * pixelStride;
    return ((DataBufferByte) dataBuffer).getData().length >= requiredBytes;
  }

  /** Copy a region between rasters without dropping either DataBuffer's offsets. */
  private static void copyPixels(Raster source, WritableRaster destination, Rectangle region) {
    if (copyRowsDirectly(source, destination, region)) {
      return;
    }

    SampleModel sourceModel = source.getSampleModel();
    SampleModel destinationModel = destination.getSampleModel();
    DataBuffer sourceBuffer = source.getDataBuffer();
    DataBuffer destinationBuffer = destination.getDataBuffer();
    int sourceTranslateX = source.getSampleModelTranslateX();
    int sourceTranslateY = source.getSampleModelTranslateY();
    int destinationTranslateX = destination.getSampleModelTranslateX();
    int destinationTranslateY = destination.getSampleModelTranslateY();
    int[] row = new int[region.width * sourceModel.getNumBands()];
    for (int y = region.y; y < region.y + region.height; y++) {
      sourceModel.getPixels(
          region.x - sourceTranslateX, y - sourceTranslateY, region.width, 1, row, sourceBuffer);
      destinationModel.setPixels(
          region.x - destinationTranslateX,
          y - destinationTranslateY,
          region.width,
          1,
          row,
          destinationBuffer);
    }
  }

  /**
   * Copies {@code region} row by row for compatible byte component layouts, applying each side's
   * DataBuffer bank offsets explicitly. The region must lie within both rasters. Returns {@code
   * false} without writing when the layout pair is ineligible; indexing and copy failures propagate
   * to the caller.
   */
  static boolean copyRowsDirectly(Raster source, WritableRaster destination, Rectangle region) {
    if (!(source.getSampleModel() instanceof ComponentSampleModel)
        || !(destination.getSampleModel() instanceof ComponentSampleModel)
        || !(source.getDataBuffer() instanceof DataBufferByte)
        || !(destination.getDataBuffer() instanceof DataBufferByte)) {
      return false;
    }

    ComponentSampleModel sourceModel = (ComponentSampleModel) source.getSampleModel();
    ComponentSampleModel destinationModel = (ComponentSampleModel) destination.getSampleModel();
    DataBufferByte sourceBuffer = (DataBufferByte) source.getDataBuffer();
    DataBufferByte destinationBuffer = (DataBufferByte) destination.getDataBuffer();
    int pixelStride = sourceModel.getPixelStride();
    if (destinationModel.getPixelStride() != pixelStride
        || sourceModel.getNumBands() != destinationModel.getNumBands()) {
      return false;
    }

    int sourceX = region.x - source.getSampleModelTranslateX();
    int sourceY = region.y - source.getSampleModelTranslateY();
    int destinationX = region.x - destination.getSampleModelTranslateX();
    int destinationY = region.y - destination.getSampleModelTranslateY();
    int sourceStride = sourceModel.getScanlineStride();
    int destinationStride = destinationModel.getScanlineStride();

    if (pixelStride == 1) {
      // Each logical band uses its configured bank and offset.
      int[] sourceBanks = sourceModel.getBankIndices();
      int[] destinationBanks = destinationModel.getBankIndices();
      int[] sourceBandOffsets = sourceModel.getBandOffsets();
      int[] destinationBandOffsets = destinationModel.getBandOffsets();
      int[] sourceBufferOffsets = sourceBuffer.getOffsets();
      int[] destinationBufferOffsets = destinationBuffer.getOffsets();
      for (int band = 0; band < sourceModel.getNumBands(); band++) {
        int sourceBank = sourceBanks[band];
        int destinationBank = destinationBanks[band];
        byte[] sourceArray = sourceBuffer.getData(sourceBank);
        byte[] destinationArray = destinationBuffer.getData(destinationBank);
        int sourceBase = sourceBufferOffsets[sourceBank] + sourceBandOffsets[band];
        int destinationBase =
            destinationBufferOffsets[destinationBank] + destinationBandOffsets[band];
        for (int y = 0; y < region.height; y++) {
          System.arraycopy(
              sourceArray,
              sourceBase + (sourceY + y) * sourceStride + sourceX,
              destinationArray,
              destinationBase + (destinationY + y) * destinationStride + destinationX,
              region.width);
        }
      }
      return true;
    }

    // Interleaved rows are contiguous only when both sides use identical single-bank packing.
    int[] bandOffsets = sourceModel.getBandOffsets();
    if (!Arrays.equals(bandOffsets, destinationModel.getBandOffsets())
        || sourceBuffer.getNumBanks() != 1
        || destinationBuffer.getNumBanks() != 1) {
      return false;
    }
    int minOffset = pixelStride;
    int maxOffset = -1;
    for (int offset : bandOffsets) {
      minOffset = Math.min(minOffset, offset);
      maxOffset = Math.max(maxOffset, offset);
    }
    if (minOffset != 0 || maxOffset != pixelStride - 1) {
      return false;
    }

    byte[] sourceArray = sourceBuffer.getData();
    byte[] destinationArray = destinationBuffer.getData();
    int sourceBase = sourceBuffer.getOffset();
    int destinationBase = destinationBuffer.getOffset();
    int length = region.width * pixelStride;
    for (int y = 0; y < region.height; y++) {
      System.arraycopy(
          sourceArray,
          sourceBase + (sourceY + y) * sourceStride + sourceX * pixelStride,
          destinationArray,
          destinationBase + (destinationY + y) * destinationStride + destinationX * pixelStride,
          length);
    }
    return true;
  }
}
