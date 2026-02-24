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

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.utils.ImageUtils;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;

public class TileGenerator {

  public static class Tile {

    private final int tileX;
    private final int tileY;
    private final GridCoverage2D coverage;

    public Tile(int tileX, int tileY, GridCoverage2D coverage) {
      this.tileX = tileX;
      this.tileY = tileY;
      this.coverage = coverage;
    }

    public int getTileX() {
      return tileX;
    }

    public int getTileY() {
      return tileY;
    }

    public GridCoverage2D getCoverage() {
      return coverage;
    }
  }

  /**
   * Generate tiles from an in-db grid coverage. The generated tiles are also in-db grid coverages.
   * Pixel data will be copied into the tiles one tile at a time via a lazy iterator.
   *
   * @param gridCoverage2D the in-db grid coverage
   * @param bandIndices the indices of the bands to select (1-based)
   * @param tileWidth the width of the tiles
   * @param tileHeight the height of the tiles
   * @param padWithNoData whether to pad the tiles with no data value
   * @param padNoDataValue the no data value for padded tiles, only used when padWithNoData is true.
   *     If the value is NaN, the no data value of the original band will be used.
   * @return a lazy iterator of tiles
   */
  public static InDbTileIterator generateInDbTiles(
      GridCoverage2D gridCoverage2D,
      int[] bandIndices,
      int tileWidth,
      int tileHeight,
      boolean padWithNoData,
      double padNoDataValue) {
    return new InDbTileIterator(
        gridCoverage2D, bandIndices, tileWidth, tileHeight, padWithNoData, padNoDataValue);
  }

  public abstract static class TileIterator implements Iterator<Tile> {
    protected final GridCoverage2D gridCoverage2D;
    protected int numTileX;
    protected int numTileY;
    protected int tileX;
    protected int tileY;
    protected boolean autoDisposeSource = false;
    protected Runnable disposeFunction = null;

    TileIterator(GridCoverage2D gridCoverage2D) {
      this.gridCoverage2D = gridCoverage2D;
    }

    protected void initialize(int numTileX, int numTileY) {
      this.numTileX = numTileX;
      this.numTileY = numTileY;
      this.tileX = 0;
      this.tileY = 0;
    }

    /**
     * Set whether to dispose the grid coverage when the iterator reaches the end. Default is false.
     *
     * @param autoDisposeSource whether to dispose the grid coverage
     */
    public void setAutoDisposeSource(boolean autoDisposeSource) {
      this.autoDisposeSource = autoDisposeSource;
    }

    public void setDisposeFunction(Runnable disposeFunction) {
      this.disposeFunction = disposeFunction;
    }

    public int getNumTileX() {
      return numTileX;
    }

    public int getNumTileY() {
      return numTileY;
    }

    public int getNumTiles() {
      return numTileX * numTileY;
    }

    protected abstract Tile generateTile();

    @Override
    public boolean hasNext() {
      if (numTileX == 0 || numTileY == 0) {
        return false;
      }
      return tileY < numTileY;
    }

    @Override
    public Tile next() {
      // Check if current tile coordinate is valid
      if (tileX >= numTileX || tileY >= numTileY) {
        throw new NoSuchElementException();
      }

      Tile tile = generateTile();

      // Advance to the next tile
      tileX += 1;
      if (tileX >= numTileX) {
        tileX = 0;
        tileY += 1;

        // Dispose the grid coverage if we are at the end
        if (tileY >= numTileY) {
          if (autoDisposeSource) {
            gridCoverage2D.dispose(true);
          }
          if (disposeFunction != null) {
            disposeFunction.run();
          }
        }
      }

      return tile;
    }
  }

  public static class InDbTileIterator extends TileIterator {
    private final int[] bandIndices;
    private final int tileWidth;
    private final int tileHeight;
    private final boolean padWithNoData;
    private final double padNoDataValue;
    private final AffineTransform2D affine;
    private final RenderedImage image;
    private final double[] noDataValues;
    private final int imageWidth;
    private final int imageHeight;

    public InDbTileIterator(
        GridCoverage2D gridCoverage2D,
        int[] bandIndices,
        int tileWidth,
        int tileHeight,
        boolean padWithNoData,
        double padNoDataValue) {
      super(gridCoverage2D);
      this.bandIndices = bandIndices;
      this.tileWidth = tileWidth;
      this.tileHeight = tileHeight;
      this.padWithNoData = padWithNoData;
      this.padNoDataValue = padNoDataValue;

      affine = RasterUtils.getAffineTransform(gridCoverage2D, PixelOrientation.CENTER);
      image = gridCoverage2D.getRenderedImage();
      noDataValues = new double[bandIndices.length];
      for (int i = 0; i < bandIndices.length; i++) {
        noDataValues[i] =
            RasterUtils.getNoDataValue(gridCoverage2D.getSampleDimension(bandIndices[i] - 1));
      }
      imageWidth = image.getWidth();
      imageHeight = image.getHeight();
      int numTileX = (int) Math.ceil((double) imageWidth / tileWidth);
      int numTileY = (int) Math.ceil((double) imageHeight / tileHeight);
      initialize(numTileX, numTileY);
    }

    @Override
    protected Tile generateTile() {
      // Process the current tile
      int x0 = tileX * tileWidth + image.getMinX();
      int y0 = tileY * tileHeight + image.getMinY();

      // Rect to copy from the original image
      int rectWidth = Math.min(tileWidth, imageWidth - x0);
      int rectHeight = Math.min(tileHeight, imageHeight - y0);

      // If we don't pad with no data, the tiles on the boundary may have a different size
      int currentTileWidth = padWithNoData ? tileWidth : rectWidth;
      int currentTileHeight = padWithNoData ? tileHeight : rectHeight;
      boolean needPadding = padWithNoData && (rectWidth < tileWidth || rectHeight < tileHeight);

      // Create a new affine transformation for this tile
      AffineTransform2D tileAffine = RasterUtils.translateAffineTransform(affine, x0, y0);
      GridGeometry2D gridGeometry2D =
          new GridGeometry2D(
              new GridEnvelope2D(0, 0, currentTileWidth, currentTileHeight),
              PixelInCell.CELL_CENTER,
              tileAffine,
              gridCoverage2D.getCoordinateReferenceSystem(),
              null);

      // Prepare a new image for this tile, and copy the data from the original image
      WritableRaster raster =
          RasterFactory.createBandedRaster(
              image.getSampleModel().getDataType(),
              currentTileWidth,
              currentTileHeight,
              bandIndices.length,
              null);
      GridSampleDimension[] sampleDimensions = new GridSampleDimension[bandIndices.length];
      Raster sourceRaster = image.getData(new Rectangle(x0, y0, rectWidth, rectHeight));
      for (int k = 0; k < bandIndices.length; k++) {
        int bandIndex = bandIndices[k] - 1;

        // Copy sample dimensions from source bands, and pad with no data value if necessary
        GridSampleDimension sampleDimension = gridCoverage2D.getSampleDimension(bandIndex);
        double noDataValue = noDataValues[k];
        if (needPadding && !Double.isNaN(padNoDataValue)) {
          sampleDimension =
              RasterUtils.createSampleDimensionWithNoDataValue(sampleDimension, padNoDataValue);
          noDataValue = padNoDataValue;
        }
        sampleDimensions[k] = sampleDimension;

        // Copy data from original image to tile image
        ImageUtils.copyRasterWithPadding(sourceRaster, bandIndex, raster, k, noDataValue);
      }

      GridCoverage2D tile = RasterUtils.create(raster, gridGeometry2D, sampleDimensions);
      return new Tile(tileX, tileY, tile);
    }
  }
}
