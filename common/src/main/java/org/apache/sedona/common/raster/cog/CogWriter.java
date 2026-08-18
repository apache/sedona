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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.imageio.ImageWriteParam;
import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationBicubic;
import javax.media.jai.InterpolationBilinear;
import javax.media.jai.InterpolationNearest;
import javax.media.jai.PlanarImage;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.coverage.grid.GridCoverageWriter;
import org.geotools.api.parameter.GeneralParameterValue;
import org.geotools.api.parameter.ParameterValueGroup;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.processing.Operations;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.referencing.operation.transform.AffineTransform2D;

/**
 * Creates Cloud Optimized GeoTIFF (COG) files from GeoTools GridCoverage2D rasters.
 *
 * <p>The COG generation process:
 *
 * <ol>
 *   <li>Compute overview decimation factors (power of 2: 2, 4, 8, ...)
 *   <li>Generate overview images by downsampling
 *   <li>Write each (full-res + overviews) as a separate tiled GeoTIFF via GeoTools
 *   <li>Parse each TIFF's IFD structure
 *   <li>Reassemble into COG byte order using {@link CogAssembler}
 * </ol>
 *
 * <p>Overview decimation algorithm ported from GeoTrellis's {@code
 * GeoTiff.defaultOverviewDecimations}.
 */
public class CogWriter {

  /** Default tile size for COG output, matching GDAL's default */
  public static final int DEFAULT_TILE_SIZE = 256;

  /** Minimum image dimension to create an overview for */
  private static final int MIN_OVERVIEW_SIZE = 2;

  /**
   * Write a GridCoverage2D as a Cloud Optimized GeoTIFF byte array using the given options.
   *
   * @param raster The input raster
   * @param options COG generation options (compression, tileSize, resampling, overviewCount)
   * @return COG file as byte array
   * @throws IOException if writing fails
   */
  public static byte[] write(GridCoverage2D raster, CogOptions options) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    write(raster, options, bos);
    return bos.toByteArray();
  }

  /**
   * Write a GridCoverage2D as a Cloud Optimized GeoTIFF directly to an output stream. This avoids
   * allocating a byte[] for the entire COG, making it suitable for writing large rasters to disk or
   * network streams.
   *
   * @param raster The input raster
   * @param options COG generation options (compression, tileSize, resampling, overviewCount)
   * @param outputStream The stream to write the COG to. Not closed by this method.
   * @throws IOException if writing fails
   */
  public static void write(GridCoverage2D raster, CogOptions options, OutputStream outputStream)
      throws IOException {
    List<TiffIfdParser.ParsedTiff> parsedTiffs = encodeAndParse(raster, options);
    CogAssembler.assemble(parsedTiffs, outputStream);
  }

  /**
   * Internal: encode the raster and all overviews as tiled GeoTIFFs, then parse each into a
   * ParsedTiff. Each overview's tiled GeoTIFF bytes are parsed immediately, allowing the overview
   * GridCoverage2D to be released before the next level is generated.
   *
   * <p>With zero-copy parsing, each ParsedTiff holds a reference to the source byte array (no
   * separate imageData copy), so peak memory is: live raster + one overview + all compressed TIFF
   * byte arrays.
   */
  private static List<TiffIfdParser.ParsedTiff> encodeAndParse(
      GridCoverage2D raster, CogOptions options) throws IOException {
    String compressionType = options.getCompression();
    double compressionQuality = options.getCompressionQuality();
    int tileSize = options.getTileSize();
    String resampling = options.getResampling();
    int requestedOverviewCount = options.getOverviewCount();

    RenderedImage image = raster.getRenderedImage();
    int cols = image.getWidth();
    int rows = image.getHeight();

    // Step 1: Compute overview decimation factors
    List<Integer> decimations;
    if (requestedOverviewCount == 0) {
      decimations = new ArrayList<>();
    } else {
      decimations = computeOverviewDecimations(cols, rows, tileSize);
      if (requestedOverviewCount > 0 && requestedOverviewCount < decimations.size()) {
        decimations = decimations.subList(0, requestedOverviewCount);
      }
    }

    Interpolation interpolation = getInterpolation(resampling);
    List<TiffIfdParser.ParsedTiff> parsedTiffs = new ArrayList<>(1 + decimations.size());

    // Step 2: Encode full-res → parse immediately
    byte[] fullResBytes =
        writeAsTiledGeoTiff(raster, compressionType, compressionQuality, tileSize);
    parsedTiffs.add(TiffIfdParser.parse(fullResBytes));

    // Step 3: For each overview level, generate → encode → parse.
    // The overview GridCoverage2D becomes eligible for GC after parsing.
    for (int decimation : decimations) {
      GridCoverage2D overview = generateOverview(raster, decimation, interpolation);
      byte[] overviewBytes =
          writeAsTiledGeoTiff(overview, compressionType, compressionQuality, tileSize);
      parsedTiffs.add(TiffIfdParser.parse(overviewBytes));
      // overview and its RenderedImage are now eligible for GC
    }

    return parsedTiffs;
  }

  /**
   * Compute overview decimation factors. Each level is a power of 2.
   *
   * <p>Ported from GeoTrellis: {@code GeoTiff.defaultOverviewDecimations()}
   *
   * @param cols Image width in pixels
   * @param rows Image height in pixels
   * @param blockSize Tile size for the overview
   * @return List of decimation factors [2, 4, 8, ...] or empty if image is too small
   */
  static List<Integer> computeOverviewDecimations(int cols, int rows, int blockSize) {
    List<Integer> decimations = new ArrayList<>();
    double pixels = Math.max(cols, rows);
    double blocks = pixels / blockSize;
    int overviewLevels = (int) Math.ceil(Math.log(blocks) / Math.log(2));

    for (int level = 0; level < overviewLevels; level++) {
      int decimation = (int) Math.pow(2, level + 1);
      int overviewCols = (int) Math.ceil((double) cols / decimation);
      int overviewRows = (int) Math.ceil((double) rows / decimation);
      if (overviewCols < MIN_OVERVIEW_SIZE || overviewRows < MIN_OVERVIEW_SIZE) {
        break;
      }
      decimations.add(decimation);
    }
    return decimations;
  }

  /**
   * Generate an overview (reduced resolution) coverage by downsampling.
   *
   * @param raster The full resolution raster
   * @param decimationFactor Factor to reduce by (2 = half size, 4 = quarter, etc.)
   * @param interpolation The interpolation method to use for resampling
   * @return A new GridCoverage2D at reduced resolution
   */
  static GridCoverage2D generateOverview(
      GridCoverage2D raster, int decimationFactor, Interpolation interpolation) {
    RenderedImage image = raster.getRenderedImage();
    int newWidth = (int) Math.ceil((double) image.getWidth() / decimationFactor);
    int newHeight = (int) Math.ceil((double) image.getHeight() / decimationFactor);

    // Use GeoTools Operations.DEFAULT.resample to downsample
    CoordinateReferenceSystem crs = raster.getCoordinateReferenceSystem2D();

    AffineTransform2D originalTransform =
        (AffineTransform2D) raster.getGridGeometry().getGridToCRS2D();
    double newScaleX = originalTransform.getScaleX() * decimationFactor;
    double newScaleY = originalTransform.getScaleY() * decimationFactor;

    AffineTransform2D newTransform =
        new AffineTransform2D(
            newScaleX,
            originalTransform.getShearY(),
            originalTransform.getShearX(),
            newScaleY,
            originalTransform.getTranslateX(),
            originalTransform.getTranslateY());

    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, newWidth, newHeight),
            PixelInCell.CELL_CORNER,
            newTransform,
            crs,
            null);

    return (GridCoverage2D) Operations.DEFAULT.resample(raster, null, gridGeometry, interpolation);
  }

  /**
   * Generate an overview using default nearest-neighbor interpolation. Kept for backward
   * compatibility with tests.
   */
  static GridCoverage2D generateOverview(GridCoverage2D raster, int decimationFactor) {
    return generateOverview(raster, decimationFactor, new InterpolationNearest());
  }

  /**
   * Map a resampling algorithm name to a JAI Interpolation instance.
   *
   * @param resampling One of "Nearest", "Bilinear", "Bicubic"
   * @return The corresponding JAI Interpolation
   */
  private static Interpolation getInterpolation(String resampling) {
    switch (resampling) {
      case "Bilinear":
        return new InterpolationBilinear();
      case "Bicubic":
        return new InterpolationBicubic(8);
      case "Nearest":
      default:
        return new InterpolationNearest();
    }
  }

  /**
   * Rebuild byte-band coverages on the output tile grid before writing (GH-3245).
   *
   * <p>imageio-ext's TIFFDeflater passes {@code (offset, height * scanlineStride)} verbatim to
   * {@link java.util.zip.Deflater#setInput(byte[], int, int)}. For 8-bit images, TIFFImageWriter's
   * optimized path hands the compressor the raster's backing array directly, ignoring both the
   * layout of that array (a buffer wider than the output tile overruns and throws
   * ArrayIndexOutOfBoundsException) and the DataBuffer's own offset (a nonzero offset silently
   * shifts every pixel). Whether a given tile is safe depends on its DataBuffer, which cannot be
   * verified up front, so every byte-band image is rewrapped unconditionally: the writer only ever
   * sees freshly materialized tiles backed by tight, zero-offset buffers. Pixel data and the
   * written TIFF are unchanged; tiles are copied one at a time and never cached, so no second copy
   * of the raster is retained.
   */
  private static GridCoverage2D alignTileLayoutForByteBands(GridCoverage2D raster, int tileSize) {
    RenderedImage image = raster.getRenderedImage();
    if (image.getSampleModel().getDataType() != DataBuffer.TYPE_BYTE) {
      return raster;
    }
    RenderedImage retiled = new RetilingImage(image, tileSize);
    return RasterUtils.clone(retiled, raster.getSampleDimensions(), raster, null, true);
  }

  /**
   * A retiling view over a source image: each tile is materialized on demand as a tight,
   * zero-offset raster and is not cached, so writing does not retain a second copy of the raster's
   * pixel data (unlike {@link javax.media.jai.TiledImage}, which holds every realized tile
   * strongly).
   *
   * <p>Pixels are copied with explicit DataBuffer bank offsets (or through SampleModel/DataBuffer
   * accessors, which honor them). Raster bulk-copy shortcuts must not be used here: {@code
   * WritableRaster.setRect} and the getDataElements-based cobbling inside {@code
   * PlanarImage.getData} read the backing array directly and silently drop a nonzero DataBuffer
   * offset, corrupting every pixel. For the same reason tiles are read straight from the source
   * with {@code getTile} rather than through {@code getData}.
   *
   * <p>When the source's tile grid already coincides with the output grid, each source tile whose
   * concrete raster is verifiably safe for the writer (full-size, tight stride, zero buffer offset)
   * is returned as-is, so already well-tiled sources are written without any copy.
   */
  private static final class RetilingImage extends PlanarImage {
    private final RenderedImage source;
    /** Source tile grid coincides with this image's grid, so source tiles can be reused. */
    private final boolean sameTileGrid;

    RetilingImage(RenderedImage source, int tileSize) {
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
      if (sameTileGrid) {
        Raster srcTile = source.getTile(tileX, tileY);
        if (isWriterSafe(srcTile)) {
          return srcTile;
        }
      }
      WritableRaster tile =
          Raster.createWritableRaster(
              getSampleModel(), new Point(tileXToX(tileX), tileYToY(tileY)));
      Rectangle bounds = tile.getBounds().intersection(getBounds());
      if (bounds.isEmpty()) {
        return tile;
      }
      int tw = source.getTileWidth();
      int th = source.getTileHeight();
      int minTx = Math.floorDiv(bounds.x - source.getTileGridXOffset(), tw);
      int maxTx = Math.floorDiv(bounds.x + bounds.width - 1 - source.getTileGridXOffset(), tw);
      int minTy = Math.floorDiv(bounds.y - source.getTileGridYOffset(), th);
      int maxTy = Math.floorDiv(bounds.y + bounds.height - 1 - source.getTileGridYOffset(), th);
      for (int ty = minTy; ty <= maxTy; ty++) {
        for (int tx = minTx; tx <= maxTx; tx++) {
          Raster srcTile = source.getTile(tx, ty);
          Rectangle region = bounds.intersection(srcTile.getBounds());
          if (!region.isEmpty()) {
            copyPixels(srcTile, tile, region);
          }
        }
      }
      return tile;
    }

    /**
     * Whether the writer can consume this concrete tile as-is: a full-size byte component tile
     * whose samples fill a single-bank, zero-offset buffer exactly, with the pixel's bytes packed
     * in [0, pixelStride). This must be judged per tile — the DataBuffer offset is a property of
     * each tile's buffer, not of the image layout.
     */
    private boolean isWriterSafe(Raster tile) {
      int tileSize = getTileWidth();
      if (tile.getWidth() != tileSize
          || tile.getHeight() != tileSize
          || tile.getSampleModelTranslateX() != tile.getMinX()
          || tile.getSampleModelTranslateY() != tile.getMinY()) {
        return false;
      }
      SampleModel sm = tile.getSampleModel();
      if (!(sm instanceof ComponentSampleModel)
          || sm.getWidth() != tileSize
          || sm.getHeight() != tileSize) {
        return false;
      }
      ComponentSampleModel csm = (ComponentSampleModel) sm;
      int ps = csm.getPixelStride();
      if (csm.getScanlineStride() != tileSize * ps) {
        return false;
      }
      int[] bandOffsets = csm.getBandOffsets();
      int maxOff = 0;
      for (int off : bandOffsets) {
        if (off < 0 || off >= ps) {
          return false;
        }
        maxOff = Math.max(maxOff, off);
      }
      if (bandOffsets[0] != 0 || maxOff != ps - 1) {
        return false;
      }
      DataBuffer db = tile.getDataBuffer();
      return db instanceof DataBufferByte && db.getNumBanks() == 1 && db.getOffset() == 0;
    }

    /** Copy a region between rasters without ever dropping DataBuffer offsets. */
    private static void copyPixels(Raster src, WritableRaster dest, Rectangle region) {
      if (copyRowsDirectly(src, dest, region)) {
        return;
      }
      // Fallback for layout pairs the bulk path does not cover: per-sample accessors, which
      // are offset-aware by specification.
      SampleModel srcSm = src.getSampleModel();
      SampleModel destSm = dest.getSampleModel();
      DataBuffer srcDb = src.getDataBuffer();
      DataBuffer destDb = dest.getDataBuffer();
      int srcTx = src.getSampleModelTranslateX();
      int srcTy = src.getSampleModelTranslateY();
      int destTx = dest.getSampleModelTranslateX();
      int destTy = dest.getSampleModelTranslateY();
      int[] row = new int[region.width * srcSm.getNumBands()];
      for (int y = region.y; y < region.y + region.height; y++) {
        srcSm.getPixels(region.x - srcTx, y - srcTy, region.width, 1, row, srcDb);
        destSm.setPixels(region.x - destTx, y - destTy, region.width, 1, row, destDb);
      }
    }

    /**
     * Row-wise System.arraycopy for standard byte component layouts, applying each side's
     * DataBuffer bank offsets explicitly. Returns false when the layout pair is not eligible.
     */
    private static boolean copyRowsDirectly(Raster src, WritableRaster dest, Rectangle region) {
      if (!(src.getSampleModel() instanceof ComponentSampleModel)
          || !(dest.getSampleModel() instanceof ComponentSampleModel)
          || !(src.getDataBuffer() instanceof DataBufferByte)
          || !(dest.getDataBuffer() instanceof DataBufferByte)) {
        return false;
      }
      ComponentSampleModel srcSm = (ComponentSampleModel) src.getSampleModel();
      ComponentSampleModel destSm = (ComponentSampleModel) dest.getSampleModel();
      DataBufferByte srcDb = (DataBufferByte) src.getDataBuffer();
      DataBufferByte destDb = (DataBufferByte) dest.getDataBuffer();
      int ps = srcSm.getPixelStride();
      if (destSm.getPixelStride() != ps || srcSm.getNumBands() != destSm.getNumBands()) {
        return false;
      }
      int srcX = region.x - src.getSampleModelTranslateX();
      int srcY = region.y - src.getSampleModelTranslateY();
      int destX = region.x - dest.getSampleModelTranslateX();
      int destY = region.y - dest.getSampleModelTranslateY();
      int srcStride = srcSm.getScanlineStride();
      int destStride = destSm.getScanlineStride();
      if (ps == 1) {
        // One byte per band sample: copy each band's rows within its own bank.
        for (int b = 0; b < srcSm.getNumBands(); b++) {
          byte[] srcArr = srcDb.getData(srcSm.getBankIndices()[b]);
          byte[] destArr = destDb.getData(destSm.getBankIndices()[b]);
          int srcBase = srcDb.getOffsets()[srcSm.getBankIndices()[b]] + srcSm.getBandOffsets()[b];
          int destBase =
              destDb.getOffsets()[destSm.getBankIndices()[b]] + destSm.getBandOffsets()[b];
          for (int y = 0; y < region.height; y++) {
            System.arraycopy(
                srcArr,
                srcBase + (srcY + y) * srcStride + srcX,
                destArr,
                destBase + (destY + y) * destStride + destX,
                region.width);
          }
        }
        return true;
      }
      // Interleaved pixels: rows are contiguous byte blocks only when both sides pack the
      // pixel's bytes identically across [0, pixelStride) in a single bank.
      int[] bandOffsets = srcSm.getBandOffsets();
      if (!Arrays.equals(bandOffsets, destSm.getBandOffsets())
          || srcDb.getNumBanks() != 1
          || destDb.getNumBanks() != 1) {
        return false;
      }
      int minOff = ps;
      int maxOff = -1;
      for (int off : bandOffsets) {
        minOff = Math.min(minOff, off);
        maxOff = Math.max(maxOff, off);
      }
      if (minOff != 0 || maxOff != ps - 1) {
        return false;
      }
      byte[] srcArr = srcDb.getData();
      byte[] destArr = destDb.getData();
      int srcBase = srcDb.getOffset();
      int destBase = destDb.getOffset();
      int len = region.width * ps;
      for (int y = 0; y < region.height; y++) {
        System.arraycopy(
            srcArr,
            srcBase + (srcY + y) * srcStride + srcX * ps,
            destArr,
            destBase + (destY + y) * destStride + destX * ps,
            len);
      }
      return true;
    }
  }

  /**
   * Write a GridCoverage2D as a tiled GeoTIFF byte array using GeoTools.
   *
   * @param raster The input raster
   * @param compressionType Compression type
   * @param compressionQuality Quality 0.0 to 1.0
   * @param tileSize Tile dimensions in pixels
   * @return Tiled GeoTIFF as byte array
   * @throws IOException if writing fails
   */
  private static byte[] writeAsTiledGeoTiff(
      GridCoverage2D raster, String compressionType, double compressionQuality, int tileSize)
      throws IOException {
    raster = alignTileLayoutForByteBands(raster, tileSize);

    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      GridCoverageWriter writer = new GeoTiffWriter(out);
      try {
        ParameterValueGroup defaultParams = writer.getFormat().getWriteParameters();
        GeoTiffWriteParams params = new GeoTiffWriteParams();

        // Set tiling — must use the 2-arg overload from GeoToolsWriteParams
        // which delegates to the inner write param. The 4-arg ImageWriteParam.setTiling()
        // writes to the wrong fields (parent vs inner param).
        params.setTilingMode(ImageWriteParam.MODE_EXPLICIT);
        params.setTiling(tileSize, tileSize);

        // Set compression
        params.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        params.setCompressionType(compressionType);
        params.setCompressionQuality((float) compressionQuality);

        defaultParams
            .parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString())
            .setValue(params);

        GeneralParameterValue[] wps = defaultParams.values().toArray(new GeneralParameterValue[0]);

        writer.write(raster, wps);
      } finally {
        writer.dispose();
      }
      return out.toByteArray();
    }
  }
}
