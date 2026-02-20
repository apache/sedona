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

import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageWriteParam;
import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationBicubic;
import javax.media.jai.InterpolationBilinear;
import javax.media.jai.InterpolationNearest;
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
