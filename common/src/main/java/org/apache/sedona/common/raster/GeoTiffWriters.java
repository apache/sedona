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

import it.geosolutions.imageio.plugins.tiff.TIFFImageWriteParam;
import it.geosolutions.imageioimpl.plugins.tiff.TIFFImageWriter;
import it.geosolutions.imageioimpl.plugins.tiff.TIFFImageWriterSpi;
import java.awt.geom.AffineTransform;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.OutputStream;
import javax.imageio.IIOImage;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageOutputStream;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.crs.GeographicCRS;
import org.geotools.api.referencing.crs.ProjectedCRS;
import org.geotools.api.referencing.operation.MathTransform2D;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.imageio.geotiff.CRS2GeoTiffMetadataAdapter;
import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffConstants;
import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffIIOMetadataEncoder;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.image.io.ImageIOExt;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.XAffineTransform;

/**
 * Encodes a {@link GridCoverage2D} as a GeoTIFF through the public GeoTools and ImageIO-Ext APIs.
 *
 * <p>Sedona does not go through the GeoTools {@code GeoTiffWriter} because of how that writer
 * resolves the {@code GDAL_NODATA} tag: it never writes a NaN no data value, it invents one for
 * integer rasters that declare none (0 for byte bands), and it prefers the {@code GC_NODATA} image
 * property, copied verbatim from the source file, over the no data value the raster actually
 * declares. Here the tag is resolved from band 1's NODATA category and written exactly when one is
 * declared, so it always agrees with {@code RS_BandNoDataValue}.
 *
 * <p>Only whole coverages are written. The sub-image, world file and progress listener options of
 * the GeoTools writer are not needed by Sedona and are not supported.
 */
public final class GeoTiffWriters {

  /** The ImageIO-Ext TIFF writer provider; building one per write is needless work. */
  private static final TIFFImageWriterSpi WRITER_PROVIDER = new TIFFImageWriterSpi();

  private GeoTiffWriters() {}

  /**
   * Writes the coverage as a GeoTIFF.
   *
   * @param coverage the coverage to write; its CRS must be projected or geographic
   * @param params tiling, compression and BigTIFF settings; null for the defaults
   * @param out the stream to write to; it is flushed but not closed
   * @throws IOException if encoding fails
   * @throws IllegalArgumentException if the CRS or the grid to CRS transform cannot be described by
   *     a GeoTIFF
   */
  public static void write(GridCoverage2D coverage, GeoTiffWriteParams params, OutputStream out)
      throws IOException {
    GridGeometry2D gridGeometry = coverage.getGridGeometry();
    // getCoordinateReferenceSystem2D() throws when the coverage has no CRS; it never returns null.
    CoordinateReferenceSystem crs = gridGeometry.getCoordinateReferenceSystem2D();
    if (!(crs instanceof ProjectedCRS) && !(crs instanceof GeographicCRS)) {
      throw new IllegalArgumentException(
          "GeoTIFF can only describe projected and geographic coordinate reference systems, not "
              + crs.getName());
    }
    if (params == null) {
      params = new GeoTiffWriteParams();
    }
    rejectPartialWrites(params);

    GeoTiffIIOMetadataEncoder metadata =
        new CRS2GeoTiffMetadataAdapter(crs).parseCoordinateReferenceSystem();
    // Each pixel is an area; the model transformation maps its upper-left corner.
    metadata.addGeoShortParam(
        GeoTiffConstants.GTRasterTypeGeoKey, GeoTiffConstants.RasterPixelIsArea);
    metadata.setModelTransformation(modelTransformation(gridGeometry, crs));
    Double noDataValue = noDataValue(coverage);
    if (noDataValue != null) {
      metadata.setNoData(noDataValue);
    }

    encode(coverage.getRenderedImage(), metadata, params, out);
  }

  /**
   * The GeoTIFF model transformation: file pixel corner (column, row) to (easting, northing) or
   * (longitude, latitude).
   *
   * <p>GeoTIFF model space is always easting or longitude first, whatever axis order the CRS
   * declares, so the outputs are swapped when the CRS lists latitude first, or when the transform
   * itself maps grid columns onto the CRS's second axis.
   */
  static AffineTransform modelTransformation(
      GridGeometry2D gridGeometry, CoordinateReferenceSystem crs) {
    MathTransform2D gridToCrs = gridGeometry.getGridToCRS2D(PixelOrientation.UPPER_LEFT);
    if (!(gridToCrs instanceof AffineTransform)) {
      throw new IllegalArgumentException(
          "GeoTIFF can only describe affine grid to CRS transforms, not "
              + gridToCrs.getClass().getName());
    }
    AffineTransform transform = new AffineTransform((AffineTransform) gridToCrs);

    // The file's pixel (0, 0) is the grid's low corner, which need not be grid index (0, 0).
    GridEnvelope2D range = gridGeometry.getGridRange2D();
    if (range.x != 0 || range.y != 0) {
      transform.translate(range.x, range.y);
    }

    boolean latitudeFirst = CRS.getAxisOrder(crs) == CRS.AxisOrder.NORTH_EAST;
    boolean columnsMapToSecondAxis = XAffineTransform.getSwapXY(transform) == -1;
    if (latitudeFirst || columnsMapToSecondAxis) {
      transform.preConcatenate(new AffineTransform(0, 1, 1, 0, 0, 0));
    }
    return transform;
  }

  /**
   * The georeference written above describes the whole coverage, so a source region, subsampling or
   * band selection on the write parameters would encode different pixels under it and silently
   * misplace them. None of Sedona's callers set these; refuse them rather than guess.
   */
  private static void rejectPartialWrites(GeoTiffWriteParams params) {
    ImageWriteParam writeParam = params.getAdaptee();
    if (writeParam.getSourceRegion() != null
        || writeParam.getSourceXSubsampling() != 1
        || writeParam.getSourceYSubsampling() != 1
        || writeParam.getSourceBands() != null) {
      throw new IllegalArgumentException(
          "GeoTiffWriters writes whole coverages only; source region, subsampling and source "
              + "band selection are not supported");
    }
  }

  /** GeoTIFF carries a single no data value per file; band 1's is the one written. */
  private static Double noDataValue(GridCoverage2D coverage) {
    GridSampleDimension band = coverage.getSampleDimension(0);
    return RasterUtils.hasNoDataValue(band) ? RasterUtils.getNoDataValue(band) : null;
  }

  private static void encode(
      RenderedImage image,
      GeoTiffIIOMetadataEncoder metadata,
      GeoTiffWriteParams params,
      OutputStream out)
      throws IOException {
    ImageWriteParam writeParam = params.getAdaptee();
    if (writeParam instanceof TIFFImageWriteParam) {
      ((TIFFImageWriteParam) writeParam).setForceToBigTIFF(params.isForceToBigTIFF());
    }
    TIFFImageWriter writer = (TIFFImageWriter) WRITER_PROVIDER.createWriterInstance();
    // Closing the image stream releases its cache and pushes the remaining bytes to the caller's
    // stream, which stays open. A failure while closing is attached to the write failure as a
    // suppressed exception rather than replacing it.
    try (ImageOutputStream stream = ImageIOExt.createImageOutputStream(null, out)) {
      IIOMetadata imageMetadata =
          GeoTiffWriter.createGeoTiffIIOMetadata(
              writer, ImageTypeSpecifier.createFromRenderedImage(image), metadata, writeParam);
      writer.setOutput(stream);
      writer.write(
          writer.getDefaultStreamMetadata(writeParam),
          new IIOImage(image, null, imageMetadata),
          writeParam);
      stream.flush();
    } finally {
      writer.dispose();
    }
  }
}
