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

import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.awt.image.renderable.ParameterBlock;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Base64;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.media.jai.InterpolationNearest;
import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;
import org.apache.sedona.common.raster.cog.CogOptions;
import org.apache.sedona.common.raster.cog.CogWriter;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.api.coverage.grid.GridCoverageWriter;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.parameter.GeneralParameterValue;
import org.geotools.api.parameter.ParameterValueGroup;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.gce.arcgrid.ArcGridWriteParams;
import org.geotools.gce.arcgrid.ArcGridWriter;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.referencing.operation.transform.AffineTransform2D;

public class RasterOutputs {
  public static byte[] asGeoTiff(
      GridCoverage2D raster, String compressionType, double compressionQuality) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GridCoverageWriter writer;
    try {
      writer = new GeoTiffWriter(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ParameterValueGroup defaultParams = writer.getFormat().getWriteParameters();
    if (compressionType != null && compressionQuality >= 0 && compressionQuality <= 1) {
      GeoTiffWriteParams params = new GeoTiffWriteParams();
      params.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
      // Available compression types: None, PackBits, Deflate, Huffman, LZW and JPEG
      params.setCompressionType(compressionType);
      // Should be a value between 0 and 1
      // 0 means max compression, 1 means no compression
      params.setCompressionQuality((float) compressionQuality);
      defaultParams
          .parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString())
          .setValue(params);
    }
    GeneralParameterValue[] wps = defaultParams.values().toArray(new GeneralParameterValue[0]);
    try {
      writer.write(raster, wps);
      writer.dispose();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  public static byte[] asGeoTiff(GridCoverage2D raster) {
    return asGeoTiff(raster, null, -1);
  }

  /**
   * Creates a Cloud Optimized GeoTIFF (COG) byte array from the given raster. The COG format
   * arranges tiles and overviews in an order optimized for HTTP range-request based access,
   * enabling efficient partial reads from cloud storage.
   *
   * @param raster The input raster
   * @param options COG generation options (compression, tileSize, resampling, overviewCount). Use
   *     {@link CogOptions#defaults()} for default settings or {@link CogOptions#builder()} to
   *     customize.
   * @return COG file as byte array
   */
  public static byte[] asCloudOptimizedGeoTiff(GridCoverage2D raster, CogOptions options) {
    try {
      return CogWriter.write(raster, options);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write Cloud Optimized GeoTIFF", e);
    }
  }

  /**
   * Creates a GeoTiff file with the provided raster. Primarily used for testing.
   *
   * @param bytes The bytes to be stored on a disk file
   * @param filePath The path where the .tiff should be stored.
   * @return true if file is created, otherwise throws IOException
   */
  public static boolean writeToDiskFile(byte[] bytes, String filePath) {
    File outputFile = new File(filePath);
    try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
      outputStream.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public static byte[] asPNG(GridCoverage2D raster, int maxWidth) throws IOException {
    String bandType = RasterBandAccessors.getBandType(raster);
    if ("B".equals(bandType) || "UNSIGNED_8BITS".equals(bandType)) {
      byte[] out = doAsPNGByteBands(raster, maxWidth);
      if (out.length > 0) {
        return out;
      } else {
        // Directly writing the image as-is failed, try a more reliable way
        return doAsPNGMultiBand(raster, maxWidth);
      }
    } else if (raster.getRenderedImage().getSampleModel().getNumBands() == 1) {
      return doAsPNGSingleBand(raster, maxWidth);
    } else {
      return doAsPNGMultiBand(raster, maxWidth);
    }
  }

  private static byte[] doAsPNGByteBands(GridCoverage2D raster, int maxWidth) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RenderedImage image = raster.getRenderedImage();
    RenderedOp scaleOp = scaleImageToFitMaxWidth(image, maxWidth);
    if (scaleOp != null) {
      image = scaleOp.getAsBufferedImage();
    }
    try {
      ImageIO.write(image, "png", out);
      return out.toByteArray();
    } finally {
      if (scaleOp != null) {
        scaleOp.dispose();
      }
    }
  }

  private static byte[] doAsPNGSingleBand(GridCoverage2D raster, int maxWidth) throws IOException {
    RenderedImage image = raster.getRenderedImage();
    RenderedOp scaleOp = scaleImageToFitMaxWidth(image, maxWidth);
    if (scaleOp != null) {
      image = scaleOp.getAsBufferedImage();
    }

    try {
      Raster inputRaster = RasterUtils.getRaster(image);
      double noDataValue = RasterUtils.getNoDataValue(raster.getSampleDimension(0));

      // Find the min and max pixel values
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
      for (int y = 0; y < image.getHeight(); y++) {
        for (int x = 0; x < image.getWidth(); x++) {
          double value = inputRaster.getSampleDouble(x, y, 0);
          if (Double.compare(value, noDataValue) != 0) {
            min = Math.min(min, value);
            max = Math.max(max, value);
          }
        }
      }
      double scale = (max != min) ? 255.0 / (max - min) : 1;

      // Create a new image with the same dimensions but with grayscale and alpha channel
      BufferedImage outputImage =
          new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_ARGB);

      // Normalize the pixel values and create the alpha channel
      WritableRaster outputRaster = outputImage.getRaster();
      double[] pixel = new double[1];
      for (int y = 0; y < image.getHeight(); y++) {
        for (int x = 0; x < image.getWidth(); x++) {
          pixel = inputRaster.getPixel(x, y, pixel);
          if (Double.compare(pixel[0], noDataValue) == 0) {
            // transparent pixel for nodata value
            outputRaster.setPixel(x, y, new int[] {0, 0, 0, 0});
          } else {
            // opaque pixel for valid data
            int normalizedValue = (int) ((pixel[0] - min) * scale);
            outputRaster.setPixel(
                x, y, new int[] {normalizedValue, normalizedValue, normalizedValue, 255});
          }
        }
      }

      // Write the PNG image
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      ImageIO.write(outputImage, "png", os);
      return os.toByteArray();
    } finally {
      if (scaleOp != null) {
        scaleOp.dispose();
      }
    }
  }

  private static byte[] doAsPNGMultiBand(GridCoverage2D raster, int maxWidth) throws IOException {
    RenderedImage image = raster.getRenderedImage();
    RenderedOp scaleOp = scaleImageToFitMaxWidth(image, maxWidth);
    if (scaleOp != null) {
      image = scaleOp.getAsBufferedImage();
    }

    try {
      // Create a new image with RGB data
      BufferedImage outputImage =
          new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);

      // Convert pixel values to RGB using the color model
      ColorModel colorModel = image.getColorModel();
      Raster inputRaster = RasterUtils.getRaster(image);
      WritableRaster outputRaster = outputImage.getRaster();
      for (int y = 0; y < image.getHeight(); y++) {
        for (int x = 0; x < image.getWidth(); x++) {
          Object pixel = inputRaster.getDataElements(x, y, null);
          try {
            int rgb = colorModel.getRGB(pixel);
            outputRaster.setPixel(
                x, y, new int[] {(rgb >> 16) & 0xFF, (rgb >> 8) & 0xFF, rgb & 0xFF});
          } catch (Exception e) {
            outputRaster.setPixel(x, y, new int[] {0, 0, 0});
          }
        }
      }

      // Write the PNG image
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      ImageIO.write(outputImage, "png", os);
      return os.toByteArray();
    } finally {
      if (scaleOp != null) {
        scaleOp.dispose();
      }
    }
  }

  private static RenderedOp scaleImageToFitMaxWidth(RenderedImage image, int maxWidth) {
    int width = maxWidth < 0 ? image.getWidth() : Math.min(image.getWidth(), maxWidth);
    if (width == image.getWidth()) {
      return null;
    }
    double scaleFactor = (double) width / image.getWidth();
    ParameterBlock pb = new ParameterBlock();
    pb.addSource(image);
    pb.add((float) scaleFactor);
    pb.add((float) scaleFactor);
    pb.add(0.0F);
    pb.add(0.0F);
    pb.add(new InterpolationNearest());
    return JAI.create("scale", pb, null);
  }

  public static byte[] asPNG(GridCoverage2D raster) throws IOException {
    return asPNG(raster, -1);
  }

  public static byte[] asArcGrid(GridCoverage2D raster, int sourceBand) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GridCoverageWriter writer;
    try {
      writer = new ArcGridWriter(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ParameterValueGroup defaultParams = writer.getFormat().getWriteParameters();
    if (sourceBand >= 0) {
      ArcGridWriteParams params = new ArcGridWriteParams();
      params.setSourceBands(new int[] {sourceBand});
      defaultParams
          .parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString())
          .setValue(params);
    }
    GeneralParameterValue[] wps = defaultParams.values().toArray(new GeneralParameterValue[0]);
    try {
      writer.write(raster, wps);
      writer.dispose();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  public static byte[] asArcGrid(GridCoverage2D raster) {
    return asArcGrid(raster, -1);
  }

  public static String asBase64(GridCoverage2D raster, int maxWidth) throws IOException {
    byte[] png = asPNG(raster, maxWidth);
    return Base64.getEncoder().encodeToString(png);
  }

  public static String asBase64(GridCoverage2D raster) throws IOException {
    byte[] png = asPNG(raster, -1);
    return Base64.getEncoder().encodeToString(png);
  }

  public static String asMatrixPretty(GridCoverage2D raster, int band, int precision) {
    try {
      RasterUtils.ensureBand(raster, band);
      RenderedImage img = raster.getRenderedImage();
      Raster r = RasterUtils.getRaster(img);

      final int minX = img.getMinX();
      final int minY = img.getMinY();
      final int width = r.getWidth();
      final int height = r.getHeight();
      final int bandIdx = band - 1;

      // Affine transform (upper-left corners)
      AffineTransform2D at = RasterUtils.getAffineTransform(raster, PixelOrientation.UPPER_LEFT);
      final double a = at.getScaleX(), b = at.getShearX(), d = at.getShearY(), e = at.getScaleY();
      final double tx = at.getTranslateX(), ty = at.getTranslateY();

      final boolean integral = RasterUtils.isDataTypeIntegral(r.getDataBuffer().getDataType());
      final String numPat = precision <= 0 ? "0" : "0." + "#".repeat(precision);
      final DecimalFormat df = new DecimalFormat(numPat);

      String[][] cells = new String[height + 1][width + 1];
      cells[0][0] = "y\\x";

      // Header: use upper-left corner (no +0.5)
      for (int x = 0; x < width; x++) {
        double xc = minX + x;
        double yc = minY;
        double worldX = a * xc + b * yc + tx;
        cells[0][x + 1] = "[" + (minX + x) + ", " + df.format(worldX) + "]";
      }

      // Rows
      for (int y = 0; y < height; y++) {
        double yc = minY + y;
        double xc = minX;
        double worldY = d * xc + e * yc + ty;
        cells[y + 1][0] = "[" + (minY + y) + ", " + df.format(worldY) + "]";

        if (integral) {
          int[] vals = r.getSamples(0, y, width, 1, bandIdx, (int[]) null);
          for (int x = 0; x < width; x++) cells[y + 1][x + 1] = String.valueOf(vals[x]);
        } else {
          double[] vals = r.getSamples(0, y, width, 1, bandIdx, (double[]) null);
          for (int x = 0; x < width; x++) cells[y + 1][x + 1] = df.format(vals[x]);
        }
      }

      // --- Pretty print ---
      int rows = height + 1, cols = width + 1;
      int[] colWidths = new int[cols];
      for (int c = 0; c < cols; c++) {
        int max = 0;
        for (int rIdx = 0; rIdx < rows; rIdx++) max = Math.max(max, cells[rIdx][c].length());
        colWidths[c] = max;
      }

      StringBuilder out = new StringBuilder();
      String sep = "+";
      for (int c = 0; c < cols; c++) sep += "-".repeat(colWidths[c] + 2) + "+";
      sep += "\n";

      out.append(sep);
      for (int rIdx = 0; rIdx < rows; rIdx++) {
        out.append("| ");
        for (int c = 0; c < cols; c++) {
          String val = cells[rIdx][c];
          boolean leftAlign = (c == 0 || rIdx == 0);
          if (leftAlign) out.append(String.format("%-" + colWidths[c] + "s", val));
          else out.append(String.format("%" + colWidths[c] + "s", val));
          out.append(" | ");
        }
        out.append("\n");
        if (rIdx == 0) out.append(sep);
      }
      out.append(sep);
      return out.toString();

    } catch (Exception e) {
      throw new RuntimeException("Failed to format raster matrix", e);
    }
  }

  public static String asMatrix(GridCoverage2D raster, int band, int postDecimalPrecision) {
    RasterUtils.ensureBand(raster, band);
    Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
    int dataTypeCode = rasterData.getDataBuffer().getDataType();
    int width = rasterData.getWidth(), height = rasterData.getHeight();
    if (RasterUtils.isDataTypeIntegral(dataTypeCode)) {
      int[] bandValues = rasterData.getSamples(0, 0, width, height, band - 1, (int[]) null);
      return createPaddedMatrixStringFromInt(bandValues, width);
    } else {
      double[] bandValues = rasterData.getSamples(0, 0, width, height, band - 1, (double[]) null);
      return createPaddedMatrixStringFromDouble(bandValues, width, postDecimalPrecision);
    }
  }

  public static String asMatrix(GridCoverage2D raster, int band) {
    return asMatrix(raster, band, 6);
  }

  public static String asMatrix(GridCoverage2D raster) {
    return asMatrix(raster, 1);
  }

  private static String createPaddedMatrixStringFromDouble(
      double[] values, int width, int decimalPrecision) {
    StringBuilder res = new StringBuilder();
    int maxPreDecimal = 0;
    int maxDecimalPrecision = 0;
    for (double value : values) {
      String[] splitByDecimal = String.valueOf(value).split("\\.");
      int preDecimal = splitByDecimal[0].length(),
          postDecimal =
              Math.min(
                  decimalPrecision, splitByDecimal.length > 1 ? splitByDecimal[1].length() : 0);
      maxDecimalPrecision = Math.max(maxDecimalPrecision, postDecimal);
      int currWidth = preDecimal + 1; // add 1 for space occupied for decimal point
      maxPreDecimal = Math.max(maxPreDecimal, currWidth);
    }
    int maxColWidth = maxDecimalPrecision + maxPreDecimal;
    for (int i = 0; i < values.length; i++) {
      int row = i / width, col = i % width;
      String fmt =
          String.format(
              "%s%%%d.%df%s",
              col == 0 ? "|" : "  ",
              maxColWidth,
              maxDecimalPrecision,
              col < width - 1 ? "" : "|%n");
      res.append(String.format(fmt, values[i]));
    }

    return res.toString();
  }

  private static String createPaddedMatrixStringFromInt(int[] values, int width) {
    StringBuilder res = new StringBuilder();
    int maxColWidth = 0;
    for (int value : values) {
      int currWidth = String.valueOf(value).length();
      maxColWidth = Math.max(maxColWidth, currWidth);
    }
    for (int i = 0; i < values.length; i++) {
      int row = i / width, col = i % width;
      String fmt =
          String.format(
              "%s%%%dd%s", col == 0 ? "|" : "  ", maxColWidth, col < width - 1 ? "" : "|%n");
      res.append(String.format(fmt, values[i]));
    }

    return res.toString();
  }

  public static String createHTMLString(GridCoverage2D raster, int imageWidth) throws IOException {
    String rasterAsBase64 = asBase64(raster, imageWidth);
    String imageString = String.format("data:image/png;base64,%s", rasterAsBase64);
    String htmlString = "<img src=\"" + imageString + "\" width=\"" + imageWidth + "\" />";
    return new String(htmlString.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
  }

  public static String createHTMLString(GridCoverage2D raster) throws IOException {
    return createHTMLString(raster, 200);
  }
}
