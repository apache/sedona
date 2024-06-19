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

import it.geosolutions.jaiext.jiffle.JiffleBuilder;
import it.geosolutions.jaiext.jiffle.runtime.JiffleDirectRuntime;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.awt.image.WritableRenderedImage;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;

public class MapAlgebra {
  /**
   * Returns the values of the given band as a 1D array.
   *
   * @param rasterGeom
   * @param bandIndex starts at 1
   * @return
   */
  public static double[] bandAsArray(GridCoverage2D rasterGeom, int bandIndex) {
    int numBands = rasterGeom.getNumSampleDimensions();
    if (bandIndex < 1 || bandIndex > numBands) {
      // Invalid band index. Return nulls.
      return null;
    }

    Raster raster = RasterUtils.getRaster(rasterGeom.getRenderedImage());
    // Get the width and height of the raster
    int width = raster.getWidth();
    int height = raster.getHeight();

    // Array to hold the band values
    double[] bandValues = new double[width * height];
    return raster.getSamples(0, 0, width, height, bandIndex - 1, bandValues);
  }

  /**
   * Adds a new band to the given raster, using the given array as the band values.
   *
   * @param rasterGeom
   * @param bandValues
   * @param bandIndex starts at 1, and no larger than numBands + 1
   * @return
   */
  public static GridCoverage2D addBandFromArray(
      GridCoverage2D rasterGeom, double[] bandValues, int bandIndex, Double noDataValue) {
    int numBands = rasterGeom.getNumSampleDimensions();
    // Allow the band index to be one larger than the number of bands, which will append the band to
    // the end
    if (bandIndex < 1 || bandIndex > numBands + 1) {
      throw new IllegalArgumentException(
          "Band index is out of bounds. Must be between 1 and " + (numBands + 1) + ")");
    }

    if (bandIndex == numBands + 1) {
      return RasterUtils.copyRasterAndAppendBand(rasterGeom, bandValues, noDataValue);
    } else {
      return RasterUtils.copyRasterAndReplaceBand(
          rasterGeom, bandIndex, bandValues, noDataValue, true);
    }
  }

  public static GridCoverage2D addBandFromArray(
      GridCoverage2D rasterGeom, double[] bandValues, int bandIndex) {
    int numBands = rasterGeom.getNumSampleDimensions();
    // Allow the band index to be one larger than the number of bands, which will append the band to
    // the end
    if (bandIndex < 1 || bandIndex > numBands + 1) {
      throw new IllegalArgumentException(
          "Band index is out of bounds. Must be between 1 and " + (numBands + 1) + ")");
    }

    if (bandIndex == numBands + 1) {
      return RasterUtils.copyRasterAndAppendBand(rasterGeom, bandValues);
    } else {
      return RasterUtils.copyRasterAndReplaceBand(rasterGeom, bandIndex, bandValues);
    }
  }

  /**
   * Adds a new band to the given raster, using the given array as the band values. The new band is
   * appended to the end.
   *
   * @param rasterGeom
   * @param bandValues
   * @return
   */
  public static GridCoverage2D addBandFromArray(GridCoverage2D rasterGeom, double[] bandValues) {
    return addBandFromArray(rasterGeom, bandValues, rasterGeom.getNumSampleDimensions() + 1);
  }

  private static final ThreadLocal<String> previousScript = new ThreadLocal<>();
  private static final ThreadLocal<JiffleDirectRuntime> previousRuntime = new ThreadLocal<>();

  /**
   * Applies a map algebra script to the given raster.
   *
   * @param gridCoverage2D The raster to apply the script to
   * @param pixelType The pixel type of the output raster. If null, the pixel type of the input
   *     raster is used.
   * @param script The script to apply
   * @param noDataValue The no data value of the output raster.
   * @return The result of the map algebra script
   */
  public static GridCoverage2D mapAlgebra(
      GridCoverage2D gridCoverage2D, String pixelType, String script, Double noDataValue) {
    if (gridCoverage2D == null || script == null) {
      return null;
    }
    RenderedImage renderedImage = gridCoverage2D.getRenderedImage();
    int rasterDataType =
        pixelType != null
            ? RasterUtils.getDataTypeCode(pixelType)
            : renderedImage.getSampleModel().getDataType();
    int width = renderedImage.getWidth();
    int height = renderedImage.getHeight();
    // ImageUtils.createConstantImage is slow, manually constructing a buffered image proved to be
    // faster.
    // It also eliminates the data-copying overhead when converting raster data types after running
    // jiffle script.
    WritableRaster resultRaster =
        RasterFactory.createBandedRaster(DataBuffer.TYPE_DOUBLE, width, height, 1, null);
    ColorModel cm = fetchColorModel(renderedImage.getColorModel(), resultRaster);
    WritableRenderedImage resultImage = new BufferedImage(cm, resultRaster, false, null);
    try {
      String prevScript = previousScript.get();
      JiffleDirectRuntime prevRuntime = previousRuntime.get();
      JiffleDirectRuntime runtime;
      if (prevRuntime != null && script.equals(prevScript)) {
        // Reuse the runtime to avoid recompiling the script
        runtime = prevRuntime;
        runtime.setSourceImage("rast", renderedImage);
        runtime.setDestinationImage("out", resultImage);
        runtime.setDefaultBounds();
      } else {
        JiffleBuilder builder = new JiffleBuilder();
        runtime =
            builder
                .script(script)
                .source("rast", renderedImage)
                .dest("out", resultImage)
                .getRuntime();
        previousScript.set(script);
        previousRuntime.set(runtime);
      }

      runtime.evaluateAll(null);

      // If pixelType does not match with the data type of the result image (should be double since
      // Jiffle only supports
      // double destination image), we need to convert the resultImage to the specified pixel type.
      if (rasterDataType != resultImage.getSampleModel().getDataType()) {
        // Copy the resultImage to a new raster with the specified pixel type
        WritableRaster convertedRaster =
            RasterFactory.createBandedRaster(rasterDataType, width, height, 1, null);
        double[] samples = resultRaster.getSamples(0, 0, width, height, 0, (double[]) null);
        convertedRaster.setSamples(0, 0, width, height, 0, samples);
        return RasterUtils.clone(convertedRaster, null, gridCoverage2D, noDataValue, false);
      } else {
        // build a new GridCoverage2D from the resultImage
        return RasterUtils.clone(resultImage, null, gridCoverage2D, noDataValue, false);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to run map algebra", e);
    }
  }

  public static GridCoverage2D mapAlgebra(
      GridCoverage2D gridCoverage2D, String pixelType, String script) {
    return mapAlgebra(gridCoverage2D, pixelType, script, null);
  }

  private static ColorModel fetchColorModel(
      ColorModel originalColorModel, WritableRaster resultRaster) {
    if (originalColorModel.isCompatibleRaster(resultRaster)) {
      return originalColorModel;
    } else {
      return PlanarImage.createColorModel(resultRaster.getSampleModel());
    }
  }

  public static GridCoverage2D mapAlgebra(
      GridCoverage2D rast0,
      GridCoverage2D rast1,
      String pixelType,
      String script,
      Double noDataValue) {
    if (rast0 == null || rast1 == null || script == null) {
      return null;
    }
    RasterUtils.isRasterSameShape(rast0, rast1);

    RenderedImage renderedImageRast0 = rast0.getRenderedImage();
    int rasterDataType =
        pixelType != null
            ? RasterUtils.getDataTypeCode(pixelType)
            : renderedImageRast0.getSampleModel().getDataType();
    int width = renderedImageRast0.getWidth();
    int height = renderedImageRast0.getHeight();
    // ImageUtils.createConstantImage is slow, manually constructing a buffered image proved to be
    // faster.
    // It also eliminates the data-copying overhead when converting raster data types after running
    // jiffle script.
    WritableRaster resultRaster =
        RasterFactory.createBandedRaster(DataBuffer.TYPE_DOUBLE, width, height, 1, null);

    ColorModel cmRast0 = fetchColorModel(renderedImageRast0.getColorModel(), resultRaster);
    RenderedImage renderedImageRast1 = rast1.getRenderedImage();

    WritableRenderedImage resultImage = new BufferedImage(cmRast0, resultRaster, false, null);
    try {
      String prevScript = previousScript.get();
      JiffleDirectRuntime prevRuntime = previousRuntime.get();
      JiffleDirectRuntime runtime;
      if (prevRuntime != null && script.equals(prevScript)) {
        // Reuse the runtime to avoid recompiling the script
        runtime = prevRuntime;
        runtime.setSourceImage("rast0", renderedImageRast0);
        runtime.setSourceImage("rast1", renderedImageRast1);
        runtime.setDestinationImage("out", resultImage);
        runtime.setDefaultBounds();
      } else {
        JiffleBuilder builder = new JiffleBuilder();
        runtime =
            builder
                .script(script)
                .source("rast0", renderedImageRast0)
                .source("rast1", renderedImageRast1)
                .dest("out", resultImage)
                .getRuntime();
        previousScript.set(script);
        previousRuntime.set(runtime);
      }

      runtime.evaluateAll(null);

      // If pixelType does not match with the data type of the result image (should be double since
      // Jiffle only supports
      // double destination image), we need to convert the resultImage to the specified pixel type.
      if (rasterDataType != resultImage.getSampleModel().getDataType()) {
        // Copy the resultImage to a new raster with the specified pixel type
        WritableRaster convertedRaster =
            RasterFactory.createBandedRaster(rasterDataType, width, height, 1, null);
        double[] samples = resultRaster.getSamples(0, 0, width, height, 0, (double[]) null);
        convertedRaster.setSamples(0, 0, width, height, 0, samples);
        return RasterUtils.clone(convertedRaster, null, rast0, noDataValue, false);
      } else {
        // build a new GridCoverage2D from the resultImage
        return RasterUtils.clone(resultImage, null, rast0, noDataValue, false);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to run map algebra", e);
    }
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return a sum of the provided band values
   */
  public static double[] add(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      result[i] = band1[i] + band2[i];
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return result of subtraction of the two bands, (band2 - band1).
   */
  public static double[] subtract(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      result[i] = band2[i] - band1[i];
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return result of multiplication of the two bands.
   */
  public static double[] multiply(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      result[i] = band1[i] * band2[i];
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return result of subtraction of the two bands, (band1 / band2).
   */
  public static double[] divide(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      result[i] = (double) Math.round(band1[i] / band2[i] * 100) / 100;
    }

    return result;
  }

  /**
   * @param band band values
   * @param factor multiplying factor
   * @return an array where all the elements has been multiplied with the factor given.
   */
  public static double[] multiplyFactor(double[] band, double factor) {
    double[] result = new double[band.length];
    for (int i = 0; i < band.length; i++) {
      result[i] = band[i] * factor;
    }

    return result;
  }

  /**
   * @param band band values
   * @param dividend dividend for modulo
   * @return an array with modular remainders calculated from the given dividend
   */
  public static double[] modulo(double[] band, double dividend) {
    double[] result = new double[band.length];
    for (int i = 0; i < band.length; i++) {
      result[i] = band[i] % dividend;
    }

    return result;
  }

  /**
   * @param band band values
   * @return an array, where each pixel has been applied square root operation.
   */
  public static double[] squareRoot(double[] band) {
    double[] result = new double[band.length];
    for (int i = 0; i < band.length; i++) {
      result[i] = (double) Math.round(Math.sqrt(band[i]) * 100) / 100;
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return an array, where each pixel is result of bitwise AND operator from provided 2 bands.
   */
  public static double[] bitwiseAnd(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      result[i] = (int) band1[i] & (int) band2[i];
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return an array, where each pixel is result of bitwise OR operator from provided 2 bands.
   */
  public static double[] bitwiseOr(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      result[i] = (int) band1[i] | (int) band2[i];
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return an array; if a value at an index in band1 is different in band2 then band1 value is
   *     taken otherwise 0.
   */
  public static double[] logicalDifference(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      if (band1[i] != band2[i]) {
        result[i] = band1[i];
      } else {
        result[i] = 0d;
      }
    }

    return result;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return an array; if a value at an index in band1 is not equal to 0 then band1 value will be
   *     taken otherwise band2's value
   */
  public static double[] logicalOver(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      if (band1[i] != 0d) {
        result[i] = band1[i];
      } else {
        result[i] = band2[i];
      }
    }

    return result;
  }

  /**
   * @param bandValues band values
   * @return an array with normalized band values to be within [0 - 255] range
   */
  public static double[] normalize(double[] bandValues) {
    Double minValue = Arrays.stream(bandValues).min().orElse(Double.NaN);
    Double maxValue = Arrays.stream(bandValues).max().orElse(Double.NaN);

    if (Double.compare(maxValue, minValue) == 0) {
      // Set default value for constant bands to 0
      Arrays.fill(bandValues, 0);
    } else {
      for (int i = 0; i < bandValues.length; i++) {
        bandValues[i] = ((bandValues[i] - minValue) * 255) / (maxValue - minValue);
      }
    }

    return bandValues;
  }

  /**
   * @param band1 band values
   * @param band2 band values
   * @return an array with the normalized difference of the provided bands
   */
  public static double[] normalizedDifference(double[] band1, double[] band2) {
    ensureBandShape(band1.length, band2.length);

    double[] result = new double[band1.length];
    for (int i = 0; i < band1.length; i++) {
      if (band1[i] == 0) {
        band1[i] = -1;
      }
      if (band2[i] == 0) {
        band2[i] = -1;
      }

      result[i] = (double) Math.round(((band2[i] - band1[i]) / (band2[i] + band1[i])) * 100) / 100;
    }

    return result;
  }

  /**
   * @param band band values
   * @return mean of the band values
   */
  public static double mean(double[] band) {
    return (Arrays.stream(band).sum() / band.length) * 100 / 100;
  }

  /**
   * @param band band values
   * @param coordinates defines the region by minX, maxX, minY, and maxY respectively
   * @param dimension dimensions
   * @return an array of the specified region
   */
  public static double[] fetchRegion(double[] band, int[] coordinates, int[] dimension) {
    double[] result =
        new double[(coordinates[2] - coordinates[0] + 1) * (coordinates[3] - coordinates[1] + 1)];
    int k = 0;
    for (int i = coordinates[0]; i < coordinates[2] + 1; i++) {
      for (int j = coordinates[1]; j < coordinates[3] + 1; j++) {
        result[k] = band[(i * dimension[0]) + j];
        k++;
      }
    }

    return result;
  }

  /**
   * @param band band values
   * @return an array with the most reoccurring value or if every value occurs once then return the
   *     provided array
   */
  public static double[] mode(double[] band) {
    Map<Double, Long> frequency =
        Arrays.stream(band)
            .boxed()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    if (frequency.values().stream().max(Long::compare).orElse(0L) == 1L) {
      return band;
    } else {
      return new double[] {
        frequency.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null)
      };
    }
  }

  /**
   * @param band band values
   * @param target target to compare
   * @return an array; mark all band values 1 that are greater than target, otherwise 0
   */
  public static double[] greaterThan(double[] band, double target) {
    double[] result = new double[band.length];

    for (int i = 0; i < band.length; i++) {
      if (band[i] > target) {
        result[i] = 1;
      } else {
        result[i] = 0;
      }
    }

    return result;
  }

  /**
   * @param band band values
   * @param target target to compare
   * @return an array; mark all band values 1 that are greater than or equal to target, otherwise 0
   */
  public static double[] greaterThanEqual(double[] band, double target) {
    double[] result = new double[band.length];

    for (int i = 0; i < band.length; i++) {
      if (band[i] >= target) {
        result[i] = 1;
      } else {
        result[i] = 0;
      }
    }

    return result;
  }

  /**
   * @param band band values
   * @param target target to compare
   * @return an array; mark all band values 1 that are less than target, otherwise 0
   */
  public static double[] lessThan(double[] band, double target) {
    double[] result = new double[band.length];

    for (int i = 0; i < band.length; i++) {
      if (band[i] < target) {
        result[i] = 1;
      } else {
        result[i] = 0;
      }
    }

    return result;
  }

  /**
   * @param band band values
   * @param target target to compare
   * @return an array; mark all band values 1 that are less than or equal to target, otherwise 0
   */
  public static double[] lessThanEqual(double[] band, double target) {
    double[] result = new double[band.length];

    for (int i = 0; i < band.length; i++) {
      if (band[i] <= target) {
        result[i] = 1;
      } else {
        result[i] = 0;
      }
    }

    return result;
  }

  /**
   * @param band band values
   * @param target target to count
   * @return count of the target in the band values
   */
  public static int countValue(double[] band, double target) {
    return (int) Arrays.stream(band).filter(x -> x == target).count();
  }

  /**
   * Throws an IllegalArgumentException if the lengths of the bands are not the same.
   *
   * @param band1 length of band values
   * @param band2 length of band values
   */
  private static void ensureBandShape(int band1, int band2) {
    if (band1 != band2) {
      throw new IllegalArgumentException(
          "The shape of the provided bands is not same. Please check your inputs, it should be same.");
    }
  }
}
