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
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;

import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import java.awt.Point;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.awt.image.WritableRenderedImage;
import java.util.Arrays;

public class MapAlgebra
{
    /**
     * Returns the values of the given band as a 1D array.
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
     * @param rasterGeom
     * @param bandValues
     * @param bandIndex starts at 1, and no larger than numBands + 1
     * @return
     */
    public static GridCoverage2D addBandFromArray(GridCoverage2D rasterGeom, double[] bandValues, int bandIndex, Double noDataValue) {
        int numBands = rasterGeom.getNumSampleDimensions();
        // Allow the band index to be one larger than the number of bands, which will append the band to the end
        if (bandIndex < 1 || bandIndex > numBands + 1) {
            throw new IllegalArgumentException("Band index is out of bounds. Must be between 1 and " + (numBands + 1) + ")");
        }

        if (bandIndex == numBands + 1) {
            return copyRasterAndAppendBand(rasterGeom, bandValues, noDataValue);
        }
        else {
            return copyRasterAndReplaceBand(rasterGeom, bandIndex, bandValues, noDataValue, true);
        }
    }

    public static GridCoverage2D addBandFromArray(GridCoverage2D rasterGeom, double[] bandValues, int bandIndex) {
        int numBands = rasterGeom.getNumSampleDimensions();
        // Allow the band index to be one larger than the number of bands, which will append the band to the end
        if (bandIndex < 1 || bandIndex > numBands + 1) {
            throw new IllegalArgumentException("Band index is out of bounds. Must be between 1 and " + (numBands + 1) + ")");
        }

        if (bandIndex == numBands + 1) {
            return copyRasterAndAppendBand(rasterGeom, bandValues);
        }
        else {
            return copyRasterAndReplaceBand(rasterGeom, bandIndex, bandValues);
        }
    }


    /**
     * Adds a new band to the given raster, using the given array as the band values. The new band is appended to the end.
     * @param rasterGeom
     * @param bandValues
     * @return
     */
    public static GridCoverage2D addBandFromArray(GridCoverage2D rasterGeom, double[] bandValues) {
        return addBandFromArray(rasterGeom, bandValues, rasterGeom.getNumSampleDimensions() + 1);
    }

    /**
     * This is an experimental method as it does not copy the original raster properties (e.g. color model, sample model, etc.)
     * TODO: Copy the original raster properties
     * @param gridCoverage2D
     * @param bandValues
     * @return
     */
    private static GridCoverage2D copyRasterAndAppendBand(GridCoverage2D gridCoverage2D, double[] bandValues, Double noDataValue) {
        // Get the original image and its properties
        RenderedImage originalImage = gridCoverage2D.getRenderedImage();
        Raster raster = RasterUtils.getRaster(originalImage);
        Point location = raster.getBounds().getLocation();
        WritableRaster wr = RasterFactory.createBandedRaster(raster.getDataBuffer().getDataType(), originalImage.getWidth(), originalImage.getHeight(), gridCoverage2D.getNumSampleDimensions() + 1, location);
        // Copy the raster data and append the new band values
        for (int i = 0; i < raster.getWidth(); i++) {
            for (int j = 0; j < raster.getHeight(); j++) {
                double[] pixels = raster.getPixel(i, j, (double[]) null);
                double[] copiedPixels = new double[pixels.length + 1];
                System.arraycopy(pixels, 0, copiedPixels, 0, pixels.length);
                copiedPixels[pixels.length] = bandValues[j * raster.getWidth() + i];
                wr.setPixel(i, j, copiedPixels);
            }
        }
        // Add a sample dimension for newly added band
        int numBand = wr.getNumBands();
        GridSampleDimension[] originalSampleDimensions = gridCoverage2D.getSampleDimensions();
        GridSampleDimension[] sampleDimensions = new GridSampleDimension[numBand];
        System.arraycopy(originalSampleDimensions, 0, sampleDimensions, 0, originalSampleDimensions.length);
        if (noDataValue != null) {
            sampleDimensions[numBand - 1] = RasterUtils.createSampleDimensionWithNoDataValue("band" + numBand, noDataValue);
        } else {
            sampleDimensions[numBand - 1] = new GridSampleDimension("band" + numBand);
        }
        // Construct a GridCoverage2D with the copied image.
        return RasterUtils.create(wr, gridCoverage2D.getGridGeometry(), sampleDimensions);
    }

    private static GridCoverage2D copyRasterAndAppendBand(GridCoverage2D gridCoverage2D, double[] bandValues) {
        return copyRasterAndAppendBand(gridCoverage2D, bandValues, null);
    }

    private static GridCoverage2D copyRasterAndReplaceBand(GridCoverage2D gridCoverage2D, int bandIndex, double[] bandValues, Double noDataValue, boolean removeNoDataIfNull) {
        // Do not allow the band index to be out of bounds
        if (bandIndex < 1 || bandIndex > gridCoverage2D.getNumSampleDimensions()) {
            throw new IllegalArgumentException("Band index is out of bounds. Must be between 1 and " + gridCoverage2D.getNumSampleDimensions() + ")");
        }
        // Get the original image and its properties
        RenderedImage originalImage = gridCoverage2D.getRenderedImage();
        Raster raster = RasterUtils.getRaster(originalImage);
        WritableRaster wr = raster.createCompatibleWritableRaster();
        // Copy the raster data and replace the band values
        for (int i = 0; i < raster.getWidth(); i++) {
            for (int j = 0; j < raster.getHeight(); j++) {
                double[] bands = raster.getPixel(i, j, (double[]) null);
                bands[bandIndex - 1] = bandValues[j * raster.getWidth() + i];
                wr.setPixel(i, j, bands);
            }
        }
        GridSampleDimension[] sampleDimensions = gridCoverage2D.getSampleDimensions();
        GridSampleDimension sampleDimension = sampleDimensions[bandIndex - 1];
        if (noDataValue == null && removeNoDataIfNull) {
            sampleDimensions[bandIndex - 1] = RasterUtils.removeNoDataValue(sampleDimension);
        } else if (noDataValue != null) {
            sampleDimensions[bandIndex - 1] = RasterUtils.createSampleDimensionWithNoDataValue(sampleDimension, noDataValue);
        }
        return RasterUtils.create(wr, gridCoverage2D.getGridGeometry(), sampleDimensions);
    }

    private static GridCoverage2D copyRasterAndReplaceBand(GridCoverage2D gridCoverage2D, int bandIndex, double[] bandValues) {
        return copyRasterAndReplaceBand(gridCoverage2D, bandIndex, bandValues, null, false);
    }

    private static final ThreadLocal<String> previousScript = new ThreadLocal<>();
    private static final ThreadLocal<JiffleDirectRuntime> previousRuntime = new ThreadLocal<>();

    /**
     * Applies a map algebra script to the given raster.
     * @param gridCoverage2D The raster to apply the script to
     * @param pixelType The pixel type of the output raster. If null, the pixel type of the input raster is used.
     * @param script The script to apply
     * @param noDataValue The no data value of the output raster.
     * @return The result of the map algebra script
     */
    public static GridCoverage2D mapAlgebra(GridCoverage2D gridCoverage2D, String pixelType, String script, Double noDataValue) {
        if (gridCoverage2D == null || script == null) {
            return null;
        }
        RenderedImage renderedImage = gridCoverage2D.getRenderedImage();
        int rasterDataType = pixelType != null? RasterUtils.getDataTypeCode(pixelType) : renderedImage.getSampleModel().getDataType();
        int width = renderedImage.getWidth();
        int height = renderedImage.getHeight();
        // ImageUtils.createConstantImage is slow, manually constructing a buffered image proved to be faster.
        // It also eliminates the data-copying overhead when converting raster data types after running jiffle script.
        WritableRaster resultRaster = RasterFactory.createBandedRaster(DataBuffer.TYPE_DOUBLE, width, height, 1, null);
        ColorModel cm = PlanarImage.createColorModel(resultRaster.getSampleModel());
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
                runtime = builder.script(script).source("rast", renderedImage).dest("out", resultImage).getRuntime();
                previousScript.set(script);
                previousRuntime.set(runtime);
            }

            runtime.evaluateAll(null);

            // If pixelType does not match with the data type of the result image (should be double since Jiffle only supports
            // double destination image), we need to convert the resultImage to the specified pixel type.
            if (rasterDataType != resultImage.getSampleModel().getDataType()) {
                // Copy the resultImage to a new raster with the specified pixel type
                WritableRaster convertedRaster = RasterFactory.createBandedRaster(rasterDataType, width, height, 1, null);
                double[] samples = resultRaster.getSamples(0, 0, width, height, 0, (double[]) null);
                convertedRaster.setSamples(0, 0, width, height, 0, samples);
                return RasterUtils.create(convertedRaster, gridCoverage2D.getGridGeometry(), null, noDataValue);
            } else {
                // build a new GridCoverage2D from the resultImage
                return RasterUtils.create(resultImage, gridCoverage2D.getGridGeometry(), null, noDataValue);
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
        for(int i = 0; i < band1.length; i++) {
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
        for(int i = 0; i < band1.length; i++) {
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
            result[i] = (double) Math.round(band1[i] / band2[i] * 100) /100;
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
     * @return an array; if a value at an index in band1 is different in band2 then band1 value is taken otherwise 0.
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
     * @return an array; if a value at an index in band1 is not equal to 0 then band1 value will be taken otherwise band2's value
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
     * @param band band values
     * @return an array with normalized band values to be within [0 - 255] range
     */
    public static double[] normalize(double[] band) {
        double[] result = new double[band.length];
        double normalizer = Arrays.stream(band).max().getAsDouble() / 255d;

        for (int i = 0; i < band.length; i++) {
            result[i] = (int) (band[i] / normalizer);
        }

        return result;
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
     * Throws an IllegalArgumentException if the lengths of the bands are not the same.
     * @param band1 length of band values
     * @param band2 length of band values
     */
    private static void ensureBandShape(int band1, int band2) {
        if (band1 != band2) {
            throw new IllegalArgumentException("The shape of the provided bands is not same. Please check your inputs, it should be same.");
        }
    }
}
