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
}
