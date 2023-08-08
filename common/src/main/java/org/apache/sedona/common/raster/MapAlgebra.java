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

import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.metadata.i18n.Vocabulary;
import org.geotools.metadata.i18n.VocabularyKeys;
import org.geotools.util.NumberRange;

import javax.media.jai.RasterFactory;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;

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

        Raster raster = rasterGeom.getRenderedImage().getData();
        // Get the width and height of the raster
        int width = raster.getWidth();
        int height = raster.getHeight();

        // Array to hold the band values
        double[] bandValues = new double[width * height];

        // Iterate over each pixel
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                // Get the value and store it in the 1D array
                bandValues[y * width + x] = raster.getSample(x, y, bandIndex - 1);
            }
        }
        return bandValues;
    }

    /**
     * Adds a new band to the given raster, using the given array as the band values.
     * @param rasterGeom
     * @param bandValues
     * @param bandIndex starts at 1, and no larger than numBands + 1
     * @return
     */
    public static GridCoverage2D addBandFromArray(GridCoverage2D rasterGeom, double[] bandValues, int bandIndex, double noDataValue) {
        int numBands = rasterGeom.getNumSampleDimensions();
        // Allow the band index to be one larger than the number of bands, which will append the band to the end
        if (bandIndex < 1 || bandIndex > numBands + 1) {
            throw new IllegalArgumentException("Band index is out of bounds. Must be between 1 and " + (numBands + 1) + ")");
        }

        if (bandIndex == numBands + 1) {
            return copyRasterAndAppendBand(rasterGeom, bandValues, noDataValue);
        }
        else {
            return copyRasterAndReplaceBand(rasterGeom, bandIndex, bandValues);
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
        Raster raster = originalImage.getData();
        Point location = gridCoverage2D.getRenderedImage().getData().getBounds().getLocation();
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
            Category noDataCategory = new Category(
                    Category.NODATA.getName(),
                    new Color[] {new Color(0, 0, 0, 0)},
                    NumberRange.create(noDataValue, noDataValue));
            Category[] categories = new Category[] {noDataCategory};
            sampleDimensions[numBand - 1] = new GridSampleDimension("band" + numBand, categories, null);
        }else {
            sampleDimensions[numBand - 1] = new GridSampleDimension("band" + numBand);
        }
        // Construct a GridCoverage2D with the copied image.
        return RasterUtils.create(wr, gridCoverage2D.getGridGeometry(), sampleDimensions);
    }

    private static GridCoverage2D copyRasterAndAppendBand(GridCoverage2D gridCoverage2D, double[] bandValues) {
        return copyRasterAndAppendBand(gridCoverage2D, bandValues, null);
    }

    private static GridCoverage2D copyRasterAndReplaceBand(GridCoverage2D gridCoverage2D, int bandIndex, double[] bandValues) {
        // Do not allow the band index to be out of bounds
        if (bandIndex < 1 || bandIndex > gridCoverage2D.getNumSampleDimensions()) {
            throw new IllegalArgumentException("Band index is out of bounds. Must be between 1 and " + gridCoverage2D.getNumSampleDimensions() + ")");
        }
        // Get the original image and its properties
        RenderedImage originalImage = gridCoverage2D.getRenderedImage();
        Raster raster = originalImage.getData();
        WritableRaster wr = raster.createCompatibleWritableRaster();
        // Copy the raster data and replace the band values
        for (int i = 0; i < raster.getWidth(); i++) {
            for (int j = 0; j < raster.getHeight(); j++) {
                double[] bands = raster.getPixel(i, j, (double[]) null);
                bands[bandIndex - 1] = bandValues[j * raster.getWidth() + i];
                wr.setPixel(i, j, bands);
            }
        }
        // Create a new GridCoverage2D with the copied image
        return RasterUtils.create(wr, gridCoverage2D.getGridGeometry(), gridCoverage2D.getSampleDimensions());
    }
}
