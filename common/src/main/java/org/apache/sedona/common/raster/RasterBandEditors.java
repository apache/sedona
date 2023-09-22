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
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.metadata.spatial.PixelOrientation;

import java.awt.image.Raster;
import java.util.Arrays;

public class RasterBandEditors {
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, int bandIndex, double noDataValue) {
        RasterUtils.ensureBand(raster, bandIndex);
        Double rasterNoData = RasterBandAccessors.getBandNoDataValue(raster, bandIndex);
        if ( !(rasterNoData == null) && rasterNoData == noDataValue) {
            return raster;
        }
        GridSampleDimension[] bands = raster.getSampleDimensions();
        bands[bandIndex - 1] = RasterUtils.createSampleDimensionWithNoDataValue(bands[bandIndex - 1], noDataValue);

        int width = RasterAccessors.getWidth(raster), height = RasterAccessors.getHeight(raster);
        AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
        GridGeometry2D gridGeometry2D = new GridGeometry2D(
                new GridEnvelope2D(0, 0, width, height),
                PixelOrientation.UPPER_LEFT,
                affine, raster.getCoordinateReferenceSystem2D(), null
        );

        return RasterUtils.create(raster.getRenderedImage(), gridGeometry2D, bands, null);
    }

    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, double noDataValue) {
        return setBandNoDataValue(raster, 1, noDataValue);
    }

    /**
     * @param toRaster Raster to add the band
     * @param fromRaster Raster from the band will be copied
     * @param fromBand Index of Raster band that will be copied
     * @param toRasterIndex Index of Raster where the copied band will be changed
     * @return A raster with either replacing a band or adding a new band at the end the toRaster, with the specified band values extracted from the fromRaster at given band
     */
    public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand, int toRasterIndex) {
        RasterUtils.ensureBand(fromRaster, fromBand);
        ensureBandAppend(toRaster, toRasterIndex);
        isRasterSameShape(toRaster, fromRaster);

        int width = RasterAccessors.getWidth(toRaster), height = RasterAccessors.getHeight(toRaster);

        Raster rasterData = RasterUtils.getRaster(fromRaster.getRenderedImage());

        // get datatype of toRaster to preserve data type
        int dataTypeCode = RasterUtils.getRaster(toRaster.getRenderedImage()).getDataBuffer().getDataType();
        int numBands = RasterAccessors.numBands(toRaster);
        Double noDataValue = RasterBandAccessors.getBandNoDataValue(fromRaster, fromBand);

        if (RasterUtils.isDataTypeIntegral(dataTypeCode)) {
            int[] bandValues = rasterData.getSamples(0, 0, width, height, fromBand - 1, (int[]) null);
            if (numBands + 1 == toRasterIndex) {
                return RasterUtils.copyRasterAndAppendBand(toRaster, Arrays.stream(bandValues).boxed().toArray(Integer[]::new), noDataValue);
            } else {
                return RasterUtils.copyRasterAndReplaceBand(toRaster, fromBand, Arrays.stream(bandValues).boxed().toArray(Integer[]::new), noDataValue, false);
            }
        } else {
            double[] bandValues = rasterData.getSamples(0, 0, width, height, fromBand - 1, (double[]) null);
            if (numBands + 1 == toRasterIndex) {
                return RasterUtils.copyRasterAndAppendBand(toRaster, Arrays.stream(bandValues).boxed().toArray(Double[]::new), noDataValue);
            } else {
                return RasterUtils.copyRasterAndReplaceBand(toRaster, fromBand, Arrays.stream(bandValues).boxed().toArray(Double[]::new), noDataValue, false);
            }
        }
    }

    /**
     * The new band will be added to the end of the toRaster
     * @param toRaster Raster to add the band
     * @param fromRaster Raster from the band will be copied
     * @param fromBand Index of Raster band that will be copied
     * @return A raster with either replacing a band or adding a new band at the end the toRaster, with the specified band values extracted from the fromRaster at given band
     */
    public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster, int fromBand) {
        int endBand = RasterAccessors.numBands(toRaster) + 1;
        return addBand(toRaster, fromRaster, fromBand, endBand);
    }

    /**
     * Index of fromRaster will be taken as 1 and will be copied at the end of the toRaster
     * @param toRaster Raster to add the band
     * @param fromRaster Raster from the band will be copied
     * @return A raster with either replacing a band or adding a new band at the end the toRaster, with the specified band values extracted from the fromRaster at given band
     */
    public static GridCoverage2D addBand(GridCoverage2D toRaster, GridCoverage2D fromRaster) {
        return addBand(toRaster, fromRaster, 1);
    }

    /**
     * Check if the band index is either present or at the end of the raster
     * @param raster Raster to check
     * @param band Band index to append to the raster
     */
    private static void ensureBandAppend(GridCoverage2D raster, int band) {
        if (band < 1 || band > RasterAccessors.numBands(raster) + 1) {
            throw new IllegalArgumentException(String.format("Provided band index %d is not present in the raster", band));
        }
    }

    /**
     * Check if the two rasters are of the same shape
     * @param raster1
     * @param raster2
     */
    private static void isRasterSameShape(GridCoverage2D raster1, GridCoverage2D raster2) {
        int width1 = RasterAccessors.getWidth(raster1), height1 = RasterAccessors.getHeight(raster1);
        int width2 = RasterAccessors.getWidth(raster2), height2 = RasterAccessors.getHeight(raster2);

        if (width1 != width2 && height1 != height2) {
            throw new IllegalArgumentException(String.format("Provided rasters are not of same shape. \n" +
                    "First raster having width of %d and height of %d. \n" +
                    "Second raster having width of %d and height of %d", width1, height1, width2, height2));
        }
    }
}
