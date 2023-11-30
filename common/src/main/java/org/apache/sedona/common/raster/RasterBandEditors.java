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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.operation.Crop;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Geometry;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import javax.media.jai.RasterFactory;
import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.util.Arrays;
import java.util.Collections;

public class RasterBandEditors {
    /**
     * Adds no-data value to the raster.
     * @param raster Source raster to add no-data value
     * @param bandIndex Band index to add no-data value
     * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
     * @return Raster with no-data value
     */
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, int bandIndex, Double noDataValue) {
        RasterUtils.ensureBand(raster, bandIndex);
        Double rasterNoData = RasterBandAccessors.getBandNoDataValue(raster, bandIndex);

        // Remove no-Data if it is null
        if (noDataValue == null) {
            if (RasterBandAccessors.getBandNoDataValue(raster) == null) {
                return raster;
            }
            GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
            sampleDimensions [bandIndex - 1] = RasterUtils.removeNoDataValue(sampleDimensions[bandIndex - 1]);
            return RasterUtils.clone(raster.getRenderedImage(), null, sampleDimensions, raster, null, true);
        }

        if ( !(rasterNoData == null) && rasterNoData.equals(noDataValue)) {
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
        return RasterUtils.clone(raster.getRenderedImage(), null, bands, raster, null, true);
    }

    /**
     * Adds no-data value to the raster.
     * @param raster Source raster to add no-data value
     * @param noDataValue Value to set as no-data value, if null then remove existing no-data value
     * @return Raster with no-data value
     */
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, Double noDataValue) {
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

    /**
     * Return a clipped raster with the specified ROI by the geometry
     * @param raster Raster to clip
     * @param band Band number to perform clipping
     * @param geometry Specify ROI
     * @param noDataValue no-Data value for empty cells
     * @param crop Specifies to keep the original extent or not
     * @return A clip Raster with defined ROI by the geometry
     */
    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry, double noDataValue, boolean crop) throws FactoryException, TransformException {

        // Selecting the band from original raster
        RasterUtils.ensureBand(raster, band);
        GridCoverage2D singleBandRaster = RasterBandAccessors.getBand(raster, new int[]{band});

        // Crop the raster
        // this will shrink the extent of the raster to the geometry
        Crop cropObject = new Crop();
        ParameterValueGroup parameters = cropObject.getParameters();
        parameters.parameter("Source").setValue(singleBandRaster);
        parameters.parameter(Crop.PARAMNAME_DEST_NODATA).setValue(new double[]{noDataValue});
        parameters.parameter(Crop.PARAMNAME_ROI).setValue(geometry);

        GridCoverage2D newRaster = (GridCoverage2D) cropObject.doOperation(parameters, null);

        if (!crop) {
            double[] metadataOriginal = RasterAccessors.metadata(raster);
            int widthOriginalRaster = (int) metadataOriginal[2], heightOriginalRaster = (int) metadataOriginal[3];
            Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());


            // create a new raster and set a default value that's the no-data value
            String bandType = RasterBandAccessors.getBandType(raster, 1);
            int dataTypeCode = RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, 1));
            boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(dataTypeCode);
            WritableRaster resultRaster = RasterFactory.createBandedRaster(dataTypeCode, widthOriginalRaster, heightOriginalRaster, 1, null);
            int sizeOfArray = widthOriginalRaster * heightOriginalRaster;
            if (isDataTypeIntegral) {
                int[] array = ArrayUtils.toPrimitive(Collections.nCopies(sizeOfArray, (int) noDataValue).toArray(new Integer[sizeOfArray]));
                resultRaster.setSamples(0, 0, widthOriginalRaster, heightOriginalRaster, 0, array);
            } else {
                double[] array = ArrayUtils.toPrimitive(Collections.nCopies(sizeOfArray, noDataValue).toArray(new Double[sizeOfArray]));
                resultRaster.setSamples(0, 0, widthOriginalRaster, heightOriginalRaster, 0, array);
            }

            // rasterize the geometry to iterate over the clipped raster
            GridCoverage2D rasterized = RasterConstructors.asRaster(geometry, raster, bandType, 150);
            Raster rasterizedData = RasterUtils.getRaster(rasterized.getRenderedImage());
            double[] metadataRasterized = RasterAccessors.metadata(rasterized);
            int widthRasterized = (int) metadataRasterized[2], heightRasterized = (int) metadataRasterized[3];

            for (int j = 0; j < heightRasterized; j++) {
                for(int i = 0; i < widthRasterized; i++) {
                    Point2D point = RasterUtils.getWorldCornerCoordinates(rasterized, i, j);
                    int[] rasterCoord = RasterUtils.getGridCoordinatesFromWorld(raster, point.getX(), point.getY());
                    int x = Math.abs(rasterCoord[0]), y = Math.abs(rasterCoord[1]);

                    if (rasterizedData.getPixel(i, j, (int[]) null)[0] == 0) {
                        continue;
                    }

                    if (isDataTypeIntegral) {
                        int[] pixelValue = rasterData.getPixel(x, y, (int[]) null);

                        resultRaster.setPixel(x, y, new int[]{pixelValue[band - 1]});
                    } else {
                        double[] pixelValue = rasterData.getPixel(x, y, (double[]) null);

                        resultRaster.setPixel(x, y, new double[]{pixelValue[band - 1]});
                    }
                }
            }
            newRaster = RasterUtils.clone(resultRaster, raster.getGridGeometry(), newRaster.getSampleDimensions(), newRaster, noDataValue, true);
        } else {
            newRaster = RasterUtils.clone(newRaster.getRenderedImage(), newRaster.getGridGeometry(), newRaster.getSampleDimensions(), newRaster, noDataValue, true);
        }

        return newRaster;
    }

    /**
     * Return a clipped raster with the specified ROI by the geometry.
     * @param raster Raster to clip
     * @param band Band number to perform clipping
     * @param geometry Specify ROI
     * @param noDataValue no-Data value for empty cells
     * @return A clip Raster with defined ROI by the geometry
     */
    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry, double noDataValue) throws FactoryException, TransformException {
        return clip(raster, band, geometry, noDataValue, true);
    }

    /**
     * Return a clipped raster with the specified ROI by the geometry. No-data value will be taken as the lowest possible value for the data type and crop will be `true`.
     * @param raster Raster to clip
     * @param band Band number to perform clipping
     * @param geometry Specify ROI
     * @return A clip Raster with defined ROI by the geometry
     */
    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry) throws FactoryException, TransformException {
        boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));

        if (isDataTypeIntegral) {
            double noDataValue = Integer.MIN_VALUE;
            return clip(raster, band, geometry, noDataValue, true);
        } else {
            double noDataValue = Double.MIN_VALUE;
            return clip(raster, band, geometry, noDataValue, true);
        }
    }

//    /**
//     * Return a clipped raster with the specified ROI by the geometry.
//     * @param raster Raster to clip
//     * @param band Band number to perform clipping
//     * @param geometry Specify ROI
//     * @param crop Specifies to keep the original extent or not
//     * @return A clip Raster with defined ROI by the geometry
//     */
//    public static GridCoverage2D clip(GridCoverage2D raster, int band, Geometry geometry, boolean crop) {
//        boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(RasterUtils.getDataTypeCode(RasterBandAccessors.getBandType(raster, band)));
//
//        if (isDataTypeIntegral) {
//            double noDataValue = Integer.MIN_VALUE;
//            return clip(raster, band, geometry, noDataValue, crop);
//        } else {
//            double noDataValue = Double.MIN_VALUE;
//            return clip(raster, band, geometry, noDataValue, crop);
//        }
//    }
}
