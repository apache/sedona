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

import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;

import javax.media.jai.Interpolation;
import java.awt.geom.Point2D;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.apache.sedona.common.raster.MapAlgebra.addBandFromArray;
import static org.apache.sedona.common.raster.MapAlgebra.bandAsArray;

public class RasterEditors
{

    public static GridCoverage2D setBandPixelType(GridCoverage2D raster, String bandDataType) {
        return setBandPixelType(raster, bandDataType, null);
    }

    /**
     * Changes the pixel type of a specified band or all bands in a raster.
     *
     * @param raster The raster whose band pixel type is to be changed.
     * @param bandDataType The target data type for the band ("D", "F", "I", "S", "US", "B").
     * @param bandIndex The index of the band to be modified (1-based index), or null to modify all bands.
     * @return The raster with the modified band pixel type.
     */
    public static GridCoverage2D setBandPixelType(GridCoverage2D raster, String bandDataType, Integer bandIndex) {
        int numBands = raster.getNumSampleDimensions();

        if (bandIndex == null) {
            for (int i = 1; i <= numBands; i++) {
                double[] bandValues = bandAsArray(raster, i);
                bandValues = changeBandDataType(bandValues, bandDataType);
                raster = addBandFromArray(raster, bandValues, i);
            }
        } else {
            double[] bandValues = bandAsArray(raster, bandIndex);
            bandValues = changeBandDataType(bandValues, bandDataType);
            raster = addBandFromArray(raster, bandValues, bandIndex);
        }

        return raster;
    }

    private static double[] changeBandDataType(double[] bandValues, String bandDataType) {
        int dataType = mapBandDataTypeToDataBufferType(bandDataType);

        for (int i = 0; i < bandValues.length; i++) {
            bandValues[i] = castRasterDataType(bandValues[i], dataType);
        }
        return bandValues;
    }

    private static int mapBandDataTypeToDataBufferType(String bandDataType) {
        switch (bandDataType) {
            case "D":
                return DataBuffer.TYPE_DOUBLE;
            case "F":
                return DataBuffer.TYPE_FLOAT;
            case "I":
                return DataBuffer.TYPE_INT;
            case "S":
                return DataBuffer.TYPE_SHORT;
            case "US":
                return DataBuffer.TYPE_USHORT; // Assuming this maps to TYPE_USHORT
            case "B":
                return DataBuffer.TYPE_BYTE;
            default:
                throw new IllegalArgumentException("Invalid Band Data Type");
        }
    }

    public static GridCoverage2D setSrid(GridCoverage2D raster, int srid)
    {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            crs = FunctionsGeoTools.sridToCRS(srid);
        }

        GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
        MathTransform2D transform = raster.getGridGeometry().getGridToCRS2D();
        Map<?, ?> properties = raster.getProperties();
        GridCoverage[] sources = raster.getSources().toArray(new GridCoverage[0]);
        return gridCoverageFactory.create(raster.getName().toString(), raster.getRenderedImage(), crs, transform, raster.getSampleDimensions(), sources, properties);
    }

    public static GridCoverage2D setGeoReference(GridCoverage2D raster, String geoRefCoords, String format) {
        String[] coords = geoRefCoords.split(" ");
        if (coords.length != 6) {
            return null;
        }

        double scaleX = Double.parseDouble(coords[0]);
        double skewY = Double.parseDouble(coords[1]);
        double skewX = Double.parseDouble(coords[2]);
        double scaleY = Double.parseDouble(coords[3]);
        double upperLeftX = Double.parseDouble(coords[4]);
        double upperLeftY = Double.parseDouble(coords[5]);
        AffineTransform2D affine;


        if (format.equalsIgnoreCase("GDAL")) {
            affine = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        } else if (format.equalsIgnoreCase("ESRI")) {
            upperLeftX = upperLeftX - (scaleX * 0.5);
            upperLeftY = upperLeftY - (scaleY * 0.5);
            affine = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        } else {
            throw new IllegalArgumentException("Please select between the following formats GDAL and ESRI");
        }
        int height = RasterAccessors.getHeight(raster), width = RasterAccessors.getWidth(raster);

        GridGeometry2D gridGeometry2D = new GridGeometry2D(
                new GridEnvelope2D(0, 0, width, height),
                PixelOrientation.UPPER_LEFT,
                affine, raster.getCoordinateReferenceSystem(), null
        );
        return RasterUtils.clone(raster.getRenderedImage(), gridGeometry2D, raster.getSampleDimensions(), raster, null, true);
    }

    public static GridCoverage2D setGeoReference(GridCoverage2D raster, String geoRefCoords) {
        return setGeoReference(raster, geoRefCoords, "GDAL");
    }

    public static GridCoverage2D setGeoReference(GridCoverage2D raster, double upperLeftX, double upperLeftY,
                                                 double scaleX, double scaleY, double skewX, double skewY) {
        String geoRedCoord = String.format("%f %f %f %f %f %f", scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        return setGeoReference(raster, geoRedCoord, "GDAL");
    }

    public static GridCoverage2D resample(GridCoverage2D raster, double widthOrScale, double heightOrScale, double gridX, double gridY, boolean useScale,String algorithm) throws TransformException {
        /*
        * Old Parameters
        */
        AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
        int originalWidth = RasterAccessors.getWidth(raster), originalHeight = RasterAccessors.getHeight(raster);
        double upperLeftX = affine.getTranslateX(), upperLeftY = affine.getTranslateY();
        double originalSkewX = affine.getShearX(), originalSkewY = affine.getShearY();
        double originalScaleX = affine.getScaleX(), originalScaleY = affine.getScaleY();
        CoordinateReferenceSystem crs = raster.getCoordinateReferenceSystem2D();

        /*
         * New Parameters
         */
        int newWidth = useScale ? originalWidth : (int)Math.floor(widthOrScale);
        int newHeight = useScale ? originalHeight : (int)Math.floor(heightOrScale);
        double newScaleX = useScale ? widthOrScale : originalScaleX;
        double newScaleY = useScale ? heightOrScale : originalScaleY;
        double newUpperLeftX = upperLeftX, newUpperLeftY = upperLeftY;

        if (noConfigChange(originalWidth, originalHeight, upperLeftX, upperLeftY, originalScaleX, originalScaleY, newWidth, newHeight, gridX, gridY, newScaleX, newScaleY, useScale)) {
            // no reconfiguration parameters provided
            return raster;
        }


        Envelope2D envelope2D = raster.getEnvelope2D();
        //process scale changes due to changes in widthOrScale and heightOrScale
        if (!useScale) {
            newScaleX = (Math.abs(envelope2D.getMaxX() - envelope2D.getMinX())) / newWidth;
            newScaleY = Math.signum(originalScaleY) * Math.abs(envelope2D.getMaxY() - envelope2D.getMinY()) / newHeight;
        }else {
            //height and width cannot have floating point, ceil them to next greatest integer in that case.
            newWidth = (int) Math.ceil(Math.abs(envelope2D.getMaxX() - envelope2D.getMinX()) / Math.abs(newScaleX));
            newHeight = (int) Math.ceil(Math.abs(envelope2D.getMaxY() - envelope2D.getMinY()) / Math.abs(newScaleY));
        }

        if (!approximateEquals(upperLeftX, gridX) || !approximateEquals(upperLeftY, gridY)) {
            //change upperLefts to gridX/Y to check if any warping is needed
            GridCoverage2D tempRaster = setGeoReference(raster, gridX, gridY, newScaleX, newScaleY, originalSkewX, originalSkewY);

            //check expected grid coordinates for old upperLefts
            int[] expectedCellCoordinates = RasterUtils.getGridCoordinatesFromWorld(tempRaster, upperLeftX, upperLeftY);

            //get expected world coordinates at the expected grid coordinates
            Point2D expectedGeoPoint = RasterUtils.getWorldCornerCoordinates(tempRaster, expectedCellCoordinates[0] + 1, expectedCellCoordinates[1] + 1);

            //check for shift
            if (!approximateEquals(expectedGeoPoint.getX(), upperLeftX)) {
                if (!useScale) {
                    newScaleX = Math.abs(envelope2D.getMaxX() - expectedGeoPoint.getX()) / newWidth;
                }else {
                    //width cannot have floating point, ceil it to next greatest integer in that case.
                    newWidth = (int) Math.ceil(Math.abs(envelope2D.getMaxX() - expectedGeoPoint.getX()) / Math.abs(newScaleX));
                }
                newUpperLeftX = expectedGeoPoint.getX();
            }

            if (!approximateEquals(expectedGeoPoint.getY(), upperLeftY)) {
                if (!useScale) {
                    newScaleY = Math.signum(newScaleY) * Math.abs(envelope2D.getMinY() - expectedGeoPoint.getY()) / newHeight;
                }else {
                    //height cannot have floating point, ceil it to next greatest integer in that case.
                    newHeight = (int) Math.ceil(Math.abs(envelope2D.getMinY() - expectedGeoPoint.getY()) / Math.abs(newScaleY));
                }
                newUpperLeftY = expectedGeoPoint.getY();
            }
        }

        MathTransform transform = new AffineTransform2D(newScaleX, originalSkewY, originalSkewX, newScaleY, newUpperLeftX, newUpperLeftY);
        GridGeometry2D gridGeometry = new GridGeometry2D(
                new GridEnvelope2D(0, 0, newWidth, newHeight),
                PixelInCell.CELL_CORNER,
                transform, crs, null);
        Interpolation resamplingAlgorithm = Interpolation.getInstance(0);
        if (!Objects.isNull(algorithm) && !algorithm.isEmpty()) {
            if (algorithm.equalsIgnoreCase("nearestneighbor")) {
                resamplingAlgorithm = Interpolation.getInstance(0);
            }else if (algorithm.equalsIgnoreCase("bilinear")) {
                resamplingAlgorithm = Interpolation.getInstance(1);
            }else if (algorithm.equalsIgnoreCase("bicubic")) {
                resamplingAlgorithm = Interpolation.getInstance(2);
            }
        }
        GridCoverage2D newRaster = (GridCoverage2D) Operations.DEFAULT.resample(raster, null, gridGeometry, resamplingAlgorithm);
        return newRaster;
    }
    public static GridCoverage2D resample(GridCoverage2D raster, double widthOrScale, double heightOrScale, boolean useScale, String algorithm) throws TransformException {
        return resample(raster, widthOrScale, heightOrScale, RasterAccessors.getUpperLeftX(raster), RasterAccessors.getUpperLeftY(raster), useScale, algorithm);

    }

    public static GridCoverage2D resample(GridCoverage2D raster, GridCoverage2D referenceRaster, boolean useScale, String algorithm) throws FactoryException, TransformException {
        int srcSRID = RasterAccessors.srid(raster);
        int destSRID = RasterAccessors.srid(referenceRaster);
        if (srcSRID != destSRID) {
            throw new IllegalArgumentException("Provided input raster and reference raster have different SRIDs");
        }
        double[] refRasterMetadata = RasterAccessors.metadata(referenceRaster);
        int newWidth = (int) refRasterMetadata[2];
        int newHeight = (int) refRasterMetadata[3];
        double gridX = refRasterMetadata[0];
        double gridY = refRasterMetadata[1];
        double newScaleX = refRasterMetadata[4];
        double newScaleY = refRasterMetadata[5];
        if (useScale) {
            return resample(raster, newScaleX, newScaleY, gridX, gridY, useScale, algorithm);
        }
        return resample(raster, newWidth, newHeight, gridX, gridY, useScale, algorithm);
    }

    private static boolean approximateEquals(double a, double b) {
        double tolerance = 1E-6;
        return Math.abs(a - b) <= tolerance;
    }

    private static boolean noConfigChange(int oldWidth, int oldHeight, double oldUpperX, double oldUpperY, double originalScaleX, double originalScaleY,
                                          int newWidth, int newHeight, double newUpperX, double newUpperY, double newScaleX, double newScaleY, boolean useScale) {
        if (!useScale)
            return ((oldWidth == newWidth) && (oldHeight == newHeight)  && (approximateEquals(oldUpperX, newUpperX)) && (approximateEquals(oldUpperY, newUpperY)));
        return ((approximateEquals(originalScaleX, newScaleX)) && (approximateEquals(originalScaleY, newScaleY))  && (approximateEquals(oldUpperX, newUpperX)) && (approximateEquals(oldUpperY, newUpperY)));
    }

    public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom) {
        return normalizeAll(rasterGeom, 0d, 255d, true, null, null, null);
    }

    public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom, double minLim, double maxLim) {
        return normalizeAll(rasterGeom, minLim, maxLim, true, null, null, null);
    }

    public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom, double minLim, double maxLim, boolean normalizeAcrossBands) {
        return normalizeAll(rasterGeom, minLim, maxLim, normalizeAcrossBands, null, null, null);
    }

    public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom, double minLim, double maxLim, boolean normalizeAcrossBands, Double noDataValue) {
        return normalizeAll(rasterGeom, minLim, maxLim, normalizeAcrossBands, noDataValue, null, null);
    }

    public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom, double minLim, double maxLim, Double noDataValue, Double minValue, Double maxValue) {
        return normalizeAll(rasterGeom, minLim, maxLim, true, noDataValue, minValue, maxValue);
    }

    /**
     *
     * @param rasterGeom Raster to be normalized
     * @param minLim Lower limit of normalization range
     * @param maxLim Upper limit of normalization range
     * @param normalizeAcrossBands flag to determine the normalization method
     * @param noDataValue NoDataValue used in raster
     * @param minValue Minimum value in raster
     * @param maxValue Maximum value in raster
     * @return a raster with all values in all bands normalized between minLim and maxLim
     */
    public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom, double minLim, double maxLim, boolean normalizeAcrossBands, Double noDataValue, Double minValue, Double maxValue) {
        if (minLim > maxLim) {
            throw new IllegalArgumentException("minLim cannot be greater than maxLim");
        }

        int numBands = rasterGeom.getNumSampleDimensions();
        RenderedImage renderedImage = rasterGeom.getRenderedImage();
        int rasterDataType = renderedImage.getSampleModel().getDataType();

        double globalMin = minValue != null ? minValue : Double.MAX_VALUE;
        double globalMax = maxValue != null ? maxValue : -Double.MAX_VALUE;

        // Initialize arrays to store band-wise min and max values
        double[] minValues = new double[numBands];
        double[] maxValues = new double[numBands];
        Arrays.fill(minValues, Double.MAX_VALUE);
        Arrays.fill(maxValues, -Double.MAX_VALUE);

        // Trigger safe mode if noDataValue is null - noDataValue is set to maxLim and data values are normalized to range [minLim, maxLim-1].
        // This is done to prevent setting valid data as noDataValue.
        double safetyTrigger = (noDataValue == null) ? 1 : 0;

        // Compute global min and max values across all bands if necessary and not provided
        if (minValue == null || maxValue == null) {
            for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
                double[] bandValues = bandAsArray(rasterGeom, bandIndex + 1);
                double bandNoDataValue = RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(bandIndex));

                if (noDataValue == null) {
                    noDataValue = maxLim;
                }

                for (double val : bandValues) {
                    if (val != bandNoDataValue) {
                        if (normalizeAcrossBands) {
                            globalMin = Math.min(globalMin, val);
                            globalMax = Math.max(globalMax, val);
                        } else {
                            minValues[bandIndex] = Math.min(minValues[bandIndex], val);
                            maxValues[bandIndex] = Math.max(maxValues[bandIndex], val);
                        }
                    }
                }
            }
        } else {
            globalMin = minValue;
            globalMax = maxValue;
        }

        // Normalize each band
        for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
            double[] bandValues = bandAsArray(rasterGeom, bandIndex + 1);
            double bandNoDataValue = RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(bandIndex));
            double currentMin = normalizeAcrossBands ? globalMin : (minValue != null ? minValue : minValues[bandIndex]);
            double currentMax = normalizeAcrossBands ? globalMax : (maxValue != null ? maxValue : maxValues[bandIndex]);

            if (Double.compare(currentMax, currentMin) == 0) {
                Arrays.fill(bandValues, minLim);
            } else {
                for (int i = 0; i < bandValues.length; i++) {
                    if (bandValues[i] != bandNoDataValue) {
                        double normalizedValue = minLim + ((bandValues[i] - currentMin) * (maxLim - safetyTrigger - minLim)) / (currentMax - currentMin);
                        bandValues[i] = castRasterDataType(normalizedValue, rasterDataType);
                    } else {
                        bandValues[i] = noDataValue;
                    }
                }
            }

            // Update the raster with the normalized band and noDataValue
            rasterGeom = addBandFromArray(rasterGeom, bandValues, bandIndex+1);
            rasterGeom = RasterBandEditors.setBandNoDataValue(rasterGeom, bandIndex+1, noDataValue);
        }

        return rasterGeom;
    }

    private static double castRasterDataType(double value, int dataType) {
        switch (dataType) {
            case DataBuffer.TYPE_BYTE:
                return (byte) value;
            case DataBuffer.TYPE_SHORT:
                return (short) value;
            case DataBuffer.TYPE_INT:
                return (int) value;
            case DataBuffer.TYPE_USHORT:
                return (char) value;
            case DataBuffer.TYPE_FLOAT:
                return (float) value;
            case DataBuffer.TYPE_DOUBLE:
            default:
                return value;
        }
    }

}
