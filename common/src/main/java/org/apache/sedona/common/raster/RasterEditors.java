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
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.coverage.SampleDimensionType;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.Envelope;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;

import javax.media.jai.Interpolation;
import javax.media.jai.RasterFactory;
import java.awt.*;
import java.awt.geom.Point2D;
import java.awt.image.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.apache.sedona.common.raster.MapAlgebra.addBandFromArray;
import static org.apache.sedona.common.raster.MapAlgebra.bandAsArray;

public class RasterEditors
{

    /**
     * Changes the band pixel type of a specific band of a raster.
     *
     * @param raster      The input raster.
     * @param BandDataType The desired data type of the band(s).
     * @param bandID      The specific band to change. If null, all bands are changed.
     * @return The modified raster with updated band pixel type.
     */
//    public static GridCoverage2D setBandPixelType(GridCoverage2D raster, String BandDataType, Integer bandID) {
//        int newDataType = RasterUtils.getDataTypeCode(BandDataType);
//        System.out.println("Original raster band 1: "+Arrays.toString(MapAlgebra.bandAsArray(raster, 1)));
//        System.out.println("Original raster band 1: "+Arrays.toString(MapAlgebra.bandAsArray(raster, 2)));
//
//        // Extracting the WritableRaster from the original GridCoverage2D
//        RenderedImage originalImage = raster.getRenderedImage();
//        WritableRaster originalRaster = RasterUtils.getRaster(originalImage).createCompatibleWritableRaster();
//        System.out.println("originalRaster minX: "+originalRaster.getMinX());
//        System.out.println("originalRaster minY: "+originalRaster.getMinY());
//
//        // Create a new raster with modified data type for the specified band
//        WritableRaster modifiedRaster = null;
//        if (bandID == null) {
//            modifiedRaster = modifyRasterBandType(originalRaster, 0, newDataType);
//            for (int band = 1; band < raster.getNumSampleDimensions(); band++) {
//                modifiedRaster = modifyRasterBandType(modifiedRaster, band, newDataType);
//            }
//        } else {
//            modifiedRaster = modifyRasterBandType(originalRaster, bandID-1, newDataType);
//        }
//
//        // Update GridSampleDimension for the modified band
//        GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
////        GridSampleDimension modifiedDimension;
////        if (bandID == null) {
////            for (int band = 0; band < raster.getNumSampleDimensions(); band++) {
////                modifiedDimension = RasterUtils.createSampleDimensionWithNoDataValue(sampleDimensions[band], RasterUtils.getNoDataValue(sampleDimensions[band]));
////                sampleDimensions[band] = modifiedDimension;
////            }
////        } else {
////            modifiedDimension = RasterUtils.createSampleDimensionWithNoDataValue(sampleDimensions[bandID - 1], RasterUtils.getNoDataValue(sampleDimensions[bandID - 1]));
////            sampleDimensions[bandID - 1] = modifiedDimension;
////        }
//
//        // Clone the original GridCoverage2D with the modified raster and sample dimensions
//        System.out.println(modifiedRaster.getMinX());
//        System.out.println(modifiedRaster.getMinY());
//        return RasterUtils.clone(modifiedRaster, raster.getGridGeometry(), sampleDimensions, raster, null, true);
//    }

    public static GridCoverage2D setBandPixelType(GridCoverage2D raster, String BandDataType, Integer bandID) {
        int newDataType = RasterUtils.getDataTypeCode(BandDataType);

        // Extracting the original data
        RenderedImage originalImage = raster.getRenderedImage();
        Raster originalData = RasterUtils.getRaster(originalImage);

        int width = originalImage.getWidth();
        int height = originalImage.getHeight();
        int numBands = originalImage.getSampleModel().getNumBands();

        // Create a new writable raster with the specified data type
        WritableRaster modifiedRaster = RasterFactory.createBandedRaster(newDataType, width, height, numBands, null);

        // Copy data to the new raster, converting type as necessary
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                for (int band = 0; band < numBands; band++) {
                    double originalValue = originalData.getSampleDouble(x, y, band);
                    if (bandID == null || band == bandID - 1) {
                        double convertedValue = castRasterDataType(originalValue, newDataType);
                        modifiedRaster.setSample(x, y, band, convertedValue);
                    } else {
                        modifiedRaster.setSample(x, y, band, originalValue);
                    }
                }
            }
        }

        // Re-create sample dimensions if necessary
        GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
//        if (bandID != null) {
//            sampleDimensions[bandID - 1] = RasterUtils.createSampleDimensionWithNoDataValue(sampleDimensions[bandID - 1], RasterUtils.getNoDataValue(sampleDimensions[bandID - 1]));
//        }

        // Clone the original GridCoverage2D with the modified raster
        return RasterUtils.clone(modifiedRaster, raster.getGridGeometry(), sampleDimensions, raster, null, true);
    }

//    public static GridCoverage2D setBandPixelType(GridCoverage2D raster, String BandDataType, Integer bandID) {
//        int numBands = raster.getNumSampleDimensions();
//        double[] bandValues;
//        if (bandID == null) {
//            for (int band = 0; band < numBands; band++) {
//                bandValues = bandAsArray(raster, band + 1);
//                double bandNoDataValue = RasterUtils.getNoDataValue(raster.getSampleDimension(band));
//
//                for (int i = 0; i < bandValues.length; i++) {
//                    bandValues[i] = castRasterDataType(bandValues[i], RasterUtils.getDataTypeCode(BandDataType));
//                }
//
//                raster = addBandFromArray(raster, bandValues, band+1);
//            }
//        } else {
//            bandValues = bandAsArray(raster,  bandID);
//
//            for (int i = 0; i < bandValues.length; i++) {
//                bandValues[i] = castRasterDataType(bandValues[i], RasterUtils.getDataTypeCode(BandDataType));
//            }
//
//            raster = addBandFromArray(raster, bandValues, bandID);
//        }
//
//        return raster;
//    }

    private static WritableRaster modifyRasterBandType(WritableRaster raster, int bandIndex, int newDataType) {
        int width = raster.getWidth();
        int height = raster.getHeight();
        int numBands = raster.getNumBands();

        WritableRaster modifiedRaster = RasterFactory.createBandedRaster(newDataType, width, height, numBands, null);

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                for (int band = 0; band < numBands; band++) {
                    double originalValue = raster.getSampleDouble(x, y, band);
                    if (band == bandIndex) {
                        double convertedValue = castRasterDataType(originalValue, newDataType);
                        modifiedRaster.setSample(x, y, band, convertedValue);
                        System.out.println("Original Value: " + originalValue + ", Converted Value: " + convertedValue);
                    } else {
                        modifiedRaster.setSample(x, y, band, originalValue);
                    }
                }
            }
        }
        return modifiedRaster;
    }





//    private static WritableRaster modifyRasterBandType(WritableRaster raster, int bandIndex, int newDataType) {
//        int width = raster.getWidth();
//        int height = raster.getHeight();
//        WritableRaster modifiedRaster = raster.createCompatibleWritableRaster(width, height);
//
//        // Iterate over each pixel to convert the data type of the specified band
//        for (int y = 0; y < height; y++) {
//            for (int x = 0; x < width; x++) {
//                // Handle the conversion based on the data type
//                switch (newDataType) {
//                    case DataBuffer.TYPE_BYTE:
//                        byte byteValue = (byte) raster.getSample(x, y, bandIndex);
//                        modifiedRaster.setSample(x, y, bandIndex, byteValue);
//                        break;
//                    case DataBuffer.TYPE_SHORT:
//                    case DataBuffer.TYPE_USHORT:
//                        short shortValue = (short) raster.getSample(x, y, bandIndex);
//                        modifiedRaster.setSample(x, y, bandIndex, shortValue);
//                        break;
//                    case DataBuffer.TYPE_INT:
//                        int intValue = (int) raster.getSample(x, y, bandIndex);
//                        modifiedRaster.setSample(x, y, bandIndex, intValue);
//                        break;
//                    case DataBuffer.TYPE_FLOAT:
//                        float floatValue = raster.getSampleFloat(x, y, bandIndex);
//                        modifiedRaster.setSample(x, y, bandIndex, floatValue);
//                        break;
//                    case DataBuffer.TYPE_DOUBLE:
//                        double doubleValue = raster.getSampleDouble(x, y, bandIndex);
//                        modifiedRaster.setSample(x, y, bandIndex, doubleValue);
//                        break;
//                    default:
//                        throw new IllegalArgumentException("Unsupported data type: " + newDataType);
//                }
//            }
//        }
//        return modifiedRaster;
//    }

//    private static RenderedImage createNewRenderedImageWithBandType(RenderedImage image, int newDataType, Integer bandID) {
//        // This is a placeholder logic, real implementation can be significantly more complex.
//        // This example only changes the data type and copies data for a single band.
//
//        if (bandID == null) {
//            throw new IllegalArgumentException("Band ID cannot be null");
//        }
//
//        // Assuming we are changing the data type of a single band.
//        // A more complex implementation would be needed for multi-band images.
//        int band = bandID - 1; // Adjusting for 0-based index
//
//        // Get the data from the specified band
//        Raster raster = image.getData();
//        int width = image.getWidth();
//        int height = image.getHeight();
//        double[] samples = raster.getSamples(0, 0, width, height, band, (double[]) null);
//
//        // Create a new writable raster with the new data type
//        WritableRaster newRaster = Raster.createBandedRaster(newDataType, width, height, 1, new Point(0, 0));
//
//        // Set the samples to the new raster
//        for (int y = 0; y < height; y++) {
//            for (int x = 0; x < width; x++) {
//                double value = samples[y * width + x];
//                newRaster.setSample(x, y, 0, value); // Casts are applied here based on newDataType
//            }
//        }
//
//        // Return a new RenderedImage
//        return new BufferedImage(image.getColorModel(), newRaster, image.getColorModel().isAlphaPremultiplied(), null);
//    }

//    private static GridSampleDimension[] updateSampleDimensions(GridSampleDimension[] sampleDimensions, int newDataType, Integer bandID) {
//        if (bandID == null) {
//            throw new IllegalArgumentException("Band ID cannot be null");
//        }
//
//        int band = bandID - 1; // Adjusting for 0-based index
//        GridSampleDimension[] newSampleDimensions = new GridSampleDimension[sampleDimensions.length];
//
//        for (int i = 0; i < sampleDimensions.length; i++) {
//            if (i == band) {
//                // Update the sample dimension for the specified band.
//                // This example just updates the data type; you might want to modify other properties.
//                GridSampleDimension sd = sampleDimensions[i];
//                // Sample code to recreate the sample dimension with a new data type
//                // The specifics of this operation depend on your data and requirements
//                newSampleDimensions[i] = new GridSampleDimension(sd.getDescription().toString(),
//                        SampleDimensionType.valueOf(newDataType), sd.getNoDataValues());
//            } else {
//                // Keep other bands as is
//                newSampleDimensions[i] = sampleDimensions[i];
//            }
//        }
//
//        return newSampleDimensions;
//    }


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
