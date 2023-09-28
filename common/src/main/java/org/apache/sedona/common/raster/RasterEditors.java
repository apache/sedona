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
import java.util.Map;
import java.util.Objects;

public class RasterEditors
{
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

        return RasterUtils.create(raster.getRenderedImage(), gridGeometry2D, raster.getSampleDimensions(),
                null);
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
            newWidth = (int) Math.abs(((envelope2D.getMaxX() - envelope2D.getMinX())) / newScaleX);
            newHeight = (int) Math.abs(((envelope2D.getMaxY() - envelope2D.getMinY())) / newScaleY);
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

}
