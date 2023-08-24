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
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.util.Arrays;
import java.util.Set;

public class RasterAccessors
{
    public static int srid(GridCoverage2D raster) throws FactoryException
    {
        CoordinateReferenceSystem crs = raster.getCoordinateReferenceSystem();
        if (crs instanceof DefaultEngineeringCRS) {
            // GeoTools defaults to internal non-standard epsg codes, like 404000, if crs is missing.
            // We need to check for this case and return 0 instead.
            if (((DefaultEngineeringCRS) crs).isWildcard()) {
                return 0;
            }
        }
        Set<ReferenceIdentifier> crsIds = crs.getIdentifiers();
        if (crsIds.isEmpty()) {
            return 0;
        }
        for (ReferenceIdentifier crsId : crsIds) {
            if ("EPSG".equals(crsId.getCodeSpace())) {
                return Integer.parseInt(crsId.getCode());
            }
        }
        return 0;
    }

    public static int numBands(GridCoverage2D raster) {
        return raster.getNumSampleDimensions();
    }

    public static int getWidth(GridCoverage2D raster) {
        return raster.getGridGeometry().getGridRange().getSpan(0);
    }

    public static int getHeight(GridCoverage2D raster) {
        return raster.getGridGeometry().getGridRange().getSpan(1);
    }

    public static double getUpperLeftX(GridCoverage2D raster) {
        AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
        return affine.getTranslateX();
    }

    public static double getUpperLeftY(GridCoverage2D raster) {
        AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
        return affine.getTranslateY();
    }

    public static double getScaleX(GridCoverage2D raster) {
        return RasterUtils.getGDALAffineTransform(raster).getScaleX();
    }

    public static double getScaleY(GridCoverage2D raster) {
        return RasterUtils.getGDALAffineTransform(raster).getScaleY();
    }

    public static double getSkewX(GridCoverage2D raster) {
        return RasterUtils.getGDALAffineTransform(raster).getShearX();
    }

    public static double getSkewY(GridCoverage2D raster) {
        return RasterUtils.getGDALAffineTransform(raster).getShearY();
    }

    public static double getWorldCoordX(GridCoverage2D raster, int colX, int rowY) throws TransformException {
        return RasterUtils.getWorldCornerCoordinates(raster, colX, rowY).getX();
    }

    public static double getWorldCoordY(GridCoverage2D raster, int colX, int rowY) throws TransformException {
        return RasterUtils.getWorldCornerCoordinates(raster, colX, rowY).getY();
    }

    public static String getGeoReference(GridCoverage2D raster) {
        return getGeoReference(raster, "GDAL");
    }

    public static String getGeoReference(GridCoverage2D raster, String format) {
        double scaleX = getScaleX(raster);
        double skewX = getSkewX(raster);
        double skewY = getSkewY(raster);
        double scaleY = getScaleY(raster);
        double upperLeftX = getUpperLeftX(raster);
        double upperLeftY = getUpperLeftY(raster);

        if(format.equalsIgnoreCase("GDAL")) {
            return String.format("%f \n%f \n%f \n%f \n%f \n%f", scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        } else if (format.equalsIgnoreCase("ESRI")){
            return String.format("%f \n%f \n%f \n%f \n%f \n%f", scaleX, skewY, skewX, scaleY, (upperLeftX + (scaleX * 0.5)),
                    (upperLeftY + (scaleY * 0.5)));
        } else {
            throw new IllegalArgumentException("Please select between the following formats GDAL and ESRI");
        }
    }

    public static Geometry getGridCoord(GridCoverage2D raster, double x, double y) throws TransformException {
        int[] coords = RasterUtils.getGridCoordinatesFromWorld(raster, x, y);
        coords = Arrays.stream(coords).map(number -> number + 1).toArray();
        Geometry point = new GeometryFactory().createPoint(new Coordinate(coords[0], coords[1]));
        return point;
    }

    public static Geometry getGridCoord(GridCoverage2D raster, Geometry point) throws TransformException {
        ensurePoint(point);
        point = RasterUtils.convertCRSIfNeeded(point, raster.getCoordinateReferenceSystem2D());
        Point actualPoint = (Point) point;
        return getGridCoord(raster, actualPoint.getX(), actualPoint.getY());
    }

    public static int getGridCoordX(GridCoverage2D raster, double x, double y) throws TransformException {
        return RasterUtils.getGridCoordinatesFromWorld(raster, x, y)[0] + 1;
    }

    public static int getGridCoordX(GridCoverage2D raster, Geometry point) throws TransformException {
        ensurePoint(point);
        point = RasterUtils.convertCRSIfNeeded(point, raster.getCoordinateReferenceSystem2D());
        Point actualPoint = (Point) point;
        return getGridCoordX(raster, actualPoint.getX(), actualPoint.getY());
    }

    public static int getGridCoordY(GridCoverage2D raster, double x, double y) throws TransformException {
        return RasterUtils.getGridCoordinatesFromWorld(raster, x, y)[1] + 1;
    }

    public static int getGridCoordY(GridCoverage2D raster, Geometry point) throws TransformException {
        ensurePoint(point);
        point = RasterUtils.convertCRSIfNeeded(point, raster.getCoordinateReferenceSystem2D());
        Point actualPoint = (Point) point;
        return getGridCoordY(raster, actualPoint.getX(), actualPoint.getY());
    }

    private static void ensurePoint(Geometry geometry) throws IllegalArgumentException {
        if (!(geometry instanceof Point)) {
            throw new IllegalArgumentException("Only point geometries are expected as real world coordinates");
        }
    }

    /**
     * Returns the metadata of a raster as an array of doubles.
     * @param raster the raster
     * @return double[] with the following values:
     * 0: upperLeftX: upper left x
     * 1: upperLeftY: upper left y
     * 2: width: number of pixels on x axis
     * 3: height: number of pixels on y axis
     * 4: scaleX: pixel width
     * 5: scaleY: pixel height
     * 6: skewX: skew on x axis
     * 7: skewY: skew on y axis
     * 8: srid
     * 9: numBands
     * @throws FactoryException
     */
    public static double[] metadata(GridCoverage2D raster)
            throws FactoryException
    {
        // Get Geo-reference metadata
        GridEnvelope2D gridRange = raster.getGridGeometry().getGridRange2D();
        AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);

        // Get the affine parameters
        double upperLeftX = affine.getTranslateX();
        double upperLeftY = affine.getTranslateY();
        double scaleX = affine.getScaleX();
        double scaleY = affine.getScaleY();
        double skewX = affine.getShearX();
        double skewY = affine.getShearY();
        return new double[] {
                upperLeftX, upperLeftY,
                gridRange.getWidth(), gridRange.getHeight(),
                scaleX, scaleY, skewX, skewY,
                srid(raster), raster.getNumSampleDimensions()};
    }
}
