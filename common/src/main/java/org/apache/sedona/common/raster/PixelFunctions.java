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
import org.geotools.geometry.DirectPosition2D;
import org.locationtech.jts.geom.*;
import org.opengis.coverage.PointOutsideCoverageException;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PixelFunctions
{
    private static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    public static Double value(GridCoverage2D rasterGeom, Geometry geometry, int band) throws TransformException
    {
        return values(rasterGeom, Collections.singletonList(geometry), band).get(0);
    }

    public static Geometry getPixelAsPolygon(GridCoverage2D raster, int colX, int rowY) throws TransformException, FactoryException {
        int srid = RasterAccessors.srid(raster);
        Point2D point2D1 = RasterUtils.getWorldCornerCoordinates(raster, colX, rowY);
        Point2D point2D2 = RasterUtils.getWorldCornerCoordinates(raster, colX + 1, rowY);
        Point2D point2D3 = RasterUtils.getWorldCornerCoordinates(raster, colX + 1, rowY + 1);
        Point2D point2D4 = RasterUtils.getWorldCornerCoordinates(raster, colX, rowY + 1);

        Coordinate[] coordinateArray = new Coordinate[5];
        coordinateArray[0] = new Coordinate(point2D1.getX(), point2D1.getY());
        coordinateArray[1] = new Coordinate(point2D2.getX(), point2D2.getY());
        coordinateArray[2] = new Coordinate(point2D3.getX(), point2D3.getY());
        coordinateArray[3] = new Coordinate(point2D4.getX(), point2D4.getY());
        coordinateArray[4] = new Coordinate(point2D1.getX(), point2D1.getY());

        if(srid != 0) {
            GeometryFactory factory = new GeometryFactory(new PrecisionModel(), srid);
            return factory.createPolygon(coordinateArray);
        }
        return GEOMETRY_FACTORY.createPolygon(coordinateArray);
    }

    public static Geometry getPixelAsPoint(GridCoverage2D raster, int colX, int rowY) throws TransformException, FactoryException {
        int srid = RasterAccessors.srid(raster);
        Point2D point2D = RasterUtils.getWorldCornerCoordinatesWithRangeCheck(raster, colX, rowY);
        Coordinate pointCoord = new Coordinate(point2D.getX(), point2D.getY());
        if (srid != 0) {
            GeometryFactory factory = new GeometryFactory(new PrecisionModel(), srid);
            return factory.createPoint(pointCoord);
        }
        return GEOMETRY_FACTORY.createPoint(pointCoord);
    }
    public static List<Double> values(GridCoverage2D rasterGeom, List<Geometry> geometries, int band) throws TransformException {
        int numBands = rasterGeom.getNumSampleDimensions();
        if (band < 1 || band > numBands) {
            // Invalid band index. Return nulls.
            return geometries.stream().map(geom -> (Double) null).collect(Collectors.toList());
        }
        double noDataValue = RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(band - 1));
        double[] pixelBuffer = new double[numBands];

        List<Double> result = new ArrayList<>(geometries.size());
        for (Geometry geom : geometries) {
            if (geom == null) {
                result.add(null);
            } else {
                Point point = ensurePoint(geom);
                DirectPosition directPosition2D = new DirectPosition2D(point.getX(), point.getY());
                try {
                    rasterGeom.evaluate(directPosition2D, pixelBuffer);
                    double pixel = pixelBuffer[band - 1];
                    if (Double.compare(noDataValue, pixel) == 0) {
                        result.add(null);
                    } else {
                        result.add(pixel);
                    }
                } catch (PointOutsideCoverageException | ArrayIndexOutOfBoundsException exc) {
                    // Points outside the extent should return null
                    result.add(null);
                }
            }
        }
        return result;
    }

    private static Point ensurePoint(Geometry geometry) {
        if (geometry instanceof Point) {
            return (Point) geometry;
        }
        throw new IllegalArgumentException("Attempting to get the value of a pixel with a non-point geometry.");
    }
}
