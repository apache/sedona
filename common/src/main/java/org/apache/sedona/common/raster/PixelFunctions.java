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

import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;
import org.opengis.coverage.PointOutsideCoverageException;
import org.opengis.geometry.DirectPosition;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.DoublePredicate;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class PixelFunctions
{
    private static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    public static Double value(GridCoverage2D rasterGeom, Geometry geometry, int band) throws TransformException
    {
        return values(rasterGeom, Collections.singletonList(geometry), band).get(0);
    }

    public static Geometry getPixelAsPoint(GridCoverage2D raster, int colX, int rowY) throws TransformException {
        GridGeometry2D gridGeometry2D = raster.getGridGeometry();
        GridEnvelope2D gridEnvelope2D = gridGeometry2D.getGridRange2D();
        GridCoordinates2D gridCoordinates2D = new GridCoordinates2D(colX - 1, rowY - 1);
        int srid = 0;
        String srs = CRS.toSRS(raster.getCoordinateReferenceSystem2D(), true);
        if (!"Generic cartesian 2D".equalsIgnoreCase(srs)) {
           srid = Integer.parseInt(srs);
        }
        if (gridEnvelope2D.contains(gridCoordinates2D)) {
            Point2D point2D = gridGeometry2D.getGridToCRS2D(PixelOrientation.UPPER_LEFT).transform(gridCoordinates2D, null);
            DirectPosition2D directPosition2D = new DirectPosition2D(gridGeometry2D.getCoordinateReferenceSystem2D(), point2D.getX(), point2D.getY());
            Coordinate pointCoord = new Coordinate(directPosition2D.getX(), directPosition2D.getY());
            if (srid != 0) {
                GeometryFactory factory = new GeometryFactory(new PrecisionModel(), srid);
                return factory.createPoint(pointCoord);
            }
            return GEOMETRY_FACTORY.createPoint(pointCoord);
        }else {
            throw new IndexOutOfBoundsException(String.format("Specified pixel coordinates (%d, %d) do not lie in the raster", colX, rowY));
        }
    }

    public static List<Double> values(GridCoverage2D rasterGeom, List<Geometry> geometries, int band) throws TransformException {
        int numBands = rasterGeom.getNumSampleDimensions();
        if (band < 1 || band > numBands) {
            // Invalid band index. Return nulls.
            return geometries.stream().map(geom -> (Double) null).collect(Collectors.toList());
        }
        double[] noDataValues = rasterGeom.getSampleDimension(band - 1).getNoDataValues();
        DoublePredicate isNoData = d -> noDataValues != null && DoubleStream.of(noDataValues).anyMatch(noDataValue -> Double.compare(noDataValue, d) == 0);
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
                    if (isNoData.test(pixel)) {
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
