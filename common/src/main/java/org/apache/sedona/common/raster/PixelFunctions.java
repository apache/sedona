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
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.*;
import org.opengis.coverage.PointOutsideCoverageException;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PixelFunctions
{
    private static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    public static Double value(GridCoverage2D rasterGeom, Geometry geometry, int band) throws TransformException
    {
        return values(rasterGeom, Collections.singletonList(geometry), band).get(0);
    }

    public static Double value(GridCoverage2D rasterGeom, Geometry geometry) throws TransformException
    {
        return values(rasterGeom, Collections.singletonList(geometry), 1).get(0);
    }

    public static Double value(GridCoverage2D rasterGeom, int colX, int rowY, int band) throws TransformException
    {
        int[] xCoordinates = {colX};
        int[] yCoordinates = {rowY};
        return values(rasterGeom, xCoordinates, yCoordinates, band).get(0);
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

    public static List<PixelRecord> getPixelAsPolygons(GridCoverage2D rasterGeom, int band) throws TransformException, FactoryException {
        RasterUtils.ensureBand(rasterGeom, band);

        int width = RasterAccessors.getWidth(rasterGeom);
        int height = RasterAccessors.getHeight(rasterGeom);

        Raster r = RasterUtils.getRaster(rasterGeom.getRenderedImage());
        double[] pixels = r.getSamples(0, 0, width, height, band - 1, (double[]) null);

        AffineTransform2D gridToCRS = RasterUtils.getGDALAffineTransform(rasterGeom);
        double cellSizeX = gridToCRS.getScaleX();
        double cellSizeY = gridToCRS.getScaleY();
        double shearX = gridToCRS.getShearX();
        double shearY = gridToCRS.getShearY();

        int srid = RasterAccessors.srid(rasterGeom);
        GeometryFactory geometryFactory = srid != 0 ? new GeometryFactory(new PrecisionModel(), srid) : GEOMETRY_FACTORY;

        Point2D upperLeft = RasterUtils.getWorldCornerCoordinates(rasterGeom, 1, 1);
        List<PixelRecord> pixelRecords = new ArrayList<>();

        for (int y = 1; y <= height; y++) {
            for (int x = 1; x <= width; x++) {
                double pixelValue = pixels[(y - 1) * width + (x - 1)];

                double worldX1 = upperLeft.getX() + (x - 1) * cellSizeX + (y - 1) * shearX;
                double worldY1 = upperLeft.getY() + (y - 1) * cellSizeY + (x - 1) * shearY;
                double worldX2 = worldX1 + cellSizeX;
                double worldY2 = worldY1 + shearY;
                double worldX3 = worldX2 + shearX;
                double worldY3 = worldY2 + cellSizeY;
                double worldX4 = worldX1 + shearX;
                double worldY4 = worldY1 + cellSizeY;

                Coordinate[] coordinates = new Coordinate[] {
                        new Coordinate(worldX1, worldY1),
                        new Coordinate(worldX2, worldY2),
                        new Coordinate(worldX3, worldY3),
                        new Coordinate(worldX4, worldY4),
                        new Coordinate(worldX1, worldY1)
                };

                Geometry polygon = geometryFactory.createPolygon(coordinates);
                pixelRecords.add(new PixelRecord(polygon, pixelValue, x, y));
            }
        }

        return pixelRecords;
    }

    public static Geometry getPixelAsCentroid(GridCoverage2D raster, int colX, int rowY) throws FactoryException, TransformException {
        Geometry polygon = PixelFunctions.getPixelAsPolygon(raster, colX, rowY);
        return  polygon.getCentroid();
    }

    public static List<PixelRecord> getPixelAsCentroids(GridCoverage2D rasterGeom, int band) throws TransformException, FactoryException {
        RasterUtils.ensureBand(rasterGeom, band);

        int width = RasterAccessors.getWidth(rasterGeom);
        int height = RasterAccessors.getHeight(rasterGeom);

        Raster r = RasterUtils.getRaster(rasterGeom.getRenderedImage());
        double[] pixels = r.getSamples(0, 0, width, height, band - 1, (double[]) null);

        AffineTransform2D gridToCRS = RasterUtils.getGDALAffineTransform(rasterGeom);
        double cellSizeX = gridToCRS.getScaleX();
        double cellSizeY = gridToCRS.getScaleY();
        double shearX = gridToCRS.getShearX();
        double shearY = gridToCRS.getShearY();

        int srid = RasterAccessors.srid(rasterGeom);
        GeometryFactory geometryFactory = srid != 0 ? new GeometryFactory(new PrecisionModel(), srid) : GEOMETRY_FACTORY;

        Point2D upperLeft = RasterUtils.getWorldCornerCoordinates(rasterGeom, 1, 1);
        List<PixelRecord> pixelRecords = new ArrayList<>();

        for (int y = 1; y <= height; y++) {
            for (int x = 1; x <= width; x++) {
                double pixelValue = pixels[(y - 1) * width + (x - 1)];

                double worldX = upperLeft.getX() + (x - 0.5) * cellSizeX + (y - 0.5) * shearX;
                double worldY = upperLeft.getY() + (y - 0.5) * cellSizeY + (x - 0.5) * shearY;

                Coordinate centroidCoord = new Coordinate(worldX, worldY);
                Geometry centroidGeom = geometryFactory.createPoint(centroidCoord);
                pixelRecords.add(new PixelRecord(centroidGeom, pixelValue, x, y));
            }
        }

        return pixelRecords;
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

    public static List<PixelRecord> getPixelAsPoints(GridCoverage2D rasterGeom, int band) throws TransformException, FactoryException {
        RasterUtils.ensureBand(rasterGeom, band);

        int width = RasterAccessors.getWidth(rasterGeom);
        int height = RasterAccessors.getHeight(rasterGeom);

        Raster r = RasterUtils.getRaster(rasterGeom.getRenderedImage());
        double[] pixels = r.getSamples(0, 0, width, height, band - 1, (double[]) null);

        AffineTransform2D gridToCRS = RasterUtils.getGDALAffineTransform(rasterGeom);
        double cellSizeX = gridToCRS.getScaleX();
        double cellSizeY = gridToCRS.getScaleY();
        double shearX = gridToCRS.getShearX();
        double shearY = gridToCRS.getShearY();

        int srid = RasterAccessors.srid(rasterGeom);
        GeometryFactory geometryFactory = srid != 0 ? new GeometryFactory(new PrecisionModel(), srid) : GEOMETRY_FACTORY;

        Point2D upperLeft = RasterUtils.getWorldCornerCoordinates(rasterGeom, 1, 1);
        List<PixelRecord> pointRecords = new ArrayList<>();

        for (int y = 1; y <= height; y++) {
            for (int x = 1; x <= width; x++) {
                double pixelValue = pixels[(y - 1) * width + (x - 1)];

                double worldX = upperLeft.getX() + (x - 1) * cellSizeX + (y - 1) * shearX;
                double worldY = upperLeft.getY() + (y - 1) * cellSizeY + (x - 1) * shearY;

                Coordinate pointCoord = new Coordinate(worldX, worldY);
                Geometry pointGeom = geometryFactory.createPoint(pointCoord);
                pointRecords.add(new PixelRecord(pointGeom, pixelValue, x, y));
            }
        }

        return pointRecords;
    }

    public static List<Double> values(GridCoverage2D rasterGeom, int[] xCoordinates, int[] yCoordinates, int band) throws TransformException {
        RasterUtils.ensureBand(rasterGeom, band); // Check for invalid band index
        int numBands = rasterGeom.getNumSampleDimensions();

        double noDataValue = RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(band - 1));
        List<Double> result = new ArrayList<>(xCoordinates.length);
        double[] pixelBuffer = new double[numBands];

        for (int i = 0; i < xCoordinates.length; i++) {
            int x = xCoordinates[i];
            int y = yCoordinates[i];

            GridCoordinates2D gridCoord = new GridCoordinates2D(x, y);

            try {
                pixelBuffer = rasterGeom.evaluate(gridCoord, pixelBuffer);
                double pixelValue = pixelBuffer[band - 1];
                if (Double.compare(noDataValue, pixelValue) == 0) {
                    result.add(null);
                } else {
                    result.add(pixelValue);
                }
            } catch (PointOutsideCoverageException e) {
                // Points outside the extent should return null
                result.add(null);
            }
        }

        return result;
    }

    public static List<Double> values(GridCoverage2D rasterGeom, List<Geometry> geometries, int band) throws TransformException {
        RasterUtils.ensureBand(rasterGeom, band); // Check for invalid band index
        int numBands = rasterGeom.getNumSampleDimensions();

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

    public static List<Double> values(GridCoverage2D rasterGeom, List<Geometry> geometries) throws TransformException {
        return values(rasterGeom, geometries, 1);
    }

    private static Point ensurePoint(Geometry geometry) {
        if (geometry instanceof Point) {
            return (Point) geometry;
        }
        throw new IllegalArgumentException("Attempting to get the value of a pixel with a non-point geometry.");
    }
}
