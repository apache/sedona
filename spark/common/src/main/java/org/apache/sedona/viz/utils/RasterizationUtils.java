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
package org.apache.sedona.viz.utils;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class RasterizationUtils.
 */
public class RasterizationUtils
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(RasterizationUtils.class);

    /**
     * Find one pixel coordinate.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundaryOriginal the dataset boundary original
     * @param spatialCoordinateOriginal the spatial coordinate original
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @return the tuple 2
     */
    public static Tuple2<Integer, Integer> FindOnePixelCoordinate(int resolutionX, int resolutionY, Envelope datasetBoundaryOriginal, Coordinate spatialCoordinateOriginal, boolean reverseSpatialCoordinate)
    {
        Coordinate spatialCoordinate;
        Envelope datasetBoundary;
        if (reverseSpatialCoordinate) {
            spatialCoordinate = new Coordinate(spatialCoordinateOriginal.y, spatialCoordinateOriginal.x);
            datasetBoundary = new Envelope(datasetBoundaryOriginal.getMinY(), datasetBoundaryOriginal.getMaxY(), datasetBoundaryOriginal.getMinX(), datasetBoundaryOriginal.getMaxX());
        }
        else {
            spatialCoordinate = spatialCoordinateOriginal;
            datasetBoundary = datasetBoundaryOriginal;
        }
		/*
		 if(spatialCoordinate.x < datasetBoundary.getMinX() || spatialCoordinate.x > datasetBoundary.getMaxX())
		{
			throw new Exception("[RasterizationUtils][FindOnePixelCoordinate] This spatial coordinate is out of the given boundary. Should be ignored.");
		}
		if(spatialCoordinate.y < datasetBoundaryOriginal.getMinY() || spatialCoordinate.y > datasetBoundaryOriginal.getMaxY())
		{
			throw new Exception("[RasterizationUtils][FindOnePixelCoordinate] This spatial coordinate is out of the given boundary. Should be ignored.");
		}*/

        Double pixelXDouble = ((spatialCoordinate.x - datasetBoundary.getMinX()) / (datasetBoundary.getMaxX() - datasetBoundary.getMinX())) * resolutionX;
        Double xRemainder = (spatialCoordinate.x - datasetBoundary.getMinX()) % (datasetBoundary.getMaxX() - datasetBoundary.getMinX());
        Double pixelYDouble = ((spatialCoordinate.y - datasetBoundary.getMinY()) / (datasetBoundary.getMaxY() - datasetBoundary.getMinY())) * resolutionY;
        Double yRemainder = (spatialCoordinate.y - datasetBoundary.getMinY()) % (datasetBoundary.getMaxY() - datasetBoundary.getMinY());
        int pixelX = pixelXDouble.intValue();
        int pixelY = pixelYDouble.intValue();
        if (xRemainder == 0.0 && pixelXDouble != 0.0) {
            pixelX--;
        }
        if (pixelX >= resolutionX) {
            pixelX--;
        }
        if (yRemainder == 0.0 && pixelYDouble != 0) {
            pixelY--;
        }
        if (pixelY >= resolutionY) {
            pixelY--;
        }
        return new Tuple2<Integer, Integer>(pixelX, pixelY);
    }

    /**
     * Find one pixel coordinate.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundaryOriginal the dataset boundary original
     * @param spatialCoordinateOriginal the spatial coordinate original
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param flipOverX the flip over X
     * @param flipOverY the flip over Y
     * @return the tuple 2
     */
    public static Tuple2<Integer, Integer> FindOnePixelCoordinate(int resolutionX, int resolutionY, Envelope datasetBoundaryOriginal, Coordinate spatialCoordinateOriginal, boolean reverseSpatialCoordinate, boolean flipOverX, boolean flipOverY)
    {
        Coordinate spatialCoordinate;
        Envelope datasetBoundary;
        if (reverseSpatialCoordinate) {
            spatialCoordinate = new Coordinate(spatialCoordinateOriginal.y, spatialCoordinateOriginal.x);
            datasetBoundary = new Envelope(datasetBoundaryOriginal.getMinY(), datasetBoundaryOriginal.getMaxY(), datasetBoundaryOriginal.getMinX(), datasetBoundaryOriginal.getMaxX());
        }
        else {
            spatialCoordinate = spatialCoordinateOriginal;
            datasetBoundary = datasetBoundaryOriginal;
        }
        Double pixelXDouble = ((spatialCoordinate.x - datasetBoundary.getMinX()) / (datasetBoundary.getMaxX() - datasetBoundary.getMinX())) * resolutionX;
        Double xRemainder = (spatialCoordinate.x - datasetBoundary.getMinX()) % (datasetBoundary.getMaxX() - datasetBoundary.getMinX());
        Double pixelYDouble = ((spatialCoordinate.y - datasetBoundary.getMinY()) / (datasetBoundary.getMaxY() - datasetBoundary.getMinY())) * resolutionY;
        Double yRemainder = (spatialCoordinate.y - datasetBoundary.getMinY()) / (datasetBoundary.getMaxY() - datasetBoundary.getMinY());
        int pixelX = pixelXDouble.intValue();
        int pixelY = pixelYDouble.intValue();
        if (xRemainder == 0.0 && pixelXDouble != 0.0) {
            pixelX--;
        }
        if (pixelX >= resolutionX) {
            pixelX--;
        }
        if (yRemainder == 0.0 && pixelYDouble != 0) {
            pixelY--;
        }
        if (pixelY >= resolutionY) {
            pixelY--;
        }
        // Flip over X or Y value. For example, if the original X is 15, the X resolution is 100, the flipped X will be 100 -15 = 85.
        if (flipOverX) { pixelX = resolutionX - pixelX; }
        if (flipOverY) { pixelY = resolutionY - pixelY; }
        return new Tuple2<Integer, Integer>(pixelX, pixelY);
    }

    /**
     * Gets the width from height.
     *
     * @param y the y
     * @param boundary the boundary
     * @return the int
     */
    public static int GetWidthFromHeight(int y, Envelope boundary)
    {
        int x = (int) (y * (boundary.getWidth() / boundary.getHeight()));
        return x;
    }

    /**
     * Encode 2 D to 1 D id.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param twoDimensionIdX the two dimension id X
     * @param twoDimensionIdY the two dimension id Y
     * @return the int
     * @throws Exception the exception
     */
    public static int Encode2DTo1DId(int resolutionX, int resolutionY, int twoDimensionIdX, int twoDimensionIdY)
            throws Exception
    {
        if (twoDimensionIdX < 0 || twoDimensionIdX >= resolutionX || twoDimensionIdY < 0 || twoDimensionIdY >= resolutionY) {
            throw new Exception("[RasterizationUtils][Encode2DTo1DId] This given 2 dimension coordinate is " + twoDimensionIdX + " " + twoDimensionIdY + ". This coordinate is out of the given boundary and will be dropped.");
        }
		/*
		if((twoDimensionIdX+twoDimensionIdY*resolutionX)<0 ||(twoDimensionIdX+twoDimensionIdY*resolutionX)>(resolutionX*resolutionY-1))
		{
			throw new Exception("[RasterizationUtils][Encode2DTo1DId] This given 2 dimension coordinate is "+twoDimensionIdX+" "+twoDimensionIdY+". This coordinate is out of the given boundary and will be dropped.");
		}
		*/
        return twoDimensionIdX + twoDimensionIdY * resolutionX;
    }

    /**
     * Decode 1 D to 2 D id.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param oneDimensionId the one dimension id
     * @return the tuple 2
     */
    public static Tuple2<Integer, Integer> Decode1DTo2DId(int resolutionX, int resolutionY, int oneDimensionId)
    {
        int twoDimensionIdX = oneDimensionId % resolutionX;
        int twoDimensionIdY = oneDimensionId / resolutionX;
        return new Tuple2<Integer, Integer>(twoDimensionIdX, twoDimensionIdY);
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param spatialObject the spatial object
     * @param colorizeOption the colorize option
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @return the list
     */
    public static List<Tuple2<Pixel, Double>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Point spatialObject, ColorizeOption colorizeOption, boolean reverseSpatialCoordinate)
    {
        List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
        Tuple2<Integer, Integer> pixelCoordinate = null;
        try {
            pixelCoordinate = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, spatialObject.getCoordinate(), reverseSpatialCoordinate);
        }
        catch (Exception e) {
            // This pixel is out of boundary. Should be ignored.
            return result;
        }
        if (colorizeOption == ColorizeOption.EARTHOBSERVATION) {
            Pixel newPixel = new Pixel(pixelCoordinate._1, pixelCoordinate._2, resolutionX, resolutionY);
            result.add(new Tuple2<Pixel, Double>(newPixel, spatialObject.getCoordinate().z));
        }
        else {
            Pixel newPixel = new Pixel(pixelCoordinate._1, pixelCoordinate._2, resolutionX, resolutionY);
            result.add(new Tuple2<Pixel, Double>(newPixel, new Double(1.0)));
        }
        return result;
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param coordinates the coordinates
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param flipOverX the flip over X
     * @param flipOverY the flip over Y
     * @return the coordinate[]
     */
    public static Coordinate[] FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Coordinate[] coordinates, boolean reverseSpatialCoordinate, boolean flipOverX, boolean flipOverY)
    {
        /*
         * This function is used to rasterize vector image pixels
         */
        List<Coordinate> result = new ArrayList<Coordinate>();
        for (Coordinate coordinate : coordinates) {
            Tuple2<Integer, Integer> pixelCoordinate = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, coordinate, reverseSpatialCoordinate, flipOverX, flipOverY);
            result.add(new Coordinate(pixelCoordinate._1(), pixelCoordinate._2()));
        }
        Coordinate[] arrayResult = result.toArray(new Coordinate[result.size()]);
        return arrayResult;
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param coordinate the coordinate
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param flipOverX the flip over X
     * @param flipOverY the flip over Y
     * @return the coordinate
     */
    public static Coordinate FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Coordinate coordinate, boolean reverseSpatialCoordinate, boolean flipOverX, boolean flipOverY)
    {
        /*
         * This function is used to rasterize vector image pixels
         */
        Tuple2<Integer, Integer> pixelCoordinate = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, coordinate, reverseSpatialCoordinate, flipOverX, flipOverY);
        return new Coordinate(pixelCoordinate._1(), pixelCoordinate._2());
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param spatialObject the spatial object
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @return the list
     */
    public static List<Tuple2<Pixel, Double>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Polygon spatialObject, boolean reverseSpatialCoordinate)
    {
        List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
        for (int i = 0; i < spatialObject.getCoordinates().length - 1; i++) {
            Tuple2<Integer, Integer> pixelCoordinate1 = null;
            Tuple2<Integer, Integer> pixelCoordinate2 = null;
            try {
                pixelCoordinate1 = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, spatialObject.getCoordinates()[i], reverseSpatialCoordinate);
                pixelCoordinate2 = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, spatialObject.getCoordinates()[i + 1], reverseSpatialCoordinate);
            }
            catch (Exception e) {
                // This pixel is out of boundary. Should be ignored.
                continue;
            }
            result.addAll(FindPixelCoordinates(resolutionX, resolutionY, pixelCoordinate1, pixelCoordinate2, reverseSpatialCoordinate));
        }
        return result;
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param spatialObject the spatial object
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param objectWeight the object weight
     * @return the list
     */
    public static List<Tuple2<Pixel, Double>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Polygon spatialObject, boolean reverseSpatialCoordinate, Double objectWeight)
    {
        List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
        GeometryFactory geometryfactory = new GeometryFactory();
        ArrayList<Coordinate> coordinatesList = new ArrayList<Coordinate>();
        LinearRing linear;
        for (int i = 0; i < spatialObject.getCoordinates().length; i++) {
            Tuple2<Integer, Integer> pixelCoordinate = null;
            pixelCoordinate = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, spatialObject.getCoordinates()[i], reverseSpatialCoordinate);

            coordinatesList.add(new Coordinate(pixelCoordinate._1, pixelCoordinate._2));
        }

        coordinatesList.add(coordinatesList.get(0));
        linear = geometryfactory.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
        Polygon polygon = new Polygon(linear, null, geometryfactory);
        int minPixelX = (int) polygon.getEnvelopeInternal().getMinX();
        int maxPixelX = (int) polygon.getEnvelopeInternal().getMaxX();
        int minPixelY = (int) polygon.getEnvelopeInternal().getMinY();
        int maxPixelY = (int) polygon.getEnvelopeInternal().getMaxY();
        for (int j = minPixelY; j <= maxPixelY; j++) {
            for (int i = minPixelX; i <= maxPixelX; i++) {
                if (polygon.contains(geometryfactory.createPoint(new Coordinate(i, j)))) {
                    try {
                        Pixel newPixel = new Pixel(i, j, resolutionX, resolutionY);
                        result.add(new Tuple2<Pixel, Double>(newPixel, new Double(objectWeight)));
                    }
                    catch (Exception e) {
                        /*
                         * This spatial object is out of the given dataset boundary. It is ignored here.
                         */
                    }
                }
            }
        }
        return result;
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param pixelCoordinate1 the pixel coordinate 1
     * @param pixelCoordinate2 the pixel coordinate 2
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @return the list
     */
    public static List<Tuple2<Pixel, Double>> FindPixelCoordinates(int resolutionX, int resolutionY, Tuple2<Integer, Integer> pixelCoordinate1, Tuple2<Integer, Integer> pixelCoordinate2, boolean reverseSpatialCoordinate)
    {
        /*
         * This function uses Bresenham's line algorithm to plot pixels touched by a given line segment.
         */
        int x1 = pixelCoordinate1._1;
        int y1 = pixelCoordinate1._2;
        int x2 = pixelCoordinate2._1;
        int y2 = pixelCoordinate2._2;
        int dx = x2 - x1;
        int dy = y2 - y1;
        int ux = dx > 0 ? 1 : -1; // x direction
        int uy = dy > 0 ? 1 : -1; // y direction
        int x = x1, y = y1;
        int eps = 0; //cumulative errors
        dx = Math.abs(dx);
        dy = Math.abs(dy);
        List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
        if (dx > dy) {
            for (x = x1; x != x2; x += ux) {
                try {
                    Pixel newPixel = new Pixel(x, y, resolutionX, resolutionY);
                    result.add(new Tuple2<Pixel, Double>(newPixel, 1.0));
                }
                catch (Exception e) {
                    /*
                     * This spatial object is out of the given dataset boudanry. It is ignored here.
                     */
                }
                eps += dy;
                if ((eps << 1) >= dx) {
                    y += uy;
                    eps -= dx;
                }
            }
        }
        else {
            for (y = y1; y != y2; y += uy) {
                try {
                    Pixel newPixel = new Pixel(x, y, resolutionX, resolutionY);
                    result.add(new Tuple2<Pixel, Double>(newPixel, 1.0));
                }
                catch (Exception e) {
                    /*
                     * This spatial object is out of the given dataset boudanry. It is ignored here.
                     */
                }
                eps += dx;
                if ((eps << 1) >= dy) {
                    x += ux;
                    eps -= dy;
                }
            }
        }
        return result;
    }

    /**
     * Find pixel coordinates.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param spatialObject the spatial object
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @return the list
     */
    public static List<Tuple2<Pixel, Double>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, LineString spatialObject, boolean reverseSpatialCoordinate)
    {
        List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
        for (int i = 0; i < spatialObject.getCoordinates().length - 1; i++) {
            Tuple2<Integer, Integer> pixelCoordinate1 = null;
            Tuple2<Integer, Integer> pixelCoordinate2 = null;
            try {
                pixelCoordinate1 = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, spatialObject.getCoordinates()[i], reverseSpatialCoordinate);
                pixelCoordinate2 = FindOnePixelCoordinate(resolutionX, resolutionY, datasetBoundary, spatialObject.getCoordinates()[i + 1], reverseSpatialCoordinate);
            }
            catch (Exception e) {
                // This line segment is out of boundary, Should be ignored.
                continue;
            }
            result.addAll(FindPixelCoordinates(resolutionX, resolutionY, pixelCoordinate1, pixelCoordinate2, reverseSpatialCoordinate));
        }
        return result;
    }

    public static String getImageTileName(int zoomLevel, int partitionOnX, int partitionOnY, int tileSerialId)
    {
        Tuple2<Integer, Integer> tileCoordinate = RasterizationUtils.Decode1DTo2DId(partitionOnX, partitionOnY, tileSerialId);
        return zoomLevel + "-" + tileCoordinate._1() + "-" + tileCoordinate._2();
    }
}
