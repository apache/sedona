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

import java.awt.geom.Point2D;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.Envelope2D;
import org.locationtech.jts.geom.*;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class GeometryFunctions {

  /**
   * * Returns the convex hull of the input raster
   *
   * @param raster
   * @return Geometry: convex hull of the input raster
   * @throws FactoryException
   * @throws TransformException
   */
  public static Geometry convexHull(GridCoverage2D raster)
      throws FactoryException, TransformException {
    int width = RasterAccessors.getWidth(raster), height = RasterAccessors.getHeight(raster);
    GeometryFactory geometryFactory =
        new GeometryFactory(new PrecisionModel(), RasterAccessors.srid(raster));
    double upperLeftX = RasterAccessors.getUpperLeftX(raster),
        upperLeftY = RasterAccessors.getUpperLeftY(raster);
    // First and last coord (Upper left)
    Coordinate coordOne = new Coordinate(upperLeftX, upperLeftY);

    // start clockwise rotation

    // upper right
    Point2D point = RasterUtils.getWorldCornerCoordinates(raster, width + 1, 1);
    Coordinate coordTwo = new Coordinate(point.getX(), point.getY());

    // lower right
    point = RasterUtils.getWorldCornerCoordinates(raster, width + 1, height + 1);
    Coordinate coordThree = new Coordinate(point.getX(), point.getY());

    // lower left
    point = RasterUtils.getWorldCornerCoordinates(raster, 1, height + 1);
    Coordinate coordFour = new Coordinate(point.getX(), point.getY());

    return geometryFactory.createPolygon(
        new Coordinate[] {coordOne, coordTwo, coordThree, coordFour, coordOne});
  }

  public static Geometry minConvexHull(GridCoverage2D raster, Integer band)
      throws FactoryException, TransformException {
    boolean allBands = band == null;
    if (!allBands) ensureSafeBand(raster, band);
    int width = RasterAccessors.getWidth(raster),
        height = RasterAccessors.getHeight(raster),
        srid = RasterAccessors.srid(raster);
    int minX = Integer.MAX_VALUE,
        maxX = Integer.MIN_VALUE,
        minY = Integer.MAX_VALUE,
        maxY = Integer.MIN_VALUE;
    for (int j = 0; j < height; j++) {
      for (int i = 0; i < width; i++) {
        // get the value of the raster at the coordinates i, j
        // if value is not no data, update variables to track minX, maxX, minY, maxY
        GridCoordinates2D currGridCoordinate = new GridCoordinates2D(i, j);
        double[] bandPixelValues = raster.evaluate(currGridCoordinate, (double[]) null);
        int start = band == null ? 1 : band;
        int end = band == null ? RasterAccessors.numBands(raster) : band;
        for (int currBand = start; currBand <= end; currBand++) {
          double currBandValue = bandPixelValues[currBand - 1];
          Double bandNoDataValue = RasterBandAccessors.getBandNoDataValue(raster, currBand);
          if ((bandNoDataValue == null) || currBandValue != bandNoDataValue) {
            // getWorldCoordinates internally takes 1-indexed coordinates, increment and track
            minX = Math.min(minX, i + 1);
            maxX = Math.max(maxX, i + 1);
            minY = Math.min(minY, j + 1);
            maxY = Math.max(maxY, j + 1);
          }
        }
      }
    }
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
    Point2D worldUpperLeft = RasterUtils.getWorldCornerCoordinates(raster, minX, minY);
    Point2D worldUpperRight =
        RasterUtils.getWorldCornerCoordinates(
            raster, maxX + 1,
            minY); // need upper right coordinate, which is upper left coordinate of x + 1 pixel
    Point2D worldLowerRight = RasterUtils.getWorldCornerCoordinates(raster, maxX + 1, maxY + 1);
    Point2D worldLowerLeft =
        RasterUtils.getWorldCornerCoordinates(
            raster, minX,
            maxY + 1); // need bottom left coordinate, which is upper left of y + 1 pixel
    return geometryFactory.createPolygon(
        new Coordinate[] {
          convertToCoordinate(worldUpperLeft),
          convertToCoordinate(worldUpperRight),
          convertToCoordinate(worldLowerRight),
          convertToCoordinate(worldLowerLeft),
          convertToCoordinate(worldUpperLeft)
        });
  }

  private static Coordinate convertToCoordinate(Point2D point) {
    double x = point.getX(), y = point.getY();
    // Geotools returns 0 as -0 if scale is negative. Change it 0 instead to avoid confusion
    if (Math.abs(x) == 0) x = 0;
    if (Math.abs(y) == 0) y = 0;
    return new Coordinate(x, y);
  }

  public static Geometry minConvexHull(GridCoverage2D raster)
      throws FactoryException, TransformException {
    return minConvexHull(raster, null);
  }

  public static void ensureSafeBand(GridCoverage2D raster, int band)
      throws IllegalArgumentException {
    if (RasterAccessors.numBands(raster) < band) {
      throw new IllegalArgumentException(
          String.format("Provided band index %d does not lie in the raster", band));
    }
  }

  public static Geometry envelope(GridCoverage2D raster) throws FactoryException {
    Envelope2D envelope2D = raster.getEnvelope2D();

    Envelope envelope =
        new Envelope(
            envelope2D.getMinX(), envelope2D.getMaxX(), envelope2D.getMinY(), envelope2D.getMaxY());
    int srid = RasterAccessors.srid(raster);
    return new GeometryFactory(new PrecisionModel(), srid).toGeometry(envelope);
  }
}
