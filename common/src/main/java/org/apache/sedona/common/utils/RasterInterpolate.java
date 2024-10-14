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
package org.apache.sedona.common.utils;

import java.awt.image.Raster;
import java.util.*;
import org.geotools.coverage.grid.GridCoverage2D;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

public class RasterInterpolate {
  private RasterInterpolate() {}

  public static STRtree generateSTRtree(GridCoverage2D inputRaster, int band) {
    Raster rasterData = inputRaster.getRenderedImage().getData();
    int width = rasterData.getWidth();
    int height = rasterData.getHeight();
    Double noDataValue = RasterUtils.getNoDataValue(inputRaster.getSampleDimension(band));
    GeometryFactory geometryFactory = new GeometryFactory();
    STRtree rtree = new STRtree();

    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        double value = rasterData.getSampleDouble(x, y, band);
        if (!Double.isNaN(value) && value != noDataValue) {
          Point point = geometryFactory.createPoint(new Coordinate(x, y));
          RasterPoint rasterPoint = new RasterPoint(point, value, 0.0);
          rtree.insert(new Envelope(point.getCoordinate()), rasterPoint);
        }
      }
    }
    rtree.build();
    return rtree;
  }

  public static double interpolateIDW(
      int x,
      int y,
      STRtree strtree,
      int width,
      int height,
      double power,
      String mode,
      Double numPointsOrRadius,
      Double maxRadiusOrMinPoints) {
    GeometryFactory geometryFactory = new GeometryFactory();
    PriorityQueue<RasterPoint> minHeap =
        new PriorityQueue<>(Comparator.comparingDouble(RasterPoint::getDistance));

    if (mode.equalsIgnoreCase("variable")) {
      Double numPoints =
          (numPointsOrRadius == null) ? 12 : numPointsOrRadius; // Default no. of points -> 12
      Double maxRadius =
          (maxRadiusOrMinPoints == null)
              ? Math.sqrt((width * width) + (height * height))
              : maxRadiusOrMinPoints; // Default max radius -> diagonal of raster
      List<RasterPoint> queryResult =
          strtree.query(new Envelope(x - maxRadius, x + maxRadius, y - maxRadius, y + maxRadius));
      if (mode.equalsIgnoreCase("variable") && strtree.size() < numPointsOrRadius) {
        throw new IllegalArgumentException(
            "Parameter 'numPoints' defaulted to 12 which is larger than no. of valid pixels within the max search radius. Please choose an appropriate value");
      }
      for (RasterPoint rasterPoint : queryResult) {
        if (numPoints <= 0) {
          break;
        }
        Point point = rasterPoint.getPoint();
        double distance = point.distance(geometryFactory.createPoint(new Coordinate(x, y)));
        rasterPoint.setDistance(distance);
        minHeap.add(rasterPoint);
        numPoints--;
      }
    } else if (mode.equalsIgnoreCase("fixed")) {
      Double radius =
          (numPointsOrRadius == null)
              ? Math.sqrt((width * width) + (height * height))
              : numPointsOrRadius; // Default radius -> diagonal of raster
      Double minPoints =
          (maxRadiusOrMinPoints == null)
              ? 0
              : maxRadiusOrMinPoints; // Default min no. of points -> 0
      List<RasterPoint> queryResult = new ArrayList<>();
      do {
        queryResult.clear();
        Envelope searchEnvelope = new Envelope(x - radius, x + radius, y - radius, y + radius);
        queryResult = strtree.query(searchEnvelope);
        // If minimum points requirement met, break the loop
        if (queryResult.size() >= minPoints) {
          break;
        }
        radius *= 1.5; // Increase radius by 50%
      } while (true);

      for (RasterPoint rasterPoint : queryResult) {
        Point point = rasterPoint.getPoint();
        double distance = point.distance(geometryFactory.createPoint(new Coordinate(x, y)));
        if (distance <= 0 || distance > radius) {
          continue;
        }
        rasterPoint.setDistance(distance);
        minHeap.add(rasterPoint);
      }
    }

    double numerator = 0.0;
    double denominator = 0.0;

    while (!minHeap.isEmpty()) {
      RasterPoint rasterPoint = minHeap.poll();
      double value = rasterPoint.getValue();
      double distance = rasterPoint.getDistance();
      double weight = 1.0 / Math.pow(distance, power);
      numerator += weight * value;
      denominator += weight;
    }

    double interpolatedValue = (denominator > 0 ? numerator / denominator : Double.NaN);
    return interpolatedValue;
  }

  public static class RasterPoint {
    private Point point; // JTS Point
    private double value; // The associated value
    private double distance; // Distance measure

    public RasterPoint(Point point, double value, double distance) {
      this.point = point;
      this.value = value;
      this.distance = distance;
    }

    public Point getPoint() {
      return point;
    }

    public double getValue() {
      return value;
    }

    public double getDistance() {
      return distance;
    }

    public void setDistance(double distance) {
      this.distance = distance;
    }
  }
}
