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

import java.util.*;
import java.util.function.BiFunction;
import org.locationtech.jts.geom.*;

public class GeometryLocateAlongProcessor {

  private final Map<Class<?>, BiFunction<Geometry, double[], Geometry>> geometryFunctions =
      new HashMap<>();

  public GeometryLocateAlongProcessor() {
    geometryFunctions.put(
        Point.class,
        (geometry, params) -> locateAlongPoint((Point) geometry, params[0], params[1]));
    geometryFunctions.put(
        MultiPoint.class,
        (geometry, params) -> locateAlongMultiPoint((MultiPoint) geometry, params[0], params[1]));
    geometryFunctions.put(
        LineString.class,
        (geometry, params) -> locateAlongLineString((LineString) geometry, params[0], params[1]));
    geometryFunctions.put(
        MultiLineString.class,
        (geometry, params) ->
            locateAlongMultiLineString((MultiLineString) geometry, params[0], params[1]));
  }

  public static Geometry processGeometry(Geometry geometry, double measure, double offset) {
    GeometryLocateAlongProcessor processor = new GeometryLocateAlongProcessor();
    BiFunction<Geometry, double[], Geometry> function =
        processor.geometryFunctions.get(geometry.getClass());
    if (function != null) {
      return function.apply(geometry, new double[] {measure, offset});
    }
    throw new IllegalArgumentException(
        String.format(
            "%s geometry type not supported, supported types are: (Multi)Point and (Multi)LineString.",
            geometry.getGeometryType()));
  }

  private Geometry locateAlongPoint(Point point, double measure, double offset) {
    if (measure == point.getCoordinate().getM()) {
      return point;
    }
    return null;
  }

  private Geometry locateAlongMultiPoint(MultiPoint multiPoint, double measure, double offset) {
    Point[] points = new Point[multiPoint.getNumGeometries()];
    for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
      points[i] = (Point) locateAlongPoint((Point) multiPoint.getGeometryN(i), measure, offset);
    }
    return multiPoint
        .getFactory()
        .createMultiPoint(Arrays.stream(points).filter(Objects::nonNull).toArray(Point[]::new));
  }

  private Geometry locateAlongLineString(LineString lineString, double measure, double offset) {
    Coordinate[] coordinates = lineString.getCoordinates();
    CoordinateList coordinateList = new CoordinateList();

    for (int i = 1; i < coordinates.length; i++) {
      Coordinate coordinate1 = coordinates[i - 1];
      Coordinate coordinate2 = coordinates[i];
      CoordinateXYZM newCoordinate = new CoordinateXYZM();
      double position;

      double measure1 = coordinate1.getM(), measure2 = coordinate2.getM();

      if ((measure < Math.min(measure1, measure2)) || (measure > Math.max(measure1, measure2))) {
        continue;
      }

      if (measure1 == measure2) {
        // If the measures are equal then there is no valid interpolation range
        if (coordinate1.equals(coordinate2)) {
          newCoordinate.setX(coordinate1.getX());
          newCoordinate.setY(coordinate1.getY());
          newCoordinate.setZ(coordinate1.getZ());
          newCoordinate.setM(coordinate1.getM());
          coordinateList.add(newCoordinate, false);
          continue;
        }
        // the point will be in the midpoint of coordinate1 and coordinate2 as measure1 and measure2
        // are same
        position = 0.5;
      } else {
        // calculate the interpolation factor / position
        position = (measure - measure1) / (measure2 - measure1);
      }

      // apply linear interpolation to find the point along the line
      newCoordinate.setX(coordinate1.x + (coordinate2.x - coordinate1.x) * position);
      newCoordinate.setY(coordinate1.y + (coordinate2.y - coordinate1.y) * position);
      newCoordinate.setZ(coordinate1.z + (coordinate2.z - coordinate1.z) * position);
      newCoordinate.setM(measure);

      if (offset != 0D) {
        // calculate the angle of the line segment
        double theta = Math.atan2(coordinate2.y - coordinate1.y, coordinate2.x - coordinate1.x);
        // shift the coordinate left or right by the offset
        // if the offset is positive then shift to left
        // else the offset is negative then shift to right
        newCoordinate.setX(newCoordinate.x - Math.sin(theta) * offset);
        newCoordinate.setY(newCoordinate.y + Math.cos(theta) * offset);
      }
      coordinateList.add(newCoordinate, false);
    }
    return lineString
        .getFactory()
        .createMultiPointFromCoords(
            Arrays.stream(coordinateList.toCoordinateArray())
                .filter(Objects::nonNull)
                .toArray(Coordinate[]::new));
  }

  private Geometry locateAlongMultiLineString(
      MultiLineString multiLineString, double measure, double offset) {
    // iterating through LineStrings in MultiLineString object
    List<Point> points = new ArrayList<>();
    for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
      MultiPoint mPoint =
          (MultiPoint)
              locateAlongLineString((LineString) multiLineString.getGeometryN(i), measure, offset);
      for (int j = 0; j < mPoint.getNumGeometries(); j++) {
        points.add((Point) mPoint.getGeometryN(j));
      }
    }

    return multiLineString.getFactory().createMultiPoint(points.toArray(new Point[0]));
  }
}
