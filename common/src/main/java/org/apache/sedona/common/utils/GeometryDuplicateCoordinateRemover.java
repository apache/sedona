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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.locationtech.jts.geom.*;

public class GeometryDuplicateCoordinateRemover {

  public static Coordinate[] removeDuplicates(Coordinate[] coords, int minPoints) {
    Coordinate currentPoint;
    int numPoint = coords.length;
    int totalPointsOut = 1;

    double distance = Double.MAX_VALUE;

    if (numPoint <= minPoints) return new Coordinate[0];

    Coordinate lastPoint = coords[0];
    int writeIndex = 1;

    for (int i = 1; i < numPoint; i++) {
      boolean isLastPoint = (i == numPoint - 1);

      currentPoint = coords[i];

      if (numPoint + totalPointsOut > minPoints + i) {
        if (TOLERANCE > 0.0) {
          distance = currentPoint.distance(lastPoint);
          if (!isLastPoint && distance <= TOLERANCE) {
            continue;
          }
        } else {
          if (currentPoint.equals2D(lastPoint)) {
            continue;
          }
        }

        if (isLastPoint && totalPointsOut > 1 && TOLERANCE > 0.0 && distance <= TOLERANCE) {
          totalPointsOut--;
          writeIndex--;
        }
      }

      coords[writeIndex] = currentPoint;
      totalPointsOut++;
      writeIndex++;
      lastPoint = currentPoint;
    }
    Coordinate[] newCoordinates = new Coordinate[totalPointsOut];
    System.arraycopy(coords, 0, newCoordinates, 0, totalPointsOut);

    return newCoordinates;
  }

  public static Coordinate[] removeDuplicatePointsMultiPoint(
      Coordinate[] coords, boolean recursion) {
    if (TOLERANCE == 0 || recursion) {
      Set<Coordinate> uniqueCoords = new LinkedHashSet<>(Arrays.asList(coords));
      return uniqueCoords.toArray(new Coordinate[0]);
    }
    Coordinate[] deduplicated =
        Arrays.stream(removeDuplicatePointsMultiPoint(coords, true))
            .sorted()
            .toArray(Coordinate[]::new);

    for (int i = 0; i < deduplicated.length; i++) {
      for (int j = i + 1; j < deduplicated.length; j++) {
        if (deduplicated[i] != null
            && deduplicated[j] != null
            && deduplicated[i].distance(deduplicated[j]) < TOLERANCE) {
          deduplicated[j] = null;
        } else {
          break;
        }
      }
    }

    return Arrays.stream(deduplicated).filter(Objects::nonNull).toArray(Coordinate[]::new);
  }

  private static GeometryFactory FACTORY = null;

  private static double TOLERANCE = 0;

  public static Geometry process(Geometry geometry, double tolerance) {

    TOLERANCE = tolerance;

    if (geometry.isEmpty()) return geometry;

    FACTORY = geometry.getFactory();

    if (geometry.getGeometryType().equals(Geometry.TYPENAME_POINT)) return geometry;
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_MULTIPOINT))
      return processMultiPoint((MultiPoint) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_LINEARRING))
      return processLinearRing((LinearRing) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_LINESTRING))
      return processLineString((LineString) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_MULTILINESTRING))
      return processMultiLineString((MultiLineString) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_POLYGON))
      return processPolygon((Polygon) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_MULTIPOLYGON))
      return processMultiPolygon((MultiPolygon) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_GEOMETRYCOLLECTION))
      return processGeometryCollection((GeometryCollection) geometry);

    throw new IllegalArgumentException(
        "Unknown Geometry subtype: " + geometry.getClass().getName());
  }

  private static MultiPoint processMultiPoint(MultiPoint geometry) {
    Coordinate[] coords = geometry.getCoordinates();
    return FACTORY.createMultiPointFromCoords(removeDuplicatePointsMultiPoint(coords, false));
  }

  private static LinearRing processLinearRing(LinearRing geometry) {
    Coordinate[] coords = geometry.getCoordinates();
    return FACTORY.createLinearRing(removeDuplicates(coords, 4));
  }

  private static LineString processLineString(LineString geometry) {
    if (geometry.getNumPoints() <= 2) return geometry;

    Coordinate[] coords = geometry.getCoordinates();
    return FACTORY.createLineString(removeDuplicates(coords, 2));
  }

  private static MultiLineString processMultiLineString(MultiLineString geometry) {
    LineString[] lineStrings = new LineString[geometry.getNumGeometries()];
    for (int i = 0; i < lineStrings.length; i++) {
      lineStrings[i] = processLineString((LineString) geometry.getGeometryN(i));
    }
    return FACTORY.createMultiLineString(lineStrings);
  }

  private static Polygon processPolygon(Polygon geometry) {
    LinearRing shell = processLinearRing(geometry.getExteriorRing());

    LinearRing[] holes = new LinearRing[geometry.getNumInteriorRing()];
    for (int i = 0; i < holes.length; i++) {
      holes[i] = processLinearRing(geometry.getInteriorRingN(i));
    }
    return FACTORY.createPolygon(shell, holes);
  }

  private static MultiPolygon processMultiPolygon(MultiPolygon geometry) {
    Polygon[] polygons = new Polygon[geometry.getNumGeometries()];
    for (int i = 0; i < polygons.length; i++) {
      polygons[i] = processPolygon((Polygon) geometry.getGeometryN(i));
    }
    return FACTORY.createMultiPolygon(polygons);
  }

  private static GeometryCollection processGeometryCollection(GeometryCollection geometry) {
    Geometry[] geometries = new Geometry[geometry.getNumGeometries()];
    for (int i = 0; i < geometries.length; i++) {
      geometries[i] = process(geometry.getGeometryN(i), TOLERANCE);
    }
    return FACTORY.createGeometryCollection(geometries);
  }
}
