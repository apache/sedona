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
    Coordinate pt;
    int numPoint = coords.length;
    int totalPointsOut = 1;

    double distance = Double.MAX_VALUE;

    if (numPoint <= minPoints) return new Coordinate[0];

    Coordinate last = coords[0];
    int pToIndex = 1;

    for (int i = 1; i < numPoint; i++) {
      boolean lastPoint = (i == numPoint - 1);

      pt = coords[i];

      if (numPoint + totalPointsOut > minPoints + i) {
        if (TOLERANCE > 0.0) {
          distance = pt.distance(last);
          if (!lastPoint && distance <= TOLERANCE) {
            continue;
          }
        } else {
          if (pt.equals2D(last)) {
            continue;
          }
        }

        if (lastPoint && totalPointsOut > 1 && TOLERANCE > 0.0 && distance <= TOLERANCE) {
          totalPointsOut--;
          pToIndex--;
        }
      }

      coords[pToIndex] = pt;
      totalPointsOut++;
      pToIndex++;
      last = pt;
    }
    Coordinate[] newCoordinates = newCoordinates = new Coordinate[totalPointsOut];
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

  public static Geometry transform(Geometry geometry, double tolerance) {

    TOLERANCE = tolerance;

    if (geometry.isEmpty()) return geometry;

    FACTORY = geometry.getFactory();

    if (geometry.getGeometryType().equals(Geometry.TYPENAME_POINT)) return geometry;
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_MULTIPOINT))
      return transformMultiPoint((MultiPoint) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_LINEARRING))
      return transformLinearRing((LinearRing) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_LINESTRING))
      return transformLineString((LineString) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_MULTILINESTRING))
      return transformMultiLineString((MultiLineString) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_POLYGON))
      return transformPolygon((Polygon) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_MULTIPOLYGON))
      return transformMultiPolygon((MultiPolygon) geometry);
    if (geometry.getGeometryType().equals(Geometry.TYPENAME_GEOMETRYCOLLECTION))
      return transformGeometryCollection((GeometryCollection) geometry);

    throw new IllegalArgumentException(
        "Unknown Geometry subtype: " + geometry.getClass().getName());
  }

  private static MultiPoint transformMultiPoint(MultiPoint geometry) {
    Coordinate[] coords = geometry.getCoordinates();
    return FACTORY.createMultiPointFromCoords(removeDuplicatePointsMultiPoint(coords, false));
  }

  private static LinearRing transformLinearRing(LinearRing geometry) {
    Coordinate[] coords = geometry.getCoordinates();
    return FACTORY.createLinearRing(removeDuplicates(coords, 4));
  }

  private static LineString transformLineString(LineString geometry) {
    if (geometry.getNumPoints() <= 2) return geometry;

    Coordinate[] coords = geometry.getCoordinates();
    return FACTORY.createLineString(removeDuplicates(coords, 2));
  }

  private static MultiLineString transformMultiLineString(MultiLineString geometry) {
    LineString[] lineStrings = new LineString[geometry.getNumGeometries()];
    for (int i = 0; i < lineStrings.length; i++) {
      lineStrings[i] = transformLineString((LineString) geometry.getGeometryN(i));
    }
    return FACTORY.createMultiLineString(lineStrings);
  }

  private static Polygon transformPolygon(Polygon geometry) {
    LinearRing shell = transformLinearRing(geometry.getExteriorRing());

    LinearRing[] holes = new LinearRing[geometry.getNumInteriorRing()];
    for (int i = 0; i < holes.length; i++) {
      holes[i] = transformLinearRing(geometry.getInteriorRingN(i));
    }
    return FACTORY.createPolygon(shell, holes);
  }

  private static MultiPolygon transformMultiPolygon(MultiPolygon geometry) {
    Polygon[] polygons = new Polygon[geometry.getNumGeometries()];
    for (int i = 0; i < polygons.length; i++) {
      polygons[i] = transformPolygon((Polygon) geometry.getGeometryN(i));
    }
    return FACTORY.createMultiPolygon(polygons);
  }

  private static GeometryCollection transformGeometryCollection(GeometryCollection geometry) {
    Geometry[] geometries = new Geometry[geometry.getNumGeometries()];
    for (int i = 0; i < geometries.length; i++) {
      geometries[i] = transform(geometry.getGeometryN(i), TOLERANCE);
    }
    return FACTORY.createGeometryCollection(geometries);
  }
}
