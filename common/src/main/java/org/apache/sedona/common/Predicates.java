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
package org.apache.sedona.common;

import org.apache.sedona.common.geometryObjects.Box2D;
import org.apache.sedona.common.sphere.Spheroid;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.relate.RelateOp;

public class Predicates {
  public static boolean contains(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.contains(rightGeometry);
  }

  /**
   * Closed-interval bbox intersection: true if {@code a} and {@code b} overlap on <em>both</em> the
   * X and Y axes (matches PostGIS {@code &&} on box2d). Edge- and corner-touching boxes count as
   * intersecting.
   *
   * <p>Both arguments must have ordered bounds ({@code xmin <= xmax} and {@code ymin <= ymax}).
   * Sedona's Box2D type allows inverted bounds ({@code xmin > xmax}) — that ordering is reserved
   * for a future antimeridian-wraparound semantics on geography bboxes (cf. sedona-db's {@code
   * WraparoundInterval}). Until those semantics ship, planar predicates throw on inverted input
   * rather than silently returning misleading results. SQL callers see NULL in/out null
   * propagation; this Java entry point throws on null.
   */
  public static boolean boxIntersects(Box2D a, Box2D b) {
    requireOrderedPlanarBox(a, "a");
    requireOrderedPlanarBox(b, "b");
    return !(a.getXMax() < b.getXMin()
        || a.getXMin() > b.getXMax()
        || a.getYMax() < b.getYMin()
        || a.getYMin() > b.getYMax());
  }

  /**
   * True if {@code a} fully contains {@code b} on <em>both</em> the X and Y axes (closed intervals;
   * matches PostGIS {@code ~} on box2d). Equal boxes contain each other.
   *
   * <p>Same ordered-bound contract as {@link #boxIntersects(Box2D, Box2D)} — inverted bounds throw
   * because planar containment with inverted intervals has no defined meaning until antimeridian
   * wraparound semantics ship.
   */
  public static boolean boxContains(Box2D a, Box2D b) {
    requireOrderedPlanarBox(a, "a");
    requireOrderedPlanarBox(b, "b");
    return a.getXMin() <= b.getXMin()
        && a.getYMin() <= b.getYMin()
        && a.getXMax() >= b.getXMax()
        && a.getYMax() >= b.getYMax();
  }

  private static void requireOrderedPlanarBox(Box2D box, String argName) {
    if (box.getXMin() > box.getXMax() || box.getYMin() > box.getYMax()) {
      throw new IllegalArgumentException(
          "Box2D argument '"
              + argName
              + "' has inverted bounds (xmin > xmax or ymin > ymax). Planar Box2D predicates "
              + "require ordered intervals; inverted bounds are reserved for future antimeridian "
              + "wraparound semantics.");
    }
  }

  public static boolean intersects(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.intersects(rightGeometry);
  }

  public static boolean within(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.within(rightGeometry);
  }

  public static boolean covers(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.covers(rightGeometry);
  }

  public static boolean coveredBy(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.coveredBy(rightGeometry);
  }

  public static boolean crosses(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.crosses(rightGeometry);
  }

  public static boolean overlaps(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.overlaps(rightGeometry);
  }

  public static boolean touches(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.touches(rightGeometry);
  }

  public static boolean equals(Geometry leftGeometry, Geometry rightGeometry) {
    if (leftGeometry.isEmpty() && rightGeometry.isEmpty()) {
      return true;
    }
    return leftGeometry.equalsTopo(rightGeometry);
  }

  public static boolean disjoint(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.disjoint(rightGeometry);
  }

  public static boolean orderingEquals(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.equalsExact(rightGeometry);
  }

  public static boolean dWithin(Geometry leftGeometry, Geometry rightGeometry, double distance) {
    return dWithin(leftGeometry, rightGeometry, distance, false);
  }

  public static boolean dWithin(
      Geometry leftGeometry, Geometry rightGeometry, double distance, boolean useSpheroid) {
    if (useSpheroid) {
      double distanceSpheroid = Spheroid.distance(leftGeometry, rightGeometry);
      return distanceSpheroid <= distance;
    } else {
      return leftGeometry.isWithinDistance(rightGeometry, distance);
    }
  }

  public static String relate(Geometry leftGeometry, Geometry rightGeometry) {
    return RelateOp.relate(leftGeometry, rightGeometry).toString();
  }

  public static boolean relate(
      Geometry leftGeometry, Geometry rightGeometry, String intersectionMatrix) {
    String matrixFromGeom = relate(leftGeometry, rightGeometry);
    return IntersectionMatrix.matches(matrixFromGeom, intersectionMatrix);
  }

  public static boolean relateMatch(String matrix1, String matrix2) {
    return IntersectionMatrix.matches(matrix1, matrix2);
  }

  public static boolean knn(Geometry leftGeometry, Geometry rightGeometry, int k) {
    throw new UnsupportedOperationException("KNN predicate is not supported");
  }

  public static boolean knn(
      Geometry leftGeometry, Geometry rightGeometry, int k, boolean useSpheroid) {
    throw new UnsupportedOperationException("KNN predicate is not supported");
  }
}
