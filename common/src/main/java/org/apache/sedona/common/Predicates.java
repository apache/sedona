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

import org.apache.sedona.common.sphere.Spheroid;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.relate.RelateOp;

public class Predicates {
  public static boolean contains(Geometry leftGeometry, Geometry rightGeometry) {
    return leftGeometry.contains(rightGeometry);
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
    return leftGeometry.symDifference(rightGeometry).isEmpty();
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
}
