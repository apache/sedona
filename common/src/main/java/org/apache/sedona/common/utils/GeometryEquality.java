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

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/** Structural geometry equality including every coordinate dimension. */
public final class GeometryEquality {

  private GeometryEquality() {}

  /**
   * Tests whether two geometries have identical types, structure, component ordering, coordinate
   * ordering, dimensionality, and ordinate values. NaN ordinates in corresponding positions are
   * considered equal. Geometry metadata such as SRID, precision model, and user data is ignored.
   */
  public static boolean equalsIdentical(Geometry left, Geometry right) {
    if (left == null || right == null) {
      return false;
    }
    if (left == right) {
      return true;
    }
    if (!left.getGeometryType().equals(right.getGeometryType())) {
      return false;
    }

    if (left instanceof Point && right instanceof Point) {
      return coordinateSequencesEqual(
          ((Point) left).getCoordinateSequence(), ((Point) right).getCoordinateSequence());
    }
    if (left instanceof LineString && right instanceof LineString) {
      return coordinateSequencesEqual(
          ((LineString) left).getCoordinateSequence(),
          ((LineString) right).getCoordinateSequence());
    }
    if (left instanceof Polygon && right instanceof Polygon) {
      return polygonsEqual((Polygon) left, (Polygon) right);
    }
    if (left instanceof GeometryCollection && right instanceof GeometryCollection) {
      return collectionsEqual((GeometryCollection) left, (GeometryCollection) right);
    }
    return false;
  }

  private static boolean polygonsEqual(Polygon left, Polygon right) {
    if (left.getNumInteriorRing() != right.getNumInteriorRing()
        || !equalsIdentical(left.getExteriorRing(), right.getExteriorRing())) {
      return false;
    }
    for (int i = 0; i < left.getNumInteriorRing(); i++) {
      if (!equalsIdentical(left.getInteriorRingN(i), right.getInteriorRingN(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean collectionsEqual(GeometryCollection left, GeometryCollection right) {
    if (left.getNumGeometries() != right.getNumGeometries()) {
      return false;
    }
    for (int i = 0; i < left.getNumGeometries(); i++) {
      if (!equalsIdentical(left.getGeometryN(i), right.getGeometryN(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean coordinateSequencesEqual(
      CoordinateSequence left, CoordinateSequence right) {
    return left.getDimension() == right.getDimension()
        && left.getMeasures() == right.getMeasures()
        && CoordinateSequences.isEqual(left, right);
  }
}
