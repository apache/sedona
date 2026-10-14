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

import org.apache.sedona.common.geometrySerde.CoordinateType;
import org.apache.sedona.common.geometrySerde.GeometrySerializer;
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
   *
   * <p>Dimensionality is the coordinate layout the geometry serializer stores (see {@link
   * GeometrySerializer#coordinateType}), not the in-memory dimension of each coordinate sequence.
   * JTS's default sequences, which WKT parsing produces, hold ordinary XY coordinates with a NaN Z
   * ordinate, while a stored geometry reads back with a declared XY layout; both are XY. A Z
   * ordinate that is NaN everywhere counts only when the sequence declares it. Members of a
   * GeometryCollection are compared with their own layouts, as they are stored. Geometries whose
   * layout the serializer cannot encode are compared by their in-memory sequence dimensions.
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
    if (Geometry.TYPENAME_GEOMETRYCOLLECTION.equals(left.getGeometryType())) {
      return collectionsEqual((GeometryCollection) left, (GeometryCollection) right);
    }
    CoordinateType leftLayout = coordinateType(left);
    CoordinateType rightLayout = coordinateType(right);
    if (leftLayout == null || rightLayout == null) {
      return structureEqual(left, right, null);
    }
    return leftLayout == rightLayout && structureEqual(left, right, leftLayout);
  }

  /** The stored coordinate layout, or null when the serializer cannot encode it. */
  private static CoordinateType coordinateType(Geometry geometry) {
    try {
      return GeometrySerializer.coordinateType(geometry);
    } catch (IllegalArgumentException e) {
      return null;
    }
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

  /**
   * Compares two geometries of the same type, other than GeometryCollection, whose parts all share
   * {@code layout}; a null layout compares the sequences as they are stored in memory.
   */
  private static boolean structureEqual(Geometry left, Geometry right, CoordinateType layout) {
    if (left instanceof Point) {
      return sequencesEqual(
          ((Point) left).getCoordinateSequence(), ((Point) right).getCoordinateSequence(), layout);
    }
    if (left instanceof LineString) {
      return sequencesEqual(
          ((LineString) left).getCoordinateSequence(),
          ((LineString) right).getCoordinateSequence(),
          layout);
    }
    if (left instanceof Polygon) {
      Polygon leftPolygon = (Polygon) left;
      Polygon rightPolygon = (Polygon) right;
      if (leftPolygon.getNumInteriorRing() != rightPolygon.getNumInteriorRing()
          || !structureEqual(
              leftPolygon.getExteriorRing(), rightPolygon.getExteriorRing(), layout)) {
        return false;
      }
      for (int i = 0; i < leftPolygon.getNumInteriorRing(); i++) {
        if (!structureEqual(
            leftPolygon.getInteriorRingN(i), rightPolygon.getInteriorRingN(i), layout)) {
          return false;
        }
      }
      return true;
    }
    if (left instanceof GeometryCollection) {
      // A multi geometry: its parts share the layout.
      if (left.getNumGeometries() != right.getNumGeometries()) {
        return false;
      }
      for (int i = 0; i < left.getNumGeometries(); i++) {
        if (!structureEqual(left.getGeometryN(i), right.getGeometryN(i), layout)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean sequencesEqual(
      CoordinateSequence left, CoordinateSequence right, CoordinateType layout) {
    if (layout == null) {
      return left.getDimension() == right.getDimension()
          && left.getMeasures() == right.getMeasures()
          && CoordinateSequences.isEqual(left, right);
    }
    int size = left.size();
    if (size != right.size()) {
      return false;
    }
    for (int i = 0; i < size; i++) {
      if (!ordinatesEqual(left.getX(i), right.getX(i))
          || !ordinatesEqual(left.getY(i), right.getY(i))
          || (layout.hasZ && !ordinatesEqual(left.getZ(i), right.getZ(i)))
          || (layout.hasM && !ordinatesEqual(left.getM(i), right.getM(i)))) {
        return false;
      }
    }
    return true;
  }

  /** Same rule as {@link CoordinateSequences#isEqual}: equal values, or NaN in both. */
  private static boolean ordinatesEqual(double left, double right) {
    return left == right || (Double.isNaN(left) && Double.isNaN(right));
  }
}
