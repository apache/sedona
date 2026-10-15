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

import java.util.function.BiPredicate;
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
   * GeometrySerializer#getCoordinateType}), not the in-memory dimension of each coordinate
   * sequence. JTS's default sequences, which WKT parsing produces, hold ordinary XY coordinates
   * with a NaN Z ordinate, while a stored geometry reads back with a declared XY layout; both are
   * XY. A Z ordinate that is NaN everywhere counts only when the sequence declares it. Members of a
   * GeometryCollection are compared with their own layouts, as they are stored. A geometry whose
   * layout the serializer cannot encode is only identical to another such geometry with the same
   * in-memory dimensions, which keeps the relation transitive.
   */
  public static boolean equalsIdentical(Geometry left, Geometry right) {
    if (left == null || right == null) {
      return false;
    }
    if (left == right) {
      return true;
    }
    // Types, structure and X/Y values first: resolving a layout can mean reading every Z value.
    return structureEqual(left, right, GeometryEquality::xyEqual) && dimensionsEqual(left, right);
  }

  /** Compares the dimensions of two geometries whose structure and X/Y values are equal. */
  private static boolean dimensionsEqual(Geometry left, Geometry right) {
    if (Geometry.TYPENAME_GEOMETRYCOLLECTION.equals(left.getGeometryType())) {
      for (int i = 0; i < left.getNumGeometries(); i++) {
        if (!dimensionsEqual(left.getGeometryN(i), right.getGeometryN(i))) {
          return false;
        }
      }
      return true;
    }
    CoordinateType layout = storedLayout(left);
    CoordinateType rightLayout = storedLayout(right);
    if (layout == null && rightLayout == null) {
      return structureEqual(left, right, GeometryEquality::inMemorySequencesEqual);
    }
    if (layout != rightLayout) {
      return false;
    }
    if (!layout.hasZ && !layout.hasM) {
      return true;
    }
    return structureEqual(left, right, (a, b) -> zmEqual(a, b, layout));
  }

  /** The layout the serializer stores, or null when it cannot encode the geometry. */
  private static CoordinateType storedLayout(Geometry geometry) {
    try {
      return GeometrySerializer.getCoordinateType(geometry);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * True when both geometries have the same type and structure, component by component, and {@code
   * sequencesEqual} holds for every pair of corresponding coordinate sequences.
   */
  private static boolean structureEqual(
      Geometry left,
      Geometry right,
      BiPredicate<CoordinateSequence, CoordinateSequence> sequencesEqual) {
    if (!left.getGeometryType().equals(right.getGeometryType())) {
      return false;
    }
    if (left instanceof Point) {
      return sequencesEqual.test(
          ((Point) left).getCoordinateSequence(), ((Point) right).getCoordinateSequence());
    }
    if (left instanceof LineString) {
      return sequencesEqual.test(
          ((LineString) left).getCoordinateSequence(),
          ((LineString) right).getCoordinateSequence());
    }
    if (left instanceof Polygon) {
      Polygon leftPolygon = (Polygon) left;
      Polygon rightPolygon = (Polygon) right;
      if (leftPolygon.getNumInteriorRing() != rightPolygon.getNumInteriorRing()
          || !structureEqual(
              leftPolygon.getExteriorRing(), rightPolygon.getExteriorRing(), sequencesEqual)) {
        return false;
      }
      for (int i = 0; i < leftPolygon.getNumInteriorRing(); i++) {
        if (!structureEqual(
            leftPolygon.getInteriorRingN(i), rightPolygon.getInteriorRingN(i), sequencesEqual)) {
          return false;
        }
      }
      return true;
    }
    if (left instanceof GeometryCollection) {
      if (left.getNumGeometries() != right.getNumGeometries()) {
        return false;
      }
      for (int i = 0; i < left.getNumGeometries(); i++) {
        if (!structureEqual(left.getGeometryN(i), right.getGeometryN(i), sequencesEqual)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean xyEqual(CoordinateSequence left, CoordinateSequence right) {
    int size = left.size();
    if (size != right.size()) {
      return false;
    }
    for (int i = 0; i < size; i++) {
      if (!ordinatesEqual(left.getX(i), right.getX(i))
          || !ordinatesEqual(left.getY(i), right.getY(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean zmEqual(
      CoordinateSequence left, CoordinateSequence right, CoordinateType layout) {
    for (int i = 0; i < left.size(); i++) {
      if ((layout.hasZ && !ordinatesEqual(left.getZ(i), right.getZ(i)))
          || (layout.hasM && !ordinatesEqual(left.getM(i), right.getM(i)))) {
        return false;
      }
    }
    return true;
  }

  /** Compares sequences as they are stored in memory, for layouts the serializer cannot encode. */
  private static boolean inMemorySequencesEqual(CoordinateSequence left, CoordinateSequence right) {
    return left.getDimension() == right.getDimension()
        && left.getMeasures() == right.getMeasures()
        && CoordinateSequences.isEqual(left, right);
  }

  /** Same rule as {@link CoordinateSequences#isEqual}: equal values, or NaN in both. */
  private static boolean ordinatesEqual(double left, double right) {
    return left == right || (Double.isNaN(left) && Double.isNaN(right));
  }
}
