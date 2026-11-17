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
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Converts every coordinate sequence of a geometry to a target coordinate layout. Ordinates of the
 * target layout that the source already has are kept; missing (or NaN) Z and M ordinates are filled
 * with the supplied defaults. The decision is made per coordinate sequence, so geometry collections
 * whose members have different dimensions are handled member by member. An instance is not safe for
 * concurrent use; the static entry points of the subclasses create one per call.
 */
abstract class GeometryForceTransformer extends GeometryTransformer {
  private final CoordinateType layout;
  private final int measures;
  private final double zValue;
  private final double mValue;

  GeometryForceTransformer(CoordinateType layout, double zValue, double mValue) {
    this.layout = layout;
    this.measures = layout.hasM ? 1 : 0;
    this.zValue = zValue;
    this.mValue = mValue;
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    int size = coords.size();
    CoordinateSequence converted =
        factory.getCoordinateSequenceFactory().create(size, layout.ordinates, measures);
    if (!matchesLayout(converted)) {
      // The geometry's own factory could not produce the requested layout.
      converted =
          CoordinateArraySequenceFactory.instance().create(size, layout.ordinates, measures);
    }
    for (int i = 0; i < size; i++) {
      converted.setOrdinate(i, CoordinateSequence.X, coords.getX(i));
      converted.setOrdinate(i, CoordinateSequence.Y, coords.getY(i));
      if (layout.hasZ) {
        converted.setOrdinate(i, CoordinateSequence.Z, ordinateOrDefault(coords.getZ(i), zValue));
      }
      if (layout.hasM) {
        converted.setOrdinate(i, layout.ordinates - 1, ordinateOrDefault(coords.getM(i), mValue));
      }
    }
    return converted;
  }

  private static double ordinateOrDefault(double value, double defaultValue) {
    return Double.isNaN(value) ? defaultValue : value;
  }

  // GeometryTransformer turns a MULTI* with one member into that member (it rebuilds them with
  // GeometryFactory.buildGeometry), and it drops empty members and empty holes. The overrides
  // below convert one member at a time instead, so the type and every member of the input are
  // kept at any depth. Empty members are converted too: a multipart geometry needs one coordinate
  // layout for all of its parts.

  @Override
  protected Geometry transformMultiPoint(MultiPoint geom, Geometry parent) {
    Point[] points = new Point[geom.getNumGeometries()];
    for (int i = 0; i < points.length; i++) {
      points[i] = (Point) transformMember(geom.getGeometryN(i), geom);
    }
    return factory.createMultiPoint(points);
  }

  @Override
  protected Geometry transformMultiLineString(MultiLineString geom, Geometry parent) {
    LineString[] lines = new LineString[geom.getNumGeometries()];
    for (int i = 0; i < lines.length; i++) {
      lines[i] = (LineString) transformMember(geom.getGeometryN(i), geom);
    }
    return factory.createMultiLineString(lines);
  }

  @Override
  protected Geometry transformMultiPolygon(MultiPolygon geom, Geometry parent) {
    Polygon[] polygons = new Polygon[geom.getNumGeometries()];
    for (int i = 0; i < polygons.length; i++) {
      polygons[i] = (Polygon) transformMember(geom.getGeometryN(i), geom);
    }
    return factory.createMultiPolygon(polygons);
  }

  @Override
  protected Geometry transformGeometryCollection(GeometryCollection geom, Geometry parent) {
    Geometry[] members = new Geometry[geom.getNumGeometries()];
    for (int i = 0; i < members.length; i++) {
      members[i] = transformMember(geom.getGeometryN(i), geom);
    }
    return factory.createGeometryCollection(members);
  }

  /**
   * Dispatches a member to its transform method. GeometryTransformer.transform is not used here
   * because it switches to the member's own factory, so the result would take its SRID and
   * precision model from the last member instead of from the input.
   */
  private Geometry transformMember(Geometry member, Geometry parent) {
    if (member instanceof Point) {
      return transformPoint((Point) member, parent);
    }
    if (member instanceof LinearRing) {
      return transformLinearRing((LinearRing) member, parent);
    }
    if (member instanceof LineString) {
      return transformLineString((LineString) member, parent);
    }
    if (member instanceof Polygon) {
      return transformPolygon((Polygon) member, parent);
    }
    if (member instanceof MultiPoint) {
      return transformMultiPoint((MultiPoint) member, parent);
    }
    if (member instanceof MultiLineString) {
      return transformMultiLineString((MultiLineString) member, parent);
    }
    if (member instanceof MultiPolygon) {
      return transformMultiPolygon((MultiPolygon) member, parent);
    }
    if (member instanceof GeometryCollection) {
      return transformGeometryCollection((GeometryCollection) member, parent);
    }
    throw new IllegalArgumentException("Unknown Geometry subtype: " + member.getClass().getName());
  }

  @Override
  protected Geometry transformPolygon(Polygon geom, Geometry parent) {
    // The number of points does not change, so every ring stays a LinearRing.
    LinearRing shell = (LinearRing) transformLinearRing(geom.getExteriorRing(), geom);
    LinearRing[] holes = new LinearRing[geom.getNumInteriorRing()];
    for (int i = 0; i < holes.length; i++) {
      holes[i] = (LinearRing) transformLinearRing(geom.getInteriorRingN(i), geom);
    }
    return factory.createPolygon(shell, holes);
  }

  /** Returns the input unchanged when it is empty or already entirely in the target layout. */
  protected final Geometry transformIfNeeded(Geometry geometry) {
    return geometry.isEmpty() || hasTargetCoordinates(geometry) ? geometry : transform(geometry);
  }

  private boolean hasTargetCoordinates(Geometry geometry) {
    if (geometry instanceof Point) {
      return hasTargetCoordinates(((Point) geometry).getCoordinateSequence());
    }
    if (geometry instanceof LineString) {
      return hasTargetCoordinates(((LineString) geometry).getCoordinateSequence());
    }
    if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      if (!hasTargetCoordinates(polygon.getExteriorRing())) {
        return false;
      }
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        if (!hasTargetCoordinates(polygon.getInteriorRingN(i))) {
          return false;
        }
      }
      return true;
    }
    if (!(geometry instanceof GeometryCollection)) {
      // An unknown subtype: let GeometryTransformer reject it.
      return false;
    }
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      if (!hasTargetCoordinates(geometry.getGeometryN(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean matchesLayout(CoordinateSequence coords) {
    return coords.getDimension() == layout.ordinates && coords.getMeasures() == measures;
  }

  private boolean hasTargetCoordinates(CoordinateSequence coords) {
    // Empty sequences too: a multipart geometry needs one layout for all of its parts.
    if (!matchesLayout(coords)) {
      return false;
    }
    for (int i = 0; i < coords.size(); i++) {
      if ((layout.hasZ && Double.isNaN(coords.getZ(i)))
          || (layout.hasM && Double.isNaN(coords.getM(i)))) {
        return false;
      }
    }
    return true;
  }
}
