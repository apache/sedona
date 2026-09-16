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
package org.apache.sedona.common.geometryObjects;

import org.locationtech.jts.geom.*;

/** Geometry factory that retains empty components and coordinate layouts when copying. */
public final class StructurePreservingGeometryFactory extends GeometryFactory {
  private static final long serialVersionUID = 1L;

  public StructurePreservingGeometryFactory(
      PrecisionModel precisionModel, int srid, CoordinateSequenceFactory sequenceFactory) {
    super(precisionModel, srid, sequenceFactory);
  }

  @Override
  public Geometry createGeometry(Geometry geometry) {
    if (geometry instanceof Point) {
      return createPoint(copy(((Point) geometry).getCoordinateSequence()));
    }
    if (geometry instanceof LinearRing) {
      return createLinearRing(copy(((LinearRing) geometry).getCoordinateSequence()));
    }
    if (geometry instanceof LineString) {
      return createLineString(copy(((LineString) geometry).getCoordinateSequence()));
    }
    if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
      for (int i = 0; i < holes.length; i++) {
        holes[i] = (LinearRing) createGeometry(polygon.getInteriorRingN(i));
      }
      return createPolygon((LinearRing) createGeometry(polygon.getExteriorRing()), holes);
    }
    if (geometry instanceof MultiPoint) {
      Point[] points = new Point[geometry.getNumGeometries()];
      for (int i = 0; i < points.length; i++) {
        points[i] = (Point) createGeometry(geometry.getGeometryN(i));
      }
      return createMultiPoint(points);
    }
    if (geometry instanceof MultiLineString) {
      LineString[] lines = new LineString[geometry.getNumGeometries()];
      for (int i = 0; i < lines.length; i++) {
        lines[i] = (LineString) createGeometry(geometry.getGeometryN(i));
      }
      return createMultiLineString(lines);
    }
    if (geometry instanceof MultiPolygon) {
      Polygon[] polygons = new Polygon[geometry.getNumGeometries()];
      for (int i = 0; i < polygons.length; i++) {
        polygons[i] = (Polygon) createGeometry(geometry.getGeometryN(i));
      }
      return createMultiPolygon(polygons);
    }
    if (geometry instanceof GeometryCollection) {
      Geometry[] children = new Geometry[geometry.getNumGeometries()];
      for (int i = 0; i < children.length; i++) {
        children[i] = createGeometry(geometry.getGeometryN(i));
      }
      return createGeometryCollection(children);
    }
    return super.createGeometry(geometry);
  }

  private CoordinateSequence copy(CoordinateSequence sequence) {
    return getCoordinateSequenceFactory().create(sequence);
  }
}
