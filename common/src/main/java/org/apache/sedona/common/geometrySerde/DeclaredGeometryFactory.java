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
package org.apache.sedona.common.geometrySerde;

import org.locationtech.jts.geom.*;

/**
 * Geometry factory that retains empty components and their declared coordinate layouts on copies.
 */
public final class DeclaredGeometryFactory extends GeometryFactory {
  private static final long serialVersionUID = 1L;

  public DeclaredGeometryFactory(PrecisionModel precisionModel, int srid) {
    super(precisionModel, srid, DeclaredCoordinateSequenceFactory.INSTANCE);
  }

  @Override
  public Geometry createGeometry(Geometry geometry) {
    // GeometryEditor's default copy removes empty collection members and rebuilds empty polygons
    // without their shell sequence. Copy the structure directly to preserve binary declarations.
    Geometry copy;
    if (geometry instanceof Point) {
      copy =
          createPoint(
              getCoordinateSequenceFactory().create(((Point) geometry).getCoordinateSequence()));
    } else if (geometry instanceof LinearRing) {
      copy =
          createLinearRing(
              getCoordinateSequenceFactory()
                  .create(((LinearRing) geometry).getCoordinateSequence()));
    } else if (geometry instanceof LineString) {
      copy =
          createLineString(
              getCoordinateSequenceFactory()
                  .create(((LineString) geometry).getCoordinateSequence()));
    } else if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
      for (int i = 0; i < holes.length; i++)
        holes[i] = (LinearRing) createGeometry(polygon.getInteriorRingN(i));
      copy = createPolygon((LinearRing) createGeometry(polygon.getExteriorRing()), holes);
    } else if (geometry instanceof MultiPoint) {
      Point[] points = new Point[geometry.getNumGeometries()];
      for (int i = 0; i < points.length; i++)
        points[i] = (Point) createGeometry(geometry.getGeometryN(i));
      copy = createMultiPoint(points);
    } else if (geometry instanceof MultiLineString) {
      LineString[] lines = new LineString[geometry.getNumGeometries()];
      for (int i = 0; i < lines.length; i++)
        lines[i] = (LineString) createGeometry(geometry.getGeometryN(i));
      copy = createMultiLineString(lines);
    } else if (geometry instanceof MultiPolygon) {
      Polygon[] polygons = new Polygon[geometry.getNumGeometries()];
      for (int i = 0; i < polygons.length; i++)
        polygons[i] = (Polygon) createGeometry(geometry.getGeometryN(i));
      copy = createMultiPolygon(polygons);
    } else if (geometry instanceof GeometryCollection) {
      Geometry[] children = new Geometry[geometry.getNumGeometries()];
      for (int i = 0; i < children.length; i++)
        children[i] = createGeometry(geometry.getGeometryN(i));
      copy = createGeometryCollection(children);
    } else {
      return super.createGeometry(geometry);
    }
    return copy;
  }
}
