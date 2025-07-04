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
package org.apache.sedona.common.S2Geography;

import com.google.common.geometry.*;
import com.google.common.geometry.Projection;
import java.util.ArrayList;
import java.util.List;
import org.apache.sedona.common.S2Geography.Accessors.*;

public class Predicates {

  public boolean S2_intersects(
      ShapeIndexGeography geo1, ShapeIndexGeography geo2, S2BooleanOperation.Options options) {
    return S2BooleanOperation.intersects(geo1.shapeIndex, geo2.shapeIndex, options);
  }

  public boolean S2_equals(
      ShapeIndexGeography geo1, ShapeIndexGeography geo2, S2BooleanOperation.Options options) {
    return S2BooleanOperation.equals(geo1.shapeIndex, geo2.shapeIndex, options);
  }

  public boolean S2_contains(
      ShapeIndexGeography geo1, ShapeIndexGeography geo2, S2BooleanOperation.Options options) {
    if (new Accessors().S2_isEmpty(geo2)) return false;
    return S2BooleanOperation.contains(geo1.shapeIndex, geo2.shapeIndex, options);
  }

  public boolean S2_intersectsBox(
      ShapeIndexGeography geo1,
      S2LatLngRect rect,
      S2BooleanOperation.Options options,
      double tolerance) {
    // 1) Set up a PlateCarreeProjection and tessellator
    Projection.PlateCarreeProjection projection = new Projection.PlateCarreeProjection(180);
    S2EdgeTessellator tessellator = new S2EdgeTessellator(projection, S1Angle.degrees(tolerance));

    // 2) Compute the four corners in R2Vector
    R2Vector lb = new R2Vector(rect.lngLo().degrees(), rect.latLo().degrees());
    R2Vector rb = new R2Vector(rect.lngHi().degrees(), rect.latLo().degrees());
    R2Vector rt = new R2Vector(rect.lngHi().degrees(), rect.latHi().degrees());
    R2Vector lt = new R2Vector(rect.lngLo().degrees(), rect.latHi().degrees());

    // 3) Walk the four edges (bottom, right, top, left)
    List<S2Point> vertices = new ArrayList<>();
    tessellator.appendUnprojected(lb, rb, vertices);
    tessellator.appendUnprojected(rb, rt, vertices);
    tessellator.appendUnprojected(rt, lt, vertices);
    tessellator.appendUnprojected(lt, lb, vertices);

    // c++ using S2LaxLoopShape which is missing in Java
    // S2LaxLoopShape represents a closed loop of edges surrounding an interior
    // region.  It is similar to S2Loop::Shape except that this class allows
    // duplicate vertices and edges.  Loops may have any number of vertices,
    // including 0, 1, or 2.  (A one-vertex loop defines a degenerate edge
    // consisting of a single point.)
    S2Loop loop = new S2Loop(vertices);
    S2Polygon polygon = new S2Polygon(loop);
    PolygonGeography polygonGeography = new PolygonGeography(polygon);
    // c++ is using MutableS2ShapeIndex which is missing in Java
    // MutableS2ShapeIndex is a class for in-memory indexing of polygonal geometry.
    // The objects in the index are known as "shapes", and may consist of points,
    // polylines, and/or polygons, possibly overlapping.  The index makes it very
    // fast to answer queries such as finding nearby shapes, measuring distances,
    // testing for intersection and containment, etc.  It is one of several
    // implementations of the S2ShapeIndex interface (see EncodedS2ShapeIndex).
    GeographyIndex geographyIndex = new GeographyIndex();
    geographyIndex.add(polygonGeography, 0);
    return S2BooleanOperation.intersects(geo1.shapeIndex, geographyIndex.getShapeIndex(), options);
  }
}
