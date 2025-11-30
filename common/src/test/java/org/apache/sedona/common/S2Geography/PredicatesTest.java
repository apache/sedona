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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.geometry.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class PredicatesTest {
  @Test
  public void testEquals() {
    // two identical points always intersect
    S2Point p = S2LatLng.fromDegrees(10, 20).toPoint();
    ShapeIndexGeography geo1 = new ShapeIndexGeography(new PointGeography(p));
    ShapeIndexGeography geo2 = new ShapeIndexGeography(new PointGeography(p));
    Predicates pred = new Predicates();
    assertTrue(
        "Point should intersects itself",
        pred.S2_intersects(geo1, geo2, new S2BooleanOperation.Options()));
  }

  @Test
  public void testPointPoint() {
    S2Point p = S2LatLng.fromDegrees(10, 20).toPoint();
    ShapeIndexGeography geo1 = new ShapeIndexGeography(new PointGeography(p));
    ShapeIndexGeography geo2 = new ShapeIndexGeography(new PointGeography(p));
    assertTrue(
        "Two identical points should be equal",
        new Predicates().S2_equals(geo1, geo2, new S2BooleanOperation.Options()));

    // slightly different point → not equal
    S2Point q = S2LatLng.fromDegrees(10.0001, 20).toPoint();
    ShapeIndexGeography geo3 = new ShapeIndexGeography(new PointGeography(q));
    assertFalse(
        "Points far enough apart should not be equal",
        new Predicates().S2_equals(geo1, geo3, new S2BooleanOperation.Options()));
  }

  @Test
  public void testS2Contains_PolygonPoint() {
    // build a simple square polygon: corners (0,0), (0,1), (1,1), (1,0)
    Predicates pred = new Predicates();
    List<S2Point> verts = new ArrayList<>();
    verts.add(S2LatLng.fromDegrees(0, 0).toPoint());
    verts.add(S2LatLng.fromDegrees(0, 1).toPoint());
    verts.add(S2LatLng.fromDegrees(1, 1).toPoint());
    verts.add(S2LatLng.fromDegrees(1, 0).toPoint());
    S2Loop loop = new S2Loop(verts);
    S2Polygon square = new S2Polygon(loop);
    ShapeIndexGeography container = new ShapeIndexGeography(new PolygonGeography(square));

    // point inside
    S2Point inside = S2LatLng.fromDegrees(0.5, 0.5).toPoint();
    ShapeIndexGeography ptInside = new ShapeIndexGeography(new PointGeography(inside));
    assertTrue(
        "Square should contain its interior point",
        pred.S2_contains(container, ptInside, new S2BooleanOperation.Options()));

    // point outside
    S2Point outside = S2LatLng.fromDegrees(2, 2).toPoint();
    ShapeIndexGeography ptOutside = new ShapeIndexGeography(new PointGeography(outside));
    assertFalse(
        "Square should not contain a point outside",
        pred.S2_contains(container, ptOutside, new S2BooleanOperation.Options()));
  }

  @Test
  public void testS2IntersectsBox_PolygonRect() {
    Predicates pred = new Predicates();
    // reuse same square polygon
    List<S2Point> verts = new ArrayList<>();
    verts.add(S2LatLng.fromDegrees(0, 0).toPoint());
    verts.add(S2LatLng.fromDegrees(0, 1).toPoint());
    verts.add(S2LatLng.fromDegrees(1, 1).toPoint());
    verts.add(S2LatLng.fromDegrees(1, 0).toPoint());
    S2Loop loop = new S2Loop(verts);
    S2Polygon square = new S2Polygon(loop);
    ShapeIndexGeography container = new ShapeIndexGeography(new PolygonGeography(square));

    // rectangle that intersects the square: from lat 0.5–1.5, lon 0.5–1.5
    S2LatLngRect hitRect =
        S2LatLngRect.fromPointPair(S2LatLng.fromDegrees(0.5, 0.5), S2LatLng.fromDegrees(1.5, 1.5));
    boolean doesHit =
        pred.S2_intersectsBox(
            container, hitRect, new S2BooleanOperation.Options(), /* tolerance= */ 0.1);
    assertTrue("Rect overlapping the square should intersect", doesHit);

    // rectangle completely outside: lat 2–3, lon 2–3
    S2LatLngRect missRect =
        S2LatLngRect.fromPointPair(S2LatLng.fromDegrees(2, 2), S2LatLng.fromDegrees(3, 3));
    boolean doesMiss =
        pred.S2_intersectsBox(
            container, missRect, new S2BooleanOperation.Options(), /* tolerance= */ 0.1);
    assertFalse("Rect far away from square should not intersect", doesMiss);
  }
}
