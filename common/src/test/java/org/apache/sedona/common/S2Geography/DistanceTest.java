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

import static org.junit.Assert.assertEquals;

import com.google.common.geometry.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class DistanceTest {
  private static final double TOL = 1e-6;

  @Test
  public void pointDistance() throws Exception {
    // 1) Build two points
    Geography pt1 = new PointGeography(S2LatLng.fromDegrees(0, 0).toPoint());
    Geography pt2 = new PointGeography(S2LatLng.fromDegrees(90, 0).toPoint());
    ShapeIndexGeography geo1 = new ShapeIndexGeography(pt1);
    ShapeIndexGeography geo2 = new ShapeIndexGeography(pt2);

    Distance distance = new Distance();
    double res = distance.S2_distance(geo1, geo2);
    assertEquals(Math.PI / 2, res, 1e-9);
  }

  @Test
  public void furtestDistance() throws Exception {
    // 1) Build two points
    Geography pt1 = new PointGeography(S2LatLng.fromDegrees(0, 0).toPoint());
    Geography pt2 = new PointGeography(S2LatLng.fromDegrees(90, 0).toPoint());
    ShapeIndexGeography geo1 = new ShapeIndexGeography(pt1);
    ShapeIndexGeography geo2 = new ShapeIndexGeography(pt2);

    Distance distance = new Distance();
    double res = distance.S2_maxDistance(geo1, geo2);
    assertEquals(Math.PI / 2, res, 1e-9);
  }

  @Test
  public void testMinimumClearanceLineBetween() throws Exception {
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point pt_mid = S2LatLng.fromDegrees(45, 0).toPoint();
    S2Point pt_end = S2LatLng.fromDegrees(0, 0).toPoint();
    // Build a single polygon and wrap in geography
    List<S2Point> points = new ArrayList<>();
    points.add(pt);
    points.add(pt_mid);
    points.add(pt_end);
    points.add(pt);
    S2Loop polyline = new S2Loop(points);
    S2Polygon poly = new S2Polygon(polyline);
    PolygonGeography geo = new PolygonGeography(poly);

    S2Point geo2 = S2LatLng.fromDegrees(45, 0).toPoint();
    PointGeography geoPoint = new PointGeography(geo2);

    ShapeIndexGeography geog1 = new ShapeIndexGeography();
    geog1.addIndex(geo);
    ShapeIndexGeography geog2 = new ShapeIndexGeography();
    geog2.addIndex(geoPoint);

    Distance distance = new Distance();
    Pair<S2Point, S2Point> result = distance.S2_minimumClearanceLineBetween(geog1, geog2);

    // Assert: both endpoints should lie at unit‐sphere direction (0.707107, 0, 0.707107)
    S2Point expected = new S2Point(0.707107, 0.0, 0.707107);
    S2Point actualA = result.getLeft();
    S2Point actualB = result.getRight();
    assertEquals(expected.getX(), actualA.getX(), TOL);
    assertEquals(expected.getY(), actualA.getY(), TOL);
    assertEquals(expected.getZ(), actualA.getZ(), TOL);

    assertEquals(expected.getX(), actualB.getX(), TOL);
    assertEquals(expected.getY(), actualB.getY(), TOL);
    assertEquals(expected.getZ(), actualB.getZ(), TOL);
  }
}
