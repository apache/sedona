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
package org.apache.sedona.common.Geography;

import static org.junit.Assert.*;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.PolygonGeography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class FunctionTest {
  private static final double EPS = 1e-9;

  private static void assertDegAlmostEqual(double a, double b) {
    assertTrue("exp=" + b + ", got=" + a, Math.abs(a - b) <= EPS);
  }

  private static void assertLatLng(S2Point p, double expLatDeg, double expLngDeg) {
    S2LatLng ll = new S2LatLng(p).normalized();
    assertDegAlmostEqual(ll.latDegrees(), expLatDeg);
    assertDegAlmostEqual(ll.lngDegrees(), expLngDeg);
  }

  private static void assertRectLoopVertices(
      S2Loop loop, double latLo, double lngLo, double latHi, double lngHi) {
    assertEquals("rect must have 4 vertices", 4, loop.numVertices());
    assertLatLng(loop.vertex(0), latLo, lngLo);
    assertLatLng(loop.vertex(1), latLo, lngHi);
    assertLatLng(loop.vertex(2), latHi, lngHi);
    assertLatLng(loop.vertex(3), latHi, lngLo);
  }

  // ─── Envelope tests (pre-existing) ───────────────────────────────────────

  @Test
  public void envelope_noSplit_antimeridian() throws Exception {
    String wkt = "MULTIPOINT ((-179 0), (179 1), (-180 10))";
    Geography g = Constructors.geogFromWKT(wkt, 4326);
    PolygonGeography env = (PolygonGeography) Functions.getEnvelope(g, false);

    S2LatLngRect r = g.region().getRectBound();
    assertTrue(r.lng().isInverted());
    assertDegAlmostEqual(r.latLo().degrees(), 0.0);
    assertDegAlmostEqual(r.latHi().degrees(), 10.0);
    assertDegAlmostEqual(r.lngLo().degrees(), 179.0);
    assertDegAlmostEqual(r.lngHi().degrees(), -179.0);

    S2Loop loop = env.polygon.getLoops().get(0);
    assertRectLoopVertices(loop, 0, 179, 10, -179);
  }

  @Test
  public void envelope_netherlands_perVertex() throws Exception {
    String nl =
        "POLYGON ((3.314971 50.80372, 7.092053 50.80372, 7.092053 53.5104, 3.314971 53.5104, 3.314971 50.80372))";
    Geography g = Constructors.geogFromWKT(nl, 4326);
    Geography env = Functions.getEnvelope(g, true);
    String expectedWKT = "POLYGON ((3.3 50.8, 7.1 50.8, 7.1 53.5, 3.3 53.5, 3.3 50.8))";
    assertEquals(expectedWKT, env.toString());
    assertEquals(4326, env.getSRID());
  }

  @Test
  public void envelope_fiji_split_perVertex() throws Exception {
    String fiji =
        "MULTIPOLYGON ("
            + "((177.285 -18.28799, 180 -18.28799, 180 -16.02088, 177.285 -16.02088, 177.285 -18.28799)),"
            + "((-180 -18.28799, -179.7933 -18.28799, -179.7933 -16.02088, -180 -16.02088, -180 -18.28799))"
            + ")";
    Geography g = Constructors.geogFromWKT(fiji, 4326);
    Geography env = Functions.getEnvelope(g, true);
    String expectedWKT =
        "MULTIPOLYGON (((177.3 -18.3, 180 -18.3, 180 -16, 177.3 -16, 177.3 -18.3)), "
            + "((-180 -18.3, -179.8 -18.3, -179.8 -16, -180 -16, -180 -18.3)))";
    assertEquals(expectedWKT, env.toString());

    String expectedWKT2 =
        "POLYGON ((177.3 -18.3, -179.8 -18.3, -179.8 -16, 177.3 -16, 177.3 -18.3))";
    env = Functions.getEnvelope(g, false);
    assertEquals(expectedWKT2, env.toString());
  }

  @Test
  public void getEnvelopePoint() throws ParseException {
    String wkt = "POINT (-180 10)";
    Geography geography = Constructors.geogFromWKT(wkt, 0);
    Geography envelope = Functions.getEnvelope(geography, false);
    assertEquals("POINT (180 10)", envelope.toString());
  }

  @Test
  public void testEnvelopeWKTCompare() throws Exception {
    String antarctica = "POLYGON ((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90))";
    Geography g = Constructors.geogFromWKT(antarctica, 4326);
    Geography env = Functions.getEnvelope(g, true);

    String expectedWKT = "POLYGON ((-180 -63.3, 180 -63.3, 180 -90, -180 -90, -180 -63.3))";
    assertEquals((expectedWKT), (env.toString()));

    String multiCountry =
        "MULTIPOLYGON (((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90)),"
            + "((3.314971 50.80372, 7.092053 50.80372, 7.092053 53.5104, 3.314971 53.5104, 3.314971 50.80372)))";
    g = Constructors.geogFromWKT(multiCountry, 4326);
    env = Functions.getEnvelope(g, true);

    String expectedWKT2 = "POLYGON ((-180 53.5, 180 53.5, 180 -90, -180 -90, -180 53.5))";
    assertEquals((expectedWKT2), (env.toString()));
  }

  // ─── Level 1: ST_NPoints ─────────────────────────────────────────────────

  @Test
  public void nPoints_linestring() throws ParseException {
    Geography g = Constructors.geogFromWKT("LINESTRING (0 0, 1 1, 2 2)", 4326);
    assertEquals(3, Functions.nPoints(g));
  }

  @Test
  public void nPoints_polygon() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertEquals(5, Functions.nPoints(g));
  }

  @Test
  public void asText_point() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 2)", 4326);
    String wkt = Functions.asText(g);
    assertNotNull(wkt);
    Point p = (Point) new WKTReader().read(wkt);
    // S2 round-trip may introduce sub-nanometer floating-point drift; use a loose tolerance.
    assertEquals(1.0, p.getX(), 1e-9);
    assertEquals(2.0, p.getY(), 1e-9);
  }

  @Test
  public void asText_polygon() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    String wkt = Functions.asText(g);
    assertNotNull(wkt);
    Polygon poly = (Polygon) new WKTReader().read(wkt);
    Coordinate[] ring = poly.getExteriorRing().getCoordinates();
    assertEquals(5, ring.length);
    double[][] expected = {{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}};
    for (int i = 0; i < expected.length; i++) {
      assertEquals("ring[" + i + "].x", expected[i][0], ring[i].x, 1e-9);
      assertEquals("ring[" + i + "].y", expected[i][1], ring[i].y, 1e-9);
    }
  }

  @Test
  public void asText_nullHandling() {
    assertNull(Functions.asText(null));
  }

  // ─── Level 2: ST_Distance ────────────────────────────────────────────────

  @Test
  public void distance_twoPoints() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (1 1)", 4326);

    Double result = Functions.distance(g1, g2);
    assertNotNull(result);
    // S2 geometry-to-geometry distance ~157 km (spherical model)
    assertTrue("Distance should be ~157 km, got " + result, result > 155000 && result < 160000);
  }

  @Test
  public void distance_nullHandling() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    assertNull(Functions.distance(g1, null));
    assertNull(Functions.distance(null, g1));
    assertNull(Functions.distance(null, null));
  }

  // ─── Level 3: ST_Contains ────────────────────────────────────────────────

  @Test
  public void contains_pointInPolygon() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (0.5 0.5)", 4326);
    assertTrue(Functions.contains(g1, g2));
  }

  @Test
  public void contains_pointOutsidePolygon() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (2 2)", 4326);
    assertFalse(Functions.contains(g1, g2));
  }

  @Test
  public void contains_nullHandling() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.contains(g1, null));
    assertFalse(Functions.contains(null, g1));
  }

  // ─── Level 3: ST_DWithin ─────────────────────────────────────────────────

  @Test
  public void dWithin_twoPointsOneDegreeApart() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (0 1)", 4326);
    // 1° of latitude ≈ 111_195 m on the sphere
    assertFalse(Functions.dWithin(g1, g2, 100_000.0));
    assertTrue(Functions.dWithin(g1, g2, 200_000.0));
  }

  @Test
  public void dWithin_pointInsidePolygon() throws ParseException {
    Geography poly = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography pt = Constructors.geogFromWKT("POINT (0.5 0.5)", 4326);
    // Distance is zero when one contains the other; any positive threshold should pass.
    assertTrue(Functions.dWithin(poly, pt, 1.0));
  }

  @Test
  public void dWithin_boundaryInclusive() throws ParseException {
    // sedona-db parity: distance == threshold ⇒ true (inclusive <=)
    Geography g1 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (0 1)", 4326);
    double actual = Functions.distance(g1, g2);
    assertTrue(Functions.dWithin(g1, g2, actual));
    assertFalse(Functions.dWithin(g1, g2, actual - 1.0));
  }

  @Test
  public void dWithin_antimeridianCrossing() throws ParseException {
    // Two points straddling the antimeridian: great-circle distance ~22 km,
    // planar distance ~40_000 km — succeeding at 50 km proves we use spherical distance.
    Geography g1 = Constructors.geogFromWKT("POINT (179.9 0)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (-179.9 0)", 4326);
    assertTrue(Functions.dWithin(g1, g2, 50_000.0));
  }

  @Test
  public void dWithin_nullHandling() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (0 0)", 4326);
    assertFalse(Functions.dWithin(g, null, 1e6));
    assertFalse(Functions.dWithin(null, g, 1e6));
    assertFalse(Functions.dWithin(null, null, 1e6));
  }

  @Test
  public void dWithin_reflexiveZeroThreshold() throws ParseException {
    // A point is trivially within distance 0 of itself (distance == 0, threshold == 0, <= is
    // inclusive).
    Geography g = Constructors.geogFromWKT("POINT (10 20)", 4326);
    assertTrue(Functions.dWithin(g, g, 0.0));
  }

  @Test
  public void dWithin_negativeDistance() throws ParseException {
    // No two geographies can be at a negative geodesic distance, so any negative threshold =>
    // false.
    Geography g1 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    assertFalse(Functions.dWithin(g1, g2, -1.0));
  }

  @Test
  public void dWithin_nanDistance() throws ParseException {
    // NaN threshold => all comparisons are false.
    Geography g1 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (0 1)", 4326);
    assertFalse(Functions.dWithin(g1, g2, Double.NaN));
  }

  // ─── Level 3: ST_Within ──────────────────────────────────────────────────

  @Test
  public void within_pointInPolygon() throws ParseException {
    Geography pt = Constructors.geogFromWKT("POINT (0.5 0.5)", 4326);
    Geography poly = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertTrue(Functions.within(pt, poly));
  }

  @Test
  public void within_pointOutsidePolygon() throws ParseException {
    Geography pt = Constructors.geogFromWKT("POINT (2 2)", 4326);
    Geography poly = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertFalse(Functions.within(pt, poly));
  }

  @Test
  public void within_isContainsSwapped() throws ParseException {
    // OGC parity: within(A, B) == contains(B, A) for every input pair.
    Geography poly = Constructors.geogFromWKT("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", 4326);
    Geography inside = Constructors.geogFromWKT("POINT (1 1)", 4326);
    Geography outside = Constructors.geogFromWKT("POINT (3 3)", 4326);
    assertEquals(Functions.contains(poly, inside), Functions.within(inside, poly));
    assertEquals(Functions.contains(poly, outside), Functions.within(outside, poly));
  }

  @Test
  public void within_nullHandling() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.within(g, null));
    assertFalse(Functions.within(null, g));
    assertFalse(Functions.within(null, null));
  }

  @Test
  public void within_polygonInPolygon() throws ParseException {
    Geography inner = Constructors.geogFromWKT("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))", 4326);
    Geography outer = Constructors.geogFromWKT("POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))", 4326);
    assertTrue(Functions.within(inner, outer));
    // Swapped: the outer polygon is NOT within the inner one.
    assertFalse(Functions.within(outer, inner));
  }

  @Test
  public void within_overlappingNotContained() throws ParseException {
    // Two polygons that intersect but neither is contained in the other.
    Geography a = Constructors.geogFromWKT("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", 4326);
    Geography b = Constructors.geogFromWKT("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", 4326);
    assertFalse(Functions.within(a, b));
    assertFalse(Functions.within(b, a));
  }
}
