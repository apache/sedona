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
  public void numGeometries_point() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertEquals(1, Functions.numGeometries(g));
  }

  @Test
  public void numGeometries_polygon() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertEquals(1, Functions.numGeometries(g));
  }

  @Test
  public void numGeometries_multipoint() throws ParseException {
    Geography g = Constructors.geogFromWKT("MULTIPOINT ((0 0), (1 1), (2 2))", 4326);
    assertEquals(3, Functions.numGeometries(g));
  }

  @Test
  public void numGeometries_multipolygon() throws ParseException {
    Geography g =
        Constructors.geogFromWKT(
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))", 4326);
    assertEquals(2, Functions.numGeometries(g));
  }

  @Test
  public void numGeometries_nullHandling() {
    assertEquals(0, Functions.numGeometries(null));
  }

  @Test
  public void geometryType_point() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertEquals("ST_Point", Functions.geometryType(g));
  }

  @Test
  public void geometryType_linestring() throws ParseException {
    Geography g = Constructors.geogFromWKT("LINESTRING (0 0, 1 1, 2 2)", 4326);
    assertEquals("ST_LineString", Functions.geometryType(g));
  }

  @Test
  public void geometryType_polygon() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertEquals("ST_Polygon", Functions.geometryType(g));
  }

  @Test
  public void geometryType_multipoint() throws ParseException {
    Geography g = Constructors.geogFromWKT("MULTIPOINT ((0 0), (1 1))", 4326);
    assertEquals("ST_MultiPoint", Functions.geometryType(g));
  }

  @Test
  public void geometryType_nullHandling() {
    assertNull(Functions.geometryType(null));
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

  // ─── Level 2: ST_Length, ST_Distance ─────────────────────────────────────

  @Test
  public void length_equatorDegree() throws ParseException {
    Geography g = Constructors.geogFromWKT("LINESTRING (0 0, 1 0)", 4326);
    double len = Functions.length(g);
    // Sphere of radius 6371008 m: 1° along a great circle is ~111,195 m.
    assertEquals(111195.10, len, 1.0);
  }

  @Test
  public void length_meridianDegree() throws ParseException {
    Geography g = Constructors.geogFromWKT("LINESTRING (0 0, 0 1)", 4326);
    double len = Functions.length(g);
    // Meridians are great circles on a sphere — same length as the equator degree.
    assertEquals(111195.10, len, 1.0);
  }

  @Test
  public void length_point_returnsZero() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertEquals(0.0, Functions.length(g), 0.0);
  }

  @Test
  public void length_polygon_returnsZero() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertEquals(0.0, Functions.length(g), 0.0);
  }

  @Test
  public void length_multilinestring_sumsChildren() throws ParseException {
    Geography g = Constructors.geogFromWKT("MULTILINESTRING ((0 0, 1 0), (5 0, 6 0))", 4326);
    double len = Functions.length(g);
    // Two disjoint 1° equatorial arcs → 2 * (R * 1° in radians) ≈ 222,390 m.
    assertEquals(2 * 111195.10, len, 2.0);
  }

  @Test
  public void length_nullHandling() {
    assertEquals(0.0, Functions.length(null), 0.0);
  }

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
}
