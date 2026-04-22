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
import org.locationtech.jts.io.ParseException;

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
  public void intersects_overlappingPolygons() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", 4326);
    Geography g2 = Constructors.geogFromWKT("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", 4326);
    assertTrue(Functions.intersects(g1, g2));
  }

  @Test
  public void intersects_disjointPolygons() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography g2 = Constructors.geogFromWKT("POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))", 4326);
    assertFalse(Functions.intersects(g1, g2));
  }

  @Test
  public void intersects_pointInPolygon() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertTrue(Functions.intersects(g1, g2));
  }

  @Test
  public void intersects_nullHandling() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.intersects(g, null));
    assertFalse(Functions.intersects(null, g));
    assertFalse(Functions.intersects(null, null));
  }

  @Test
  public void contains_nullHandling() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.contains(g1, null));
    assertFalse(Functions.contains(null, g1));
  }
}
