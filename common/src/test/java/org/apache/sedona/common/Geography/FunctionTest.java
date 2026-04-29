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

  // S2 area-weighted centroids on small polygons differ from planar by an O(d^2/R^2)
  // spherical correction. 5e-3 deg (~500 m) is wide enough to absorb that drift on
  // 1°-scale shapes near origin, while still catching any real bug (a planar/JTS centroid
  // of an antimeridian polygon would be off by ~180°, not by hundreds of metres).
  private static final double CENTROID_TOL_DEG = 5e-3;

  @Test
  public void centroid_squarePolygon() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", 4326);
    Geography c = Functions.centroid(g);
    assertNotNull(c);
    assertEquals(4326, c.getSRID());
    org.locationtech.jts.geom.Point p =
        (org.locationtech.jts.geom.Point) Constructors.geogToGeometry(c);
    assertEquals(1.0, p.getX(), CENTROID_TOL_DEG);
    assertEquals(1.0, p.getY(), CENTROID_TOL_DEG);
  }

  @Test
  public void centroid_linestring() throws ParseException {
    Geography g = Constructors.geogFromWKT("LINESTRING (0 0, 2 0)", 4326);
    Geography c = Functions.centroid(g);
    assertNotNull(c);
    org.locationtech.jts.geom.Point p =
        (org.locationtech.jts.geom.Point) Constructors.geogToGeometry(c);
    assertEquals(1.0, p.getX(), CENTROID_TOL_DEG);
    assertEquals(0.0, p.getY(), CENTROID_TOL_DEG);
  }

  @Test
  public void centroid_point() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (3 4)", 4326);
    Geography c = Functions.centroid(g);
    assertNotNull(c);
    org.locationtech.jts.geom.Point p =
        (org.locationtech.jts.geom.Point) Constructors.geogToGeometry(c);
    // A single point's centroid is the point itself — exact.
    assertEquals(3.0, p.getX(), 1e-9);
    assertEquals(4.0, p.getY(), 1e-9);
  }

  @Test
  public void centroid_multipoint_meanOfUnitVectors() throws ParseException {
    Geography g = Constructors.geogFromWKT("MULTIPOINT ((-1 0), (1 0))", 4326);
    Geography c = Functions.centroid(g);
    assertNotNull(c);
    org.locationtech.jts.geom.Point p =
        (org.locationtech.jts.geom.Point) Constructors.geogToGeometry(c);
    // Mean of unit vectors at (-1, 0) and (1, 0) lands on (0, 0).
    assertEquals(0.0, p.getX(), CENTROID_TOL_DEG);
    assertEquals(0.0, p.getY(), CENTROID_TOL_DEG);
  }

  @Test
  public void centroid_multipolygon() throws ParseException {
    // Two unit squares at (0..1, 0..1) and (10..11, 0..1). Equal area, so the area-weighted
    // centroid should sit at the midpoint between the per-polygon centroids ≈ (5.5, 0.5).
    Geography g =
        Constructors.geogFromWKT(
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((10 0, 11 0, 11 1, 10 1, 10 0)))", 4326);
    Geography c = Functions.centroid(g);
    assertNotNull(c);
    org.locationtech.jts.geom.Point p =
        (org.locationtech.jts.geom.Point) Constructors.geogToGeometry(c);
    assertEquals(5.5, p.getX(), CENTROID_TOL_DEG);
    assertEquals(0.5, p.getY(), CENTROID_TOL_DEG);
  }

  @Test
  public void centroid_antimeridianPolygon_isOnTheAntimeridian() throws ParseException {
    // Thin band straddling 180°E. A planar JTS centroid would average the lons and land
    // at lon ≈ 0 (the wrong side of the planet). The spherical centroid stays near ±180.
    Geography g =
        Constructors.geogFromWKT("POLYGON ((170 -1, -170 -1, -170 1, 170 1, 170 -1))", 4326);
    Geography c = Functions.centroid(g);
    assertNotNull(c);
    org.locationtech.jts.geom.Point p =
        (org.locationtech.jts.geom.Point) Constructors.geogToGeometry(c);
    double lon = p.getX();
    double lat = p.getY();
    // |lon| close to 180 (either side of the wrap), lat close to 0.
    double lonDistFromAntimeridian = Math.min(Math.abs(lon - 180.0), Math.abs(lon + 180.0));
    assertTrue(
        "expected centroid near the antimeridian; got (" + lon + ", " + lat + ")",
        lonDistFromAntimeridian < 0.5);
    assertEquals(0.0, lat, 0.5);
  }

  @Test
  public void centroid_nullHandling() {
    assertNull(Functions.centroid(null));
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

  // ─── Level 2: ST_Length, ST_Area, ST_Distance ────────────────────────────

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
  public void area_unitBoxAtEquator() throws ParseException {
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    double area = Functions.area(g);
    // S2 spherical area of a 1°x1° box near equator on a sphere of radius
    // Haversine.AVG_EARTH_RADIUS = 6371008.0 m. Slightly larger than the WGS84-ellipsoid
    // value (~1.231e10 m²) by the spheroid/sphere correction (~0.5%). Tolerance of 1e7 m²
    // (~0.08%) is well above floating-point drift but tight enough to catch a model swap.
    assertEquals(1.2364e10, area, 1e7);
  }

  @Test
  public void area_rightTriangleAtOrigin() throws ParseException {
    // Right triangle with vertices (0,0), (0,1), (1,0). The polygon is wound clockwise in
    // lat/lon space, which would let a naïve sphere area function return the complementary
    // region (almost the whole Earth, ~5.1e14 m²). Asserting the small-side value (~6.18e9 m²)
    // proves the orientation-collapse branch is doing its job.
    Geography g = Constructors.geogFromWKT("POLYGON ((0 0, 0 1, 1 0, 0 0))", 4326);
    double area = Functions.area(g);
    assertEquals(6.182489e9, area, 1e6);
  }

  @Test
  public void area_multipolygon_sumsChildren() throws ParseException {
    Geography g =
        Constructors.geogFromWKT(
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((10 10, 11 10, 11 11, 10 11, 10 10)))",
            4326);
    double area = Functions.area(g);
    // ~1.236e10 (1°² near equator) + ~1.216e10 (1°² near 10°N). Tolerance 5e7 m² ~ 0.2%.
    assertEquals(2.452e10, area, 5e7);
  }

  @Test
  public void area_point_returnsZero() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertEquals(0.0, Functions.area(g), 0.0);
  }

  @Test
  public void area_linestring_returnsZero() throws ParseException {
    Geography g = Constructors.geogFromWKT("LINESTRING (0 0, 1 1)", 4326);
    assertEquals(0.0, Functions.area(g), 0.0);
  }

  @Test
  public void area_nullHandling() {
    assertEquals(0.0, Functions.area(null), 0.0);
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
  public void equals_samePoint() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertTrue(Functions.equals(g1, g2));
  }

  @Test
  public void equals_differentPoints() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (3 4)", 4326);
    assertFalse(Functions.equals(g1, g2));
  }

  @Test
  public void equals_samePolygon() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography g2 = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    assertTrue(Functions.equals(g1, g2));
  }

  @Test
  public void equals_nullHandling() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.equals(g, null));
    assertFalse(Functions.equals(null, g));
    assertFalse(Functions.equals(null, null));
  }

  @Test
  public void contains_nullHandling() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.contains(g1, null));
    assertFalse(Functions.contains(null, g1));
  }

  // ─── Level 4: ST_Buffer ──────────────────────────────────────────────────

  @Test
  public void buffer_nullInputReturnsNull() throws ParseException {
    assertNull(Functions.buffer(null, 100.0));
    assertNull(Functions.buffer(null, 100.0, "quad_segs=4"));
  }

  @Test
  public void buffer_pointProducesEnclosingPolygon() throws ParseException {
    Geography origin = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography buffered = Functions.buffer(origin, 1000.0); // 1 km on the sphere
    assertNotNull(buffered);
    assertEquals("ST_Polygon", Functions.geometryType(buffered));
    // A point ~785 m NE of origin should fall inside the 1 km buffer.
    Geography near = Constructors.geogFromWKT("POINT (0.005 0.005)", 4326);
    assertTrue(Functions.contains(buffered, near));
    // A point ~1.57 km NE should fall outside.
    Geography far = Constructors.geogFromWKT("POINT (0.01 0.01)", 4326);
    assertFalse(Functions.contains(buffered, far));
  }

  @Test
  public void buffer_polygonContainsOriginalInterior() throws ParseException {
    Geography poly =
        Constructors.geogFromWKT("POLYGON ((0 0, 0.01 0, 0.01 0.01, 0 0.01, 0 0))", 4326);
    Geography buffered = Functions.buffer(poly, 200.0);
    assertNotNull(buffered);
    Geography inside = Constructors.geogFromWKT("POINT (0.005 0.005)", 4326);
    assertTrue("buffered polygon must contain its centroid", Functions.contains(buffered, inside));
    // A point 500 m beyond the original polygon's edge but inside the 200 m band would still
    // be outside; pick a point far enough that the buffer cannot reach it.
    Geography farOutside = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.contains(buffered, farOutside));
  }

  @Test
  public void buffer_parametersStringHonored() throws ParseException {
    // quad_segs=2 produces a low-fidelity buffer (octagon for a point); quad_segs=64
    // produces a much smoother boundary. Vertex counts should differ accordingly.
    Geography origin = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography coarse = Functions.buffer(origin, 1000.0, "quad_segs=2");
    Geography fine = Functions.buffer(origin, 1000.0, "quad_segs=64");
    assertNotNull(coarse);
    assertNotNull(fine);
    assertTrue(
        "fine buffer should have more vertices than coarse",
        Functions.nPoints(fine) > Functions.nPoints(coarse));
  }

  @Test
  public void buffer_negativeRadiusShrinksPolygon() throws ParseException {
    Geography poly =
        Constructors.geogFromWKT("POLYGON ((0 0, 0.01 0, 0.01 0.01, 0 0.01, 0 0))", 4326);
    Geography shrunk = Functions.buffer(poly, -100.0);
    assertNotNull(shrunk);
    // Shrunk polygon is either smaller or empty; the original boundary point should now
    // be outside (or contains() returns false on an empty geometry, which is also acceptable).
    Geography boundary = Constructors.geogFromWKT("POINT (0 0)", 4326);
    assertFalse(Functions.contains(shrunk, boundary));
  }
}
