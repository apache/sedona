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
import com.google.common.geometry.S2Polyline;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.GeographyWKBSerializer;
import org.apache.sedona.common.S2Geography.PolygonGeography;
import org.apache.sedona.common.S2Geography.SinglePolylineGeography;
import org.apache.sedona.common.S2Geography.WKBGeography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
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

  private static Geography roundTripWKB(Geography geography) {
    assertTrue(geography instanceof WKBGeography);
    WKBGeography wkbGeography = (WKBGeography) geography;
    return WKBGeography.fromWKB(wkbGeography.getWKBBytes(), geography.getSRID());
  }

  private static Geography geographyFromJTSWKT(String wkt, int srid) throws ParseException {
    Geometry geometry = new WKTReader().read(wkt);
    geometry.setSRID(srid);
    return WKBGeography.fromJTS(geometry);
  }

  private static Geography geographyFromIsoPointWKB(
      int type, double longitude, double latitude, double thirdOrdinate, int srid) {
    java.nio.ByteBuffer buffer =
        java.nio.ByteBuffer.allocate(1 + Integer.BYTES + 3 * Double.BYTES)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.put((byte) 1);
    buffer.putInt(type);
    buffer.putDouble(longitude);
    buffer.putDouble(latitude);
    buffer.putDouble(thirdOrdinate);
    return WKBGeography.fromWKB(buffer.array(), srid);
  }

  private static Geography geographyFromIsoLineStringZWKB(double[] longitudeLatitudeZ, int srid) {
    int numPoints = longitudeLatitudeZ.length / 3;
    java.nio.ByteBuffer buffer =
        java.nio.ByteBuffer.allocate(1 + 2 * Integer.BYTES + numPoints * 3 * Double.BYTES)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.put((byte) 1);
    buffer.putInt(1002);
    buffer.putInt(numPoints);
    for (double ordinate : longitudeLatitudeZ) buffer.putDouble(ordinate);
    return WKBGeography.fromWKB(buffer.array(), srid);
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
    Geography env = Functions.getEnvelope(g, false);
    assertTrue(env instanceof WKBGeography);

    S2LatLngRect r = g.region().getRectBound();
    assertTrue(r.lng().isInverted());
    assertDegAlmostEqual(r.latLo().degrees(), 0.0);
    assertDegAlmostEqual(r.latHi().degrees(), 10.0);
    assertDegAlmostEqual(r.lngLo().degrees(), 179.0);
    assertDegAlmostEqual(r.lngHi().degrees(), -179.0);

    PolygonGeography s2Envelope = (PolygonGeography) ((WKBGeography) env).getS2Geography();
    S2Loop loop = s2Envelope.polygon.getLoops().get(0);
    assertRectLoopVertices(loop, 0, 179, 10, -179);
  }

  @Test
  public void envelope_netherlands_perVertex() throws Exception {
    String nl =
        "POLYGON ((3.314971 50.80372, 7.092053 50.80372, 7.092053 53.5104, 3.314971 53.5104, 3.314971 50.80372))";
    Geography g = Constructors.geogFromWKT(nl, 4326);
    Geography env = Functions.getEnvelope(g, true);
    String expectedWKT = "POLYGON ((3.3 50.8, 7.1 50.8, 7.1 53.5, 3.3 53.5, 3.3 50.8))";
    assertEquals(expectedWKT, env.toString(new PrecisionModel(PrecisionModel.FIXED)));
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
    assertEquals(expectedWKT, env.toString(new PrecisionModel(PrecisionModel.FIXED)));

    String expectedWKT2 =
        "POLYGON ((177.3 -18.3, -179.8 -18.3, -179.8 -16, 177.3 -16, 177.3 -18.3))";
    env = Functions.getEnvelope(g, false);
    assertEquals(expectedWKT2, env.toString(new PrecisionModel(PrecisionModel.FIXED)));
  }

  @Test
  public void getEnvelopePoint() throws ParseException {
    String wkt = "POINT (-180 10)";
    Geography geography = Constructors.geogFromWKT(wkt, 0);
    Geography envelope = Functions.getEnvelope(geography, false);
    assertEquals("POINT (-180 10)", envelope.toString());
  }

  @Test
  public void envelope_preservesExactEndpointBoundsAcrossWKBRoundTrip() throws ParseException {
    Geography line = Constructors.geogFromWKT("LINESTRING (0 0, 2 3)", 4326);
    Geography envelope = Functions.getEnvelope(line, false);
    assertEquals("SRID=4326; POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))", envelope.toEWKT());
    assertEquals(envelope.toEWKT(), roundTripWKB(envelope).toEWKT());

    Geography antiMeridianLine = Constructors.geogFromWKT("LINESTRING (170 10, -170 20)", 4326);
    Geography splitEnvelope = Functions.getEnvelope(antiMeridianLine, true);
    assertEquals(
        "SRID=4326; MULTIPOLYGON (((170 10, 180 10, 180 20, 170 20, 170 10)), "
            + "((-180 10, -170 10, -170 20, -180 20, -180 10)))",
        splitEnvelope.toEWKT());
    assertEquals(splitEnvelope.toEWKT(), roundTripWKB(splitEnvelope).toEWKT());
  }

  @Test
  public void getEnvelopeEmptyGeography() throws ParseException {
    for (String wkt :
        new String[] {
          "POINT EMPTY",
          "LINESTRING EMPTY",
          "POLYGON EMPTY",
          "MULTIPOINT EMPTY",
          "MULTILINESTRING EMPTY",
          "MULTIPOLYGON EMPTY",
          "GEOMETRYCOLLECTION EMPTY"
        }) {
      Geography geography = Constructors.geogFromWKT(wkt, 3857);
      for (boolean splitAtAntiMeridian : new boolean[] {false, true}) {
        Geography envelope = Functions.getEnvelope(geography, splitAtAntiMeridian);
        assertEquals(wkt, envelope.toString());
        assertEquals(3857, envelope.getSRID());
      }
    }
  }

  @Test
  public void testEnvelopeWKTCompare() throws Exception {
    String antarctica = "POLYGON ((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90))";
    Geography g = Constructors.geogFromWKT(antarctica, 4326);
    Geography env = Functions.getEnvelope(g, true);

    String expectedWKT = "POLYGON ((-180 -63.3, 180 -63.3, 180 -90, -180 -90, -180 -63.3))";
    assertEquals(expectedWKT, env.toString(new PrecisionModel(PrecisionModel.FIXED)));

    String multiCountry =
        "MULTIPOLYGON (((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90)),"
            + "((3.314971 50.80372, 7.092053 50.80372, 7.092053 53.5104, 3.314971 53.5104, 3.314971 50.80372)))";
    g = Constructors.geogFromWKT(multiCountry, 4326);
    env = Functions.getEnvelope(g, true);

    String expectedWKT2 = "POLYGON ((-180 53.5, 180 53.5, 180 -90, -180 -90, -180 53.5))";
    assertEquals(expectedWKT2, env.toString(new PrecisionModel(PrecisionModel.FIXED)));
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
  public void xY_point() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (-73.9857 40.7484)", 4326);
    assertEquals(-73.9857, Functions.x(g), 1e-12);
    assertEquals(40.7484, Functions.y(g), 1e-12);
  }

  @Test
  public void xY_nonPointAndNull() throws ParseException {
    Geography line = Constructors.geogFromWKT("LINESTRING (0 0, 1 1)", 4326);
    Geography emptyPoint = Constructors.geogFromWKT("POINT EMPTY", 4326);
    assertNull(Functions.x(line));
    assertNull(Functions.y(line));
    assertNull(Functions.x(emptyPoint));
    assertNull(Functions.y(emptyPoint));
    assertNull(Functions.x(null));
    assertNull(Functions.y(null));
  }

  @Test
  public void convexHull_nullAndEmptyPreserveType() throws Exception {
    assertNull(Functions.convexHull(null));

    for (String wkt : new String[] {"POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY"}) {
      Geography input = Constructors.geogFromWKT(wkt, 4326);
      Geography hull = Functions.convexHull(input);
      assertEquals(wkt, hull.toString());
      assertEquals(4326, hull.getSRID());

      Geography roundTripped =
          GeographyWKBSerializer.deserialize(GeographyWKBSerializer.serialize(hull));
      assertEquals(wkt, roundTripped.toString());
      assertEquals(4326, roundTripped.getSRID());
    }

    WKBGeography legacyEmptyLine = WKBGeography.fromWKB(new byte[] {1, 2, 0, 0, 0}, 4326);
    assertEquals(9, legacyEmptyLine.getWKBBytes().length);
    Geography legacyHull = Functions.convexHull(legacyEmptyLine);
    assertEquals("LINESTRING EMPTY", legacyHull.toString());
    assertEquals(9, ((WKBGeography) legacyHull).getWKBBytes().length);
  }

  @Test
  public void convexHull_degenerateInputsReturnPointOrLine() throws ParseException {
    Geometry coincidentJts = new WKTReader().read("LINESTRING (1 2, 1 2, 1 2)");
    coincidentJts.setSRID(4326);
    Geography coincidentLine = WKBGeography.fromJTS(coincidentJts);
    Geometry pointHull = Constructors.geogToGeometry(Functions.convexHull(coincidentLine));
    assertTrue(pointHull instanceof Point);
    assertEquals(1.0, ((Point) pointHull).getX(), 0.0);
    assertEquals(2.0, ((Point) pointHull).getY(), 0.0);

    Geography twoPoints = Constructors.geogFromWKT("MULTIPOINT ((0 0), (0 2))", 4326);
    assertLineEndpoints(Functions.convexHull(twoPoints), 0, 0, 0, 2);

    Geography preciseTwoPoints =
        Constructors.geogFromWKT("MULTIPOINT ((-73.9857 40.7484), (-122.4194 37.7749))", 4326);
    assertLineEndpoints(
        Functions.convexHull(preciseTwoPoints), -73.9857, 40.7484, -122.4194, 37.7749, 0.0);

    Geography collinear = Constructors.geogFromWKT("LINESTRING (0 0, 0 1, 0 2, 0 1)", 4326);
    assertLineEndpoints(Functions.convexHull(collinear), 0, 0, 0, 2);

    Geography fourCollinear =
        Constructors.geogFromWKT("MULTIPOINT ((0 0), (10 0), (20 0), (30 0))", 4326);
    assertLineEndpoints(Functions.convexHull(fourCollinear), 0, 0, 30, 0);

    Geography equivalentLongitudes = Constructors.geogFromWKT("MULTIPOINT ((0 0), (360 0))", 4326);
    assertTrue(
        Constructors.geogToGeometry(Functions.convexHull(equivalentLongitudes)) instanceof Point);

    Geography equivalentPoles =
        Constructors.geogFromWKT("MULTIPOINT ((0 90), (120 90), (-45 90))", 4326);
    assertTrue(Constructors.geogToGeometry(Functions.convexHull(equivalentPoles)) instanceof Point);

    Geography collinearPolygon = Constructors.geogFromWKT("POLYGON ((0 0, 10 0, 20 0, 0 0))", 4326);
    assertLineEndpoints(Functions.convexHull(collinearPolygon), 0, 0, 20, 0);

    Geography collectionWithCollinearPolygon =
        Constructors.geogFromWKT(
            "GEOMETRYCOLLECTION (POLYGON ((0 0, 10 0, 20 0, 0 0)), POINT (30 0))", 4326);
    assertLineEndpoints(Functions.convexHull(collectionWithCollinearPolygon), 0, 0, 30, 0);
  }

  @Test
  public void convexHull_polygonSkipsHolesAndPreservesSrid() throws ParseException {
    Geography input =
        Constructors.geogFromWKT(
            "POLYGON ((0 0, 2 0, 0 2, 0 0), (0.2 0.2, 0.4 0.2, 0.2 0.4, 0.2 0.2))", 4326);
    Geography hull = Functions.convexHull(input);
    Geometry jts = Constructors.geogToGeometry(hull);

    assertTrue(jts instanceof Polygon);
    assertEquals(0, ((Polygon) jts).getNumInteriorRing());
    assertEquals(4, ((Polygon) jts).getExteriorRing().getNumPoints());
    assertEquals(4326, hull.getSRID());
  }

  @Test
  public void convexHull_polygonPreservesExactSourceCoordinates() throws ParseException {
    Geography input =
        Constructors.geogFromWKT("MULTIPOINT ((0 0), (10 0), (10 10), (0 10), (5 5))", 4326);
    Geometry hull = Constructors.geogToGeometry(Functions.convexHull(input));

    assertTrue(hull instanceof Polygon);
    assertEquals(5, hull.getNumPoints());
    for (Coordinate coordinate : hull.getCoordinates()) {
      boolean isExactSourceVertex =
          (coordinate.x == 0.0 && coordinate.y == 0.0)
              || (coordinate.x == 10.0 && coordinate.y == 0.0)
              || (coordinate.x == 10.0 && coordinate.y == 10.0)
              || (coordinate.x == 0.0 && coordinate.y == 10.0);
      assertTrue(
          "Hull vertex drifted from its source coordinate: " + coordinate, isExactSourceVertex);
    }
  }

  @Test
  public void convexHull_collectionAndAntimeridianAreSpherical() throws ParseException {
    Geography collection =
        Constructors.geogFromWKT(
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 0, 5 5), "
                + "POLYGON ((0 1, 0.2 0.2, 1 0, 0 1)))",
            4326);
    assertTrue(Constructors.geogToGeometry(Functions.convexHull(collection)) instanceof Polygon);

    Geography acrossDateLine =
        Constructors.geogFromWKT("MULTIPOINT ((170 -10), (170 10), (-170 10), (-170 -10))", 4326);
    Geography hull = Functions.convexHull(acrossDateLine);
    assertTrue(Constructors.geogToGeometry(hull) instanceof Polygon);
    assertTrue("expected the small antimeridian hull", Functions.area(hull) < 1e14);
  }

  @Test
  public void convexHull_fullSphereIsRejected() throws ParseException {
    Geography input = Constructors.geogFromWKT("MULTIPOINT ((0 0), (120 0), (-120 0))", 4326);
    try {
      Functions.convexHull(input);
      fail("Expected a full-sphere hull to be rejected");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("full sphere"));
    }
  }

  @Test
  public void convexHull_publicPolygonUsesNormalizedShellInterior() throws ParseException {
    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 0 10, 10 0, 0 0))", 4326);
    Geography hull = Functions.convexHull(polygon);

    assertEquals("ST_Polygon", Functions.geometryType(hull));
    assertTrue(Functions.area(hull) < 1.0e13);
    assertTrue(Functions.contains(hull, Constructors.geogFromWKT("POINT (1 1)", 4326)));
    assertFalse(Functions.contains(hull, Constructors.geogFromWKT("POINT (20 20)", 4326)));
  }

  @Test
  public void asEWKT_usesStoredWKBForOrdinaryGeography() throws ParseException {
    Geography geography = Constructors.geogFromWKT("POINT (-122.4194 37.7749)", 4326);

    assertEquals("SRID=4326; POINT (-122.4194 37.7749)", Functions.asEWKT(geography));
  }

  @Test
  public void makeLine_points() throws ParseException {
    Geography start = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography end = Constructors.geogFromWKT("POINT (1 0)", 4326);
    Geography line = Functions.makeLine(start, end);

    assertEquals("LINESTRING (0 0, 1 0)", Functions.asText(line));
    assertEquals(4326, line.getSRID());
    assertEquals(111195.10, Functions.length(line), 1.0);
  }

  @Test
  public void makeLine_coincidentPointsRemainUsableAfterWKBRoundTrip() throws ParseException {
    Geography point = Constructors.geogFromWKT("POINT (12 34)", 4326);
    Geography line = roundTripWKB(Functions.makeLine(point, point));

    assertEquals("LINESTRING (12 34, 12 34)", Functions.asText(line));
    assertEquals("SRID=4326; LINESTRING (12 34, 12 34)", Functions.asEWKT(line));
    assertEquals("LINESTRING (12 34, 12 34)", line.toString());
    assertEquals("SRID=4326; LINESTRING (12 34, 12 34)", line.toEWKT());
    assertEquals(2, new WKTReader().read(line.toString()).getNumPoints());
    assertEquals(2, Functions.nPoints(line));
    assertEquals("ST_LineString", Functions.geometryType(line));
    assertEquals(1, Functions.numGeometries(line));
    assertEquals(0.0, Functions.length(line), 0.0);
    assertEquals(0.0, Functions.distance(line, point), 0.0);
    Geography centroid = Functions.centroid(line);
    assertNotNull(centroid);
    assertEquals(4326, centroid.getSRID());
    assertEquals(12.0, Functions.x(centroid), 1e-12);
    assertEquals(34.0, Functions.y(centroid), 1e-12);

    for (boolean splitAtAntiMeridian : new boolean[] {false, true}) {
      Geography envelope = Functions.getEnvelope(line, splitAtAntiMeridian);
      assertNotNull(envelope);
      assertEquals(4326, envelope.getSRID());
      assertEquals("ST_Point", Functions.geometryType(envelope));
      assertEquals(12.0, Functions.x(envelope), 1e-12);
      assertEquals(34.0, Functions.y(envelope), 1e-12);
    }

    Geography end = Constructors.geogFromWKT("POINT (13 34)", 4326);
    Geography extended = roundTripWKB(Functions.makeLine(line, end));
    assertEquals("LINESTRING (12 34, 12 34, 13 34)", Functions.asText(extended));
    assertEquals(3, Functions.nPoints(extended));
  }

  @Test
  public void makeLine_skipsEmptyInputsAndNormalizesSingleCoordinate() throws ParseException {
    Geography emptyPoint = Constructors.geogFromWKT("POINT EMPTY", 3857);
    Geography emptyLine = Constructors.geogFromWKT("LINESTRING EMPTY", 4326);
    Geography emptyMultiPoint = Constructors.geogFromWKT("MULTIPOINT EMPTY", 4326);
    Geography point = Constructors.geogFromWKT("POINT (12 34)", 4326);

    Geography onlySecond = roundTripWKB(Functions.makeLine(emptyPoint, point));
    assertEquals("LINESTRING (12 34, 12 34)", Functions.asText(onlySecond));
    assertEquals(2, Functions.nPoints(onlySecond));
    assertEquals(3857, onlySecond.getSRID());

    Geography onlyFirst = roundTripWKB(Functions.makeLine(point, emptyLine));
    assertEquals("LINESTRING (12 34, 12 34)", Functions.asText(onlyFirst));
    assertEquals(2, Functions.nPoints(onlyFirst));
    assertEquals(4326, onlyFirst.getSRID());

    Geography afterEmptyMultiPoint = roundTripWKB(Functions.makeLine(emptyMultiPoint, point));
    assertEquals("LINESTRING (12 34, 12 34)", Functions.asText(afterEmptyMultiPoint));
    assertEquals(2, Functions.nPoints(afterEmptyMultiPoint));

    Geography noCoordinates = roundTripWKB(Functions.makeLine(emptyPoint, emptyLine));
    assertEquals("LINESTRING EMPTY", Functions.asText(noCoordinates));
    assertEquals(0, Functions.nPoints(noCoordinates));
    assertEquals(3857, noCoordinates.getSRID());
  }

  @Test
  public void makeLine_deduplicatesLineStringSeamAndPreservesOtherRepeats() throws ParseException {
    Geography first = Constructors.geogFromWKT("LINESTRING (0 0, 1 0)", 4326);
    Geography second = Constructors.geogFromWKT("LINESTRING (1 0, 2 0)", 4326);
    Geography stitched = roundTripWKB(Functions.makeLine(first, second));
    assertEquals("LINESTRING (0 0, 1 0, 2 0)", Functions.asText(stitched));
    assertEquals("SRID=4326; LINESTRING (0 0, 1 0, 2 0)", Functions.asEWKT(stitched));
    assertEquals(3, Functions.nPoints(stitched));

    Geography point = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography line = Constructors.geogFromWKT("LINESTRING (0 0, 1 0)", 4326);
    assertEquals("LINESTRING (0 0, 1 0)", Functions.asText(Functions.makeLine(point, line)));

    Geography multiPoint = Constructors.geogFromWKT("MULTIPOINT ((0 0), (1 0))", 4326);
    assertEquals(
        "LINESTRING (0 0, 0 0, 1 0)", Functions.asText(Functions.makeLine(point, multiPoint)));

    Geography repeated = geographyFromJTSWKT("LINESTRING (0 0, 0 0, 1 0)", 4326);
    Geography end = Constructors.geogFromWKT("POINT (2 0)", 4326);
    Geography appended = roundTripWKB(Functions.makeLine(repeated, end));
    assertEquals("LINESTRING (0 0, 0 0, 1 0, 2 0)", Functions.asText(appended));
    assertEquals(4, Functions.nPoints(appended));

    Geography repeatedFromWKT = Constructors.geogFromWKT("LINESTRING (0 0, 0 0, 1 0)", 4326);
    assertEquals("LINESTRING (0 0, 0 0, 1 0)", Functions.asText(repeatedFromWKT));
    assertEquals(3, Functions.nPoints(repeatedFromWKT));
  }

  @Test
  public void makeLine_multiPointAndLineString() throws ParseException {
    Geography points = Constructors.geogFromWKT("MULTIPOINT ((0 0), (1 1))", 4326);
    Geography line = Constructors.geogFromWKT("LINESTRING (2 2, 3 3)", 4326);
    Geography result = roundTripWKB(Functions.makeLine(points, line));

    Coordinate[] coordinates = Constructors.geogToGeometry(result).getCoordinates();
    assertEquals(4, coordinates.length);
    for (int i = 0; i < coordinates.length; i++) {
      assertDegAlmostEqual(coordinates[i].x, i);
      assertDegAlmostEqual(coordinates[i].y, i);
    }
  }

  @Test
  public void createMultiGeography_preservesMembersAndFirstSrid() throws ParseException {
    Geography first = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography duplicate = Constructors.geogFromWKT("POINT (1 2)", 3857);
    Geography second = Constructors.geogFromWKT("POINT (3 4)", 4326);
    Geography collected =
        Functions.createMultiGeography(new Geography[] {first, null, duplicate, second});
    Geometry jts = Constructors.geogToGeometry(collected);

    assertTrue(jts instanceof MultiPoint);
    assertEquals(3, jts.getNumGeometries());
    assertEquals(4326, collected.getSRID());

    Geography line = Constructors.geogFromWKT("LINESTRING (0 0, 1 1)", 4326);
    Geography mixed = Functions.createMultiGeography(new Geography[] {first, line});
    assertEquals("GeometryCollection", Constructors.geogToGeometry(mixed).getGeometryType());

    Geography empty = Functions.createMultiGeography(new Geography[] {null});
    assertEquals("GEOMETRYCOLLECTION EMPTY", Functions.asText(empty));
  }

  private static void assertLineEndpoints(
      Geography geography, double x0, double y0, double x1, double y1) {
    assertLineEndpoints(geography, x0, y0, x1, y1, EPS);
  }

  private static void assertLineEndpoints(
      Geography geography, double x0, double y0, double x1, double y1, double tolerance) {
    Geometry geometry = Constructors.geogToGeometry(geography);
    assertTrue(geometry instanceof LineString);
    LineString line = (LineString) geometry;
    Coordinate start = line.getCoordinateN(0);
    Coordinate end = line.getCoordinateN(line.getNumPoints() - 1);
    boolean forward =
        Math.abs(start.x - x0) <= tolerance
            && Math.abs(start.y - y0) <= tolerance
            && Math.abs(end.x - x1) <= tolerance
            && Math.abs(end.y - y1) <= tolerance;
    boolean reverse =
        Math.abs(start.x - x1) <= tolerance
            && Math.abs(start.y - y1) <= tolerance
            && Math.abs(end.x - x0) <= tolerance
            && Math.abs(end.y - y0) <= tolerance;
    assertTrue("Unexpected line endpoints: " + geometry, forward || reverse);
  }

  @Test
  public void makeLine_antimeridianUsesGreatCircleLength() throws ParseException {
    Geography start = Constructors.geogFromWKT("POINT (179 0)", 4326);
    Geography end = Constructors.geogFromWKT("POINT (-179 0)", 4326);
    Geography line = Functions.makeLine(start, end);

    assertEquals("LINESTRING (179 0, -179 0)", Functions.asText(line));
    assertEquals(222390.13, Functions.length(line), 1.0);
  }

  @Test
  public void makeLine_usesFirstSRIDWithoutTransformation() throws ParseException {
    Geography srid4326 = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography srid3857 = Constructors.geogFromWKT("POINT (1 0)", 3857);
    assertEquals(4326, Functions.makeLine(srid4326, srid3857).getSRID());

    Geography srid0 = Constructors.geogFromWKT("POINT (0 0)", 0);
    assertEquals(0, Functions.makeLine(srid0, srid4326).getSRID());
  }

  @Test
  public void makeLine_nullArguments() throws ParseException {
    Geography point = Constructors.geogFromWKT("POINT (0 0)", 4326);
    assertNull(Functions.makeLine(null, point));
    assertNull(Functions.makeLine(point, null));
  }

  @Test
  public void makeLine_rejectsUnsupportedType() throws ParseException {
    Geography point = Constructors.geogFromWKT("POINT (0 0)", 4326);
    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 0))", 4326);

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Functions.makeLine(point, polygon));
    assertTrue(error.getMessage().contains("Point, MultiPoint and LineString geographies"));
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
  public void intersects_pointToPoint_samePoint() throws ParseException {
    // Exercises the point-to-point fast path (no ShapeIndex built on either side)
    Geography g1 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertTrue(Functions.intersects(g1, g2));
  }

  @Test
  public void intersects_pointToPoint_differentPoints() throws ParseException {
    Geography g1 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography g2 = Constructors.geogFromWKT("POINT (3 4)", 4326);
    assertFalse(Functions.intersects(g1, g2));
  }

  @Test
  public void intersects_pointOnLinestring() throws ParseException {
    // Exercises the point-to-complex fast path
    Geography line = Constructors.geogFromWKT("LINESTRING (0 0, 2 0)", 4326);
    Geography pt = Constructors.geogFromWKT("POINT (1 0)", 4326);
    assertTrue(Functions.intersects(line, pt));
    assertTrue(Functions.intersects(pt, line));
  }

  @Test
  public void intersects_pointOffLinestring() throws ParseException {
    Geography line = Constructors.geogFromWKT("LINESTRING (0 0, 2 0)", 4326);
    Geography pt = Constructors.geogFromWKT("POINT (5 5)", 4326);
    assertFalse(Functions.intersects(line, pt));
    assertFalse(Functions.intersects(pt, line));
  }

  @Test
  public void intersects_nullHandling() throws ParseException {
    Geography g = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertFalse(Functions.intersects(g, null));
    assertFalse(Functions.intersects(null, g));
    assertFalse(Functions.intersects(null, null));
  }

  @Test
  public void intersection_nullAndExplicitlyEmptyInputs() throws ParseException {
    Geography point = Constructors.geogFromWKT("POINT (0 0)", 4326);
    assertNull(Functions.intersection(null, point));
    assertNull(Functions.intersection(point, null));

    for (String emptyWkt :
        new String[] {
          "POINT EMPTY",
          "LINESTRING EMPTY",
          "POLYGON EMPTY",
          "MULTIPOINT EMPTY",
          "MULTILINESTRING EMPTY",
          "MULTIPOLYGON EMPTY",
          "GEOMETRYCOLLECTION EMPTY",
          "MULTIPOINT (EMPTY)",
          "MULTILINESTRING (EMPTY)",
          "MULTIPOLYGON (EMPTY)",
          "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING EMPTY, POLYGON EMPTY)"
        }) {
      Geography empty = Constructors.geogFromWKT(emptyWkt, 3857);
      Geography result = Functions.intersection(empty, point);
      assertEquals("GEOMETRYCOLLECTION EMPTY", Functions.asText(result));
      assertEquals(3857, result.getSRID());

      Geography reverseResult = Functions.intersection(point, empty);
      assertEquals("GEOMETRYCOLLECTION EMPTY", Functions.asText(reverseResult));
      assertEquals(4326, reverseResult.getSRID());
    }
  }

  @Test
  public void intersection_nonEmptyDisjointInputsRetainMinimumDimension() throws ParseException {
    String[][] cases = {
      {"POINT (0 0)", "POINT (0 1)", "POINT EMPTY"},
      {"POINT (20 20)", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", "POINT EMPTY"},
      {"LINESTRING (0 0, 10 0)", "LINESTRING (0 10, 10 10)", "LINESTRING EMPTY"},
      {
        "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))",
        "POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))",
        "POLYGON EMPTY"
      }
    };

    for (String[] testCase : cases) {
      Geography left = Constructors.geogFromWKT(testCase[0], 4326);
      Geography right = Constructors.geogFromWKT(testCase[1], 4326);
      assertEquals(testCase[2], Functions.asText(Functions.intersection(left, right)));
    }
  }

  @Test
  public void intersection_preservesVerticesInheritedFromEitherInput() throws ParseException {
    Geography point = Constructors.geogFromWKT("POINT (1 2)", 4326);
    assertEquals("POINT (1 2)", Functions.asText(Functions.intersection(point, point)));

    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", 4326);
    Geometry polygonIdentity =
        Constructors.geogToGeometry(Functions.intersection(polygon, polygon));
    assertTrue(polygonIdentity.toText(), polygonIdentity instanceof Polygon);
    for (Coordinate coordinate : polygonIdentity.getCoordinates()) {
      assertEquals(Math.rint(coordinate.x), coordinate.x, 0.0);
      assertEquals(Math.rint(coordinate.y), coordinate.y, 0.0);
    }

    Geometry containedPoint = Constructors.geogToGeometry(Functions.intersection(polygon, point));
    assertTrue(containedPoint.toText(), containedPoint instanceof Point);
    assertEquals(1.0, containedPoint.getCoordinate().x, 0.0);
    assertEquals(2.0, containedPoint.getCoordinate().y, 0.0);

    Geography vertical = Constructors.geogFromWKT("LINESTRING (0 0, 0 10)", 4326);
    Geography touching = Constructors.geogFromWKT("LINESTRING (0 5, 5 5)", 4326);
    assertEquals("POINT (0 5)", Functions.asText(Functions.intersection(vertical, touching)));
  }

  @Test
  public void intersection_acceptsNativeSingleVertexPolyline() {
    S2Point vertex = S2LatLng.fromDegrees(2, 1).toPoint();
    Geography polyline =
        new SinglePolylineGeography(new S2Polyline(java.util.Collections.singletonList(vertex)));

    Geometry result = Constructors.geogToGeometry(Functions.intersection(polyline, polyline));
    assertTrue(result.toText(), result instanceof Point);
    assertEquals(1.0, result.getCoordinate().x, EPS);
    assertEquals(2.0, result.getCoordinate().y, EPS);
  }

  @Test
  public void intersection_closedSetDemotesBoundaryResults() throws ParseException {
    Geography vertical = Constructors.geogFromWKT("LINESTRING (0 -5, 0 5)", 4326);
    Geography horizontal = Constructors.geogFromWKT("LINESTRING (-5 0, 5 0)", 4326);
    assertEquals("POINT (0 0)", Functions.asText(Functions.intersection(vertical, horizontal)));

    Geography interiorPoint = Constructors.geogFromWKT("POINT (0 2.5)", 4326);
    Geometry pointOnLine =
        Constructors.geogToGeometry(Functions.intersection(interiorPoint, vertical));
    assertTrue(pointOnLine.toText(), pointOnLine instanceof Point);
    assertEquals(2.5, ((Point) pointOnLine).getY(), EPS);

    Geography overlapping1 = Constructors.geogFromWKT("LINESTRING (20 0, 30 0)", 4326);
    Geography overlapping2 = Constructors.geogFromWKT("LINESTRING (20 0, 30 0)", 4326);
    Geometry overlapping =
        Constructors.geogToGeometry(Functions.intersection(overlapping1, overlapping2));
    assertTrue(overlapping.toText(), overlapping instanceof LineString);
    assertEquals(2, overlapping.getNumPoints());
    LineString overlappingLine = (LineString) overlapping;
    assertEquals(
        20.0,
        Math.min(overlappingLine.getCoordinateN(0).x, overlappingLine.getCoordinateN(1).x),
        EPS);
    assertEquals(
        30.0,
        Math.max(overlappingLine.getCoordinateN(0).x, overlappingLine.getCoordinateN(1).x),
        EPS);

    Geography partiallyOverlapping1 = Constructors.geogFromWKT("LINESTRING (0 0, 0 10)", 4326);
    Geography partiallyOverlapping2 = Constructors.geogFromWKT("LINESTRING (0 5, 0 15)", 4326);
    Geometry partiallyOverlapping =
        Constructors.geogToGeometry(
            Functions.intersection(partiallyOverlapping1, partiallyOverlapping2));
    assertTrue(partiallyOverlapping.toText(), partiallyOverlapping instanceof LineString);
    assertEquals(2, partiallyOverlapping.getNumPoints());
    Coordinate[] overlapCoordinates = partiallyOverlapping.getCoordinates();
    assertEquals(5.0, Math.min(overlapCoordinates[0].y, overlapCoordinates[1].y), EPS);
    assertEquals(10.0, Math.max(overlapCoordinates[0].y, overlapCoordinates[1].y), EPS);

    Geography containingLine = Constructors.geogFromWKT("LINESTRING (0 0, 0 20)", 4326);
    Geography containedLine = Constructors.geogFromWKT("LINESTRING (0 5, 0 15)", 4326);
    Geometry containedOverlap =
        Constructors.geogToGeometry(Functions.intersection(containingLine, containedLine));
    assertTrue(containedOverlap.toText(), containedOverlap instanceof LineString);
    assertEquals(2, containedOverlap.getNumPoints());
    Coordinate[] containedCoordinates = containedOverlap.getCoordinates();
    assertEquals(5.0, Math.min(containedCoordinates[0].y, containedCoordinates[1].y), EPS);
    assertEquals(15.0, Math.max(containedCoordinates[0].y, containedCoordinates[1].y), EPS);

    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", 4326);
    Geography edgePoint = Constructors.geogFromWKT("POINT (2.5 0)", 4326);
    Geometry pointOnPolygonEdge =
        Constructors.geogToGeometry(Functions.intersection(polygon, edgePoint));
    assertTrue(pointOnPolygonEdge.toText(), pointOnPolygonEdge instanceof Point);
    assertEquals(2.5, ((Point) pointOnPolygonEdge).getX(), EPS);

    Geography touchingLine = Constructors.geogFromWKT("LINESTRING (0 0, -10 0)", 4326);
    assertEquals("POINT (0 0)", Functions.asText(Functions.intersection(polygon, touchingLine)));

    Geography adjacentPolygon =
        Constructors.geogFromWKT("POLYGON ((5 0, 10 0, 10 5, 5 5, 5 0))", 4326);
    Geometry sharedEdge =
        Constructors.geogToGeometry(Functions.intersection(polygon, adjacentPolygon));
    assertTrue(sharedEdge.toText(), sharedEdge instanceof LineString);
    assertEquals(2, sharedEdge.getNumPoints());

    Geography partiallySharedEdge = Constructors.geogFromWKT("LINESTRING (2.5 0, -10 0)", 4326);
    Geometry partialEdge =
        Constructors.geogToGeometry(Functions.intersection(polygon, partiallySharedEdge));
    assertTrue(partialEdge.toText(), partialEdge instanceof LineString);
    assertEquals(2, partialEdge.getNumPoints());

    Geography containedEdge = Constructors.geogFromWKT("LINESTRING (1 0, 4 0)", 4326);
    Geometry containedPolygonEdge =
        Constructors.geogToGeometry(Functions.intersection(polygon, containedEdge));
    assertTrue(containedPolygonEdge.toText(), containedPolygonEdge instanceof LineString);
    assertEquals(2, containedPolygonEdge.getNumPoints());

    Geography partiallyAdjacentPolygon =
        Constructors.geogFromWKT("POLYGON ((1 0, 4 0, 4 -5, 1 -5, 1 0))", 4326);
    Geometry partialPolygonEdge =
        Constructors.geogToGeometry(Functions.intersection(polygon, partiallyAdjacentPolygon));
    assertTrue(partialPolygonEdge.toText(), partialPolygonEdge instanceof LineString);
    Coordinate[] partialPolygonCoordinates = partialPolygonEdge.getCoordinates();
    assertEquals(
        1.0, Math.min(partialPolygonCoordinates[0].x, partialPolygonCoordinates[1].x), EPS);
    assertEquals(
        4.0, Math.max(partialPolygonCoordinates[0].x, partialPolygonCoordinates[1].x), EPS);

    Geography vertexTouchingPolygon =
        Constructors.geogFromWKT("POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))", 4326);
    Geometry sharedVertex =
        Constructors.geogToGeometry(Functions.intersection(polygon, vertexTouchingPolygon));
    assertTrue(sharedVertex.toText(), sharedVertex instanceof Point);
    assertEquals(5.0, ((Point) sharedVertex).getX(), EPS);
    assertEquals(5.0, ((Point) sharedVertex).getY(), EPS);

    Geography degeneratePolygon = Constructors.geogFromWKT("POLYGON ((3 3, 3 3, 3 3, 3 3))", 4326);
    Geography degeneratePoint = Constructors.geogFromWKT("POINT (3 3)", 4326);
    Geometry degenerateIntersection =
        Constructors.geogToGeometry(Functions.intersection(degeneratePolygon, degeneratePoint));
    assertTrue(degenerateIntersection.toText(), degenerateIntersection instanceof Point);
  }

  @Test
  public void intersection_polygonOverlapHasPositiveArea() throws ParseException {
    Geography left = Constructors.geogFromWKT("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", 4326);
    Geography right = Constructors.geogFromWKT("POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))", 4326);

    Geography intersection = Functions.intersection(left, right);
    assertTrue(Constructors.geogToGeometry(intersection) instanceof Polygon);
    assertEquals(3.071055126726233e11, Functions.area(intersection), 1e5);
    assertEquals(intersection.toEWKT(), roundTripWKB(intersection).toEWKT());
  }

  @Test
  public void intersection_suppressesComponentsCoveredByHigherDimension() throws ParseException {
    Geography left =
        Constructors.geogFromWKT(
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 4 0), "
                + "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)))",
            4326);
    Geography right = Constructors.geogFromWKT("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))", 4326);

    Geometry result = Constructors.geogToGeometry(Functions.intersection(left, right));
    assertTrue(result.toText(), result instanceof Polygon);
  }

  @Test
  public void intersection_acceptsRawZAndMWkbAndReturnsXY() throws ParseException {
    Geography point = Constructors.geogFromWKT("POINT (1 2)", 4326);

    for (int type : new int[] {1001, 2001}) {
      Geography higherDimensional = geographyFromIsoPointWKB(type, 1, 2, 9, 4326);
      Geography intersection = Functions.intersection(higherDimensional, point);
      Geometry result = Constructors.geogToGeometry(intersection);
      assertTrue(result.toText(), result instanceof Point);
      Point resultPoint = (Point) result;
      assertEquals(1.0, resultPoint.getX(), EPS);
      assertEquals(2.0, resultPoint.getY(), EPS);
      assertTrue(Double.isNaN(resultPoint.getCoordinate().getZ()));
      assertEquals(21, ((WKBGeography) intersection).getWKBBytes().length);
    }

    Geography higherDimensionalLine =
        geographyFromIsoLineStringZWKB(new double[] {0, 0, 9, 0, 10, 10}, 4326);
    Geography overlappingLine = Constructors.geogFromWKT("LINESTRING (0 5, 0 15)", 4326);
    Geography lineIntersection = Functions.intersection(higherDimensionalLine, overlappingLine);
    Geometry lineResult = Constructors.geogToGeometry(lineIntersection);
    assertTrue(lineResult.toText(), lineResult instanceof LineString);
    assertEquals(2, lineResult.getNumPoints());
    assertTrue(Double.isNaN(lineResult.getCoordinate().getZ()));
    assertEquals(41, ((WKBGeography) lineIntersection).getWKBBytes().length);
  }

  @Test
  public void intersection_usesGeodesicEdges() throws ParseException {
    Geography line = Constructors.geogFromWKT("LINESTRING (-5 5, 5 5)", 4326);
    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", 4326);
    Geometry result = Constructors.geogToGeometry(Functions.intersection(line, polygon));

    assertTrue(result instanceof LineString);
    Coordinate[] coordinates = result.getCoordinates();
    assertEquals(2, coordinates.length);
    Coordinate boundary = Math.abs(coordinates[0].x) < EPS ? coordinates[0] : coordinates[1];
    assertEquals(0.0, boundary.x, EPS);
    assertEquals(5.019002, boundary.y, 1e-6);
  }

  @Test
  public void intersection_supportsMixedDimensionsAndPreservesFirstSrid() throws ParseException {
    Geography left =
        Constructors.geogFromWKT(
            "GEOMETRYCOLLECTION (POINT (20 20), LINESTRING (20 0, 30 0), "
                + "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0)))",
            3857);
    Geography right =
        Constructors.geogFromWKT(
            "GEOMETRYCOLLECTION (POINT (20 20), LINESTRING (20 0, 30 0), "
                + "POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5)))",
            4326);

    Geography intersection = Functions.intersection(left, right);
    Geometry result = Constructors.geogToGeometry(intersection);
    assertTrue(result instanceof GeometryCollection);
    assertEquals(result.toText(), 3, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof Point);
    assertTrue(result.getGeometryN(1) instanceof LineString);
    assertTrue(result.getGeometryN(2) instanceof Polygon);
    assertEquals(3857, intersection.getSRID());
    assertEquals(intersection.toEWKT(), roundTripWKB(intersection).toEWKT());
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
    // distance == threshold ⇒ true (inclusive <=)
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
