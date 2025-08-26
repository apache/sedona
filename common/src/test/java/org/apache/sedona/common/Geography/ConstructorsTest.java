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
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import java.util.ArrayList;
import java.util.List;
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.SinglePointGeography;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.apache.sedona.common.S2Geography.WKBWriter;
import org.apache.sedona.common.geography.Constructors;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;

public class ConstructorsTest {

  @Test
  public void geogFromEWKT() throws ParseException {
    assertNull(Constructors.geogFromEWKT(null));

    Geography geog = Constructors.geogFromEWKT("POINT (1 1)");
    assertEquals(0, geog.getSRID());
    assertEquals("POINT (1 1)", geog.toString());

    geog = Constructors.geogFromEWKT("SRID=4269; POINT (1 1)");
    assertEquals(4269, geog.getSRID());
    assertEquals("SRID=4269; POINT (1 1)", geog.toEWKT());

    geog = Constructors.geogFromEWKT("SRID=4269;POINT (1 1)");
    assertEquals(4269, geog.getSRID());
    assertEquals("SRID=4269; POINT (1 1)", geog.toEWKT());

    ParseException invalid =
        assertThrows(ParseException.class, () -> Constructors.geogFromEWKT("not valid"));
    assertEquals("Unknown geography type: NOT (line 1)", invalid.getMessage());
  }

  @Test
  public void geogFromWKB() throws ParseException {
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    SinglePointGeography geog = new SinglePointGeography(pt);

    // Test WKB without SRID
    WKBWriter wkbWriter = new WKBWriter();
    byte[] wkb = wkbWriter.write(geog);

    Geography result = Constructors.geogFromWKB(wkb);
    assertEquals(geog.toString(), result.toString());
    assertEquals(0, result.getSRID());

    // Test specifying SRID
    result = Constructors.geogFromWKB(wkb, 1000);
    assertEquals("POINT (-64 45)", result.toString());
    assertEquals(1000, result.getSRID());

    // Test EWKB with SRID
    wkbWriter = new WKBWriter(2, true);
    geog.setSRID(2000);
    wkb = wkbWriter.write(geog);
    result = Constructors.geogFromWKB(wkb);
    assertEquals("POINT (-64 45)", result.toString());
    assertEquals(2000, result.getSRID());

    // Test overriding SRID
    result = Constructors.geogFromWKB(wkb, 3000);
    assertEquals("POINT (-64 45)", result.toString());
    assertEquals(3000, result.getSRID());
    result = Constructors.geogFromWKB(wkb, 0);
    assertEquals("POINT (-64 45)", result.toString());
    assertEquals(0, result.getSRID());
  }

  @Test
  public void testGeogFromEWKB() throws ParseException {
    String ewkbString = "01010000A0E61000000000000000000000000000000000F03F0000000000000040";
    byte[] wkbBytes = WKBReader.hexToBytes(ewkbString);
    Geography result = Constructors.geogFromWKB(wkbBytes);
    String expectedGeom = "SRID=4326; POINT (0 1)";
    assertEquals(expectedGeom, result.toEWKT());
    assertEquals(4326, result.getSRID());

    ewkbString =
        "0103000020E61000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000";
    wkbBytes = WKBReader.hexToBytes(ewkbString);
    result = Constructors.geogFromWKB(wkbBytes);
    expectedGeom = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))";
    assertEquals(expectedGeom, result.toString());
    assertEquals(4326, result.getSRID());

    ewkbString =
        "0106000020E610000002000000"
            + "01030000000100000004000000"
            + "00000000000000000000000000000000"
            + "000000000000F03F0000000000000000"
            + "000000000000F03F000000000000F03F"
            + "00000000000000000000000000000000"
            + "01030000000100000004000000"
            + "000000000000F0BF000000000000F0BF"
            + "000000000000F0BF0000000000000000"
            + "0000000000000000000000000000F0BF"
            + "000000000000F0BF000000000000F0BF";
    wkbBytes = WKBReader.hexToBytes(ewkbString);
    result = Constructors.geogFromWKB(wkbBytes);
    expectedGeom = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((-1 -1, -1 0, 0 -1, -1 -1)))";
    assertEquals(expectedGeom, result.toString());
    assertEquals(4326, result.getSRID());
  }

  @Test
  public void geogFromGeoHash() throws ParseException {
    Geography geog = Constructors.geogFromGeoHash("9q9j8ue2v71y5zzy0s4q", 16);
    String expectedWkt =
        "POLYGON ((-122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162))";
    assertNotNull(geog);
    String actualWkt = geog.toString(new PrecisionModel(1e6));
    assertEquals(expectedWkt, actualWkt);

    geog = Constructors.geogFromGeoHash("s00twy01mt", 4);
    expectedWkt =
        "POLYGON ((0.703125 0.8789062, 1.0546875 0.8789062, 1.0546875 1.0546875, 0.703125 1.0546875, 0.703125 0.8789062))";
    assertNotNull(geog);
    actualWkt = geog.toString(new PrecisionModel(1e6));
    assertEquals(expectedWkt, actualWkt);
  }

  @Test
  public void geogToGeometry() throws ParseException {
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    S2Point pt_mid = S2LatLng.fromDegrees(45, 0).toPoint();
    S2Point pt_end = S2LatLng.fromDegrees(0, 0).toPoint();
    // Build a single polygon and wrap in geography
    List<S2Point> points = new ArrayList<>();
    points.add(pt);
    points.add(pt_mid);
    points.add(pt_end);
    S2Loop polyline = new S2Loop(points);
    S2Polygon poly = new S2Polygon(polyline);
    PolygonGeography geo = new PolygonGeography(poly);
    GeometryFactory gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));
    Geometry result = Constructors.geogToGeometry(geo, gf);
    assertEquals(geo.toString(), result.toString());

    String withHole =
        "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), " + "(20 30, 35 35, 30 20, 20 30))";
    String expected = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (30 20, 20 30, 35 35, 30 20))";
    Geography geography = new WKTReader().read(withHole);
    Geometry geom = Constructors.geogToGeometry(geography, gf);
    assertEquals(expected, geom.toString());

    String multiGeog = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))";
    geography = new WKTReader().read(multiGeog);
    geom = Constructors.geogToGeometry(geography, gf);
    assertEquals(multiGeog, geom.toString());

    multiGeog = "MULTILINESTRING " + "((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))";
    // Geography can not exceeding to more than 90 degrees / longitude
    geography = new WKTReader().read(multiGeog);
    geom = Constructors.geogToGeometry(geography, gf);
    assertEquals(multiGeog, geom.toString());

    multiGeog =
        "MULTIPOLYGON "
            + "(((30 20, 45 40, 10 40, 30 20)), "
            + "((15 5, 40 10, 10 20, 5 10, 15 5)))";
    geography = new WKTReader().read(multiGeog);
    geom = Constructors.geogToGeometry(geography, gf);
    assertEquals(multiGeog, geom.toString());
  }

  @Test
  public void deep_nesting_twoComponents() throws Exception {
    String wkt =
        "MULTIPOLYGON ("
            +
            // Component A: outer shell + lake
            "((10 10, 70 10, 70 70, 10 70, 10 10),"
            + " (20 20, 60 20, 60 60, 20 60, 20 20)),"
            +
            // Component B: island with a pond
            "((30 30, 50 30, 50 50, 30 50, 30 30),"
            + " (36 36, 44 36, 44 44, 36 44, 36 36))"
            + ")";

    Geography g = new WKTReader().read(wkt);
    g.setSRID(4326);
    Geometry got = Constructors.geogToGeometry(g);
    String expected =
        "MULTIPOLYGON (((10 10, 70 10, 70 70, 10 70, 10 10), "
            + "(20 20, 20 60, 60 60, 60 20, 20 20)), "
            + "((30 30, 50 30, 50 50, 30 50, 30 30), "
            + "(36 36, 36 44, 44 44, 44 36, 36 36)))";
    assertEquals(4326, got.getSRID());
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    String gotGeom = wktWriter.write(got);
    assertEquals(expected, gotGeom);
  }

  @Test
  public void polygon_threeHoles() throws Exception {
    String wkt =
        "POLYGON (("
            + "0 0, 95 20, 95 85, 10 85, 0 0"
            + "),("
            + "20 30, 35 25, 30 40, 20 30"
            + "),("
            + "50 50, 65 50, 65 65, 50 65, 50 50"
            + "),("
            + "25 60, 35 58, 38 66, 30 72, 22 66, 25 60"
            + "))";

    Geography g = new WKTReader().read(wkt);
    String expected =
        "POLYGON ((0 0, 95 20, 95 85, 10 85, 0 0), "
            + "(20 30, 30 40, 35 25, 20 30), "
            + "(50 50, 50 65, 65 65, 65 50, 50 50), "
            + "(25 60, 22 66, 30 72, 38 66, 35 58, 25 60))";
    Geometry got =
        Constructors.geogToGeometry(
            g, new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED)));
    assertEquals(expected, got.toString());
    assertEquals(0, got.getSRID());
  }

  @Test
  public void MultiPolygonGeomToGeography() throws Exception {
    String wkt =
        "MULTIPOLYGON ("
            +
            // Component A: outer shell + lake
            "((10 10, 70 10, 70 70, 10 70, 10 10),"
            + " (20 20, 60 20, 60 60, 20 60, 20 20)),"
            +
            // Component B: island with a pond
            " ((30 30, 50 30, 50 50, 30 50, 30 30),"
            + " (36 36, 44 36, 44 44, 36 44, 36 36))"
            + ")";
    Geometry g = new org.locationtech.jts.io.WKTReader().read(wkt);
    g.setSRID(4326);
    Geography got = Constructors.geomToGeography(g);
    String expected = "SRID=4326; " + wkt;
    assertEquals(4326, got.getSRID());
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    assertEquals(expected, got.toEWKT());
  }

  @Test
  public void PointGeomToGeography() throws Exception {
    Geometry geom = org.apache.sedona.common.Constructors.geomFromWKT("POINT (1 1)", 0);
    Geography got = Constructors.geomToGeography(geom);
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    assertEquals(geom.toString(), got.toString());

    geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "MULTIPOINT ((10 10), (20 20), (30 30))", 0);
    got = Constructors.geomToGeography(geom);
    assertEquals(geom.toString(), got.toString());
  }

  @Test
  public void PointGeomToGeographyDuplicate() throws Exception {
    Geometry geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "MULTIPOINT ((10 10), (20 20), (20 20), (30 30))", 0);
    Geography got = Constructors.geomToGeography(geom);
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    assertEquals("MULTIPOINT ((10 10), (20 20), (30 30))", got.toString());

    geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "MULTIPOINT ((10 10), (20 20), (30 30), (20 20), (10 10))", 0);
    got = Constructors.geomToGeography(geom);
    assertEquals("MULTIPOINT ((10 10), (20 20), (30 30))", got.toString());
  }

  @Test
  public void LineGeomToGeography() throws Exception {
    Geometry geom =
        org.apache.sedona.common.Constructors.geomFromWKT("LINESTRING (1 2, 3 4, 5 6)", 0);
    Geography got = Constructors.geomToGeography(geom);
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    assertEquals(geom.toString(), got.toString());

    geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", 0);
    got = Constructors.geomToGeography(geom);
    assertEquals(geom.toString(), got.toString());
  }

  @Test
  public void LineGeomToGeographyDuplicate() throws Exception {
    Geometry geom =
        org.apache.sedona.common.Constructors.geomFromWKT("LINESTRING (1 2, 3 4, 3 4, 5 6)", 0);
    Geography got = Constructors.geomToGeography(geom);
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    assertEquals("LINESTRING (1 2, 3 4, 5 6)", got.toString());

    geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7), (1 2, 3 4))", 0);
    got = Constructors.geomToGeography(geom);
    assertEquals("MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))", got.toString());

    geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "MULTILINESTRING ((1 2, 3 4), EMPTY, (4 5, 6 7), (1 2, 3 4))", 0);
    got = Constructors.geomToGeography(geom);
    assertEquals("MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))", got.toString());
  }

  @Test
  public void CollGeomToGeography() throws Exception {
    Geometry geom =
        org.apache.sedona.common.Constructors.geomFromWKT(
            "GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (30 40, 50 60, 70 80), POLYGON ((10 10, 20 20, 10 20, 10 10)))",
            0);
    Geography got = Constructors.geomToGeography(geom);
    org.locationtech.jts.io.WKTWriter wktWriter = new org.locationtech.jts.io.WKTWriter();
    wktWriter.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED));
    assertEquals(geom.toString(), got.toString());
  }
}
