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

import static org.junit.Assert.*;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import java.io.IOException;
import org.apache.sedona.common.geography.Constructors;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;

public class WKBGeographyTest {

  private static final double EPS = 1e-10;

  // ─── WKBGeography creation and lazy parsing ──────────────────────────────

  @Test
  public void fromWKB_point_lazyParse() throws ParseException {
    // Create WKB for POINT(30 10) using S2 WKBWriter
    S2Point s2Pt = S2LatLng.fromDegrees(10.0, 30.0).toPoint();
    Geography s2Geog = new SinglePointGeography(s2Pt);
    WKBWriter writer = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN, false);
    byte[] wkb = writer.write(s2Geog);

    WKBGeography geog = WKBGeography.fromWKB(wkb, 4326);
    assertEquals(4326, geog.getSRID());
    assertSame(wkb, geog.getWKBBytes());

    // Accessing JTS should parse lazily
    Geometry jts = geog.getJTSGeometry();
    assertNotNull(jts);
    assertTrue(jts instanceof Point);
    assertEquals(30.0, ((Point) jts).getX(), EPS);
    assertEquals(10.0, ((Point) jts).getY(), EPS);
    assertEquals(4326, jts.getSRID());
  }

  @Test
  public void fromJTS_point() {
    GeometryFactory gf = new GeometryFactory();
    Point jtsPoint = gf.createPoint(new Coordinate(30.0, 10.0));
    jtsPoint.setSRID(4326);

    WKBGeography geog = WKBGeography.fromJTS(jtsPoint);
    assertEquals(4326, geog.getSRID());
    assertNotNull(geog.getWKBBytes());

    // JTS should be cached from construction
    Geometry roundTrip = geog.getJTSGeometry();
    assertSame(jtsPoint, roundTrip);
  }

  @Test
  public void fromJTS_polygon() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))");
    jts.setSRID(4326);

    WKBGeography geog = WKBGeography.fromJTS(jts);
    assertEquals(4326, geog.getSRID());

    // Round-trip through JTS
    Geometry roundTrip = geog.getJTSGeometry();
    assertSame(jts, roundTrip);
  }

  @Test
  public void fromJTS_linestring() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("LINESTRING (1 2, 3 4, 5 6)");

    WKBGeography geog = WKBGeography.fromJTS(jts);
    Geometry roundTrip = geog.getJTSGeometry();
    assertSame(jts, roundTrip);
  }

  @Test
  public void fromJTS_multiPolygon() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts =
        jtsReader.read(
            "MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0)),((20 20,20 30,30 30,30 20,20 20)))");

    WKBGeography geog = WKBGeography.fromJTS(jts);
    Geometry roundTrip = geog.getJTSGeometry();
    assertSame(jts, roundTrip);
  }

  @Test
  public void fromJTS_collection() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))");

    WKBGeography geog = WKBGeography.fromJTS(jts);
    Geometry roundTrip = geog.getJTSGeometry();
    assertSame(jts, roundTrip);
  }

  @Test
  public void fromS2Geography_point() {
    S2Point s2Pt = S2LatLng.fromDegrees(10.0, 30.0).toPoint();
    Geography s2Geog = new SinglePointGeography(s2Pt);
    s2Geog.setSRID(4326);

    WKBGeography geog = WKBGeography.fromS2Geography(s2Geog);
    assertEquals(4326, geog.getSRID());
    assertNotNull(geog.getWKBBytes());

    // S2 should be cached from construction
    Geography roundTrip = geog.getS2Geography();
    assertSame(s2Geog, roundTrip);
  }

  // ─── Lazy S2 delegation ──────────────────────────────────────────────────

  @Test
  public void dimension_triggersS2Parse() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (30 10)");
    WKBGeography geog = WKBGeography.fromJTS(jts);

    // dimension() should work via lazy S2 parse
    assertEquals(0, geog.dimension()); // point = 0

    // linestring = 1
    jts = jtsReader.read("LINESTRING (0 0, 1 1)");
    geog = WKBGeography.fromJTS(jts);
    assertEquals(1, geog.dimension());

    // polygon = 2
    jts = jtsReader.read("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))");
    geog = WKBGeography.fromJTS(jts);
    assertEquals(2, geog.dimension());
  }

  @Test
  public void numShapes_triggersS2Parse() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (30 10)");
    WKBGeography geog = WKBGeography.fromJTS(jts);
    assertTrue(geog.numShapes() >= 1);
  }

  @Test
  public void region_triggersS2Parse() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (30 10)");
    WKBGeography geog = WKBGeography.fromJTS(jts);
    assertNotNull(geog.region());
  }

  @Test
  public void toString_works() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (1 1)");
    WKBGeography geog = WKBGeography.fromJTS(jts);
    assertEquals("POINT (1 1)", geog.toString());
  }

  @Test
  public void toEWKT_works() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (1 1)");
    jts.setSRID(4326);
    WKBGeography geog = WKBGeography.fromJTS(jts);
    assertEquals("SRID=4326; POINT (1 1)", geog.toEWKT());
  }

  // ─── Serializer round-trip ───────────────────────────────────────────────

  @Test
  public void serialize_deserialize_point() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (30 10)");
    jts.setSRID(4326);
    WKBGeography original = WKBGeography.fromJTS(jts);

    byte[] bytes = GeographyWKBSerializer.serialize(original);

    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);
    assertTrue(deserialized instanceof WKBGeography);
    assertEquals(4326, deserialized.getSRID());
    assertEquals("POINT (30 10)", deserialized.toString());
  }

  @Test
  public void serialize_deserialize_polygon_withSRID() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))");
    jts.setSRID(32632);
    WKBGeography original = WKBGeography.fromJTS(jts);

    byte[] bytes = GeographyWKBSerializer.serialize(original);
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);

    assertTrue(deserialized instanceof WKBGeography);
    assertEquals(32632, deserialized.getSRID());
    assertEquals(2, deserialized.dimension());
  }

  @Test
  public void serialize_deserialize_collection() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))");
    WKBGeography original = WKBGeography.fromJTS(jts);

    byte[] bytes = GeographyWKBSerializer.serialize(original);
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);

    assertTrue(deserialized instanceof WKBGeography);
    assertEquals(
        "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))", deserialized.toString());
  }

  @Test
  public void serialize_deserialize_emptyPoint() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT EMPTY");
    WKBGeography original = WKBGeography.fromJTS(jts);

    byte[] bytes = GeographyWKBSerializer.serialize(original);
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);
    assertTrue(deserialized instanceof WKBGeography);
  }

  // ─── Serialize S2 Geography via new serializer ────────────────────────────

  @Test
  public void serialize_s2Geography_producesWKBFormat() throws IOException {
    S2Point s2Pt = S2LatLng.fromDegrees(10.0, 30.0).toPoint();
    Geography s2Geog = new SinglePointGeography(s2Pt);
    s2Geog.setSRID(4326);

    // Serialize S2 Geography (not WKBGeography) via new serializer
    byte[] bytes = GeographyWKBSerializer.serialize(s2Geog);

    // Deserialize and verify
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);
    assertTrue(deserialized instanceof WKBGeography);
    assertEquals(4326, deserialized.getSRID());
    assertEquals("POINT (30 10)", deserialized.toString());
  }

  // ─── SRID preservation ───────────────────────────────────────────────────

  @Test
  public void srid_preservedThroughRoundTrip() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (1 2)");
    jts.setSRID(32632);
    WKBGeography original = WKBGeography.fromJTS(jts);

    byte[] bytes = GeographyWKBSerializer.serialize(original);
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);
    assertEquals(32632, deserialized.getSRID());
  }

  @Test
  public void srid_zero_default() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (1 2)");
    WKBGeography original = WKBGeography.fromJTS(jts);
    assertEquals(0, original.getSRID());

    byte[] bytes = GeographyWKBSerializer.serialize(original);
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);
    assertEquals(0, deserialized.getSRID());
  }

  // ─── Constructor integration ─────────────────────────────────────────────

  @Test
  public void geogFromWKB_returnsWKBGeography() throws ParseException {
    // Create WKB bytes for POINT(30 10)
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (30 10)");
    org.locationtech.jts.io.WKBWriter jtsWkbWriter = new org.locationtech.jts.io.WKBWriter();
    byte[] wkb = jtsWkbWriter.write(jts);

    Geography geog = Constructors.geogFromWKB(wkb, 4326);
    assertTrue(geog instanceof WKBGeography);
    assertEquals(4326, geog.getSRID());
    assertEquals("POINT (30 10)", geog.toString());
  }

  @Test
  public void geogFromWKT_returnsWKBGeography() throws ParseException {
    Geography geog = Constructors.geogFromWKT("POINT (1 1)", 4326);
    assertTrue(geog instanceof WKBGeography);
    assertEquals(4326, geog.getSRID());
    assertEquals("POINT (1 1)", geog.toString());
  }

  @Test
  public void geomToGeography_returnsWKBGeography() {
    GeometryFactory gf = new GeometryFactory();
    Point jtsPoint = gf.createPoint(new Coordinate(30.0, 10.0));
    jtsPoint.setSRID(4326);

    Geography geog = Constructors.geomToGeography(jtsPoint);
    assertTrue(geog instanceof WKBGeography);
    assertEquals(4326, geog.getSRID());
  }

  @Test
  public void geogToGeometry_fastPath() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (30 10)");
    jts.setSRID(4326);
    WKBGeography geog = WKBGeography.fromJTS(jts);

    // geogToGeometry should use getJTSGeometry() fast path
    Geometry result = Constructors.geogToGeometry(geog);
    assertNotNull(result);
    // The result should be the cached JTS object
    assertSame(jts, result);
  }

  @Test
  public void geogFromEWKT_returnsWKBGeography() throws ParseException {
    Geography geog = Constructors.geogFromEWKT("SRID=4269; POINT (1 1)");
    assertTrue(geog instanceof WKBGeography);
    assertEquals(4269, geog.getSRID());
    assertEquals("SRID=4269; POINT (1 1)", geog.toEWKT());
  }

  // ─── Eager ShapeIndex mode ───────────────────────────────────────────────

  @Test
  public void eagerShapeIndex_prebuildsS2AndIndex() throws ParseException {
    boolean original = WKBGeography.isEagerShapeIndex();
    try {
      WKBGeography.setEagerShapeIndex(true);

      // Create WKB bytes for a polygon
      org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
      Geometry jts = jtsReader.read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
      org.locationtech.jts.io.WKBWriter jtsWkbWriter = new org.locationtech.jts.io.WKBWriter();
      byte[] wkb = jtsWkbWriter.write(jts);

      // fromWKB should eagerly build ShapeIndex
      WKBGeography geog = WKBGeography.fromWKB(wkb, 4326);

      // ShapeIndex should already be built — getShapeIndexGeography() should return cached
      ShapeIndexGeography idx = geog.getShapeIndexGeography();
      assertNotNull(idx);
      assertTrue(idx.numShapes() >= 1);

      // S2 Geography should also be cached
      Geography s2 = geog.getS2Geography();
      assertNotNull(s2);
      assertEquals(2, s2.dimension()); // polygon = 2
    } finally {
      WKBGeography.setEagerShapeIndex(original);
    }
  }

  @Test
  public void eagerShapeIndex_defaultIsLazy() {
    assertFalse(WKBGeography.isEagerShapeIndex());
  }

  // ─── EWKB / ISO Z-M decoding ─────────────────────────────────────────────

  /** Builds a PostGIS-style EWKB Point (little-endian) with the SRID flag set. */
  private static byte[] buildEwkbPointWithSRID(double lon, double lat, int srid) {
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(25);
    buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 0x01); // little endian
    buf.putInt(1 | 0x20000000); // POINT with EWKB SRID flag
    buf.putInt(srid); // SRID
    buf.putDouble(lon);
    buf.putDouble(lat);
    return buf.array();
  }

  /** Builds an ISO WKB PointZ (little-endian) with type 1001. */
  private static byte[] buildIsoPointZ(double lon, double lat, double z) {
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(29);
    buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 0x01);
    buf.putInt(1001); // ISO PointZ
    buf.putDouble(lon);
    buf.putDouble(lat);
    buf.putDouble(z);
    return buf.array();
  }

  @Test
  public void ewkbPoint_withSRIDFlag_decodesCorrectly() throws ParseException {
    byte[] ewkb = buildEwkbPointWithSRID(30.0, 10.0, 4326);
    WKBGeography geog = WKBGeography.fromWKB(ewkb, 4326);

    // isPoint() must recognize the base type after stripping the EWKB SRID flag.
    assertTrue(geog.isPoint());
    assertEquals(0, geog.dimension());

    // extractPoint() must skip the 4 SRID bytes; lon/lat should be the original values.
    S2Point p = geog.extractPoint();
    S2LatLng ll = new S2LatLng(p);
    assertEquals(10.0, ll.latDegrees(), EPS);
    assertEquals(30.0, ll.lngDegrees(), EPS);
  }

  @Test
  public void isoPointZ_throwsUnsupported() {
    byte[] wkbZ = buildIsoPointZ(30.0, 10.0, 5.0);
    WKBGeography geog = WKBGeography.fromWKB(wkbZ, 0);
    // isPoint() is safe — just tests base type — but extractPoint/shape must refuse Z/M.
    assertTrue(geog.isPoint());
    assertThrows(UnsupportedOperationException.class, geog::extractPoint);
    assertThrows(UnsupportedOperationException.class, () -> new WkbS2Shape(wkbZ));
  }
}
