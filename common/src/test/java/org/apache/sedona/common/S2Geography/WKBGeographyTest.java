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

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
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
    assertEquals(30.0, geog.getPointX(), EPS);
    assertEquals(10.0, geog.getPointY(), EPS);

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
    WKBGeography geogWithoutSrid = WKBGeography.fromJTS(jts);
    assertEquals("POINT (1 1)", geogWithoutSrid.toEWKT());

    jts.setSRID(4326);
    WKBGeography geog = WKBGeography.fromJTS(jts);
    assertEquals("SRID=4326; POINT (1 1)", geog.toEWKT());
  }

  @Test
  public void defaultTextRendering_preservesStoredCoordinatesAndSRID() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (-122.4194 37.7749)");
    byte[] wkb = new org.locationtech.jts.io.WKBWriter().write(jts);
    WKBGeography geog = WKBGeography.fromWKB(wkb, 4326);

    assertEquals("POINT (-122.4194 37.7749)", geog.toString());
    assertEquals("SRID=4326; POINT (-122.4194 37.7749)", geog.toEWKT());
  }

  @Test
  public void defaultTextRendering_preservesRepeatedCoordinates() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("LINESTRING (0 0, 1.25 2.5, 1.25 2.5, 3 4)");
    byte[] wkb = new org.locationtech.jts.io.WKBWriter().write(jts);
    WKBGeography geog = WKBGeography.fromWKB(wkb, 4326);

    assertEquals("LINESTRING (0 0, 1.25 2.5, 1.25 2.5, 3 4)", geog.toString());
    assertEquals("SRID=4326; LINESTRING (0 0, 1.25 2.5, 1.25 2.5, 3 4)", geog.toEWKT());
  }

  @Test
  public void explicitPrecisionRendering_usesRequestedS2Formatting() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT (-122.4194 37.7749)");
    byte[] wkb = new org.locationtech.jts.io.WKBWriter().write(jts);
    WKBGeography geog = WKBGeography.fromWKB(wkb, 4326);
    PrecisionModel fixed = new PrecisionModel(PrecisionModel.FIXED);

    assertEquals("POINT (-122.4 37.8)", geog.toString(fixed));
    assertEquals("SRID=4326; POINT (-122.4 37.8)", geog.toEWKT(fixed));
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
  public void serialize_deserialize_repeatedLinePreservesStructuralSqlText()
      throws IOException, ParseException {
    Geometry jts = new org.locationtech.jts.io.WKTReader().read("LINESTRING (0 0, 1 0, 1 0, 2 0)");
    jts.setSRID(4326);

    Geography deserialized =
        GeographyWKBSerializer.deserialize(
            GeographyWKBSerializer.serialize(WKBGeography.fromJTS(jts)));
    // Populate the normalized operational S2 cache before exercising structural accessors.
    ((WKBGeography) deserialized).getS2Geography();

    String expected = "LINESTRING (0 0, 1 0, 1 0, 2 0)";
    assertEquals(expected, Functions.asText(deserialized));
    assertEquals("SRID=4326; " + expected, Functions.asEWKT(deserialized));
  }

  @Test
  public void emptyLineRetainsLegacyS2Text() throws ParseException {
    Geography geography = Constructors.geogFromWKT("LINESTRING EMPTY", 4326);

    assertEquals("LINESTRING EMPTY", geography.toString());
    assertEquals("SRID=4326; LINESTRING EMPTY", geography.toEWKT());
    assertEquals("SRID=4326; LINESTRING EMPTY", Functions.asEWKT(geography));
    assertTrue(((WKBGeography) geography).getJTSGeometry().isEmpty());
  }

  @Test
  public void nonRepeatedLineRetainsS2TextNormalization() throws ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts =
        jtsReader.read(
            "LINESTRING (-2.1047439575195312 -0.354827880859375, "
                + "-1.49606454372406 -0.6676061153411865)");
    Geography geography = WKBGeography.fromJTS(jts);

    assertEquals(
        "LINESTRING (-2.1047439575195317 -0.35482788085937506, "
            + "-1.4960645437240603 -0.6676061153411864)",
        geography.toString(new PrecisionModel(1e16)));
  }

  @Test
  public void serialize_deserialize_emptyPoint() throws IOException, ParseException {
    org.locationtech.jts.io.WKTReader jtsReader = new org.locationtech.jts.io.WKTReader();
    Geometry jts = jtsReader.read("POINT EMPTY");
    WKBGeography original = WKBGeography.fromJTS(jts);

    byte[] bytes = GeographyWKBSerializer.serialize(original);
    Geography deserialized = GeographyWKBSerializer.deserialize(bytes);
    assertTrue(deserialized instanceof WKBGeography);
    assertNull(((WKBGeography) deserialized).getPointX());
    assertNull(((WKBGeography) deserialized).getPointY());
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
    assertEquals("POINT (30 10)", deserialized.toString(new PrecisionModel(PrecisionModel.FIXED)));
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
    assertEquals("POINT (1 1)", geog.toString(new PrecisionModel(PrecisionModel.FIXED)));
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
    assertEquals("SRID=4269; POINT (1 1)", geog.toEWKT(new PrecisionModel(PrecisionModel.FIXED)));
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

  /** Builds a PostGIS-style EWKB PointZ (little-endian) using the Z flag. */
  private static byte[] buildEwkbPointZ(double lon, double lat, double z) {
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(29);
    buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 0x01);
    buf.putInt(1 | 0x80000000); // POINT with EWKB Z flag
    buf.putDouble(lon);
    buf.putDouble(lat);
    buf.putDouble(z);
    return buf.array();
  }

  /** Builds an ISO WKB LineStringZ (little-endian) with type 1002. */
  private static byte[] buildIsoLineStringZ() {
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(57);
    buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 0x01);
    buf.putInt(1002); // ISO LineStringZ
    buf.putInt(2);
    buf.putDouble(30.0);
    buf.putDouble(10.0);
    buf.putDouble(5.0);
    buf.putDouble(31.0);
    buf.putDouble(11.0);
    buf.putDouble(6.0);
    return buf.array();
  }

  @Test
  public void ewkbPoint_withSRIDFlag_decodesCorrectly() throws ParseException {
    byte[] ewkb = buildEwkbPointWithSRID(30.0, 10.0, 4326);
    WKBGeography geog = WKBGeography.fromWKB(ewkb, 4326);

    // isPoint() must recognize the base type after stripping the EWKB SRID flag.
    assertTrue(geog.isPoint());
    assertEquals(0, geog.dimension());
    assertEquals(30.0, geog.getPointX(), EPS);
    assertEquals(10.0, geog.getPointY(), EPS);

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
    // isPoint() is safe — just tests base type — but the explicitly XY-only accessors refuse Z/M.
    assertTrue(geog.isPoint());
    assertEquals(30.0, geog.getPointX(), EPS);
    assertEquals(10.0, geog.getPointY(), EPS);
    assertThrows(UnsupportedOperationException.class, geog::extractPoint);
    assertThrows(UnsupportedOperationException.class, () -> new WkbS2Shape(wkbZ));
  }

  @Test
  public void higherDimensionalSimpleWkb_sphericalAccessFallsBackToFullReader() {
    for (byte[] wkb :
        new byte[][] {
          buildIsoPointZ(30.0, 10.0, 5.0), buildEwkbPointZ(30.0, 10.0, 5.0), buildIsoLineStringZ()
        }) {
      WKBGeography geog = WKBGeography.fromWKB(wkb, 0);
      assertNotNull(geog.getShapeIndexGeography().shape(0));
      assertNotNull(geog.shape(0));
      assertNotNull(geog.region());
      List<S2CellId> cellIds = new ArrayList<>();
      geog.getCellUnionBound(cellIds);
      assertFalse(cellIds.isEmpty());
    }
  }
}
