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
import static org.junit.Assert.assertTrue;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polyline;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;

public class WKBWriterTest {

  @Test
  public void PointGeographyToHexTest() throws IOException, ParseException {
    // 1) create an S2Point at (lat=10, lon=30)
    S2Point s2Pt = S2LatLng.fromDegrees(10.0, 30.0).toPoint();
    Geography inputGeo = new SinglePointGeography(s2Pt);

    // 2) write it to 2D little‐endian WKB
    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(inputGeo);

    // 3) read it back with WKBReader
    WKBReader reader = new WKBReader();
    PointGeography outputGeo = (PointGeography) reader.read(wkb);

    // 4) extract the decoded S2Point + verify lat/lng round‐trip exactly
    S2Point decoded = outputGeo.points.get(0);
    S2LatLng decodedLL = S2LatLng.fromPoint(decoded);
    assertEquals(10.0, decodedLL.latDegrees(), 1e-12);
    assertEquals(30.0, decodedLL.lngDegrees(), 1e-12);
  }

  /** Same idea for a simple LINESTRING: hex-encode and compare. */
  @Test
  public void PolylineGeographyToHexTest() throws IOException, ParseException {
    // 1) build a simple S2Polyline LINESTRING(30 10, 12 42)
    List<S2Point> pts =
        List.of(
            S2LatLng.fromDegrees(10.0, 30.0).toPoint(), S2LatLng.fromDegrees(42.0, 12.0).toPoint());
    Geography inputLine = new SinglePolylineGeography(new S2Polyline(pts));

    // 2) write it to 2D little‐endian WKB
    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(inputLine);

    // 3) read it back
    WKBReader reader = new WKBReader();
    PolylineGeography outputLine = (PolylineGeography) reader.read(wkb);

    // 4) verify point‐by‐point round‐trip
    List<S2Polyline> polylines = outputLine.getPolylines();
    List<S2Point> decoded = new ArrayList<>();
    for (S2Polyline polyline : polylines) {
      decoded.addAll(polyline.vertices());
    }
    assertEquals(pts.size(), decoded.size());
    for (int i = 0; i < pts.size(); i++) {
      S2LatLng origLL = S2LatLng.fromPoint(pts.get(i));
      S2LatLng decLL = S2LatLng.fromPoint(decoded.get(i));
      assertEquals(origLL.latDegrees(), decLL.latDegrees(), 1e-12);
      assertEquals(origLL.lngDegrees(), decLL.lngDegrees(), 1e-12);
    }
  }

  @Test
  public void MultiPointTest() throws ParseException, IOException {
    String wkt = "MULTIPOINT ((10 40), (40 30))";
    WKTReader reader = new WKTReader();
    Geography geo = reader.read(wkt);

    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(geo);
    WKBReader readerWKB = new WKBReader();
    Geography geoWKB = readerWKB.read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, geoWKB));
  }

  @Test
  public void MultiPolylineTest() throws ParseException, IOException {
    String wkt = "MULTILINESTRING((0 1,2 3),(4 5,6 7))";
    WKTReader reader = new WKTReader();
    Geography geo = reader.read(wkt);

    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(geo);
    WKBReader readerWKB = new WKBReader();
    Geography geoWKB = readerWKB.read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, geoWKB));
  }

  @Test
  public void MultiPolygonTest() throws ParseException, IOException {
    String wkt =
        "MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),((-9 0,-9 10,-1 10,-1 0,-9 0)))";
    WKTReader reader = new WKTReader();
    Geography geo = reader.read(wkt);

    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(geo);
    WKBReader readerWKB = new WKBReader();
    Geography geoWKB = readerWKB.read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, geoWKB));
  }

  /**
   * Verify that {@link WKBWriter#write(Geography)} emits OGC-conformant POLYGON WKB: each ring's
   * point count is N+1 with the closing vertex equal to the first. S2Loop stores N distinct
   * vertices internally (closure is implicit) but the OGC WKB spec requires the closing duplicate;
   * dropping it produces non-OGC bytes that strict downstream readers (e.g., {@code
   * GeoArrowWKBReader} used by sedona-db's s2geography kernels) treat as a degenerate / open
   * ring, which makes {@code ST_Intersects} return {@code true} against every geometry.
   */
  @Test
  public void PolygonRingClosureTest() throws ParseException, IOException {
    // Single-ring polygon: 4 distinct corners in WKT, must serialize as 5 points.
    String wkt = "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";
    Geography geo = new WKTReader().read(wkt);

    byte[] wkb = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geo);
    ByteBuffer bb = ByteBuffer.wrap(wkb).order(ByteOrder.LITTLE_ENDIAN);

    // Header: 1 byte order + 4 type + 4 num_rings + 4 num_points
    assertEquals("byte order LE", 0x01, bb.get(0));
    assertEquals("type POLYGON", 3, bb.getInt(1));
    assertEquals("num rings", 1, bb.getInt(5));
    int numPoints = bb.getInt(9);
    assertEquals("ring must be OGC-closed (N+1 points with last==first)", 5, numPoints);

    // Decode all 5 (lon, lat) pairs and verify last == first.
    int coordOffset = 13;
    double firstLon = bb.getDouble(coordOffset);
    double firstLat = bb.getDouble(coordOffset + 8);
    double lastLon = bb.getDouble(coordOffset + (numPoints - 1) * 16);
    double lastLat = bb.getDouble(coordOffset + (numPoints - 1) * 16 + 8);
    assertEquals("closing vertex lon", firstLon, lastLon, 0.0);
    assertEquals("closing vertex lat", firstLat, lastLat, 0.0);
    assertEquals(wkb.length, coordOffset + numPoints * 16);

    // Verify that the closed WKB round-trips through the reader (sanity: the
    // reader must tolerate the N+1 form we now emit).
    WKBReader readerWKB = new WKBReader();
    Geography decoded = readerWKB.read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, decoded));
  }

  /**
   * Same invariant for {@code MULTIPOLYGON} (per-ring N+1 closure inside each polygon).
   */
  @Test
  public void MultiPolygonRingClosureTest() throws ParseException, IOException {
    String wkt =
        "MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),((-9 0,-9 10,-1 10,-1 0,-9 0)))";
    Geography geo = new WKTReader().read(wkt);
    byte[] wkb = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geo);

    // Crawl the WKB: outer MULTIPOLYGON header is 1+4+4 = 9 bytes.
    // Each inner POLYGON: 1 byte order + 4 type + 4 num_rings + (per ring: 4 num_points + 16*N pts).
    // We assert every ring's num_points >= 4 AND its last point equals its first.
    ByteBuffer bb = ByteBuffer.wrap(wkb).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(0x01, bb.get(0));
    assertEquals(6 /* MULTIPOLYGON */, bb.getInt(1));
    int numPolys = bb.getInt(5);
    assertEquals(2, numPolys);

    int p = 9;
    for (int pi = 0; pi < numPolys; pi++) {
      assertEquals("inner polygon byte order LE", 0x01, bb.get(p));
      assertEquals("inner geometry type POLYGON", 3, bb.getInt(p + 1));
      int numRings = bb.getInt(p + 5);
      p += 9;
      for (int ri = 0; ri < numRings; ri++) {
        int n = bb.getInt(p);
        p += 4;
        assertTrue(
            "ring " + ri + " of polygon " + pi + " must have >=4 points (got " + n + ")", n >= 4);
        double fx = bb.getDouble(p);
        double fy = bb.getDouble(p + 8);
        double lx = bb.getDouble(p + (n - 1) * 16);
        double ly = bb.getDouble(p + (n - 1) * 16 + 8);
        assertEquals("closing vertex lon (polygon " + pi + " ring " + ri + ")", fx, lx, 0.0);
        assertEquals("closing vertex lat (polygon " + pi + " ring " + ri + ")", fy, ly, 0.0);
        p += n * 16;
      }
    }
    assertEquals("consumed entire WKB", wkb.length, p);

    // Sanity round-trip.
    Geography decoded = new WKBReader().read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, decoded));
  }

  @Test
  public void CollectionTest() throws ParseException, IOException {
    String wkt =
        "GEOMETRYCOLLECTION(POINT(0 1),POINT(0 1),POINT(2 3),LINESTRING(2 3,4 5),LINESTRING(0 1,2 3),LINESTRING(4 5,6 7),POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),POLYGON((-9 0,-9 10,-1 10,-1 0,-9 0)))";

    WKTReader reader = new WKTReader();
    Geography geo = reader.read(wkt);
    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(geo);
    WKBReader readerWKB = new WKBReader();
    Geography geoWKB = readerWKB.read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, geoWKB));
  }

  @Test
  public void emptyTest() throws ParseException, IOException {
    String wkt = "POINT EMPTY";

    WKTReader reader = new WKTReader();
    Geography geo = reader.read(wkt);

    WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
    byte[] wkb = writer.write(geo);
    WKBReader readerWKB = new WKBReader();
    Geography geoWKB = readerWKB.read(wkb);
    assertEquals(0, TestHelper.compareTo(geo, geoWKB));
  }
}
