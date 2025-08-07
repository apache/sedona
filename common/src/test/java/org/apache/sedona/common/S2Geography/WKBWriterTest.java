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

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polyline;
import java.io.IOException;
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
