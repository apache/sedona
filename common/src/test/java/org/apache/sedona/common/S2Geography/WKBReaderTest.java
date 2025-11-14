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

import com.google.common.geometry.S2Point;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;

public class WKBReaderTest {
  @Test
  public void PointGeographyTest() throws ParseException {
    // WKB for POINT (30 10), little-endian
    byte[] wkb =
        new byte[] {
          0x01, // little endian
          0x01,
          0x00,
          0x00,
          0x00, // type = Point (1)
          // X = 30.0
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x3E,
          0x40,
          // Y = 10.0
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x24,
          0x40
        };

    WKBReader reader = new WKBReader();
    Geography geo = reader.read(wkb);

    // Kind should be POINT
    Assert.assertEquals(Geography.GeographyKind.SINGLEPOINT, geo.kind);

    // Extract the S2Point
    S2Point p = geo.shape(0).chain(0).get(0);
    Assert.assertNotNull(p);

    // Convert to WKT via your TestHelper (or manually check coords)
    String wkt = TestHelper.toPointWkt(p);
    // The WKTWriter default formatting uses 6 decimal places
    String expected = "POINT (30.000000 10.000000)";
    Assert.assertEquals(expected, wkt);
  }

  @Test
  public void PolylineGeographyTest() throws ParseException {
    // WKB for LINESTRING (30 10, 12 42), little-endian
    byte[] wkb =
        new byte[] {
          0x01, // little endian
          0x02,
          0x00,
          0x00,
          0x00, // type = LineString (2)
          0x02,
          0x00,
          0x00,
          0x00, // num points = 2
          // Point 1: X = 30.0, Y = 10.0
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x3E,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x24,
          0x40,
          // Point 2: X = 12.0, Y = 42.0
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x28,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x45,
          0x40
        };

    WKBReader reader = new WKBReader();
    Geography geo = reader.read(wkb);

    // Expect a POLYLINE geometry
    Assert.assertEquals(Geography.GeographyKind.SINGLEPOLYLINE, geo.kind);

    // Extract the two S2Points
    List<S2Point> pts = geo.shape(0).chain(0);
    Assert.assertEquals(2, pts.size());

    // Check their WKT
    String wkt1 = TestHelper.toPointWkt(pts.get(0));
    String wkt2 = TestHelper.toPointWkt(pts.get(1));
    Assert.assertEquals("POINT (30.000000 10.000000)", wkt1);
    Assert.assertEquals("POINT (12.000000 42.000000)", wkt2);
  }

  @Test
  public void PolygonGeographyTest() throws ParseException {
    // WKB for POLYGON ((35 10,45 45,15 40,10 20,35 10),(20 30,35 35,30 20,20 30)), little-endian
    byte[] wkb =
        new byte[] {
          0x01, // little endian
          0x03,
          0x00,
          0x00,
          0x00, // type = Polygon (3)
          0x02,
          0x00,
          0x00,
          0x00, // num rings = 2

          // Ring 1: 5 points
          0x05,
          0x00,
          0x00,
          0x00,
          // (35.0, 10.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0x80,
          0x41,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x24,
          0x40,
          // (45.0, 45.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0x80,
          0x46,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0x80,
          0x46,
          0x40,
          // (15.0, 40.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x2E,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x44,
          0x40,
          // (10.0, 20.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x24,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x34,
          0x40,
          // (35.0, 10.0) closing
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0x80,
          0x41,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x24,
          0x40,

          // Ring 2: 4 points
          0x04,
          0x00,
          0x00,
          0x00,
          // (20.0, 30.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x34,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x3E,
          0x40,
          // (35.0, 35.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0x80,
          0x41,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0x80,
          0x41,
          0x40,
          // (30.0, 20.0)
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x3E,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x34,
          0x40,
          // (20.0, 30.0) closing
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x34,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x3E,
          0x40
        };

    WKBReader reader = new WKBReader();
    Geography geo = reader.read(wkb);

    // Expect a POLYGON geometry
    Assert.assertEquals(Geography.GeographyKind.POLYGON, geo.kind);

    // Outer ring: 4 points
    List<S2Point> outer = geo.shape(0).chain(0);
    Assert.assertEquals(4, outer.size());
    Assert.assertEquals("POINT (35.000000 10.000000)", TestHelper.toPointWkt(outer.get(0)));
    Assert.assertEquals("POINT (45.000000 45.000000)", TestHelper.toPointWkt(outer.get(1)));
    Assert.assertEquals("POINT (15.000000 40.000000)", TestHelper.toPointWkt(outer.get(2)));
    Assert.assertEquals("POINT (10.000000 20.000000)", TestHelper.toPointWkt(outer.get(3)));

    // Hole ring: 3 points
    List<S2Point> hole = geo.shape(0).chain(1);
    Assert.assertEquals(3, hole.size());
    Assert.assertEquals("POINT (20.000000 30.000000)", TestHelper.toPointWkt(hole.get(0)));
    Assert.assertEquals("POINT (35.000000 35.000000)", TestHelper.toPointWkt(hole.get(1)));
    Assert.assertEquals("POINT (30.000000 20.000000)", TestHelper.toPointWkt(hole.get(2)));
  }

  @Test
  public void MultiPointTest() throws ParseException {
    String hexStr =
        "0104000000020000006901000000000000000000F03F000000000000F03F690100000000000000000000400000000000000040";
    String wkt = "MULTIPOINT((1 1),(2 2))";
    TestHelper.checkWKBGeography(hexStr, wkt);
  }

  @Test
  public void MultiPolylineTest() throws ParseException {
    String hexStr =
        "0105000020E6100000020000000102000000020000000000000000000000000000000000F03F000000000000004000000000000008400102000000020000000000000000001040000000000000144000000000000018400000000000001C40";
    String wkt = "MULTILINESTRING((0 1,2 3),(4 5,6 7))";
    TestHelper.checkWKBGeography(hexStr, wkt);
  }

  @Test
  public void MultiPolygonTest() throws ParseException {
    String hexStr =
        "0106000020E6100000020000000103000020E61000000200000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000002240000000000000224000000000000022400000000000002240000000000000F03F000000000000F03F000000000000F03F0103000020E6100000010000000500000000000000000022C0000000000000000000000000000022C00000000000002440000000000000F0BF0000000000002440000000000000F0BF000000000000000000000000000022C00000000000000000";
    String multiPolygonWkt =
        "MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),((-9 0,-9 10,-1 10,-1 0,-9 0)))";
    WKBReader reader = new WKBReader();
    Geography geo = reader.read(WKBReader.hexToBytes(hexStr));
    // Expect a MULTIPOLYGON geometry
    Assert.assertEquals(Geography.GeographyKind.MULTIPOLYGON, geo.kind);

    TestHelper.checkWKBGeography(hexStr, multiPolygonWkt);
  }

  @Test
  public void CollectionGeographyTest() throws ParseException {
    String hexStr =
        "0107000020E6100000090000000101000020E61000000000000000000000000000000000F03F0101000020E61000000000000000000000000000000000F03F0101000020E6100000000000000000004000000000000008400102000020E61000000200000000000000000000400000000000000840000000000000104000000000000014400102000020E6100000020000000000000000000000000000000000F03F000000000000004000000000000008400102000020E6100000020000000000000000001040000000000000144000000000000018400000000000001C400103000020E61000000200000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000002240000000000000224000000000000022400000000000002240000000000000F03F000000000000F03F000000000000F03F0103000020E61000000200000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000002240000000000000224000000000000022400000000000002240000000000000F03F000000000000F03F000000000000F03F0103000020E6100000010000000500000000000000000022C0000000000000000000000000000022C00000000000002440000000000000F0BF0000000000002440000000000000F0BF000000000000000000000000000022C00000000000000000";
    String geometryCollectionWkt =
        "GEOMETRYCOLLECTION(POINT(0 1),POINT(0 1),POINT(2 3),LINESTRING(2 3,4 5),LINESTRING(0 1,2 3),LINESTRING(4 5,6 7),POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1)),POLYGON((-9 0,-9 10,-1 10,-1 0,-9 0)))";
    TestHelper.checkWKBGeography(hexStr, geometryCollectionWkt);
  }

  @Test
  public void EmptyTest() throws ParseException {
    TestHelper.checkWKBGeography("0101000000", "POINT EMPTY");
    TestHelper.checkWKBGeography("010300000000000000", "POLYGON EMPTY");
    TestHelper.checkWKBGeography("010200000000000000", "LINESTRING EMPTY");
  }
}
