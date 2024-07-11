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
package org.apache.sedona.common;

import static org.junit.Assert.*;

import org.apache.sedona.common.utils.GeomUtils;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;

public class ConstructorsTest {

  @Test
  public void geomFromWKT() throws ParseException {
    assertNull(Constructors.geomFromWKT(null, 0));

    Geometry geom = Constructors.geomFromWKT("POINT (1 1)", 0);
    assertEquals(0, geom.getSRID());
    assertEquals("POINT (1 1)", geom.toText());

    geom = Constructors.geomFromWKT("POINT (1 1)", 3006);
    assertEquals(3006, geom.getSRID());
    assertEquals("POINT (1 1)", geom.toText());

    ParseException invalid =
        assertThrows(ParseException.class, () -> Constructors.geomFromWKT("not valid", 0));
    assertEquals("Unknown geometry type: NOT (line 1)", invalid.getMessage());
  }

  @Test
  public void geomFromEWKT() throws ParseException {
    assertNull(Constructors.geomFromEWKT(null));

    Geometry geom = Constructors.geomFromEWKT("POINT (1 1)");
    assertEquals(0, geom.getSRID());
    assertEquals("POINT (1 1)", geom.toText());

    geom = Constructors.geomFromEWKT("SRID=4269; POINT (1 1)");
    assertEquals(4269, geom.getSRID());
    assertEquals("POINT (1 1)", geom.toText());

    geom = Constructors.geomFromEWKT("SRID=4269;POINT (1 1)");
    assertEquals(4269, geom.getSRID());
    assertEquals("POINT (1 1)", geom.toText());

    ParseException invalid =
        assertThrows(ParseException.class, () -> Constructors.geomFromEWKT("not valid"));
    assertEquals("Unknown geometry type: NOT (line 1)", invalid.getMessage());
  }

  @Test
  public void geomFromWKB() throws ParseException {
    GeometryFactory factory = new GeometryFactory();
    Geometry geom = factory.createPoint(new Coordinate(1, 2));

    // Test WKB without SRID
    WKBWriter wkbWriter = new WKBWriter();
    byte[] wkb = wkbWriter.write(geom);
    Geometry result = Constructors.geomFromWKB(wkb);
    assertEquals(geom, result);
    assertEquals(0, result.getSRID());
    assertEquals(0, result.getFactory().getSRID());

    // Test specifying SRID
    result = Constructors.geomFromWKB(wkb, 1000);
    assertEquals(geom, result);
    assertEquals(1000, result.getSRID());
    assertEquals(1000, result.getFactory().getSRID());

    // Test EWKB with SRID
    wkbWriter = new WKBWriter(2, true);
    geom.setSRID(2000);
    wkb = wkbWriter.write(geom);
    result = Constructors.geomFromWKB(wkb);
    assertEquals(geom, result);
    assertEquals(2000, result.getSRID());
    assertEquals(2000, result.getFactory().getSRID());

    // Test overriding SRID
    result = Constructors.geomFromWKB(wkb, 3000);
    assertEquals(geom, result);
    assertEquals(3000, result.getSRID());
    assertEquals(3000, result.getFactory().getSRID());
    result = Constructors.geomFromWKB(wkb, 0);
    assertEquals(geom, result);
    assertEquals(0, result.getSRID());
    assertEquals(0, result.getFactory().getSRID());
  }

  @Test
  public void mLineFromWKT() throws ParseException {
    assertNull(Constructors.mLineFromText(null, 0));
    assertNull(Constructors.mLineFromText("POINT (1 1)", 0));
    Geometry geom = Constructors.mLineFromText("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", 0);
    assertEquals(0, geom.getSRID());
    assertEquals("MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))", geom.toText());

    geom = Constructors.mLineFromText("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", 3306);
    assertEquals(3306, geom.getSRID());
    assertEquals("MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))", geom.toText());

    ParseException invalid =
        assertThrows(
            ParseException.class,
            () -> Constructors.mLineFromText("MULTILINESTRING(not valid)", 0));
    assertEquals("Expected EMPTY or ( but found 'not' (line 1)", invalid.getMessage());
  }

  @Test
  public void mPolyFromWKT() throws ParseException {
    assertNull(Constructors.mPolyFromText(null, 0));
    assertNull(Constructors.mPolyFromText("POINT (1 1)", 0));
    Geometry geom =
        Constructors.mPolyFromText(
            "MULTIPOLYGON(((0 0 ,20 0 ,20 20 ,0 20 ,0 0 ),(5 5 ,5 7 ,7 7 ,7 5 ,5 5)))", 0);
    assertEquals(0, geom.getSRID());
    assertEquals(
        "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))", geom.toText());

    geom =
        Constructors.mPolyFromText(
            "MULTIPOLYGON(((0 0 ,20 0 ,20 20 ,0 20 ,0 0 ),(5 5 ,5 7 ,7 7 ,7 5 ,5 5)))", 3306);
    assertEquals(3306, geom.getSRID());
    assertEquals(
        "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))", geom.toText());

    IllegalArgumentException invalid =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                Constructors.mPolyFromText(
                    "MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))", 0));
    assertEquals("Points of LinearRing do not form a closed linestring", invalid.getMessage());

    ParseException parseException =
        assertThrows(
            ParseException.class, () -> Constructors.mPolyFromText("MULTIPOLYGON(not valid)", 0));
    assertEquals("Expected EMPTY or ( but found 'not' (line 1)", parseException.getMessage());
  }

  @Test
  public void mPointFromText() throws ParseException {
    assertNull(Constructors.mPointFromText(null, 0));
    assertNull(
        Constructors.mPointFromText(
            "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))", 0));
    Geometry geom = Constructors.mPointFromText("MULTIPOINT ((10 10), (20 20), (30 30))", 0);
    assertEquals(0, geom.getSRID());
    assertEquals("MULTIPOINT ((10 10), (20 20), (30 30))", geom.toText());

    geom = Constructors.mPointFromText("MULTIPOINT ((10 10), (20 20), (30 30))", 3306);
    assertEquals(3306, geom.getSRID());
    assertEquals("MULTIPOINT ((10 10), (20 20), (30 30))", geom.toText());
  }

  @Test
  public void geomCollFromText() throws ParseException {
    assertNull(Constructors.geomCollFromText(null, 0));
    assertNull(
        Constructors.geomCollFromText(
            "MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))", 0));
    Geometry geom =
        Constructors.geomCollFromText(
            "GEOMETRYCOLLECTION (POINT (10 20),LINESTRING (30 40, 50 60, 70 80),POLYGON ((10 10, 20 20, 10 20, 10 10)))",
            0);
    assertEquals(0, geom.getSRID());
    assertEquals(
        "GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (30 40, 50 60, 70 80), POLYGON ((10 10, 20 20, 10 20, 10 10)))",
        geom.toText());

    geom =
        Constructors.geomCollFromText(
            "GEOMETRYCOLLECTION (POINT (10 20),LINESTRING (30 40, 50 60, 70 80),POLYGON ((10 10, 20 20, 10 20, 10 10)))",
            3306);
    assertEquals(3306, geom.getSRID());
    assertEquals(
        "GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (30 40, 50 60, 70 80), POLYGON ((10 10, 20 20, 10 20, 10 10)))",
        geom.toText());

    ParseException parseException =
        assertThrows(
            ParseException.class,
            () -> Constructors.geomCollFromText("GEOMETRYCOLLECTION (POLYGON(not valid))", 0));
    assertEquals("Expected EMPTY or ( but found 'not' (line 1)", parseException.getMessage());
  }

  @Test
  public void point() {
    Geometry point = Constructors.point(1.0d, 2.0d);

    assertTrue(point instanceof Point);
    assertEquals(0, point.getSRID());
    assertEquals("POINT (1 2)", point.toText());
  }

  @Test
  public void pointFromGeoHash() {
    String point = Functions.asWKT(Constructors.pointFromGeoHash("9qqj7nmxncgyy4d0dbxqz0", 4));
    assertEquals("POINT (-115.13671875 36.123046875)", point);

    point = Functions.asWKT(Constructors.pointFromGeoHash("9qqj7nmxncgyy4d0dbxqz0", null));
    assertEquals("POINT (-115.17281600000001 36.11464599999999)", point);

    point = Functions.asWKT(Constructors.pointFromGeoHash("9qqj7nmxncgyy4d0dbxqz0", 1));
    assertEquals("POINT (-112.5 22.5)", point);
  }

  @Test
  public void makePointM() {
    Geometry point = Constructors.makePointM(1, 2, 3);

    assertTrue(point instanceof Point);
    String actual = Functions.asWKT(point);
    String expected = "POINT M(1 2 3)";
    assertEquals(expected, actual);
  }

  @Test
  public void point2d() {
    Geometry point = Constructors.makePoint(1.0d, 2.0d, null, null);

    assertTrue(point instanceof Point);
    assertEquals(0, point.getSRID());
    assertEquals("POINT (1 2)", Functions.asWKT(point));
  }

  @Test
  public void point3DZ() {
    Geometry point = Constructors.makePoint(1.0d, 2.0d, 3.0d, null);

    assertTrue(point instanceof Point);
    assertEquals(0, point.getSRID());
    assertEquals("POINT Z(1 2 3)", Functions.asWKT(point));
  }

  @Test
  public void point4DZM() {
    Geometry point = Constructors.makePoint(1.0d, 2.0d, 3.0d, 4.0d);

    assertTrue(point instanceof Point);
    assertTrue(GeomUtils.isMeasuredGeometry(point));
    assertEquals(0, point.getSRID());
    assertEquals("POINT ZM(1 2 3 4)", Functions.asWKT(point));
  }

  @Test
  public void pointZ() {
    Geometry point = Constructors.pointZ(0.0d, 1.0d, 2.0d, 4326);

    assertTrue(point instanceof Point);
    assertEquals(4326, point.getSRID());
    assertEquals("POINT Z(0 1 2)", Functions.asWKT(point));
  }

  @Test
  public void pointM() {
    Geometry point = Constructors.pointM(0.0d, 1.0d, 2.0d, 4326);

    assertTrue(point instanceof Point);
    assertEquals(4326, point.getSRID());
    assertEquals("SRID=4326;POINT ZM(0 1 0 2)", Functions.asEWKT(point));
  }

  @Test
  public void pointZM() {
    Geometry point = Constructors.pointZM(0.0d, 1.0d, 2.0d, 10.0, 4326);

    assertTrue(point instanceof Point);
    assertEquals(4326, point.getSRID());
    assertEquals("POINT ZM(0 1 2 10)", Functions.asWKT(point));
  }
}
