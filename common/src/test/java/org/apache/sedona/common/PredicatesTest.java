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

import static org.apache.sedona.common.Constructors.geomFromEWKT;
import static org.apache.sedona.common.Functions.crossesDateLine;
import static org.junit.Assert.*;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

public class PredicatesTest extends TestBase {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @Test
  public void testDWithinSuccess() {
    Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2));
    double distance = 1.42;
    boolean actual = Predicates.dWithin(point1, point2, distance);
    assertTrue(actual);
  }

  @Test
  public void testDWithinFailure() {
    Geometry polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 1, 1, 1, 1, 0, 0, 0));
    Geometry polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 0, 3, 3, 6, 3, 6, 0, 3, 0));

    double distance = 1.2;
    boolean actual = Predicates.dWithin(polygon1, polygon2, distance);
    assertFalse(actual);
  }

  @Test
  public void testDWithinGeomCollection() {
    Geometry polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 1, 1, 1, 1, 0, 0, 0));
    Geometry polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 0, 3, 3, 6, 3, 6, 0, 3, 0));
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(1.1, 0));
    Geometry geometryCollection =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {polygon2, point});

    double distance = 1.2;
    boolean actual = Predicates.dWithin(polygon1, geometryCollection, distance);
    assertTrue(actual);
  }

  @Test
  public void testDWithinSpheroid() {
    Geometry seattlePoint = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.335167, 47.608013));
    Geometry newYorkPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(-73.935242, 40.730610));

    double distance = 4000 * 1e3; // distance between NY and Seattle is less than 4000 km
    boolean actual = Predicates.dWithin(newYorkPoint, seattlePoint, distance, true);
    assertTrue(actual);
  }

  @Test
  public void testRelateString() throws ParseException {
    Geometry geom1 = geomFromEWKT("POINT(1 2)");
    Geometry geom2 = Functions.buffer(geomFromEWKT("POINT(1 2)"), 2);
    String actual = Predicates.relate(geom1, geom2);
    assertEquals("0FFFFF212", actual);

    geom1 = geomFromEWKT("LINESTRING(1 2, 3 4)");
    geom2 = geomFromEWKT("LINESTRING(5 6, 7 8)");
    actual = Predicates.relate(geom1, geom2);
    assertEquals("FF1FF0102", actual);

    geom1 = geomFromEWKT("LINESTRING (1 1, 5 5)");
    geom2 = geomFromEWKT("POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))");
    actual = Predicates.relate(geom1, geom2);
    assertEquals("1010F0212", actual);
  }

  @Test
  public void testRelateBoolean() throws ParseException {
    Geometry geom1 = geomFromEWKT("POINT(1 2)");
    Geometry geom2 = Functions.buffer(geomFromEWKT("POINT(1 2)"), 2);
    boolean actual = Predicates.relate(geom1, geom2, "0FFFFF212");
    assertTrue(actual);

    actual = Predicates.relate(geom1, geom2, "0F0FFF212");
    assertFalse(actual);

    geom1 = geomFromEWKT("LINESTRING(1 2, 3 4)");
    geom2 = geomFromEWKT("LINESTRING(5 6, 7 8)");
    actual = Predicates.relate(geom1, geom2, "FF1F***02");
    assertTrue(actual);

    actual = Predicates.relate(geom1, geom2, "FF10***02");
    assertFalse(actual);

    geom1 = geomFromEWKT("LINESTRING (1 1, 5 5)");
    geom2 = geomFromEWKT("POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))");
    actual = Predicates.relate(geom1, geom2, "1010F0212");
    assertTrue(actual);
  }

  @Test
  public void testRelateMatch() {
    String matrix1 = "101202FFF";
    String matrix2 = "TTTTTTFFF";
    boolean actual = Predicates.relateMatch(matrix1, matrix2);
    assertTrue(actual);

    matrix2 = "TTFTTTFFF";
    actual = Predicates.relateMatch(matrix1, matrix2);
    assertFalse(actual);

    matrix1 = "FF1FF0102";
    matrix2 = "FF1F***02";
    actual = Predicates.relateMatch(matrix1, matrix2);
    assertTrue(actual);
  }

  @Test
  public void testCrossesDateLine() throws ParseException {
    Geometry geom1 = geomFromEWKT("LINESTRING(170 30, -170 30)");
    Geometry geom2 = geomFromEWKT("LINESTRING(-120 30, -130 40)");
    Geometry geom3 = geomFromEWKT("POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))");
    Geometry geom4 = geomFromEWKT("POLYGON((-120 10, -130 10, -130 -10, -120 -10, -120 10))");
    Geometry geom5 = geomFromEWKT("POINT(180 30)");
    Geometry geom6 =
        geomFromEWKT(
            "POLYGON((160 20, 180 20, 180 -20, 160 -20, 160 20), (165 15, 175 15, 175 -15, 165 -15, 165 15))");
    Geometry geom8 =
        geomFromEWKT(
            "POLYGON((170 -10, -170 -10, -170 10, 170 10, 170 -10), (175 -5, -175 -5, -175 5, 175 5, 175 -5))");

    // Multi-geometry test cases
    Geometry multiGeom1 = geomFromEWKT("MULTILINESTRING((170 30, -170 30), (-120 30, -130 40))");
    Geometry multiGeom2 =
        geomFromEWKT(
            "MULTIPOLYGON(((175 10, -175 10, -175 -10, 175 -10, 175 10)), ((-120 10, -130 10, -130 -10, -120 -10, -120 10)))");
    Geometry multiGeom3 = geomFromEWKT("MULTIPOINT((180 30), (170 -20))");
    Geometry multiGeom4 =
        geomFromEWKT(
            "MULTIPOLYGON(((160 20, 180 20, 180 -20, 160 -20, 160 20)), ((-120 10, -130 10, -130 -10, -120 -10, -120 10)))");

    assertEquals(true, crossesDateLine(geom1));
    assertEquals(false, crossesDateLine(geom2));
    assertEquals(true, crossesDateLine(geom3));
    assertEquals(false, crossesDateLine(geom4));
    assertEquals(false, crossesDateLine(geom5));
    assertEquals(false, crossesDateLine(geom6));
    assertEquals(true, crossesDateLine(geom8));
    assertEquals(true, crossesDateLine(multiGeom1));
    assertEquals(true, crossesDateLine(multiGeom2));
    assertEquals(false, crossesDateLine(multiGeom3));
    assertEquals(false, crossesDateLine(multiGeom4));
  }
}
