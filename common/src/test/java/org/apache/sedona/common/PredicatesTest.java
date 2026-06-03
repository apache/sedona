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

import org.apache.sedona.common.geometryObjects.Box2D;
import org.apache.sedona.common.geometryObjects.Box3D;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

public class PredicatesTest extends TestBase {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @Test
  public void testBoxIntersects() {
    Box2D a = new Box2D(0.0, 0.0, 5.0, 5.0);

    // Full overlap
    assertTrue(Predicates.boxIntersects(a, new Box2D(1.0, 1.0, 2.0, 2.0)));
    // Partial overlap
    assertTrue(Predicates.boxIntersects(a, new Box2D(3.0, 3.0, 7.0, 7.0)));
    // Edge-touching (closed intervals)
    assertTrue(Predicates.boxIntersects(a, new Box2D(5.0, 0.0, 10.0, 5.0)));
    // Corner-touching (closed intervals)
    assertTrue(Predicates.boxIntersects(a, new Box2D(5.0, 5.0, 10.0, 10.0)));
    // Disjoint on X
    assertFalse(Predicates.boxIntersects(a, new Box2D(6.0, 0.0, 10.0, 5.0)));
    // Disjoint on Y
    assertFalse(Predicates.boxIntersects(a, new Box2D(0.0, 6.0, 5.0, 10.0)));
  }

  @Test
  public void testBoxContains() {
    Box2D outer = new Box2D(0.0, 0.0, 10.0, 10.0);

    assertTrue(Predicates.boxContains(outer, new Box2D(2.0, 2.0, 5.0, 5.0)));
    // Boundaries are inclusive
    assertTrue(Predicates.boxContains(outer, new Box2D(0.0, 0.0, 10.0, 10.0)));
    assertTrue(Predicates.boxContains(outer, new Box2D(0.0, 0.0, 1.0, 1.0)));
    // Outside on X
    assertFalse(Predicates.boxContains(outer, new Box2D(-1.0, 0.0, 5.0, 5.0)));
    // Crosses boundary on X
    assertFalse(Predicates.boxContains(outer, new Box2D(5.0, 0.0, 11.0, 5.0)));
  }

  @Test
  public void testDWithinBox2D() {
    Box2D a = new Box2D(0.0, 0.0, 10.0, 10.0);

    // Overlapping → distance 0, matches any non-negative radius.
    assertTrue(Predicates.dWithin(a, new Box2D(5.0, 5.0, 15.0, 15.0), 0.0));
    // Edge-touching → distance 0 (closed-interval).
    assertTrue(Predicates.dWithin(a, new Box2D(10.0, 0.0, 20.0, 10.0), 0.0));
    // Corner-touching diagonally → distance 0.
    assertTrue(Predicates.dWithin(a, new Box2D(10.0, 10.0, 20.0, 20.0), 0.0));
    // Separated by 1 on X only → distance 1.0.
    Box2D rightOf = new Box2D(11.0, 0.0, 20.0, 10.0);
    assertTrue(Predicates.dWithin(a, rightOf, 1.0));
    assertFalse(Predicates.dWithin(a, rightOf, 0.999));
    // Separated by (3, 4) → distance 5 (Pythagorean).
    Box2D diagonal = new Box2D(13.0, 14.0, 20.0, 20.0);
    assertTrue(Predicates.dWithin(a, diagonal, 5.0));
    assertFalse(Predicates.dWithin(a, diagonal, 4.999));
    // Negative radius never matches, even for overlapping boxes.
    assertFalse(Predicates.dWithin(a, a, -1.0));
  }

  @Test
  public void testDWithinBox2DRejectInvertedBounds() {
    Box2D normal = new Box2D(0.0, 0.0, 5.0, 5.0);
    Box2D wrapX = new Box2D(170.0, 10.0, -170.0, 20.0);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Predicates.dWithin(wrapX, normal, 1.0));
    assertTrue(ex.getMessage().contains("inverted bounds"));
  }

  @Test
  public void testBoxPredicatesRejectInvertedBounds() {
    // Box2D allows xmin > xmax (reserved for future antimeridian wraparound); planar predicates
    // refuse to evaluate them rather than silently returning misleading results.
    Box2D normal = new Box2D(0.0, 0.0, 5.0, 5.0);
    Box2D wrapX = new Box2D(170.0, 10.0, -170.0, 20.0); // longitude crosses antimeridian
    Box2D wrapY = new Box2D(0.0, 5.0, 5.0, 0.0); // ymin > ymax

    IllegalArgumentException ex1 =
        assertThrows(IllegalArgumentException.class, () -> Predicates.boxIntersects(wrapX, normal));
    assertTrue(ex1.getMessage().contains("inverted bounds"));

    IllegalArgumentException ex2 =
        assertThrows(IllegalArgumentException.class, () -> Predicates.boxContains(normal, wrapY));
    assertTrue(ex2.getMessage().contains("inverted bounds"));
  }

  @Test
  public void testBox3DIntersects() {
    Box3D a = new Box3D(0.0, 0.0, 0.0, 5.0, 5.0, 5.0);

    // Full overlap on all axes
    assertTrue(Predicates.box3dIntersects(a, new Box3D(1.0, 1.0, 1.0, 2.0, 2.0, 2.0)));
    // Partial overlap on all axes
    assertTrue(Predicates.box3dIntersects(a, new Box3D(3.0, 3.0, 3.0, 7.0, 7.0, 7.0)));
    // Face-touching (closed intervals)
    assertTrue(Predicates.box3dIntersects(a, new Box3D(5.0, 0.0, 0.0, 10.0, 5.0, 5.0)));
    // Corner-touching (closed intervals)
    assertTrue(Predicates.box3dIntersects(a, new Box3D(5.0, 5.0, 5.0, 10.0, 10.0, 10.0)));
    // Disjoint on Z only
    assertFalse(Predicates.box3dIntersects(a, new Box3D(0.0, 0.0, 6.0, 5.0, 5.0, 10.0)));
  }

  @Test
  public void testBox3DContains() {
    Box3D outer = new Box3D(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);

    assertTrue(Predicates.box3dContains(outer, new Box3D(2.0, 2.0, 2.0, 5.0, 5.0, 5.0)));
    // Equal boxes contain each other
    assertTrue(Predicates.box3dContains(outer, new Box3D(0.0, 0.0, 0.0, 10.0, 10.0, 10.0)));
    // Crosses on Z
    assertFalse(Predicates.box3dContains(outer, new Box3D(2.0, 2.0, 2.0, 5.0, 5.0, 11.0)));
  }

  @Test
  public void testBox3DRejectInvertedBounds() {
    Box3D normal = new Box3D(0.0, 0.0, 0.0, 5.0, 5.0, 5.0);
    Box3D wrapZ = new Box3D(0.0, 0.0, 5.0, 5.0, 5.0, 0.0); // zmin > zmax
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> Predicates.box3dIntersects(wrapZ, normal));
    assertTrue(ex.getMessage().contains("inverted bounds"));
  }

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
  public void testDWithin3DGeometryXY() {
    // XY-only inputs fold to z=0, so 3D distance equals 2D distance.
    Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2));
    assertTrue(Predicates.dWithin3D(point1, point2, 1.42));
    assertFalse(Predicates.dWithin3D(point1, point2, 1.41));
  }

  @Test
  public void testDWithin3DGeometryXYZ() {
    // Pure Z offset of 3 between two points at the same XY.
    Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 0));
    Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 3));
    assertTrue(Predicates.dWithin3D(point1, point2, 3.0));
    assertFalse(Predicates.dWithin3D(point1, point2, 2.999));
  }

  @Test
  public void testDWithin3DGeometryThresholdEdge() {
    // 1-1-1 offset gives distance sqrt(3) ≈ 1.7320508; threshold equal to it is inclusive.
    Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0, 0));
    Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    assertTrue(Predicates.dWithin3D(point1, point2, Math.sqrt(3)));
  }

  @Test
  public void testDWithin3DGeometryXYLineToPoint() throws ParseException {
    // XY LineString vs XY Point — the path that previously hit Distance3DOp's NaN guard
    // and threw "Ordinates must not be NaN". After force3D folding, distance is 0.
    Geometry line = geomFromEWKT("LINESTRING(0 0, 3 4)");
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    assertTrue(Predicates.dWithin3D(line, point, 0.0));
    assertTrue(Predicates.dWithin3D(line, point, 1.0));
  }

  @Test
  public void testDWithin3DGeometryXYPolygonToPoint() throws ParseException {
    // XY Polygon vs XY Point at the centroid → distance 0 after Z fold.
    Geometry polygon = geomFromEWKT("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    Geometry inside = GEOMETRY_FACTORY.createPoint(new Coordinate(5, 5));
    Geometry outside = GEOMETRY_FACTORY.createPoint(new Coordinate(12, 5));
    assertTrue(Predicates.dWithin3D(polygon, inside, 0.0));
    assertTrue(Predicates.dWithin3D(polygon, outside, 2.0));
    assertFalse(Predicates.dWithin3D(polygon, outside, 1.999));
  }

  @Test
  public void testDWithin3DGeometryMixedDimensions() throws ParseException {
    // XYZ point vs XY line. The line's Z folds to 0; the point sits 5 units above the
    // origin, so the 3D distance from (0,0,5) to the nearest point on z=0 line is 5.
    Geometry pointXYZ = geomFromEWKT("POINT Z(0 0 5)");
    Geometry lineXY = geomFromEWKT("LINESTRING(0 0, 10 0)");
    assertTrue(Predicates.dWithin3D(pointXYZ, lineXY, 5.0));
    assertFalse(Predicates.dWithin3D(pointXYZ, lineXY, 4.999));
  }

  @Test
  public void testDWithin3DBoxOverlapping() {
    Box3D a = new Box3D(0.0, 0.0, 0.0, 5.0, 5.0, 5.0);
    Box3D b = new Box3D(4.0, 4.0, 4.0, 6.0, 6.0, 6.0);
    // Overlap → distance 0, matches any non-negative radius.
    assertTrue(Predicates.dWithin3D(a, b, 0.0));
  }

  @Test
  public void testDWithin3DBoxFaceTouching() {
    Box3D a = new Box3D(0.0, 0.0, 0.0, 5.0, 5.0, 5.0);
    // Face touching on x = 5 → distance 0.
    assertTrue(Predicates.dWithin3D(a, new Box3D(5.0, 0.0, 0.0, 10.0, 5.0, 5.0), 0.0));
  }

  @Test
  public void testDWithin3DBoxSeparatedAlongZ() {
    // Separated only on Z by 3 units (Y and X intervals overlap).
    Box3D a = new Box3D(0.0, 0.0, 0.0, 5.0, 5.0, 5.0);
    Box3D b = new Box3D(0.0, 0.0, 8.0, 5.0, 5.0, 10.0);
    assertTrue(Predicates.dWithin3D(a, b, 3.0));
    assertFalse(Predicates.dWithin3D(a, b, 2.999));
  }

  @Test
  public void testDWithin3DBoxDiagonalThreshold() {
    // Diagonally separated by (3, 4, 12) → distance 13 exactly.
    Box3D a = new Box3D(0.0, 0.0, 0.0, 1.0, 1.0, 1.0);
    Box3D b = new Box3D(4.0, 5.0, 13.0, 5.0, 6.0, 14.0);
    assertTrue(Predicates.dWithin3D(a, b, 13.0));
    assertFalse(Predicates.dWithin3D(a, b, 12.999));
  }

  @Test
  public void testDWithin3DBoxRejectInvertedBounds() {
    Box3D normal = new Box3D(0.0, 0.0, 0.0, 5.0, 5.0, 5.0);
    Box3D wrapZ = new Box3D(0.0, 0.0, 5.0, 5.0, 5.0, 0.0);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> Predicates.dWithin3D(wrapZ, normal, 1.0));
    assertTrue(ex.getMessage().contains("inverted bounds"));
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
  public void testEqualsGeometryCollection() throws ParseException {
    Geometry gc1 = geomFromEWKT("GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))");
    Geometry gc2 = geomFromEWKT("GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))");
    assertTrue(Predicates.equals(gc1, gc2));

    Geometry gc3 = geomFromEWKT("GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 2 2))");
    assertFalse(Predicates.equals(gc1, gc3));
  }

  @Test
  public void testEqualsEmptyGeometries() throws ParseException {
    // Empty geometries of different types should be considered equal
    Geometry pointEmpty = geomFromEWKT("POINT EMPTY");
    Geometry multipolygonEmpty = geomFromEWKT("MULTIPOLYGON EMPTY");
    assertTrue(Predicates.equals(pointEmpty, multipolygonEmpty));

    // Empty geometries of the same type should be equal
    Geometry linestringEmpty = geomFromEWKT("LINESTRING EMPTY");
    Geometry linestringEmpty2 = geomFromEWKT("LINESTRING EMPTY");
    assertTrue(Predicates.equals(linestringEmpty, linestringEmpty2));

    // An empty geometry and a non-empty geometry should not be equal
    Geometry point = geomFromEWKT("POINT(0 0)");
    assertFalse(Predicates.equals(pointEmpty, point));
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
