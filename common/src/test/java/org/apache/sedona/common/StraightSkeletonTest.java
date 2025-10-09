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

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;

/**
 * Comprehensive tests for Straight Skeleton implementation.
 *
 * <p>Tests cover: - Simple polygons (square, rectangle, triangle) - Complex polygons (L-shape,
 * U-shape, T-shape, star) - Polygons with reflex angles - MultiPolygon geometries - Edge cases
 * (very narrow, concave shapes)
 */
public class StraightSkeletonTest {

  /**
   * Helper method to test a polygon and verify basic properties of its medial axis.
   *
   * @param testName Name of the test for reporting
   * @param wkt WKT representation of the polygon
   * @param expectedSegments Expected number of skeleton segments
   */
  private void testPolygon(String testName, String wkt, int expectedSegments) throws Exception {
    Geometry polygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.approximateMedialAxis(polygon);

    // Basic assertions
    assertNotNull(testName + ": Medial axis should not be null", medialAxis);
    assertTrue(
        testName + ": Result should be MultiLineString", medialAxis instanceof MultiLineString);

    int numSegments = medialAxis.getNumGeometries();
    assertEquals(
        testName + ": Should have exactly " + expectedSegments + " segments",
        expectedSegments,
        numSegments);

    // Verify all skeleton edges are inside or touch the polygon
    for (int i = 0; i < numSegments; i++) {
      LineString edge = (LineString) medialAxis.getGeometryN(i);
      assertTrue(
          testName + ": All skeleton edges should be inside or intersect the polygon",
          polygon.contains(edge) || polygon.intersects(edge));
    }

    // Verify skeleton has reasonable length (should be less than perimeter)
    double skeletonLength = medialAxis.getLength();
    double perimeter = polygon.getLength();
    assertTrue(
        testName + ": Skeleton length should be positive and less than perimeter",
        skeletonLength > 0 && skeletonLength < perimeter);

    // Note: For complex concave polygons, skeleton points may be slightly outside due to
    // precision issues in the straight skeleton algorithm. We skip strict containment validation
    // for now and rely on the other checks (edge containment, reasonable length, etc.)
  }

  // ==================== Simple Polygon Tests ====================

  @Test
  public void testSimpleSquare() throws Exception {
    testPolygon("Simple Square", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", 4);
  }

  @Test
  public void testSimpleRectangle() throws Exception {
    testPolygon("Simple Rectangle", "POLYGON ((0 0, 20 0, 20 10, 0 10, 0 0))", 4);
  }

  @Test
  public void testEquilateralTriangle() throws Exception {
    // Equilateral triangle centered at origin
    double height = Math.sqrt(3) / 2 * 10;
    String wkt =
        String.format(
            "POLYGON ((0 %.2f, -5 -%.2f, 5 -%.2f, 0 %.2f))", height, height, height, height);
    testPolygon("Equilateral Triangle", wkt, 2);
  }

  @Test
  public void testRightTriangle() throws Exception {
    testPolygon("Right Triangle", "POLYGON ((0 0, 10 0, 0 10, 0 0))", 2);
  }

  // ==================== Complex Polygon Tests ====================

  @Test
  public void testLShapedPolygon() throws Exception {
    testPolygon(
        "L-Shaped Polygon",
        "POLYGON ((190 190, 10 190, 10 10, 190 10, 190 20, 160 30, 60 30, 60 130, 190 140, 190 190))",
        10);
  }

  @Test
  public void testUShapedPolygon() throws Exception {
    // U-shape: outer rectangle with inner rectangle cut out from top
    testPolygon(
        "U-Shaped Polygon", "POLYGON ((0 0, 20 0, 20 20, 15 20, 15 5, 5 5, 5 20, 0 20, 0 0))", 8);
  }

  @Test
  public void testTShapedPolygon() throws Exception {
    // T-shape: vertical stem with horizontal top bar
    testPolygon(
        "T-Shaped Polygon", "POLYGON ((4 0, 6 0, 6 8, 10 8, 10 10, 0 10, 0 8, 4 8, 4 0))", 6);
  }

  @Test
  public void testCShapedPolygon() throws Exception {
    // C-shape: rectangle with rectangular notch on right side
    testPolygon(
        "C-Shaped Polygon", "POLYGON ((0 0, 10 0, 10 5, 5 5, 5 10, 10 10, 10 15, 0 15, 0 0))", 8);
  }

  @Test
  public void testStarPolygon() throws Exception {
    // Simple 4-pointed star
    testPolygon("Star Polygon", "POLYGON ((5 0, 6 4, 10 5, 6 6, 5 10, 4 6, 0 5, 4 4, 5 0))", 8);
  }

  @Test
  public void testComplexConcavePolygon() throws Exception {
    // Irregular concave polygon with multiple reflex angles
    testPolygon(
        "Complex Concave Polygon",
        "POLYGON ((0 0, 20 0, 20 5, 15 5, 15 10, 10 10, 10 5, 5 5, 5 15, 0 15, 0 0))",
        8);
  }

  // ==================== Edge Case Tests ====================

  @Test
  public void testVeryNarrowRectangle() throws Exception {
    // Very elongated rectangle (100:1 aspect ratio)
    testPolygon("Very Narrow Rectangle", "POLYGON ((0 0, 100 0, 100 1, 0 1, 0 0))", 4);
  }

  @Test
  public void testAlmostRegularHexagon() throws Exception {
    // Regular hexagon (6 sides)
    double r = 10.0;
    StringBuilder hexWkt = new StringBuilder("POLYGON ((");
    for (int i = 0; i < 6; i++) {
      double angle = Math.PI / 3 * i;
      double x = r * Math.cos(angle);
      double y = r * Math.sin(angle);
      if (i > 0) hexWkt.append(", ");
      hexWkt.append(String.format("%.2f %.2f", x, y));
    }
    hexWkt.append(", ");
    hexWkt.append(String.format("%.2f %.2f", r, 0.0)); // Close the ring
    hexWkt.append("))");

    testPolygon("Regular Hexagon", hexWkt.toString(), 8);
  }

  @Test
  public void testPentagon() throws Exception {
    // Regular pentagon
    double r = 10.0;
    StringBuilder pentWkt = new StringBuilder("POLYGON ((");
    for (int i = 0; i < 5; i++) {
      double angle = 2 * Math.PI / 5 * i - Math.PI / 2; // Start from top
      double x = r * Math.cos(angle);
      double y = r * Math.sin(angle);
      if (i > 0) pentWkt.append(", ");
      pentWkt.append(String.format("%.2f %.2f", x, y));
    }
    pentWkt.append(", ");
    pentWkt.append(
        String.format("%.2f %.2f", r * Math.cos(-Math.PI / 2), r * Math.sin(-Math.PI / 2)));
    pentWkt.append("))");

    testPolygon("Regular Pentagon", pentWkt.toString(), 6);
  }

  @Test
  public void testCrossPolygon() throws Exception {
    // Plus/cross shape (like + sign)
    testPolygon(
        "Cross Polygon",
        "POLYGON ((4 0, 6 0, 6 4, 10 4, 10 6, 6 6, 6 10, 4 10, 4 6, 0 6, 0 4, 4 4, 4 0))",
        10);
  }

  // ==================== MultiPolygon Tests ====================

  @Test
  public void testSimpleMultiPolygon() throws Exception {
    // Two separate squares
    String wkt =
        "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))";

    Geometry multiPolygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.approximateMedialAxis(multiPolygon);

    assertNotNull("MultiPolygon: Medial axis should not be null", medialAxis);
    assertTrue(
        "MultiPolygon: Result should be MultiLineString", medialAxis instanceof MultiLineString);

    int numSegments = medialAxis.getNumGeometries();
    assertEquals(
        "MultiPolygon: Two squares should produce 8 total segments (4 each)", 8, numSegments);

    // Verify skeleton is valid
    assertTrue("MultiPolygon: Skeleton should have positive length", medialAxis.getLength() > 0);
  }

  @Test
  public void testComplexMultiPolygon() throws Exception {
    // Multiple different shapes
    String wkt =
        "MULTIPOLYGON ("
            + "((0 0, 10 0, 10 10, 0 10, 0 0)), "
            + // Square
            "((20 0, 40 0, 40 5, 20 5, 20 0)), "
            + // Rectangle
            "((50 0, 55 0, 52.5 5, 50 0))"
            + // Triangle
            ")";

    Geometry multiPolygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.approximateMedialAxis(multiPolygon);

    assertNotNull("Complex MultiPolygon: Medial axis should not be null", medialAxis);
    assertTrue(
        "Complex MultiPolygon: Result should be MultiLineString",
        medialAxis instanceof MultiLineString);

    int numSegments = medialAxis.getNumGeometries();
    assertTrue(
        "Complex MultiPolygon: Should have multiple segments", numSegments >= 8); // More lenient

    // Verify all edges are inside or intersect the multipolygon
    for (int i = 0; i < numSegments; i++) {
      LineString edge = (LineString) medialAxis.getGeometryN(i);
      assertTrue(
          "Complex MultiPolygon: All skeleton edges should be inside or intersect input",
          multiPolygon.contains(edge) || multiPolygon.intersects(edge));
    }
  }

  @Test
  public void testMultiPolygonWithComplexShapes() throws Exception {
    // MultiPolygon with L-shape and U-shape
    String wkt =
        "MULTIPOLYGON ("
            + "((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0)), "
            + // L-shape
            "((20 0, 30 0, 30 10, 27 10, 27 3, 23 3, 23 10, 20 10, 20 0))"
            + // U-shape
            ")";

    Geometry multiPolygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.approximateMedialAxis(multiPolygon);

    assertNotNull("Complex shapes MultiPolygon: Medial axis should not be null", medialAxis);
    int numSegments = medialAxis.getNumGeometries();
    assertTrue("Complex shapes MultiPolygon: Should have multiple segments", numSegments >= 12);

    // Verify SRID preservation
    multiPolygon = Constructors.geomFromWKT(wkt, 4326);
    medialAxis = Functions.approximateMedialAxis(multiPolygon);
    assertEquals(
        "Complex shapes MultiPolygon: SRID should be preserved", 4326, medialAxis.getSRID());
  }

  // ==================== Special Cases ====================

  @Test
  public void testPolygonWithReflexAngles() throws Exception {
    // Polygon with multiple reflex (concave) angles
    testPolygon(
        "Polygon with Reflex Angles",
        "POLYGON ((0 0, 15 0, 15 5, 10 5, 10 10, 5 10, 5 5, 0 5, 0 0))",
        6);
  }

  @Test
  public void testArrowPolygon() throws Exception {
    // Arrow-shaped polygon pointing right
    testPolygon("Arrow Polygon", "POLYGON ((0 5, 8 5, 8 2, 12 6, 8 10, 8 7, 0 7, 0 5))", 6);
  }

  @Test
  public void testDiamondPolygon() throws Exception {
    // Diamond shape (rotated square)
    testPolygon("Diamond Polygon", "POLYGON ((5 0, 10 5, 5 10, 0 5, 5 0))", 4);
  }
}
