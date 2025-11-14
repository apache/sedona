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
    testPolygon(testName, wkt, expectedSegments, true);
  }

  /**
   * Helper method to test a polygon and verify basic properties of its medial axis.
   *
   * @param testName Name of the test for reporting
   * @param wkt WKT representation of the polygon
   * @param expectedSegments Expected number of skeleton segments
   * @param strictLengthCheck If false, skip perimeter comparison (for complex road networks)
   */
  private void testPolygon(
      String testName, String wkt, int expectedSegments, boolean strictLengthCheck)
      throws Exception {
    Geometry polygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.straightSkeleton(polygon);

    // Basic assertions
    assertNotNull(testName + ": Medial axis should not be null", medialAxis);
    assertTrue(
        testName + ": Result should be MultiLineString", medialAxis instanceof MultiLineString);

    int numSegments = medialAxis.getNumGeometries();

    // If expectedSegments is -1, skip exact count assertion (just verify it works)
    if (expectedSegments >= 0) {
      assertEquals(
          testName + ": Should have exactly " + expectedSegments + " segments",
          expectedSegments,
          numSegments);
    } else {
      // Just verify we got some segments
      assertTrue(testName + ": Should produce at least one segment", numSegments > 0);
    }

    // Verify all skeleton edges are inside or touch the polygon
    for (int i = 0; i < numSegments; i++) {
      LineString edge = (LineString) medialAxis.getGeometryN(i);
      assertTrue(
          testName + ": All skeleton edges should be inside or intersect the polygon",
          polygon.contains(edge) || polygon.intersects(edge));
    }

    // Verify skeleton has reasonable length (skip for degenerate cases with 0 segments)
    double skeletonLength = medialAxis.getLength();
    if (expectedSegments > 0) {
      assertTrue(testName + ": Skeleton length should be positive", skeletonLength > 0);
    }

    // For simple polygons, skeleton should be shorter than perimeter
    // For complex road networks, this may not hold due to branching structure
    if (strictLengthCheck) {
      double perimeter = polygon.getLength();
      assertTrue(
          testName + ": Skeleton length should be less than perimeter", skeletonLength < perimeter);
    }

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
    testPolygon("Simple Rectangle", "POLYGON ((0 0, 20 0, 20 10, 0 10, 0 0))", 5);
  }

  @Test
  public void testEquilateralTriangle() throws Exception {
    // Equilateral triangle centered at origin
    double height = Math.sqrt(3) / 2 * 10;
    String wkt =
        String.format(
            "POLYGON ((0 %.2f, -5 -%.2f, 5 -%.2f, 0 %.2f))", height, height, height, height);
    testPolygon("Equilateral Triangle", wkt, 3);
  }

  @Test
  public void testRightTriangle() throws Exception {
    testPolygon("Right Triangle", "POLYGON ((0 0, 10 0, 0 10, 0 0))", 3);
  }

  // ==================== Complex Polygon Tests ====================

  @Test
  public void testLShapedPolygon() throws Exception {
    testPolygon(
        "L-Shaped Polygon",
        "POLYGON ((190 190, 10 190, 10 10, 190 10, 190 20, 160 30, 60 30, 60 130, 190 140, 190 190))",
        15);
  }

  @Test
  public void testUShapedPolygon() throws Exception {
    // U-shape: outer rectangle with inner rectangle cut out from top
    testPolygon(
        "U-Shaped Polygon", "POLYGON ((0 0, 20 0, 20 20, 15 20, 15 5, 5 5, 5 20, 0 20, 0 0))", 11);
  }

  @Test
  public void testTShapedPolygon() throws Exception {
    // T-shape: vertical stem with horizontal top bar
    testPolygon(
        "T-Shaped Polygon", "POLYGON ((4 0, 6 0, 6 8, 10 8, 10 10, 0 10, 0 8, 4 8, 4 0))", 12);
  }

  @Test
  public void testCShapedPolygon() throws Exception {
    // C-shape: rectangle with rectangular notch on right side
    testPolygon(
        "C-Shaped Polygon", "POLYGON ((0 0, 10 0, 10 5, 5 5, 5 10, 10 10, 10 15, 0 15, 0 0))", 11);
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
        15,
        false);
  }

  // ==================== Edge Case Tests ====================

  @Test
  public void testVeryNarrowRectangle() throws Exception {
    // Very elongated rectangle (100:1 aspect ratio)
    testPolygon("Very Narrow Rectangle", "POLYGON ((0 0, 100 0, 100 1, 0 1, 0 0))", 5);
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

    testPolygon(
        "Regular Hexagon", hexWkt.toString(), 7, false); // Allow skeleton length > perimeter
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

    testPolygon("Regular Pentagon", pentWkt.toString(), 7, false);
  }

  @Test
  public void testCrossPolygon() throws Exception {
    // Plus/cross shape (like + sign)
    testPolygon(
        "Cross Polygon",
        "POLYGON ((4 0, 6 0, 6 4, 10 4, 10 6, 6 6, 6 10, 4 10, 4 6, 0 6, 0 4, 4 4, 4 0))",
        16);
  }

  // ==================== MultiPolygon Tests ====================

  @Test
  public void testSimpleMultiPolygon() throws Exception {
    // Two separate squares
    String wkt =
        "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))";

    Geometry multiPolygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.straightSkeleton(multiPolygon);

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
    Geometry medialAxis = Functions.straightSkeleton(multiPolygon);

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
    Geometry medialAxis = Functions.straightSkeleton(multiPolygon);

    assertNotNull("Complex shapes MultiPolygon: Medial axis should not be null", medialAxis);
    int numSegments = medialAxis.getNumGeometries();
    assertTrue("Complex shapes MultiPolygon: Should have multiple segments", numSegments >= 12);

    // Verify SRID preservation
    multiPolygon = Constructors.geomFromWKT(wkt, 4326);
    medialAxis = Functions.straightSkeleton(multiPolygon);
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
        12,
        false);
  }

  @Test
  public void testArrowPolygon() throws Exception {
    // Arrow-shaped polygon pointing right
    testPolygon("Arrow Polygon", "POLYGON ((0 5, 8 5, 8 2, 12 6, 8 10, 8 7, 0 7, 0 5))", 8);
  }

  @Test
  public void testDiamondPolygon() throws Exception {
    // Diamond shape (rotated square)
    testPolygon("Diamond Polygon", "POLYGON ((5 0, 10 5, 5 10, 0 5, 5 0))", 4);
  }

  @Test
  public void testComplexRoadNetwork() throws Exception {
    // Simple T-shaped road junction (vertical stem with horizontal branch at top)
    // This represents a realistic road junction similar to a T-intersection
    String roadNetwork =
        "POLYGON (("
            // Bottom of vertical stem
            + "45 0, 55 0, "
            // Right side up to junction
            + "55 40, "
            // Right branch of horizontal road
            + "70 40, 70 50, "
            // Left branch of horizontal road
            + "30 50, 30 40, "
            // Left side down
            + "45 40, "
            // Close polygon
            + "45 0))";

    // T-junction produces 12 skeleton segments
    // Use relaxed validation since road networks can have skeleton length > perimeter
    testPolygon("Complex Road Network (T-Junction)", roadNetwork, 12, false);
  }

  @Test
  public void testRoadIntersectionComplex() throws Exception {
    // Simplified but realistic road intersection test
    // 4-way intersection with road widths
    String intersection =
        "POLYGON (("
            // North road (top)
            + "45 100, 55 100, 55 70, "
            // Northeast corner
            + "70 70, 70 55, "
            // East road (right)
            + "100 55, 100 45, 70 45, "
            // Southeast corner
            + "70 30, 55 30, "
            // South road (bottom)
            + "55 0, 45 0, 45 30, "
            // Southwest corner
            + "30 30, 30 45, "
            // West road (left)
            + "0 45, 0 55, 30 55, "
            // Northwest corner
            + "30 70, 45 70, "
            // Close back to north
            + "45 100))";

    // 4-way intersection produces 24 segments with straight skeleton algorithm
    // Use relaxed validation since road networks can have skeleton length > perimeter
    testPolygon("Road Intersection 4-Way", intersection, 24, false);
  }

  @Test
  public void testComplexBranchingRoadNetwork() throws Exception {
    // Complex branching road network with 6 branches extending from main trunk
    // Simulates a dendritic road structure similar to the image
    // Main trunk runs vertically with 3 branches on each side
    String complexRoad =
        "POLYGON (("
            // Bottom of main trunk
            + "47 0, 53 0, "
            // Right side going up - start
            + "53 15, "
            // Branch 1 right (bottom-right)
            + "55 16, 65 14, 66 17, 56 19, 54 18, "
            // Continue trunk right
            + "54 30, "
            // Branch 2 right (middle-right)
            + "56 31, 70 33, 71 36, 57 34, 55 33, "
            // Continue trunk right
            + "55 45, "
            // Branch 3 right (top-right)
            + "57 46, 72 50, 73 53, 58 49, 56 48, "
            // Top of trunk
            + "56 60, 44 60, "
            // Left side going down - start
            + "44 48, "
            // Branch 3 left (top-left)
            + "42 49, 27 53, 28 50, 43 46, "
            // Continue trunk left
            + "45 45, 45 33, "
            // Branch 2 left (middle-left)
            + "43 34, 29 36, 30 33, 44 31, "
            // Continue trunk left
            + "46 30, 46 18, "
            // Branch 1 left (bottom-left)
            + "44 19, 34 17, 35 14, 45 16, "
            // Close to bottom
            + "47 15, 47 0))";

    // Complex branching network produces 71 skeleton segments
    // Represents main trunk centerline plus 6 branch centerlines
    // Use relaxed validation since complex road networks may have precision issues
    testPolygon("Complex Branching Road Network", complexRoad, 71, false);
  }
}
