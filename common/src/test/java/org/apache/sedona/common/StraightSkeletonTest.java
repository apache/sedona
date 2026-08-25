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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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

  // ==================== GH-3276: stderr flooding ====================

  @Test
  public void testDegenerateCollinearPolygonDoesNotFloodStderr() throws Exception {
    // Many short, near-collinear edges (e.g. a rasterized/ST_SubDivide-derived outline) can
    // trigger campskeleton's internal "Planes do not intersect at a single point" error
    // hundreds of times per call. The library catches it and prints the stack trace to
    // stderr itself; Sedona filters that specific, harmless trace out. See GH-3276.
    String wkt =
        "POLYGON ((462449 1165081, 462465 1165111, 462503.8891237283 1165108.3035137968, 462511 1165081, 462599 1165081, 462615 1165291, 462689.7820725202 1165281.122934918, 462691 1165081, 462839 1165081, 462851.87855484773 1165110.6925644865, 463019 1165111, 463031.87855484773 1165140.6925644865, 463139 1165141, 463173.68629150104 1165196.313708499, 463229 1165201, 463266.1108762717 1165258.3035137968, 463368.12144515227 1165290.6925644865, 463376.31370849896 1165233.686291501, 463283.8891237283 1165141.6964862032, 463231 1165139, 463196.31370849896 1165083.686291501, 463141 1165079, 463131.12293491786 1165050.21792748, 462991 1165049, 462975 1165019, 462691 1165019, 462683.8891237283 1164991.6964862032, 462652.58608969144 1164989, 462661 1164961, 462721 1164901, 462776.31370849896 1164896.313708499, 462961 1164691, 463016.31370849896 1164686.313708499, 463201 1164481, 463256.31370849896 1164476.313708499, 463411 1164301, 463466.31370849896 1164296.313708499, 463651 1164091, 463703.8891237283 1164088.3035137968, 463741 1164031, 463793.8891237283 1164028.3035137968, 463831 1163971, 463881.12293491786 1163969.78207252, 463891 1163941, 464003.8891237283 1163908.3035137968, 464011 1163881, 464129 1163881, 464129.30743551353 1164258.1214451522, 464175 1164271, 464191 1164255, 464191 1163941, 464219.7820725202 1163931.122934918, 464221 1163851, 464271.12293491786 1163849.78207252, 464281 1163821, 464467.41391030856 1163821, 464460.2179274798 1163931.122934918, 464505 1163941, 464521 1163842.5860896916, 464579 1163851, 464595 1163881, 464805 1163881, 464821 1163851, 464947.41391030856 1163851, 464941.6964862032 1163903.8891237283, 464985 1163911, 465000.69256448647 1163861.8785548478, 464962.58608969144 1163849, 464971 1163821, 465051.12293491786 1163819.78207252, 465061 1163791, 465258.12144515227 1163790.6925644865, 465271 1163761, 465558.12144515227 1163760.6925644865, 465571 1163731, 465975 1163731, 465991 1163701, 466349 1163701, 466349 1164089, 466319 1164105, 466319 1164435, 466365 1164451, 466381 1164435, 466381 1164121, 466411 1164105, 466411 1163701, 466518.12144515227 1163700.6925644865, 466531 1163671, 467415 1163671, 467431 1163641, 468105 1163641, 468121 1163611, 468465 1163611, 468481 1163551, 468531.12293491786 1163549.78207252, 468541 1163521, 468678.12144515227 1163520.6925644865, 468691 1163491, 468727.41391030856 1163491, 468719 1163579, 468691.6964862032 1163586.1108762717, 468689 1163617.4139103084, 468479.30743551353 1163621.8785548478, 468491.87855484773 1163670.6925644865, 468689 1163662.5860896916, 468689 1163759, 468660.2179274798 1163768.877065082, 468659 1163879, 468630.2179274798 1163888.877065082, 468599 1164059, 468573.68629150104 1164063.686291501, 468569 1164119, 468540.2179274798 1164128.877065082, 468539 1164209, 468511.6964862032 1164216.1108762717, 468479 1164329, 468450.2179274798 1164338.877065082, 468449 1164389, 468420.2179274798 1164398.877065082, 468419 1164479, 468393.68629150104 1164483.686291501, 468389 1164539, 468360.2179274798 1164548.877065082, 468359 1164629, 468333.68629150104 1164633.686291501, 468329 1164689, 468300.2179274798 1164698.877065082, 468299 1164779, 468271.6964862032 1164786.1108762717, 468239 1164899, 468210.2179274798 1164908.877065082, 468209 1164989, 468180.2179274798 1164998.877065082, 468179 1165079, 468151.6964862032 1165086.1108762717, 468149 1165169, 468120.2179274798 1165178.877065082, 468121.6964862032 1165283.8891237283, 468198.12144515227 1165290.6925644865, 468211 1165201, 468240.69256448647 1165188.1214451522, 468241 1165111, 468270.69256448647 1165098.1214451522, 468271 1165021, 468300.69256448647 1165008.1214451522, 468301 1164931, 468329.7820725202 1164921.122934918, 468331 1164871, 468356.31370849896 1164866.313708499, 468361 1164811, 468389.7820725202 1164801.122934918, 468391 1164721, 468418.3035137968 1164713.8891237283, 468451 1164601, 468479.7820725202 1164591.122934918, 468481 1164511, 468506.31370849896 1164506.313708499, 468511 1164451, 468539.7820725202 1164441.122934918, 468541 1164361, 468568.3035137968 1164353.8891237283, 468571 1164301, 468600.69256448647 1164288.1214451522, 468601 1164211, 468628.3035137968 1164203.8891237283, 468661 1164091, 468689.7820725202 1164081.122934918, 468691 1164001, 468718.3035137968 1163993.8891237283, 468721 1163911, 468750.69256448647 1163898.1214451522, 468751 1163791, 468780.69256448647 1163778.1214451522, 468781 1163641, 468808.3035137968 1163633.8891237283, 468811 1163551, 468840.69256448647 1163538.1214451522, 468841 1163461, 469019 1163371, 469049 1163401, 469053.68629150104 1163456.313708499, 469146.1108762717 1163548.3035137968, 469193.8891237283 1163548.3035137968, 469198.3035137968 1163466.1108762717, 469070.4045242952 1163340, 469081 1163281, 469260 1163120.4045242954, 469289 1163131, 469289 1163159, 469259 1163175, 469259.30743551353 1163238.1214451522, 469289 1163251, 469289 1163287.4139103084, 469241.87855484773 1163279.3074355135, 469229 1163309, 469143.68629150104 1163313.686291501, 469140.2179274798 1163361.122934918, 469176.1108762717 1163398.3035137968, 469248.12144515227 1163400.6925644865, 469261 1163371, 469316.31370849896 1163366.313708499, 469321 1163332.5860896916, 469371.12293491786 1163339.78207252, 469381 1163272.5860896916, 469461.12293491786 1163279.78207252, 469462.58608969144 1163221, 469529 1163221, 469541.87855484773 1163250.6925644865, 469769 1163281, 469769 1163309, 469739.30743551353 1163321.8785548478, 469739 1163429, 469656.1108762717 1163431.6964862032, 469650.2179274798 1163481.122934918, 469709 1163491, 469721.87855484773 1163520.6925644865, 469859 1163521, 469889 1163551, 469889 1163819, 469859.30743551353 1163831.8785548478, 469859.30743551353 1164048.1214451522, 469889 1164061, 469893.68629150104 1164116.313708499, 470009 1164211, 470017.41391030856 1164329, 469979.30743551353 1164341.8785548478, 469979 1164629, 469949.30743551353 1164641.8785548478, 469949 1164869, 469919 1164885, 469921.6964862032 1165133.8891237283, 469978.3035137968 1165133.8891237283, 469981 1164991, 470010.69256448647 1164978.1214451522, 470011 1164721, 470041 1164705, 470032.58608969144 1164331, 470099 1164301, 470103.68629150104 1164356.313708499, 470369 1164601, 470371.6964862032 1164653.8891237283, 470459 1164721, 470459.30743551353 1164828.1214451522, 470489 1164841, 470489 1164989, 470461.6964862032 1164996.1108762717, 470459 1165035, 470489 1165051, 470489.30743551353 1165278.1214451522, 470538.12144515227 1165290.6925644865, 470551 1165051, 470669 1165051, 470685 1165081, 470879 1165081, 470886.1108762717 1165108.3035137968, 470925 1165111, 470941 1165081, 470996.31370849896 1165076.313708499, 470999.7820725202 1165028.877065082, 470955 1164989, 470916.1108762717 1164991.6964862032, 470909 1165019, 470881 1165019, 470865 1164989, 470551 1164989, 470551 1164705, 470521 1164689, 470518.3035137968 1164636.1108762717, 470401 1164539, 470396.31370849896 1164483.686291501, 470341 1164479, 470338.3035137968 1164426.1108762717, 470161 1164269, 470158.3035137968 1164216.1108762717, 469981 1164059, 469979.7820725202 1164008.877065082, 469951 1163999, 469951 1163521, 469980.69256448647 1163508.1214451522, 469981 1163311, 470041 1163295, 470039.7820725202 1163228.877065082, 469981 1163219, 469981 1163161, 470031.12293491786 1163159.78207252, 470041 1163131, 470129 1163131, 470136.1108762717 1163158.3035137968, 470175 1163161, 470220 1163120.4045242954, 470415 1163131, 470431 1163115, 470418.12144515227 1163039.3074355135, 470321.87855484773 1163039.3074355135, 470309 1163069, 470221 1163077.4139103084, 470251 1162995, 470211.12293491786 1162950.21792748, 470071 1162987.4139103084, 470068.3035137968 1162956.1108762717, 470041 1162949, 470041 1162912.5860896916, 470205 1162921, 470221 1162905, 470208.12144515227 1162859.3074355135, 470041 1162867.4139103084, 470040.69256448647 1162661.8785548478, 470011 1162649, 470009.7820725202 1162478.877065082, 469981 1162469, 469981 1162261, 470011 1162245, 470011 1161991, 470039.7820725202 1161981.122934918, 470041 1161922.5860896916, 470161 1161915, 470148.12144515227 1161869.3074355135, 470040 1161879.5954757046, 470041 1161811, 470178.12144515227 1161810.6925644865, 470191 1161781, 470243.8891237283 1161778.3035137968, 470251 1161751, 470309 1161751, 470313.68629150104 1161806.313708499, 470361.12293491786 1161809.78207252, 470371 1161781, 470486.31370849896 1161746.313708499, 470491 1161601, 470521 1161571, 470601.12293491786 1161569.78207252, 470611 1161541, 470669 1161541, 470681.87855484773 1161570.6925644865, 470759 1161571, 470789 1161629, 470759.30743551353 1161641.8785548478, 470767.41391030856 1161719, 470733.68629150104 1161723.686291501, 470639 1161839, 470550.2179274798 1161848.877065082, 470553.68629150104 1161896.313708499, 470609 1161901, 470609 1161989, 470579 1162005, 470583.68629150104 1162196.313708499, 470849 1162201, 470879 1162231, 470881.6964862032 1162313.8891237283, 470931.12293491786 1162319.78207252, 470941 1162261, 471060.69256448647 1162248.1214451522, 471053.8891237283 1162201.6964862032, 470971 1162199, 470971 1162141, 471000.69256448647 1162128.1214451522, 470992.58608969144 1162081, 471143.8891237283 1162078.3035137968, 471268.3035137968 1161953.8891237283, 471271 1161901, 471299 1161901, 471308.87706508214 1161929.78207252, 471359 1161931, 471369.5954757048 1162080, 471331.6964862032 1162223.8891237283, 471438.12144515227 1162230.6925644865, 471442.58608969144 1162171, 471539 1162171, 471541.6964862032 1162223.8891237283, 471606.1108762717 1162288.3035137968, 471719 1162291, 471749 1162321, 471750.2179274798 1162371.122934918, 471809 1162411, 471811.6964862032 1162463.8891237283, 471929 1162561, 471961.6964862032 1162673.8891237283, 472019 1162711, 472020.2179274798 1162761.122934918, 472049 1162771, 472049 1162799, 472020.2179274798 1162808.877065082, 472021.6964862032 1162853.8891237283, 472080 1162850.4045242954, 472139 1162891, 472141.6964862032 1162943.8891237283, 472199 1162981, 472200.2179274798 1163031.122934918, 472259 1163071, 472291.6964862032 1163183.8891237283, 472379 1163251, 472379 1163279, 471991 1163279, 471975 1163249, 471451 1163249, 471435 1163219, 471338.87706508214 1163220.21792748, 471329 1163249, 471246.1108762717 1163251.6964862032, 471239 1163279, 471031 1163279, 471021.12293491786 1163250.21792748, 470941 1163249, 470928.12144515227 1163219.3074355135, 470881 1163227.4139103084, 470871.12293491786 1163190.21792748, 470819.30743551353 1163201.8785548478, 470826.1108762717 1163248.3035137968, 470879 1163242.5860896916, 470916.1108762717 1163308.3035137968, 470969 1163311, 470981.87855484773 1163340.6925644865, 471059 1163341, 471071.87855484773 1163370.6925644865, 471198.12144515227 1163370.6925644865, 471211 1163341, 471321.12293491786 1163339.78207252, 471331 1163311, 471779 1163311, 471791.87855484773 1163340.6925644865, 471989 1163341, 472001.87855484773 1163370.6925644865, 472169 1163371, 472173.68629150104 1163426.313708499, 472229 1163431, 472229 1163489, 472169.30743551353 1163501.8785548478, 472170.2179274798 1163541.122934918, 472207.41391030856 1163579, 471871 1163549, 471858.12144515227 1163519.3074355135, 471721 1163519, 471705 1163489, 471541 1163489, 471528.12144515227 1163459.3074355135, 471331 1163459, 471318.12144515227 1163429.3074355135, 471121 1163429, 471111.12293491786 1163400.21792748, 470941 1163399, 470928.12144515227 1163369.3074355135, 470761 1163369, 470748.12144515227 1163339.3074355135, 470611 1163339, 470595 1163309, 470431 1163309, 470418.12144515227 1163279.3074355135, 470348.87706508214 1163280.21792748, 470311 1163317.4139103084, 470303.8891237283 1163281.6964862032, 470258.87706508214 1163280.21792748, 470251.6964862032 1163333.8891237283, 470309 1163332.5860896916, 470316.1108762717 1163368.3035137968, 470399 1163371, 470411.87855484773 1163400.6925644865, 470669 1163401, 470678.87706508214 1163429.78207252, 470819 1163431, 470831.87855484773 1163460.6925644865, 471059 1163461, 471071.87855484773 1163490.6925644865, 471239 1163491, 471248.87706508214 1163519.78207252, 471419 1163521, 471431.87855484773 1163550.6925644865, 471629 1163551, 471641.87855484773 1163580.6925644865, 471809 1163581, 471821.87855484773 1163610.6925644865, 472169 1163641, 472181.87855484773 1163670.6925644865, 472289 1163671, 472329.5954757048 1163730, 472326.1108762717 1163788.3035137968, 472379 1163791, 472391.87855484773 1163820.6925644865, 472469 1163821, 472469 1163999, 472439 1164015, 472439 1164539, 472409 1164555, 472409 1164929, 471811 1164929, 471808.3035137968 1164876.1108762717, 471738.12144515227 1164809.3074355135, 471641.87855484773 1164809.3074355135, 471629 1164839, 471601 1164839, 471585 1164779, 471518.87706508214 1164780.21792748, 471509 1164809, 470971 1164809, 470969.7820725202 1164728.877065082, 470925 1164719, 470909 1164735, 470921.87855484773 1164930.6925644865, 470963.8891237283 1164928.3035137968, 470971 1164901, 471119 1164901, 471135 1164931, 471318.12144515227 1164930.6925644865, 471331 1164901, 471719 1164901, 471749 1164931, 471756.1108762717 1164988.3035137968, 471899 1164991, 471911.87855484773 1165020.6925644865, 472409 1165021, 472411.6964862032 1165283.8891237283, 472469.7820725202 1165281.122934918, 472471 1165132.5860896916, 472499 1165141, 472499.30743551353 1165218.1214451522, 472529 1165231, 472533.68629150104 1165286.313708499, 472613.8891237283 1165288.3035137968, 472619.7820725202 1165238.877065082, 472591 1165229, 472558.3035137968 1165116.1108762717, 472501 1165079, 472501 1165021, 472531 1165005, 472528.3035137968 1164966.1108762717, 472501 1164959, 472501 1164211, 472530.69256448647 1164198.1214451522, 472531 1163941, 472560.69256448647 1163928.1214451522, 472561 1163761, 472618.3035137968 1163723.8891237283, 472620 1163660.4045242954, 472709 1163671, 472717.41391030856 1163729, 472680.2179274798 1163738.877065082, 472688.87706508214 1163789.78207252, 472731.12293491786 1163789.78207252, 472741 1163752.5860896916, 472791.12293491786 1163759.78207252, 472829 1163722.5860896916, 472838.87706508214 1163759.78207252, 472949 1163761, 472951.6964862032 1163813.8891237283, 473069 1163911, 473071.6964862032 1163963.8891237283, 473189 1164061, 473191.6964862032 1164113.8891237283, 473309 1164211, 473313.68629150104 1164266.313708499, 473429 1164361, 473431.6964862032 1164413.8891237283, 473519 1164481, 473521.6964862032 1164533.8891237283, 473639 1164631, 473639 1150616.313708499, 462449 1150616.313708499, 462449 1165081))";
    Geometry polygon = Constructors.geomFromWKT(wkt, 0);

    PrintStream originalErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      Geometry medialAxis = Functions.straightSkeleton(polygon);
      assertNotNull(medialAxis);
      assertTrue(medialAxis.getNumGeometries() > 0);
    } finally {
      System.setErr(originalErr);
    }

    String stderr = captured.toString(StandardCharsets.UTF_8.name());
    assertFalse(
        "Successful call should not print campskeleton's internal degenerate collision "
            + "stack traces to stderr: "
            + stderr,
        stderr.contains("Planes do not intersect at a single point")
            || stderr.contains("CoSitedCollision.validateChains"));
  }
}
