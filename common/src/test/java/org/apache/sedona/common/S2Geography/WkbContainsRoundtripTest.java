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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2ShapeUtil;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.junit.Test;

/**
 * Regression coverage for WKB-backed Geography containment, including direct shape access,
 * ShapeIndex predicates, the full S2 parse path, and serializer round-trips.
 */
public class WkbContainsRoundtripTest {

  /**
   * Direct Functions.contains call, no round-trip. Should return false: (10, 10) is far outside the
   * small polygon at (2..3, 2..3).
   */
  @Test
  public void containsIsFalseWithoutRoundTrip() throws Exception {
    Geography poly = Constructors.geogFromWKT("POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))", 4326);
    Geography pt = Constructors.geogFromWKT("POINT(10 10)", 4326);
    assertFalse(Functions.contains(poly, pt));
    assertTrue(Functions.contains(poly, Constructors.geogFromWKT("POINT(2.5 2.5)", 4326)));
  }

  /** Control test mirroring GeographyFunctionTest's "ST_Contains point outside polygon". */
  @Test
  public void controlPolygonAtOrigin() throws Exception {
    Geography poly = Constructors.geogFromWKT("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography ptOutside = Constructors.geogFromWKT("POINT(2 2)", 4326);
    Geography ptInside = Constructors.geogFromWKT("POINT(0.5 0.5)", 4326);
    assertFalse("polygon at origin must NOT contain (2, 2)", Functions.contains(poly, ptOutside));
    assertTrue("polygon at origin must contain (0.5, 0.5)", Functions.contains(poly, ptInside));
  }

  /**
   * Bypass WkbS2Shape and feed the polygon through PolygonGeography directly. If this passes while
   * the equivalent WKBGeography case fails, the bug is localised to WkbS2Shape (or to the
   * `result.shapeIndex.add(new WkbS2Shape(...))` path in WKBGeography.getShapeIndexGeography).
   */
  @Test
  public void bypassWkbS2ShapeViaPolygonGeography() throws Exception {
    // Force the slow path: parse via WKTReader then DON'T wrap in WKBGeography.
    Geography poly = new WKTReader().read("POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))");
    poly.setSRID(4326);
    Geography ptOutside = new WKTReader().read("POINT(10 10)");
    ptOutside.setSRID(4326);
    Geography ptInside = new WKTReader().read("POINT(2.5 2.5)");
    ptInside.setSRID(4326);
    assertFalse(
        "[slow path] polygon at (2..3,2..3) must NOT contain (10, 10)",
        Functions.contains(poly, ptOutside));
    assertTrue(
        "[slow path] polygon at (2..3,2..3) must contain (2.5, 2.5)",
        Functions.contains(poly, ptInside));
  }

  /**
   * Same logical inputs, but each Geography goes through the WKB serializer round-trip first —
   * which is what happens whenever a GeographyUDT column is read back from a DataFrame.
   */
  @Test
  public void containsIsFalseAfterWkbRoundTrip() throws Exception {
    Geography poly =
        GeographyWKBSerializer.deserialize(
            GeographyWKBSerializer.serialize(
                Constructors.geogFromWKT("POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))", 4326)));
    Geography ptOutside =
        GeographyWKBSerializer.deserialize(
            GeographyWKBSerializer.serialize(Constructors.geogFromWKT("POINT(10 10)", 4326)));
    Geography ptInside =
        GeographyWKBSerializer.deserialize(
            GeographyWKBSerializer.serialize(Constructors.geogFromWKT("POINT(2.5 2.5)", 4326)));
    assertFalse(
        "polygon at (2..3,2..3) must NOT contain (10, 10)", Functions.contains(poly, ptOutside));
    assertTrue(
        "polygon at (2..3,2..3) must contain (2.5, 2.5)", Functions.contains(poly, ptInside));
  }

  @Test
  public void clockwiseShellUsesSimpleFeaturesInteriorWithoutChangingWkb() throws Exception {
    String wkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))";
    WKBGeography polygon = (WKBGeography) Constructors.geogFromWKT(wkt, 4326);
    byte[] originalWkb = polygon.getWKBBytes().clone();
    Geography inside = Constructors.geogFromWKT("POINT (0.5 0.5)", 4326);
    Geography outside = Constructors.geogFromWKT("POINT (5 5)", 4326);

    // The direct WkbS2Shape path and the full WKBReader path must use the same normalized view.
    assertTrue(Functions.contains(polygon, inside));
    assertFalse(Functions.contains(polygon, outside));
    assertTrue(Functions.contains(polygon.getS2Geography(), inside));
    assertFalse(Functions.contains(polygon.getS2Geography(), outside));
    assertTrue(
        S2ShapeUtil.containsBruteForce(polygon.shape(0), S2LatLng.fromDegrees(0.5, 0.5).toPoint()));
    assertFalse(
        S2ShapeUtil.containsBruteForce(polygon.shape(0), S2LatLng.fromDegrees(5, 5).toPoint()));

    assertEquals(wkt, Functions.asText(polygon));
    assertArrayEquals(originalWkb, polygon.getWKBBytes());

    WKBGeography roundTripped =
        (WKBGeography)
            GeographyWKBSerializer.deserialize(GeographyWKBSerializer.serialize(polygon));
    assertArrayEquals(originalWkb, roundTripped.getWKBBytes());
    assertTrue(Functions.contains(roundTripped, inside));
    assertFalse(Functions.contains(roundTripped, outside));
  }

  @Test
  public void directedShapeFactoryPreservesComplementaryInterior() throws Exception {
    org.locationtech.jts.geom.Geometry polygon =
        new org.locationtech.jts.io.WKTReader().read("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
    byte[] wkb = new org.locationtech.jts.io.WKBWriter().write(polygon);
    WkbS2Shape normalized = new WkbS2Shape(wkb);
    WkbS2Shape directed = WkbS2Shape.withPreservedLoopOrientation(wkb);
    com.google.common.geometry.S2Point inside = S2LatLng.fromDegrees(0.5, 0.5).toPoint();
    com.google.common.geometry.S2Point outside = S2LatLng.fromDegrees(5, 5).toPoint();

    assertTrue(S2ShapeUtil.containsBruteForce(normalized, inside));
    assertFalse(S2ShapeUtil.containsBruteForce(normalized, outside));
    assertFalse(S2ShapeUtil.containsBruteForce(directed, inside));
    assertTrue(S2ShapeUtil.containsBruteForce(directed, outside));
  }

  @Test
  public void singleRingFastPathHandlesRepeatedVertex() throws Exception {
    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 0 0, 0 1, 1 1, 1 0, 0 0))", 4326);

    assertTrue(Functions.contains(polygon, Constructors.geogFromWKT("POINT (0.5 0.5)", 4326)));
    assertFalse(Functions.contains(polygon, Constructors.geogFromWKT("POINT (5 5)", 4326)));
  }

  @Test
  public void polygonRingRolesOverrideInputWinding() throws Exception {
    String[] shells = {
      "0 0, 4 0, 4 4, 0 4, 0 0", // counter-clockwise
      "0 0, 0 4, 4 4, 4 0, 0 0" // clockwise
    };
    String[] holes = {
      "1 1, 3 1, 3 3, 1 3, 1 1", // counter-clockwise
      "1 1, 1 3, 3 3, 3 1, 1 1" // clockwise
    };
    Geography shellPoint = Constructors.geogFromWKT("POINT (0.5 0.5)", 4326);
    Geography holePoint = Constructors.geogFromWKT("POINT (2 2)", 4326);
    Geography outsidePoint = Constructors.geogFromWKT("POINT (5 5)", 4326);

    for (String shell : shells) {
      for (String hole : holes) {
        String wkt = "POLYGON ((" + shell + "), (" + hole + "))";
        WKBGeography polygon = (WKBGeography) Constructors.geogFromWKT(wkt, 4326);

        assertTrue(wkt, Functions.contains(polygon, shellPoint));
        assertFalse(wkt, Functions.contains(polygon, holePoint));
        assertFalse(wkt, Functions.contains(polygon, outsidePoint));

        Geography parsed = polygon.getS2Geography();
        assertTrue(wkt, Functions.contains(parsed, shellPoint));
        assertFalse(wkt, Functions.contains(parsed, holePoint));
        assertFalse(wkt, Functions.contains(parsed, outsidePoint));

        Geography parsedFromOperationalWkt = new WKTReader().read(wkt);
        assertTrue(wkt, Functions.contains(parsedFromOperationalWkt, shellPoint));
        assertFalse(wkt, Functions.contains(parsedFromOperationalWkt, holePoint));
        assertFalse(wkt, Functions.contains(parsedFromOperationalWkt, outsidePoint));
      }
    }
  }

  @Test
  public void shellsLargerThanAHemisphereUseComplementRegardlessOfWinding() throws Exception {
    Geography forward =
        Constructors.geogFromWKT("POLYGON ((0 -80, 120 -80, -120 -80, 0 -80))", 4326);
    Geography reversed =
        Constructors.geogFromWKT("POLYGON ((0 -80, -120 -80, 120 -80, 0 -80))", 4326);
    Geography equator = Constructors.geogFromWKT("POINT (0 0)", 4326);

    for (Geography polygon : new Geography[] {forward, reversed}) {
      assertFalse(Functions.contains(polygon, equator));
      assertFalse(Functions.intersects(polygon, equator));
      assertFalse(Functions.within(equator, polygon));
      double area = Functions.area(polygon);
      assertTrue(area > 1.0e12 && area < 2.0e12);
    }
    assertEquals(Functions.area(forward), Functions.area(reversed), 1.0);
  }

  @Test
  public void multipolygonFallbackNormalizesEachShellAndHole() throws Exception {
    String wkt =
        "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0), "
            + "(0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5)), "
            + "((10 0, 12 0, 12 2, 10 2, 10 0), "
            + "(10.5 0.5, 11.5 0.5, 11.5 1.5, 10.5 1.5, 10.5 0.5)))";
    Geography polygon = Constructors.geogFromWKT(wkt, 4326);

    assertTrue(Functions.contains(polygon, Constructors.geogFromWKT("POINT (0.25 0.25)", 4326)));
    assertFalse(Functions.contains(polygon, Constructors.geogFromWKT("POINT (1 1)", 4326)));
    assertTrue(Functions.contains(polygon, Constructors.geogFromWKT("POINT (10.25 0.25)", 4326)));
    assertFalse(Functions.contains(polygon, Constructors.geogFromWKT("POINT (11 1)", 4326)));
    assertFalse(Functions.contains(polygon, Constructors.geogFromWKT("POINT (5 5)", 4326)));
    assertEquals(wkt, Functions.asText(polygon));
  }

  @Test
  public void holeContainingS2OriginIsExcluded() throws Exception {
    Geography polygon =
        Constructors.geogFromWKT(
            "POLYGON ((160 88, 160 89.8, 170 89.8, 170 88, 160 88), "
                + "(164 89, 167 89, 167 89.6, 164 89.6, 164 89))",
            4326);
    Geography inShell = Constructors.geogFromWKT("POINT (161 88.5)", 4326);
    Geography inHole =
        Constructors.geogFromWKT("POINT (165.4655449194599 89.40812060946742)", 4326);

    assertTrue(Functions.contains(polygon, inShell));
    assertFalse(Functions.contains(polygon, inHole));
  }
}
