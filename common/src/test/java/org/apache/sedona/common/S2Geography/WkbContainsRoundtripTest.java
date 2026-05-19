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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.junit.Test;

/**
 * Localises a Geography ST_Contains correctness bug seen when arguments come from a DataFrame (i.e.
 * after a GeographyWKBSerializer round-trip). The WKBGeography fast path (getShapeIndexGeography →
 * WkbS2Shape) returns the wrong answer for some polygon-point pairs; the slow path through
 * S2Polygon (and direct ST_GeogFromWKT in a SELECT) is correct.
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
}
