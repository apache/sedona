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

import static org.apache.sedona.common.utils.CoverageValidation.invalidEdges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class CoverageValidationTest {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final WKTReader WKT_READER = new WKTReader(GEOMETRY_FACTORY);

  @Test
  public void returnsAnEmptyLineForMatchingCoverageEdges() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry adjacent = read("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))");

    Geometry result = invalidEdges(target, new Geometry[] {adjacent}, 0.0);

    assertTrue(result.isEmpty());
    assertEquals("LineString", result.getGeometryType());
  }

  @Test
  public void returnsTargetEdgesForOverlappingPolygons() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry overlapping = read("POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))");

    Geometry result = invalidEdges(target, new Geometry[] {overlapping}, 0.0);

    assertFalse(result.isEmpty());
    assertEquals(1, result.getDimension());
    assertTrue(target.getBoundary().covers(result));
  }

  @Test
  public void reportsNarrowGapsOnlyWhenAGapWidthIsRequested() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry acrossGap = read("POLYGON ((1.1 0, 2.1 0, 2.1 1, 1.1 1, 1.1 0))");

    assertTrue(invalidEdges(target, new Geometry[] {acrossGap}, 0.0).isEmpty());
    assertFalse(invalidEdges(target, new Geometry[] {acrossGap}, 0.2).isEmpty());
  }

  @Test
  public void rejectsNegativeAndNonFiniteGapWidths() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

    for (double gapWidth :
        new double[] {-0.1, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> invalidEdges(target, new Geometry[0], gapWidth));
      assertEquals("gapWidth must be finite and non-negative", error.getMessage());
    }
  }

  @Test
  public void ignoresNonPolygonalAdjacentMembersAndUsesTheTargetFactory() throws ParseException {
    GeometryFactory targetFactory = new GeometryFactory(new PrecisionModel(), 4326);
    WKTReader targetReader = new WKTReader(targetFactory);
    Geometry target = targetReader.read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry point = read("POINT (0.5 0.5)");

    Geometry result = invalidEdges(target, new Geometry[] {point, null}, 0.0);

    assertTrue(result.isEmpty());
    assertEquals(4326, result.getSRID());
    assertSame(targetFactory, result.getFactory());
  }

  @Test
  public void returnsAnEmptyLineForNonPolygonalTargets() throws ParseException {
    Geometry result = invalidEdges(read("POINT (0 0)"), new Geometry[0], 0.0);

    assertTrue(result.isEmpty());
    assertEquals("LineString", result.getGeometryType());
  }

  private static Geometry read(String wkt) throws ParseException {
    return WKT_READER.read(wkt);
  }
}
