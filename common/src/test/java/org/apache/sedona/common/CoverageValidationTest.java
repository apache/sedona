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
import static org.apache.sedona.common.utils.CoverageValidation.invalidEdgesIgnoringOneMatchingTarget;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.sedona.common.geometrySerde.GeometrySerializer;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
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
  public void preservesZInInvalidEdges() throws ParseException {
    Geometry target = read("POLYGON Z ((0 0 5, 2 0 5, 2 2 5, 0 2 5, 0 0 5))");
    Geometry overlapping = read("POLYGON Z ((1 0 5, 3 0 5, 3 2 5, 1 2 5, 1 0 5))");

    LineString result = (LineString) invalidEdges(target, new Geometry[] {overlapping}, 0.0);

    assertEquals(3, result.getCoordinateSequence().getDimension());
    assertEquals(0, result.getCoordinateSequence().getMeasures());
    assertEquals(5.0, result.getCoordinate().getZ(), 0.0);
  }

  @Test
  public void dropsMFromInvalidEdges() throws ParseException {
    Geometry target = read("POLYGON M ((0 0 7, 2 0 7, 2 2 7, 0 2 7, 0 0 7))");
    Geometry overlapping = read("POLYGON M ((1 0 7, 3 0 7, 3 2 7, 1 2 7, 1 0 7))");

    LineString result = (LineString) invalidEdges(target, new Geometry[] {overlapping}, 0.0);

    assertEquals(0, result.getCoordinateSequence().getMeasures());
    assertTrue(Double.isNaN(result.getCoordinate().getZ()));
    assertTrue(Double.isNaN(result.getCoordinate().getM()));
  }

  @Test
  public void normalizesMixedZAndMInvalidEdgesForSerialization() throws ParseException {
    Geometry target =
        read(
            "GEOMETRYCOLLECTION ("
                + "POLYGON Z ((0 0 5, 2 0 5, 2 2 5, 0 2 5, 0 0 5)), "
                + "POLYGON M ((10 0 7, 12 0 7, 12 2 7, 10 2 7, 10 0 7)))");
    Geometry overlapping =
        read(
            "GEOMETRYCOLLECTION ("
                + "POLYGON Z ((1 0 5, 3 0 5, 3 2 5, 1 2 5, 1 0 5)), "
                + "POLYGON M ((11 0 7, 13 0 7, 13 2 7, 11 2 7, 11 0 7)))");

    Geometry result = invalidEdges(target, new Geometry[] {overlapping}, 0.0);
    Geometry roundTrip = GeometrySerializer.deserialize(GeometrySerializer.serialize(result));
    LineString zPart = (LineString) roundTrip.getGeometryN(0);
    LineString formerMPart = (LineString) roundTrip.getGeometryN(1);

    assertEquals("MultiLineString", roundTrip.getGeometryType());
    assertEquals(2, roundTrip.getNumGeometries());
    assertEquals(0, zPart.getCoordinateSequence().getMeasures());
    assertEquals(0, formerMPart.getCoordinateSequence().getMeasures());
    assertEquals(5.0, zPart.getCoordinate().getZ(), 0.0);
    assertTrue(Double.isNaN(formerMPart.getCoordinate().getZ()));
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

  @Test
  public void ignoresOneMatchingTargetInTheAdjacentCandidates() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

    Geometry result = invalidEdgesIgnoringOneMatchingTarget(target, new Geometry[] {target}, 0.0);

    assertTrue(result.isEmpty());
  }

  @Test
  public void retainsAnExactDuplicateAfterIgnoringOneMatchingTarget() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry duplicate = target.copy();

    Geometry explicitNeighborResult = invalidEdges(target, new Geometry[] {duplicate}, 0.0);
    Geometry result =
        invalidEdgesIgnoringOneMatchingTarget(target, new Geometry[] {target, duplicate}, 0.0);

    assertFalse(explicitNeighborResult.isEmpty());
    assertFalse(result.isEmpty());
  }

  @Test
  public void skipsNullMembersWhenLookingForTheMatchingTarget() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry adjacent = read("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))");

    Geometry result =
        invalidEdgesIgnoringOneMatchingTarget(
            target, new Geometry[] {null, target, adjacent, null}, 0.0);

    assertTrue(result.isEmpty());
  }

  @Test
  public void preservesCandidatesWhenThereIsNoMatchingTarget() throws ParseException {
    Geometry target = read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    Geometry overlapping = read("POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))");

    Geometry expected = invalidEdges(target, new Geometry[] {overlapping}, 0.0);
    Geometry actual =
        invalidEdgesIgnoringOneMatchingTarget(target, new Geometry[] {overlapping}, 0.0);

    assertTrue(expected.equalsExact(actual));
  }

  private static Geometry read(String wkt) throws ParseException {
    return WKT_READER.read(wkt);
  }
}
