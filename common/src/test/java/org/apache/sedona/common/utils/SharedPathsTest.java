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
package org.apache.sedona.common.utils;

import static org.junit.Assert.*;

import org.apache.sedona.common.Functions;
import org.apache.sedona.common.geometrySerde.GeometrySerializer;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;

public class SharedPathsTest {
  private final WKTReader reader = new WKTReader();

  @Test
  public void matchesPostGISRegressionCases() throws ParseException {
    assertSharedPaths(
        "LINESTRING (0 0, 10 0)",
        "LINESTRING (10 0, 0 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((0 0, 10 0)))");
    assertSharedPaths(
        "LINESTRING (0 0, 10 0)",
        "LINESTRING (20 0, 30 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING EMPTY)");
    assertSharedPaths(
        "LINESTRING (0 0, 100 0)",
        "LINESTRING (20 0, 30 0, 30 50, 80 0, 70 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING ((20 0, 30 0)), MULTILINESTRING ((70 0, 80 0)))");
    assertSharedPaths(
        "MULTILINESTRING ((1 3, 4 2, 7 2, 7 5), (13 10, 14 7, 11 6, 15 5))",
        "LINESTRING (2 1, 4 2, 7 2, 8 3, 10 6, 11 6, 14 7, 16 9)",
        "GEOMETRYCOLLECTION (MULTILINESTRING ((4 2, 7 2)), MULTILINESTRING ((14 7, 11 6)))");
  }

  @Test
  public void orientsPathsLikeFirstInput() throws ParseException {
    assertSharedPaths(
        "LINESTRING (15 0, 5 0)",
        "LINESTRING (0 0, 10 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((10 0, 5 0)))");
  }

  @Test
  public void classifiesShortSharedPathOnLongSourceSegment() throws ParseException {
    assertSharedPaths(
        "LINESTRING (-1000000000 0, 1000000000 0)",
        "LINESTRING (0 0, 0.0000000001 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING ((0 0, 0.0000000001 0)), " + "MULTILINESTRING EMPTY)");
  }

  @Test
  public void locatesFixedPrecisionOverlayOnOriginalLinework() throws ParseException {
    GeometryFactory fixedFactory = new GeometryFactory(new PrecisionModel(1.0));
    LineString left =
        fixedFactory.createLineString(
            new Coordinate[] {new Coordinate(0.24, 0.24), new Coordinate(10.24, 0.24)});
    LineString right =
        fixedFactory.createLineString(
            new Coordinate[] {new Coordinate(5.24, 0.24), new Coordinate(15.24, 0.24)});

    Geometry result = Functions.sharedPaths(left, right);

    Geometry expected =
        reader.read("GEOMETRYCOLLECTION (MULTILINESTRING ((5 0, 10 0)), MULTILINESTRING EMPTY)");
    assertTrue("Expected " + expected + " but found " + result, expected.equalsExact(result));
    assertEquals(fixedFactory.getPrecisionModel(), result.getPrecisionModel());
  }

  @Test
  public void classifiesDirectionWhenFixedPrecisionChangesTheDominantAxis() throws ParseException {
    GeometryFactory fixedFactory = new GeometryFactory(new PrecisionModel(1.0));
    LineString left =
        fixedFactory.createLineString(
            new Coordinate[] {new Coordinate(0.49, 0.1), new Coordinate(0.51, 0.4)});
    LineString sameDirection =
        fixedFactory.createLineString(
            new Coordinate[] {new Coordinate(0.49, 0.1), new Coordinate(0.51, 0.4)});
    LineString oppositeDirection = (LineString) sameDirection.reverse();

    Geometry sameResult = Functions.sharedPaths(left, sameDirection);
    Geometry oppositeResult = Functions.sharedPaths(left, oppositeDirection);

    Geometry expectedSame =
        reader.read("GEOMETRYCOLLECTION (MULTILINESTRING ((0 0, 1 0)), MULTILINESTRING EMPTY)");
    Geometry expectedOpposite =
        reader.read("GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((0 0, 1 0)))");
    assertTrue(
        "Expected " + expectedSame + " but found " + sameResult,
        expectedSame.equalsExact(sameResult));
    assertTrue(
        "Expected " + expectedOpposite + " but found " + oppositeResult,
        expectedOpposite.equalsExact(oppositeResult));
  }

  @Test
  public void keepsFixedPrecisionPathsThatOverlapAfterRounding() throws ParseException {
    GeometryFactory fixedFactory = new GeometryFactory(new PrecisionModel(1.0));
    LineString left =
        fixedFactory.createLineString(
            new Coordinate[] {new Coordinate(0, 0.24), new Coordinate(10, 0.24)});
    LineString right =
        fixedFactory.createLineString(
            new Coordinate[] {new Coordinate(5, 0.26), new Coordinate(15, 0.26)});

    assertTrue(left.getEnvelopeInternal().disjoint(right.getEnvelopeInternal()));
    Geometry result = Functions.sharedPaths(left, right);

    Geometry expected =
        reader.read("GEOMETRYCOLLECTION (MULTILINESTRING ((5 0, 10 0)), MULTILINESTRING EMPTY)");
    assertTrue("Expected " + expected + " but found " + result, expected.equalsExact(result));
  }

  @Test
  public void choosesTheClosestSegmentBeforeTheEarliestTraversal() throws ParseException {
    assertSharedPaths(
        "MULTILINESTRING ((1000000000000010 1, 1000000000000000 1), "
            + "(1000000000000000 0, 1000000000000010 0))",
        "LINESTRING (1000000000000000 0, 1000000000000010 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING ((1000000000000000 0, "
            + "1000000000000010 0)), MULTILINESTRING EMPTY)");
  }

  @Test
  public void handlesClosedAndNonSimpleLines() throws ParseException {
    assertSharedPaths(
        "LINESTRING (0 0, 10 0)",
        "LINESTRING (0 0, 0 10, 10 10, 10 0, 0 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((0 0, 10 0)))");
    assertSharedPaths(
        "LINESTRING (0 0, 2 0, 0 0)",
        "LINESTRING (0 0, 2 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING ((0 0, 2 0)), MULTILINESTRING EMPTY)");
    assertSharedPaths(
        "LINESTRING (0 0, 2 2, 0 2, 2 0)",
        "LINESTRING (0 0, 2 2)",
        "GEOMETRYCOLLECTION (MULTILINESTRING ((0 0, 1 1), (1 1, 2 2)), MULTILINESTRING EMPTY)");
  }

  @Test
  public void matchesPostGISDirectionBucketsForAmbiguousNonSimpleTraversals()
      throws ParseException {
    String left = "LINESTRING (2.5 0.5, 1 2, 2.5 0.5, 0 0.5, 2 1.5)";
    String right = "LINESTRING (1 2, 2.5 0.5, 2.000678017814481 2.5, 2.5 0.5)";

    // Both lines revisit the same path, so a shared coordinate can belong to more than one source
    // traversal. PostGIS does not promise operand-order symmetry for this ambiguous case: swapping
    // the operands changes the direction buckets. Compare bucket topology because a repeated path
    // also has no unique output orientation.
    assertSharedPathBuckets(
        left,
        right,
        "GEOMETRYCOLLECTION (MULTILINESTRING ((2.5 0.5, 1.6666666666666667 "
            + "1.3333333333333333)), MULTILINESTRING ((1.6666666666666667 "
            + "1.3333333333333333, 1 2)))");
    assertSharedPathBuckets(
        right,
        left,
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((1 2, "
            + "1.6666666666666667 1.3333333333333333), (1.6666666666666667 "
            + "1.3333333333333333, 2.5 0.5)))");
  }

  @Test
  public void ignoresPointIntersections() throws ParseException {
    assertSharedPaths(
        "LINESTRING (0 0, 10 0)",
        "LINESTRING (5 -5, 5 5)",
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING EMPTY)");
    assertSharedPaths(
        "LINESTRING (0 0, 10 0)",
        "LINESTRING (10 0, 20 0)",
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING EMPTY)");
  }

  @Test
  public void handlesEmptyLinealInputsAndNulls() throws ParseException {
    Geometry emptyResult =
        Functions.sharedPaths(
            reader.read("LINESTRING EMPTY"), reader.read("MULTILINESTRING EMPTY"));
    assertBucketsEmpty(emptyResult);

    emptyResult =
        Functions.sharedPaths(
            reader.read("LINESTRING EMPTY"), reader.read("LINESTRING (0 0, 1 0)"));
    assertBucketsEmpty(emptyResult);

    Geometry disjointLeft = reader.read("LINESTRING Z (0 0 1, 1 0 2)");
    Geometry disjointRight = reader.read("LINESTRING Z (2 0 3, 3 0 4)");
    disjointLeft.setSRID(4326);
    disjointRight.setSRID(4326);
    emptyResult = Functions.sharedPaths(disjointLeft, disjointRight);
    assertEquals(
        "GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING EMPTY)",
        Functions.asWKT(emptyResult));
    assertBucketsEmpty(emptyResult);
    assertEquals(4326, emptyResult.getSRID());
    assertEquals(4326, emptyResult.getGeometryN(0).getSRID());
    assertEquals(4326, emptyResult.getGeometryN(1).getSRID());
    assertNull(Functions.sharedPaths(null, reader.read("LINESTRING (0 0, 1 0)")));
    assertNull(Functions.sharedPaths(reader.read("LINESTRING (0 0, 1 0)"), null));
  }

  @Test
  public void rejectsNonLinealInputs() throws ParseException {
    Geometry line = reader.read("LINESTRING (0 0, 1 0)");
    for (String invalidWkt :
        new String[] {
          "POINT (100 100)",
          "POLYGON ((0 0, 1 0, 1 1, 0 0))",
          "GEOMETRYCOLLECTION (LINESTRING (0 0, 1 0))",
          "GEOMETRYCOLLECTION EMPTY"
        }) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> Functions.sharedPaths(readUnchecked(invalidWkt), line));
      assertEquals("Geometry is not lineal", error.getMessage());

      error =
          assertThrows(
              IllegalArgumentException.class,
              () -> Functions.sharedPaths(line, readUnchecked(invalidWkt)));
      assertEquals("Geometry is not lineal", error.getMessage());
    }
  }

  @Test
  public void rejectsMixedSRIDsAndRetainsMatchingSRID() throws ParseException {
    Geometry left = reader.read("LINESTRING (0 0, 10 0)");
    Geometry right = reader.read("LINESTRING (20 0, 30 0)");
    left.setSRID(10);
    right.setSRID(5);

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Functions.sharedPaths(left, right));
    assertEquals("Operation on mixed SRID geometries (10 != 5)", error.getMessage());

    right.setSRID(10);
    Geometry result = Functions.sharedPaths(left, right);
    assertEquals(10, result.getSRID());
    assertEquals(10, result.getGeometryN(0).getSRID());
    assertEquals(10, result.getGeometryN(1).getSRID());
  }

  @Test
  public void matchesPostGISZAndMBehavior() throws ParseException {
    Geometry result =
        Functions.sharedPaths(
            reader.read("LINESTRING ZM (0 1 5 4, 0 0 6 5, 1 0 7 6, 1 1 8 7)"),
            reader.read("LINESTRING ZM (0 -1 3 8, 0 0 2 9, 1 0 1 10, 1 -1 0 11)"));
    CoordinateSequence sequence = firstPath(result).getCoordinateSequence();
    assertTrue(sequence.hasZ());
    assertFalse(sequence.hasM());
    assertArrayEquals(new double[] {6, 7}, zValues(sequence), 0.0);

    result =
        Functions.sharedPaths(
            reader.read("LINESTRING (0 1, 0 0, 1 0, 1 1)"),
            reader.read("LINESTRING Z (0 -1 3, 0 0 2, 1 0 1, 1 -1 0)"));
    sequence = firstPath(result).getCoordinateSequence();
    assertTrue(sequence.hasZ());
    assertArrayEquals(new double[] {2, 1}, zValues(sequence), 0.0);

    result =
        Functions.sharedPaths(
            reader.read("LINESTRING M (0 0 1, 10 0 2)"),
            reader.read("LINESTRING M (0 0 3, 10 0 4)"));
    sequence = firstPath(result).getCoordinateSequence();
    assertFalse(sequence.hasZ());
    assertFalse(sequence.hasM());
    assertEquals(2, sequence.getDimension());
  }

  @Test
  public void writesParseableWKTFor3DResultWithEmptyBucket() throws ParseException {
    Geometry result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (0 0 6, 1 0 7)"), reader.read("LINESTRING Z (0 0 1, 1 0 2)"));

    String wkt = Functions.asWKT(result);

    assertEquals(
        "GEOMETRYCOLLECTION Z(MULTILINESTRING Z((0 0 6, 1 0 7)), " + "MULTILINESTRING Z EMPTY)",
        wkt);
    assertTrue(result.equalsExact(reader.read(wkt)));
  }

  @Test
  public void downgradesWholeResultWhenAnySharedPathHasNoSourceZ() throws Exception {
    Geometry result =
        Functions.sharedPaths(
            reader.read("MULTILINESTRING ((0 0 0, 10 0 10), (0 1, 10 1))"),
            reader.read("MULTILINESTRING ((2 0, 8 0), (2 1, 8 1))"));
    String expectedWkt =
        "GEOMETRYCOLLECTION (MULTILINESTRING ((2 0, 8 0), (2 1, 8 1)), " + "MULTILINESTRING EMPTY)";
    Geometry expected = reader.read(expectedWkt);

    assertEquals(expectedWkt, Functions.asWKT(result));
    assertFalse(firstPath(result).getCoordinateSequence().hasZ());
    assertFalse(
        ((LineString) result.getGeometryN(0).getGeometryN(1)).getCoordinateSequence().hasZ());

    Geometry wktRoundTrip = reader.read(Functions.asWKT(result));
    assertTrue(expected.equalsExact(wktRoundTrip));
    Geometry wkbRoundTrip = new WKBReader().read(Functions.asWKB(result));
    assertTrue(expected.equalsExact(wkbRoundTrip));
    assertFalse(firstPath(wkbRoundTrip).getCoordinateSequence().hasZ());
    Geometry serdeRoundTrip = GeometrySerializer.deserialize(GeometrySerializer.serialize(result));
    assertTrue(expected.equalsExact(serdeRoundTrip));
    assertFalse(firstPath(serdeRoundTrip).getCoordinateSequence().hasZ());
  }

  @Test
  public void selectsAndInterpolatesZFromTheSourceLinework() throws ParseException {
    Geometry result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (2 0 12, 8 0 18)"),
            reader.read("LINESTRING Z (0 0 100, 10 0 110)"));
    assertArrayEquals(
        new double[] {12, 18}, zValues(firstPath(result).getCoordinateSequence()), 0.0);

    result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (0 0 0, 10 0 10)"),
            reader.read("LINESTRING Z (2 0 102, 8 0 108)"));
    assertArrayEquals(
        new double[] {102, 108}, zValues(firstPath(result).getCoordinateSequence()), 0.0);

    result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (0 0 0, 10 0 10)"), reader.read("LINESTRING (2 0, 8 0)"));
    assertArrayEquals(new double[] {2, 8}, zValues(firstPath(result).getCoordinateSequence()), 0.0);
  }

  @Test
  public void selectsZFromTheTraversalContainingTheSharedPath() throws ParseException {
    Geometry result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (0 0 0, 2 2 2, 0 2 20, 2 0 22)"),
            reader.read("LINESTRING (1 1, 2 0)"));

    assertArrayEquals(
        new double[] {21, 22}, zValues(firstPath(result).getCoordinateSequence()), 0.0);
  }

  @Test
  public void disambiguatesAPathWhoseMidpointIsASelfIntersection() throws ParseException {
    Geometry result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (0 0 0, 2 2 2, 0 2 20, 2 0 22)"),
            reader.read("LINESTRING (0 2, 2 0)"));

    MultiLineString sameDirection = (MultiLineString) result.getGeometryN(0);
    assertEquals(2, sameDirection.getNumGeometries());
    assertArrayEquals(
        new double[] {20, 21},
        zValues(((LineString) sameDirection.getGeometryN(0)).getCoordinateSequence()),
        0.0);
    assertArrayEquals(
        new double[] {21, 22},
        zValues(((LineString) sameDirection.getGeometryN(1)).getCoordinateSequence()),
        0.0);
  }

  @Test
  public void givesAnExactVertexPriorityOverSegmentInterpolation() throws ParseException {
    Geometry result =
        Functions.sharedPaths(
            reader.read("LINESTRING Z (0 1 100, 1 1 999, 2 1 102, 0 0 0, 2 2 2)"),
            reader.read("LINESTRING (0 0, 2 2)"));

    MultiLineString sameDirection = (MultiLineString) result.getGeometryN(0);
    assertEquals(2, sameDirection.getNumGeometries());
    assertArrayEquals(
        new double[] {0, 999},
        zValues(((LineString) sameDirection.getGeometryN(0)).getCoordinateSequence()),
        0.0);
    assertArrayEquals(
        new double[] {999, 2},
        zValues(((LineString) sameDirection.getGeometryN(1)).getCoordinateSequence()),
        0.0);
  }

  @Test
  public void indexesZSourcesInsteadOfRescanningThemForEveryOutputCoordinate() {
    int pointCount = 1000;
    Coordinate[] coordinates = new Coordinate[pointCount];
    for (int i = 0; i < pointCount; i++) {
      coordinates[i] = new Coordinate(i, i % 2, i);
    }
    CountingCoordinateSequence leftSequence = new CountingCoordinateSequence(coordinates);
    CountingCoordinateSequence rightSequence = new CountingCoordinateSequence(coordinates);
    GeometryFactory factory = new GeometryFactory();
    LineString left = factory.createLineString(leftSequence);
    LineString right = factory.createLineString(rightSequence);

    Geometry result = Functions.sharedPaths(left, right);

    MultiLineString sameDirection = (MultiLineString) result.getGeometryN(0);
    assertEquals(pointCount - 1, sameDirection.getNumGeometries());
    assertArrayEquals(
        new double[] {0, 1},
        zValues(((LineString) sameDirection.getGeometryN(0)).getCoordinateSequence()),
        0.0);
    assertArrayEquals(
        new double[] {pointCount - 2, pointCount - 1},
        zValues(((LineString) sameDirection.getGeometryN(pointCount - 2)).getCoordinateSequence()),
        0.0);
    long xyReads = leftSequence.xyReads + rightSequence.xyReads;
    assertTrue(
        "Source coordinate reads should scale linearly, but found " + xyReads, xyReads < 100000);
    long coordinateReads = leftSequence.coordinateReads + rightSequence.coordinateReads;
    assertTrue(
        "Source segment reads should scale linearly, but found " + coordinateReads,
        coordinateReads < 100000);
  }

  private void assertSharedPaths(String leftWkt, String rightWkt, String expectedWkt)
      throws ParseException {
    Geometry actual = Functions.sharedPaths(reader.read(leftWkt), reader.read(rightWkt));
    Geometry expected = reader.read(expectedWkt);
    assertTrue("Expected " + expected + " but found " + actual, expected.equalsExact(actual));
  }

  private void assertSharedPathBuckets(String leftWkt, String rightWkt, String expectedWkt)
      throws ParseException {
    Geometry actual = Functions.sharedPaths(reader.read(leftWkt), reader.read(rightWkt));
    Geometry expected = reader.read(expectedWkt);
    for (int bucket = 0; bucket < 2; bucket++) {
      Geometry actualBucket = actual.getGeometryN(bucket);
      Geometry expectedBucket = expected.getGeometryN(bucket);
      boolean matches =
          expectedBucket.isEmpty()
              ? actualBucket.isEmpty()
              : expectedBucket.equalsTopo(actualBucket);
      assertTrue("Expected bucket " + expectedBucket + " but found " + actualBucket, matches);
    }
  }

  private void assertBucketsEmpty(Geometry result) {
    assertTrue(result instanceof GeometryCollection);
    assertEquals(2, result.getNumGeometries());
    assertTrue(result.getGeometryN(0) instanceof MultiLineString);
    assertTrue(result.getGeometryN(1) instanceof MultiLineString);
    assertTrue(result.getGeometryN(0).isEmpty());
    assertTrue(result.getGeometryN(1).isEmpty());
  }

  private LineString firstPath(Geometry result) {
    return (LineString) result.getGeometryN(0).getGeometryN(0);
  }

  private double[] zValues(CoordinateSequence sequence) {
    double[] values = new double[sequence.size()];
    for (int i = 0; i < sequence.size(); i++) {
      values[i] = sequence.getZ(i);
    }
    return values;
  }

  private Geometry readUnchecked(String wkt) {
    try {
      return reader.read(wkt);
    } catch (ParseException e) {
      throw new AssertionError(e);
    }
  }

  private static final class CountingCoordinateSequence extends CoordinateArraySequence {
    private long xyReads;
    private long coordinateReads;

    private CountingCoordinateSequence(Coordinate[] coordinates) {
      super(coordinates, 3, 0);
    }

    @Override
    public double getX(int index) {
      xyReads++;
      return super.getX(index);
    }

    @Override
    public double getY(int index) {
      xyReads++;
      return super.getY(index);
    }

    @Override
    public Coordinate getCoordinate(int index) {
      coordinateReads++;
      return super.getCoordinate(index);
    }

    @Override
    public Coordinate getCoordinateCopy(int index) {
      coordinateReads++;
      return super.getCoordinateCopy(index);
    }
  }
}
