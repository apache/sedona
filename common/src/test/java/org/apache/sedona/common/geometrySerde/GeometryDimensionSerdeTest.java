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
package org.apache.sedona.common.geometrySerde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class GeometryDimensionSerdeTest {
  private static final GeometryFactory FACTORY = new GeometryFactory();

  @Test
  public void leadingNaNDoesNotDropLaterOrdinates() {
    LineString xyz =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0, 0, Double.NaN), new Coordinate(1, 1, 3)});
    LineString xym =
        FACTORY.createLineString(
            new Coordinate[] {new CoordinateXYM(0, 0, Double.NaN), new CoordinateXYM(1, 1, 4)});
    LineString xyzm =
        FACTORY.createLineString(
            new Coordinate[] {
              new CoordinateXYZM(0, 0, Double.NaN, Double.NaN), new CoordinateXYZM(1, 1, 3, 4)
            });

    assertLineRoundTrip(xyz, CoordinateType.XYZ, 3, 0, 3, Double.NaN);
    assertLineRoundTrip(xym, CoordinateType.XYM, 3, 1, Double.NaN, 4);
    assertLineRoundTrip(xyzm, CoordinateType.XYZM, 4, 1, 3, 4);
  }

  @Test
  public void dimensionDetectionTraversesMultipartMembers() {
    LineString first =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0, 0, Double.NaN), new Coordinate(1, 1, Double.NaN)});
    LineString second =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(2, 2, 7), new Coordinate(3, 3, 8)});
    MultiLineString input = FACTORY.createMultiLineString(new LineString[] {first, second});

    byte[] bytes = GeometrySerializer.serialize(input);
    MultiLineString output = (MultiLineString) GeometrySerializer.deserialize(bytes);

    assertEquals(CoordinateType.XYZ, coordinateType(bytes));
    assertSequenceLayout(((LineString) output.getGeometryN(0)).getCoordinateSequence(), 3, 0);
    assertSequenceLayout(((LineString) output.getGeometryN(1)).getCoordinateSequence(), 3, 0);
    assertEquals(7, output.getGeometryN(1).getCoordinate().getZ(), 0);
  }

  @Test
  public void measureMetadataAndTypedEmptyMembersKeepTheirLayout() {
    Point allNaNMeasure = FACTORY.createPoint(new CoordinateXYM(1, 2, Double.NaN));
    Point allNaNZM = FACTORY.createPoint(new CoordinateXYZM(1, 2, Double.NaN, Double.NaN));
    assertSequenceLayout(((Point) roundTrip(allNaNMeasure)).getCoordinateSequence(), 3, 1);
    assertSequenceLayout(((Point) roundTrip(allNaNZM)).getCoordinateSequence(), 4, 1);

    Point emptyM = FACTORY.createPoint(emptySequence(3, 1));
    Point emptyZM = FACTORY.createPoint(emptySequence(4, 1));
    assertSequenceLayout(((Point) roundTrip(emptyM)).getCoordinateSequence(), 3, 1);
    assertSequenceLayout(((Point) roundTrip(emptyZM)).getCoordinateSequence(), 4, 1);

    Point measuredPoint = FACTORY.createPoint(new CoordinateXYM(1, 2, 8));
    MultiPoint multiPoint = FACTORY.createMultiPoint(new Point[] {emptyM, measuredPoint});
    MultiPoint multiPointOutput = (MultiPoint) roundTrip(multiPoint);
    assertSequenceLayout(((Point) multiPointOutput.getGeometryN(0)).getCoordinateSequence(), 3, 1);
    assertEquals(8, multiPointOutput.getGeometryN(1).getCoordinate().getM(), 0);

    LineString emptyZMLine = FACTORY.createLineString(emptySequence(4, 1));
    LineString xyzmLine =
        FACTORY.createLineString(
            new Coordinate[] {new CoordinateXYZM(0, 0, 1, 2), new CoordinateXYZM(1, 1, 3, 4)});
    MultiLineString multiLine =
        FACTORY.createMultiLineString(new LineString[] {emptyZMLine, xyzmLine});
    MultiLineString multiLineOutput = (MultiLineString) roundTrip(multiLine);
    assertSequenceLayout(
        ((LineString) multiLineOutput.getGeometryN(0)).getCoordinateSequence(), 4, 1);

    Polygon emptyMPolygon = emptyPolygon(3, 1);
    assertSequenceLayout(
        ((Polygon) roundTrip(emptyMPolygon)).getExteriorRing().getCoordinateSequence(), 3, 1);
    Polygon measuredPolygon = measuredPolygon();
    MultiPolygon multiPolygon =
        FACTORY.createMultiPolygon(new Polygon[] {emptyMPolygon, measuredPolygon});
    MultiPolygon multiPolygonOutput = (MultiPolygon) roundTrip(multiPolygon);
    assertSequenceLayout(
        ((Polygon) multiPolygonOutput.getGeometryN(0)).getExteriorRing().getCoordinateSequence(),
        3,
        1);
    assertEquals(9, multiPolygonOutput.getGeometryN(1).getCoordinates()[0].getM(), 0);
  }

  @Test
  public void geometryCollectionPreservesChildLayoutsIndependently() {
    LineString xyz =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0, 0, Double.NaN), new Coordinate(1, 1, 3)});
    Point xym = FACTORY.createPoint(new CoordinateXYM(2, 2, 4));
    Polygon xyzm =
        FACTORY.createPolygon(
            FACTORY.createLinearRing(
                new Coordinate[] {
                  new CoordinateXYZM(0, 0, 1, 5),
                  new CoordinateXYZM(1, 0, 2, 6),
                  new CoordinateXYZM(0, 1, 3, 7),
                  new CoordinateXYZM(0, 0, 1, 5)
                }));
    GeometryCollection input = FACTORY.createGeometryCollection(new Geometry[] {xyz, xym, xyzm});

    GeometryCollection output = (GeometryCollection) roundTrip(input);

    assertSequenceLayout(((LineString) output.getGeometryN(0)).getCoordinateSequence(), 3, 0);
    assertSequenceLayout(((Point) output.getGeometryN(1)).getCoordinateSequence(), 3, 1);
    assertSequenceLayout(
        ((Polygon) output.getGeometryN(2)).getExteriorRing().getCoordinateSequence(), 4, 1);
  }

  @Test
  public void ordinaryJtsXyRemainsXy() throws ParseException {
    LineString input = (LineString) new WKTReader().read("LINESTRING (0 0, 1 1)");
    assertEquals(3, input.getCoordinateSequence().getDimension());

    byte[] bytes = GeometrySerializer.serialize(input);
    LineString output = (LineString) GeometrySerializer.deserialize(bytes);

    assertEquals(CoordinateType.XY, coordinateType(bytes));
    assertSequenceLayout(output.getCoordinateSequence(), 2, 0);
  }

  @Test
  public void rejectsRecoverablyHeterogeneousMultipartLayouts() {
    LineString xy =
        FACTORY.createLineString(new Coordinate[] {new CoordinateXY(0, 0), new CoordinateXY(1, 1)});
    LineString xyz =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(2, 2, 3), new Coordinate(3, 3, 4)});
    MultiLineString mixedZ = FACTORY.createMultiLineString(new LineString[] {xy, xyz});

    LineString xym =
        FACTORY.createLineString(
            new Coordinate[] {new CoordinateXYM(0, 0, 1), new CoordinateXYM(1, 1, 2)});
    MultiLineString mixedM = FACTORY.createMultiLineString(new LineString[] {xym, xyz});

    assertThrows(IllegalArgumentException.class, () -> GeometrySerializer.serialize(mixedZ));
    assertThrows(IllegalArgumentException.class, () -> GeometrySerializer.serialize(mixedM));
  }

  @Test
  public void rejectsHeterogeneousPolygonRingLayouts() {
    LinearRing shell =
        FACTORY.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0, 1),
              new Coordinate(10, 0, 1),
              new Coordinate(0, 10, 1),
              new Coordinate(0, 0, 1)
            });
    LinearRing hole =
        FACTORY.createLinearRing(
            new Coordinate[] {
              new CoordinateXY(1, 1),
              new CoordinateXY(2, 1),
              new CoordinateXY(1, 2),
              new CoordinateXY(1, 1)
            });
    Polygon polygon = FACTORY.createPolygon(shell, new LinearRing[] {hole});

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> GeometrySerializer.serialize(polygon));

    assertEquals(
        "GeometrySerializer cannot encode heterogeneous dimensional layouts in one Polygon "
            + "or multipart geometry. Use homogeneous components or a GeometryCollection.",
        error.getMessage());
  }

  private static void assertLineRoundTrip(
      LineString input,
      CoordinateType expectedType,
      int expectedDimension,
      int expectedMeasures,
      double expectedZ,
      double expectedM) {
    byte[] bytes = GeometrySerializer.serialize(input);
    LineString output = (LineString) GeometrySerializer.deserialize(bytes);
    CoordinateSequence sequence = output.getCoordinateSequence();

    assertEquals(expectedType, coordinateType(bytes));
    assertSequenceLayout(sequence, expectedDimension, expectedMeasures);
    assertOrdinate(expectedZ, sequence.getCoordinate(1).getZ());
    assertOrdinate(expectedM, sequence.getCoordinate(1).getM());
    assertTrue(Double.isNaN(sequence.getCoordinate(0).getZ()));
  }

  private static void assertSequenceLayout(
      CoordinateSequence sequence, int expectedDimension, int expectedMeasures) {
    assertEquals(expectedDimension, sequence.getDimension());
    assertEquals(expectedMeasures, sequence.getMeasures());
  }

  private static void assertOrdinate(double expected, double actual) {
    if (Double.isNaN(expected)) {
      assertTrue(Double.isNaN(actual));
    } else {
      assertEquals(expected, actual, 0);
    }
  }

  private static CoordinateType coordinateType(byte[] bytes) {
    return CoordinateType.valueOf(((bytes[0] & 0xFF) & 0x0F) >> 1);
  }

  private static Geometry roundTrip(Geometry geometry) {
    return GeometrySerializer.deserialize(GeometrySerializer.serialize(geometry));
  }

  private static CoordinateSequence emptySequence(int dimension, int measures) {
    return new CoordinateArraySequence(new Coordinate[0], dimension, measures);
  }

  private static Polygon emptyPolygon(int dimension, int measures) {
    return FACTORY.createPolygon(FACTORY.createLinearRing(emptySequence(dimension, measures)));
  }

  private static Polygon measuredPolygon() {
    LinearRing shell =
        FACTORY.createLinearRing(
            new Coordinate[] {
              new CoordinateXYM(0, 0, 9),
              new CoordinateXYM(1, 0, 10),
              new CoordinateXYM(0, 1, 11),
              new CoordinateXYM(0, 0, 9)
            });
    return FACTORY.createPolygon(shell);
  }
}
