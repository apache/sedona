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
import static org.apache.sedona.common.Constructors.geomFromWKT;
import static org.junit.Assert.*;

import com.google.common.geometry.S2CellId;
import com.google.common.math.DoubleMath;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.sedona.common.sphere.Haversine;
import org.apache.sedona.common.sphere.Spheroid;
import org.apache.sedona.common.utils.*;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.projection.ProjectionException;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class FunctionsTest extends TestBase {
  public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  protected static final double FP_TOLERANCE = 1e-12;
  protected static final double FP_TOLERANCE2 = 1e-4;
  protected static final CoordinateSequenceComparator COORDINATE_SEQUENCE_COMPARATOR =
      new CoordinateSequenceComparator(2) {
        @Override
        protected int compareCoordinate(
            CoordinateSequence s1, CoordinateSequence s2, int i, int dimension) {
          for (int d = 0; d < dimension; d++) {
            double ord1 = s1.getOrdinate(i, d);
            double ord2 = s2.getOrdinate(i, d);
            int comp = DoubleMath.fuzzyCompare(ord1, ord2, FP_TOLERANCE);
            if (comp != 0) return comp;
          }
          return 0;
        }
      };

  private final WKTReader wktReader = new WKTReader();

  @Test
  public void asEWKT() throws Exception {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4236);
    Geometry geometry = geometryFactory.createPoint(new Coordinate(1.0, 2.0));
    String actualResult = Functions.asEWKT(geometry);
    String expectedResult = "SRID=4236;POINT (1 2)";
    Geometry actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);

    geometry = geometryFactory.createPoint(new Coordinate(1.0, 2.0, 3.0));
    actualResult = Functions.asEWKT(geometry);
    expectedResult = "SRID=4236;POINT Z(1 2 3)";
    actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);

    geometry = geometryFactory.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    actualResult = Functions.asEWKT(geometry);
    expectedResult = "SRID=4236;POINT M(1 2 3)";
    actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);

    geometry = geometryFactory.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    actualResult = Functions.asEWKT(geometry);
    expectedResult = "SRID=4236;POINT ZM(1 2 3 4)";
    actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void asWKT() throws Exception {
    Geometry geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0));
    String actualResult = Functions.asWKT(geometry);
    String expectedResult = "POINT (1 2)";
    Geometry actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);

    geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    actualResult = Functions.asWKT(geometry);
    expectedResult = "POINT Z(1 2 3)";
    actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);

    geometry = GEOMETRY_FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    actualResult = Functions.asWKT(geometry);
    expectedResult = "POINT M(1 2 3)";
    actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);

    geometry = GEOMETRY_FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    actualResult = Functions.asWKT(geometry);
    expectedResult = "POINT ZM(1 2 3 4)";
    actual = geomFromEWKT(expectedResult);
    assertEquals(geometry, actual);
    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void asWKB() throws Exception {
    Geometry geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0));
    byte[] actualResult = Functions.asWKB(geometry);
    Geometry expected = Constructors.geomFromWKB(actualResult);
    assertEquals(expected, geometry);

    geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    actualResult = Functions.asWKB(geometry);
    expected = Constructors.geomFromWKB(actualResult);
    assertEquals(expected, geometry);
  }

  @Test
  public void asHexEWKB() throws ParseException {
    String[] geoms = {
      "POINT(1 2)",
      "LINESTRING (30 20, 20 25, 20 15, 30 20)",
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))"
    };
    String[] expected = {
      // NDR - little endian
      "0101000000000000000000F03F0000000000000040",
      "0102000000040000000000000000003E4000000000000034400000000000003440000000000000394000000000000034400000000000002E400000000000003E400000000000003440",
      "010300000002000000050000000000000000000000000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000000000000000000000050000000000000000001440000000000000144000000000000014400000000000002040000000000000204000000000000020400000000000002040000000000000144000000000000014400000000000001440",

      // XDR - big endian
      "00000000013FF00000000000004000000000000000",
      "000000000200000004403E0000000000004034000000000000403400000000000040390000000000004034000000000000402E000000000000403E0000000000004034000000000000",
      "000000000300000002000000050000000000000000000000000000000040240000000000000000000000000000402400000000000040240000000000000000000000000000402400000000000000000000000000000000000000000000000000054014000000000000401400000000000040140000000000004020000000000000402000000000000040200000000000004020000000000000401400000000000040140000000000004014000000000000"
    };
    testAsHexEWKB(geoms, expected);
  }

  public void testAsHexEWKB(String[] geoms, String[] expected) throws ParseException {
    String[] endians = {"NDR", "XDR"};
    int offset = 0;
    for (String e : endians) {
      for (int i = 0; i < geoms.length; i++) {
        assertEquals(
            expected[i + offset], Functions.asHexEWKB(Constructors.geomFromWKT(geoms[i], 0), e));
      }
      offset = 3;
    }
  }

  @Test
  public void pointFromWKB() throws Exception {
    Geometry geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0));
    byte[] wkbGeom = Functions.asWKB(geometry);
    Geometry result = Constructors.pointFromWKB(wkbGeom);
    assertEquals(geometry, result);

    geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    wkbGeom = Functions.asWKB(geometry);
    result = Constructors.pointFromWKB(wkbGeom, 4326);
    assertEquals(geometry, result);
    assertEquals(4326, Objects.requireNonNull(result).getSRID());

    geometry = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0));
    wkbGeom = Functions.asWKB(geometry);
    result = Constructors.pointFromWKB(wkbGeom, 0);
    assertNull(result);
  }

  @Test
  public void lineFromWKB() throws Exception {
    Geometry geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0));
    byte[] wkbGeom = Functions.asWKB(geometry);
    Geometry result = Constructors.lineFromWKB(wkbGeom);
    assertNull(result);

    geometry = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 5.0, 5.0, 5.0, 2.0));
    wkbGeom = Functions.asWKB(geometry);
    result = Constructors.lineFromWKB(wkbGeom, 4326);
    assertEquals(geometry, result);
    assertEquals(4326, Objects.requireNonNull(result).getSRID());

    geometry = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0));
    wkbGeom = Functions.asWKB(geometry);
    result = Constructors.lineFromWKB(wkbGeom);
    assertEquals(geometry, result);
    assertEquals(0, Objects.requireNonNull(result).getSRID());
  }

  @Test
  public void splitLineStringByMultipoint() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0));
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0));

    String actualResult = Functions.split(lineString, multiPoint).norm().toText();
    String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitMultiLineStringByMultiPoint() {
    LineString[] lineStrings =
        new LineString[] {
          GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0)),
          GEOMETRY_FACTORY.createLineString(coordArray(3.0, 3.0, 4.5, 4.5, 5.0, 5.0))
        };
    MultiLineString multiLineString = GEOMETRY_FACTORY.createMultiLineString(lineStrings);
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(
            coordArray(0.5, 0.5, 1.0, 1.0, 3.5, 3.5, 4.0, 4.0));

    String actualResult = Functions.split(multiLineString, multiPoint).norm().toText();
    String expectedResult =
        "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2), (3 3, 3.5 3.5), (3.5 3.5, 4 4), (4 4, 4.5 4.5, 5 5))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitLineStringByMultiPointWithReverseOrder() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0));
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(1.0, 1.0, 0.5, 0.5));

    String actualResult = Functions.split(lineString, multiPoint).norm().toText();
    String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitLineStringWithReverseOrderByMultiPoint() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray(2.0, 2.0, 1.5, 1.5, 0.0, 0.0));
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0));

    String actualResult = Functions.split(lineString, multiPoint).norm().toText();
    String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitLineStringWithVerticalSegmentByMultiPoint() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray(1.5, 0.0, 1.5, 1.5, 2.0, 2.0));
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(1.5, 0.5, 1.5, 1.0));

    String actualResult = Functions.split(lineString, multiPoint).norm().toText();
    String expectedResult =
        "MULTILINESTRING ((1.5 0, 1.5 0.5), (1.5 0.5, 1.5 1), (1.5 1, 1.5 1.5, 2 2))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitLineStringByLineString() {
    LineString lineStringInput = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 2.0, 2.0));
    LineString lineStringBlade = GEOMETRY_FACTORY.createLineString(coordArray(1.0, 0.0, 1.0, 3.0));

    String actualResult = Functions.split(lineStringInput, lineStringBlade).norm().toText();
    String expectedResult = "MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitLineStringByPolygon() {
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 2.0, 2.0));
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(1.0, 0.0, 1.0, 3.0, 3.0, 3.0, 3.0, 0.0, 1.0, 0.0));

    String actualResult = Functions.split(lineString, polygon).norm().toText();
    String expectedResult = "MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitLineStringFpPrecisionIssue() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                -8.961173822708158, -3.93776773106963, -8.08908227533288, -3.8845245068873444));
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(
                -6.318936372442209, -6.44985859539768,
                -8.669092633645995, -3.0659222341103956,
                -6.264600073171498, -3.075347218794894,
                -5.3654318906014495, -3.1019726170919877,
                -5.488002156793005, -5.892626167859213,
                -6.318936372442209, -6.44985859539768));

    Geometry result = Functions.split(lineString, polygon);
    assertEquals(2, result.getNumGeometries());
    assertEquals(lineString.getLength(), result.getLength(), 1e-6);
  }

  @Test
  public void tempTest() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                -8.961173822708158, -3.93776773106963, -8.08908227533288, -3.8845245068873444));
    Point point =
        GEOMETRY_FACTORY.createPoint(new Coordinate(-8.100103048843774, -3.885197350829553));
    PreparedGeometryFactory factory = new PreparedGeometryFactory();
    PreparedGeometry prepLineString = factory.create(lineString);
    boolean intersects = prepLineString.intersects(point);
    System.out.println(intersects);
  }

  @Test
  public void splitPolygonByLineString() {
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0));
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(3.0, 0.0, 3.0, 6.0));

    String actualResult = Functions.split(polygon, lineString).norm().toText();
    String expectedResult =
        "MULTIPOLYGON (((1 1, 1 5, 3 5, 3 1, 1 1)), ((3 1, 3 5, 5 5, 5 1, 3 1)))";

    assertEquals(actualResult, expectedResult);
  }

  // // overlapping multipolygon by linestring
  @Test
  public void splitOverlappingMultiPolygonByLineString() {
    Polygon[] polygons =
        new Polygon[] {
          GEOMETRY_FACTORY.createPolygon(
              coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0)),
          GEOMETRY_FACTORY.createPolygon(
              coordArray(2.0, 1.0, 6.0, 1.0, 6.0, 5.0, 2.0, 5.0, 2.0, 1.0))
        };
    MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(polygons);
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(3.0, 0.0, 3.0, 6.0));

    String actualResult = Functions.split(multiPolygon, lineString).norm().toText();
    String expectedResult =
        "MULTIPOLYGON (((1 1, 1 5, 3 5, 3 1, 1 1)), ((2 1, 2 5, 3 5, 3 1, 2 1)), ((3 1, 3 5, 5 5, 5 1, 3 1)), ((3 1, 3 5, 6 5, 6 1, 3 1)))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitPolygonByInsideRing() {
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0));
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(
            coordArray(2.0, 2.0, 3.0, 2.0, 3.0, 3.0, 2.0, 3.0, 2.0, 2.0));

    String actualResult = Functions.split(polygon, lineString).norm().toText();
    String expectedResult =
        "MULTIPOLYGON (((1 1, 1 5, 5 5, 5 1, 1 1), (2 2, 3 2, 3 3, 2 3, 2 2)), ((2 2, 2 3, 3 3, 3 2, 2 2)))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitPolygonByLineOutsideOfPolygon() {
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0));
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(10.0, 10.0, 11.0, 11.0));

    String actualResult = Functions.split(polygon, lineString).norm().toText();
    String expectedResult = "MULTIPOLYGON (((1 1, 1 5, 5 5, 5 1, 1 1)))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitPolygonByPolygon() {
    Polygon polygonInput =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 0.0));
    Polygon polygonBlade =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(2.0, 0.0, 6.0, 0.0, 6.0, 4.0, 2.0, 4.0, 2.0, 0.0));

    String actualResult = Functions.split(polygonInput, polygonBlade).norm().toText();
    String expectedResult =
        "MULTIPOLYGON (((0 0, 0 4, 2 4, 2 0, 0 0)), ((2 0, 2 4, 4 4, 4 0, 2 0)))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitPolygonWithHoleByLineStringThroughHole() {
    LinearRing shell =
        GEOMETRY_FACTORY.createLinearRing(
            coordArray(0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 0.0));
    LinearRing[] holes =
        new LinearRing[] {
          GEOMETRY_FACTORY.createLinearRing(
              coordArray(1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0))
        };
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes);
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1.5, -1.0, 1.5, 5.0));

    String actualResult = Functions.split(polygon, lineString).norm().toText();
    String expectedResult =
        "MULTIPOLYGON (((0 0, 0 4, 1.5 4, 1.5 2, 1 2, 1 1, 1.5 1, 1.5 0, 0 0)), ((1.5 0, 1.5 1, 2 1, 2 2, 1.5 2, 1.5 4, 4 4, 4 0, 1.5 0)))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitPolygonWithHoleByLineStringNotThroughHole() {
    LinearRing shell =
        GEOMETRY_FACTORY.createLinearRing(
            coordArray(0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 0.0));
    LinearRing[] holes =
        new LinearRing[] {
          GEOMETRY_FACTORY.createLinearRing(
              coordArray(1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0))
        };
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes);
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(3.0, -1.0, 3.0, 5.0));

    String actualResult = Functions.split(polygon, lineString).norm().toText();
    String expectedResult =
        "MULTIPOLYGON (((0 0, 0 4, 3 4, 3 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((3 0, 3 4, 4 4, 4 0, 3 0)))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitHomogeneousLinealGeometryCollectionByMultiPoint() {
    LineString[] lineStrings =
        new LineString[] {
          GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0)),
          GEOMETRY_FACTORY.createLineString(coordArray(3.0, 3.0, 4.5, 4.5, 5.0, 5.0))
        };
    GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(lineStrings);
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(
            coordArray(0.5, 0.5, 1.0, 1.0, 3.5, 3.5, 4.0, 4.0));

    String actualResult = Functions.split(geometryCollection, multiPoint).norm().toText();
    String expectedResult =
        "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2), (3 3, 3.5 3.5), (3.5 3.5, 4 4), (4 4, 4.5 4.5, 5 5))";

    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void splitHeterogeneousGeometryCollection() {
    Geometry[] geometry =
        new Geometry[] {
          GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5)),
          GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
        };
    GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(geometry);
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.5, 0.0, 0.5, 5.0));

    Geometry actualResult = Functions.split(geometryCollection, lineString);

    assertNull(actualResult);
  }

  @Test
  public void dimensionGeometry2D() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
    Integer actualResult = Functions.dimension(point);
    Integer expectedResult = 0;
    assertEquals(actualResult, expectedResult);

    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5));
    actualResult = Functions.dimension(lineString);
    expectedResult = 1;
    assertEquals(actualResult, expectedResult);

    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0));
    actualResult = Functions.dimension(polygon);
    expectedResult = 2;
    assertEquals(actualResult, expectedResult);

    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.0, 0.0, 1.0, 1.0));
    actualResult = Functions.dimension(multiPoint);
    expectedResult = 0;
    assertEquals(actualResult, expectedResult);

    LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(1.0, 1.0, 2.0, 2.0));
    MultiLineString multiLineString =
        GEOMETRY_FACTORY.createMultiLineString(new LineString[] {lineString, lineString2});
    actualResult = Functions.dimension(multiLineString);
    expectedResult = 1;
    assertEquals(actualResult, expectedResult);

    Polygon polygon2 =
        GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon, polygon2});
    actualResult = Functions.dimension(multiPolygon);
    expectedResult = 2;
    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void hasZ() throws ParseException {
    Geometry geom = Constructors.geomFromWKT("POINT ZM(1 2 3 4)", 0);
    assertTrue(Functions.hasZ(geom));

    geom = Constructors.geomFromWKT("POINT(1 2)", 0);
    assertFalse(Functions.hasZ(geom));

    geom = Constructors.geomFromWKT("POINT(34 25)", 0);
    assertFalse(Functions.hasZ(geom));

    geom =
        Constructors.geomFromWKT(
            "POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))", 0);
    assertTrue(Functions.hasZ(geom));
  }

  @Test
  public void hasM() throws ParseException {
    Geometry geom = Constructors.geomFromWKT("POINT ZM(1 2 3 4)", 0);
    assertTrue(Functions.hasM(geom));

    geom = Constructors.geomFromWKT("POINT Z(1 2 3)", 0);
    assertFalse(Functions.hasM(geom));

    geom = Constructors.geomFromWKT("POINT(34 25)", 0);
    assertFalse(Functions.hasM(geom));

    geom =
        Constructors.geomFromWKT(
            "POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))", 0);
    assertTrue(Functions.hasM(geom));
  }

  @Test
  public void testM() throws ParseException {
    Geometry geom = Constructors.geomFromWKT("POINT ZM(1 2 3 4)", 0);
    double actual = Functions.m(geom);
    double expected = 4;
    assertEquals(expected, actual, FP_TOLERANCE2);

    geom = Constructors.geomFromWKT("POINT M(1 2 3)", 0);
    actual = Functions.m(geom);
    expected = 3;
    assertEquals(expected, actual, FP_TOLERANCE2);

    geom = Constructors.geomFromWKT("POINT Z(1 2 3)", 0);
    actual = Functions.m(geom);
    assertTrue(Double.isNaN(actual));

    geom = Constructors.geomFromWKT("LINESTRING ZM(1 2 3 4, 2 3 4 5)", 0);
    Double actualRes = Functions.m(geom);
    assertNull(actualRes);
  }

  @Test
  public void testMMin() throws ParseException {
    Geometry geom =
        Constructors.geomFromWKT("LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)", 0);
    Double actual = Functions.mMin(geom);
    Double expected = -1.0;
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("LINESTRING(1 1 1, 2 2 2, 3 3 3, -1 -1 -1)", 0);
    actual = Functions.mMin(geom);
    assertNull(actual);
  }

  @Test
  public void testMMax() throws ParseException {
    Geometry geom =
        Constructors.geomFromWKT("LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)", 0);
    Double actual = Functions.mMax(geom);
    Double expected = 3.0;
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("LINESTRING(1 1 1, 2 2 2, 3 3 3, -1 -1 -1)", 0);
    actual = Functions.mMax(geom);
    assertNull(actual);
  }

  @Test
  public void dimensionGeometry3D() {
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    Integer actualResult = Functions.dimension(point3D);
    Integer expectedResult = 0;
    assertEquals(actualResult, expectedResult);

    LineString lineString3D = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 1, 1, 2));
    actualResult = Functions.dimension(lineString3D);
    expectedResult = 1;
    assertEquals(actualResult, expectedResult);

    Polygon polygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1));
    actualResult = Functions.dimension(polygon3D);
    expectedResult = 2;
    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void dimensionGeometryCollection() {
    Geometry[] geometry =
        new Geometry[] {
          GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5)),
          GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
        };
    GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(geometry);

    Integer actualResult = Functions.dimension(geometryCollection);
    Integer expectedResult = 2;
    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void dimensionGeometryEmpty() {
    GeometryCollection emptyGeometryCollection = GEOMETRY_FACTORY.createGeometryCollection();

    Integer actualResult = Functions.dimension(emptyGeometryCollection);
    Integer expectedResult = 0;
    assertEquals(actualResult, expectedResult);
  }

  private static boolean intersects(Set<?> s1, Set<?> s2) {
    Set<?> copy = new HashSet<>(s1);
    copy.retainAll(s2);
    return !copy.isEmpty();
  }

  @Test
  public void convexAndConcaveHullSRID() throws ParseException {
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((-92.8125 37.857507, -89.362793 36.013561, -92.548828 35.782171, -92.197266 34.669359, -94.614258 36.4036, -92.834473 36.580247, -92.8125 37.857507))",
            4326);
    Geometry convex = Functions.convexHull(geom);
    Geometry concave = Functions.concaveHull(geom, 1, false);
    assertEquals(4326, convex.getSRID());
    assertEquals(4326, concave.getSRID());
  }

  @Test
  public void envelopeAndCentroidSRID() throws ParseException {
    Geometry geom = Constructors.geomFromWKT("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 3857);
    Geometry envelope = Functions.envelope(geom);
    assertEquals(3857, envelope.getSRID());
    Geometry centroid = Functions.getCentroid(geom);
    assertEquals(3857, centroid.getSRID());
  }

  @Test
  public void getGoogleS2CellIDsPoint() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
    Long[] cid = Functions.s2CellIDs(point, 30);
    Polygon reversedPolygon = S2Utils.toJTSPolygon(new S2CellId(cid[0]));
    // cast the cell to a rectangle, it must be able to cover the points
    assertTrue(reversedPolygon.contains(point));
  }

  @Test
  public void getGoogleS2CellIDsPolygon() {
    // polygon with holes
    Polygon target =
        GEOMETRY_FACTORY.createPolygon(
            GEOMETRY_FACTORY.createLinearRing(
                coordArray(0.1, 0.1, 0.5, 0.1, 1.0, 0.3, 1.0, 1.0, 0.1, 1.0, 0.1, 0.1)),
            new LinearRing[] {
              GEOMETRY_FACTORY.createLinearRing(
                  coordArray(0.2, 0.2, 0.5, 0.2, 0.6, 0.7, 0.2, 0.6, 0.2, 0.2))
            });
    // polygon inside the hole, shouldn't intersect with the polygon
    Polygon polygonInHole =
        GEOMETRY_FACTORY.createPolygon(coordArray(0.3, 0.3, 0.4, 0.3, 0.3, 0.4, 0.3, 0.3));
    // mbr of the polygon that cover all
    Geometry mbr = target.getEnvelope();
    HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
    HashSet<Long> inHoleCells =
        new HashSet<>(Arrays.asList(Functions.s2CellIDs(polygonInHole, 10)));
    HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
    assert mbrCells.containsAll(targetCells);
    assert !intersects(targetCells, inHoleCells);
    assert mbrCells.containsAll(targetCells);
  }

  @Test
  public void getGoogleS2CellIDsLineString() {
    // polygon with holes
    LineString target = GEOMETRY_FACTORY.createLineString(coordArray(0.2, 0.2, 0.3, 0.4, 0.4, 0.6));
    LineString crossLine = GEOMETRY_FACTORY.createLineString(coordArray(0.4, 0.1, 0.1, 0.4));
    // mbr of the polygon that cover all
    Geometry mbr = target.getEnvelope();
    // cover the target polygon, and convert cells back to polygons
    HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 15)));
    HashSet<Long> crossCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(crossLine, 15)));
    HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 15)));
    assert intersects(targetCells, crossCells);
    assert mbrCells.containsAll(targetCells);
  }

  @Test
  public void getGoogleS2CellIDsMultiPolygon() {
    // polygon with holes
    Polygon[] geoms =
        new Polygon[] {
          GEOMETRY_FACTORY.createPolygon(coordArray(0.1, 0.1, 0.5, 0.1, 0.1, 0.6, 0.1, 0.1)),
          GEOMETRY_FACTORY.createPolygon(
              coordArray(0.2, 0.1, 0.6, 0.3, 0.7, 0.6, 0.2, 0.5, 0.2, 0.1))
        };
    MultiPolygon target = GEOMETRY_FACTORY.createMultiPolygon(geoms);
    Geometry mbr = target.getEnvelope();
    HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
    HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
    HashSet<Long> separateCoverCells = new HashSet<>();
    for (Geometry geom : geoms) {
      separateCoverCells.addAll(Arrays.asList(Functions.s2CellIDs(geom, 10)));
    }
    assert mbrCells.containsAll(targetCells);
    assert targetCells.equals(separateCoverCells);
  }

  @Test
  public void getGoogleS2CellIDsMultiLineString() {
    // polygon with holes
    MultiLineString target =
        GEOMETRY_FACTORY.createMultiLineString(
            new LineString[] {
              GEOMETRY_FACTORY.createLineString(coordArray(0.1, 0.1, 0.2, 0.1, 0.3, 0.4, 0.5, 0.9)),
              GEOMETRY_FACTORY.createLineString(coordArray(0.5, 0.1, 0.1, 0.5, 0.3, 0.1))
            });
    Geometry mbr = target.getEnvelope();
    Point outsidePoint = GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.7));
    HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
    HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
    Long outsideCell = Functions.s2CellIDs(outsidePoint, 10)[0];
    // the cells should all be within mbr
    assert mbrCells.containsAll(targetCells);
    // verify point within mbr but shouldn't intersect with linestring
    assert mbrCells.contains(outsideCell);
    assert !targetCells.contains(outsideCell);
  }

  @Test
  public void getGoogleS2CellIDsMultiPoint() {
    // polygon with holes
    MultiPoint target =
        GEOMETRY_FACTORY.createMultiPoint(
            new Point[] {
              GEOMETRY_FACTORY.createPoint(new Coordinate(0.1, 0.1)),
              GEOMETRY_FACTORY.createPoint(new Coordinate(0.2, 0.1)),
              GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.2)),
              GEOMETRY_FACTORY.createPoint(new Coordinate(0.5, 0.4))
            });
    Geometry mbr = target.getEnvelope();
    Point outsidePoint = GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.7));
    HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
    HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
    // the cells should all be within mbr
    assert mbrCells.containsAll(targetCells);
    assert targetCells.size() == 4;
  }

  @Test
  public void getGoogleS2CellIDsGeometryCollection() {
    // polygon with holes
    Geometry[] geoms =
        new Geometry[] {
          GEOMETRY_FACTORY.createLineString(coordArray(0.1, 0.1, 0.2, 0.1, 0.3, 0.4, 0.5, 0.9)),
          GEOMETRY_FACTORY.createPolygon(coordArray(0.1, 0.1, 0.5, 0.1, 0.1, 0.6, 0.1, 0.1)),
          GEOMETRY_FACTORY.createMultiPoint(
              new Point[] {
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.1, 0.1)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.2, 0.1)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.2)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.5, 0.4))
              })
        };
    GeometryCollection target = GEOMETRY_FACTORY.createGeometryCollection(geoms);
    Geometry mbr = target.getEnvelope();
    HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
    HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
    HashSet<Long> separateCoverCells = new HashSet<>();
    for (Geometry geom : geoms) {
      separateCoverCells.addAll(Arrays.asList(Functions.s2CellIDs(geom, 10)));
    }
    // the cells should all be within mbr
    assert mbrCells.containsAll(targetCells);
    // separately cover should return same result as covered together
    assert separateCoverCells.equals(targetCells);
  }

  @Test
  public void getGoogleS2CellIDsAllSameLevel() {
    // polygon with holes
    GeometryCollection target =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createPolygon(coordArray(0.3, 0.3, 0.4, 0.3, 0.3, 0.4, 0.3, 0.3)),
              GEOMETRY_FACTORY.createPoint(new Coordinate(0.7, 1.2))
            });
    Long[] cellIds = Functions.s2CellIDs(target, 10);
    HashSet<Integer> levels =
        Arrays.stream(cellIds)
            .map(c -> new S2CellId(c).level())
            .collect(Collectors.toCollection(HashSet::new));
    HashSet<Integer> expects = new HashSet<>();
    expects.add(10);
    assertEquals(expects, levels);
  }

  @Test
  public void testS2ToGeom() {
    Geometry target =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(0.1, 0.1, 0.5, 0.1, 1.0, 0.3, 1.0, 1.0, 0.1, 1.0, 0.1, 0.1));
    Long[] cellIds = Functions.s2CellIDs(target, 10);
    Geometry[] polygons =
        Functions.s2ToGeom(Arrays.stream(cellIds).mapToLong(Long::longValue).toArray());
    assertTrue(polygons[0].intersects(target));
    assertTrue(polygons[20].intersects(target));
    assertTrue(polygons[100].intersects(target));
  }

  @Test
  public void testUnion() throws ParseException {
    long[] cellIds =
        new long[] {
          1152991873351024640L, 1153132610839379968L, 1153273348327735296L, 1153414085816090624L
        };
    Geometry[] polygons = Functions.s2ToGeom(cellIds);
    String actual = Functions.asWKT(Functions.union(polygons));
    String expected =
        "POLYGON ((0.6014716838554667 -0.0000000000000254, 0.6014716838554158 -0.0000000000000254, -0.0000000000000254 -0.0000000000000254, -0.0000000000000254 0.6014385452363985, -0.0000000000000254 0.6014716838554667, -0.0000000000000254 1.2121321753162642, 0.6014716838554667 1.2121321753162642, 0.6014716838554667 1.2120654068310366, 1.2121321753162642 1.2120654068310366, 1.2121321753162642 0.6014385452364494, 1.2121321753162642 0.6013371003640015, 1.2121321753162642 -0.0000000000000254, 0.6014716838554667 -0.0000000000000254))";
    assertEquals(expected, actual);

    Geometry[] points =
        new Geometry[] {
          Constructors.point(0, 5), Constructors.point(5, 0), Constructors.point(5, 5)
        };
    actual = Functions.asWKT(Functions.union(points));
    expected = "MULTIPOINT ((0 5), (5 0), (5 5))";
    assertEquals(expected, actual);

    Geometry[] mPoly =
        new Geometry[] {
          Constructors.geomFromWKT(
              "MULTIPOLYGON (((-0.0000000000000254 -0.0000000000000254, 0.6014716838554667 -0.0000000000000254, 0.6014716838554667 0.6014716838554667, -0.0000000000000254 0.6014716838554667, -0.0000000000000254 -0.0000000000000254)))",
              0),
          Constructors.geomFromWKT(
              "MULTIPOLYGON (((0.6014716838554158 -0.0000000000000254, 1.2121321753162642 -0.0000000000000254, 1.2121321753162642 0.6014385452364494, 0.6014716838554158 0.6014385452364494, 0.6014716838554158 -0.0000000000000254)))",
              0),
          Constructors.geomFromWKT(
              "MULTIPOLYGON (((0.6014716838554158 0.6013371003640015, 1.2121321753162642 0.6013371003640015, 1.2121321753162642 1.2120654068310366, 0.6014716838554158 1.2120654068310366, 0.6014716838554158 0.6013371003640015)))",
              0),
          Constructors.geomFromWKT(
              "MULTIPOLYGON (((-0.0000000000000254 0.6014385452363985, 0.6014716838554667 0.6014385452363985, 0.6014716838554667 1.2121321753162642, -0.0000000000000254 1.2121321753162642, -0.0000000000000254 0.6014385452363985)))",
              0)
        };
    actual = Functions.asWKT(Functions.union(mPoly));
    expected =
        "POLYGON ((0.6014716838554667 -0.0000000000000254, 0.6014716838554158 -0.0000000000000254, -0.0000000000000254 -0.0000000000000254, -0.0000000000000254 0.6014385452363985, -0.0000000000000254 0.6014716838554667, -0.0000000000000254 1.2121321753162642, 0.6014716838554667 1.2121321753162642, 0.6014716838554667 1.2120654068310366, 1.2121321753162642 1.2120654068310366, 1.2121321753162642 0.6014385452364494, 1.2121321753162642 0.6013371003640015, 1.2121321753162642 -0.0000000000000254, 0.6014716838554667 -0.0000000000000254))";
    assertEquals(expected, actual);
  }

  @Test
  public void testUnaryUnion() throws ParseException {
    Geometry geometry =
        Constructors.geomFromEWKT(
            "MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))");
    String actual = Functions.unaryUnion(geometry).toString();
    String expected = "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromEWKT(
            "MULTILINESTRING ((10 10, 20 20, 30 30),(25 25, 35 35, 45 45),(40 40, 50 50, 60 60),(55 55, 65 65, 75 75))");
    actual = Functions.unaryUnion(geometry).toString();
    expected =
        "MULTILINESTRING ((10 10, 20 20, 25 25), (25 25, 30 30), (30 30, 35 35, 40 40), (40 40, 45 45), (45 45, 50 50, 55 55), (55 55, 60 60), (60 60, 65 65, 75 75))";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromEWKT(
            "GEOMETRYCOLLECTION (POINT (10 10),LINESTRING (20 20, 30 30),POLYGON ((25 25, 35 35, 35 35, 25 25)),MULTIPOINT (30 30, 40 40),MULTILINESTRING ((40 40, 50 50), (45 45, 55 55)),MULTIPOLYGON (((50 50, 60 60, 60 60, 50 50)), ((55 55, 65 65, 65 65, 55 55))))");
    actual = Functions.unaryUnion(geometry).toString();
    expected =
        "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (20 20, 30 30), LINESTRING (40 40, 45 45), LINESTRING (45 45, 50 50), LINESTRING (50 50, 55 55))";
    assertEquals(expected, actual);
  }

  @Test
  public void testH3ToGeom() {
    Geometry target =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(0.1, 0.1, 0.5, 0.1, 1.0, 0.3, 1.0, 1.0, 0.1, 1.0, 0.1, 0.1));
    Long[] cellIds = Functions.h3CellIDs(target, 4, true);
    Geometry[] polygons =
        Functions.h3ToGeom(Arrays.stream(cellIds).mapToLong(Long::longValue).toArray());
    assertTrue(polygons[0].intersects(target));
    assertTrue(polygons[11].intersects(target));
    assertTrue(polygons[20].intersects(target));
  }

  /** Test H3CellIds: pass in all the types of geometry, test if the function cover */
  @Test
  public void h3CellIDs() {
    Geometry[] combinedGeoms =
        new Geometry[] {
          GEOMETRY_FACTORY.createPoint(new Coordinate(0.1, 0.1)),
          GEOMETRY_FACTORY.createPoint(new Coordinate(0.2, 0.1)),
          GEOMETRY_FACTORY.createLineString(coordArray(0.1, 0.1, 0.2, 0.1, 0.3, 0.4, 0.5, 0.9)),
          GEOMETRY_FACTORY.createLineString(coordArray(0.5, 0.1, 0.1, 0.5, 0.3, 0.1)),
          GEOMETRY_FACTORY.createPolygon(coordArray(0.1, 0.1, 0.5, 0.1, 0.1, 0.6, 0.1, 0.1)),
          GEOMETRY_FACTORY.createPolygon(
              coordArray(0.2, 0.1, 0.6, 0.3, 0.7, 0.6, 0.2, 0.5, 0.2, 0.1))
        };
    // The test geometries, cover all 7 geometry types targeted
    Geometry[] targets =
        new Geometry[] {
          GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2)),
          GEOMETRY_FACTORY.createPolygon(
              GEOMETRY_FACTORY.createLinearRing(
                  coordArray(0.1, 0.1, 0.5, 0.1, 1.0, 0.3, 1.0, 1.0, 0.1, 1.0, 0.1, 0.1)),
              new LinearRing[] {
                GEOMETRY_FACTORY.createLinearRing(
                    coordArray(0.2, 0.2, 0.5, 0.2, 0.6, 0.7, 0.2, 0.6, 0.2, 0.2))
              }),
          GEOMETRY_FACTORY.createLineString(coordArray(0.2, 0.2, 0.3, 0.4, 0.4, 0.6)),
          GEOMETRY_FACTORY.createGeometryCollection(
              new Geometry[] {
                GEOMETRY_FACTORY.createMultiPoint(
                    new Point[] {(Point) combinedGeoms[0], (Point) combinedGeoms[1]}),
                GEOMETRY_FACTORY.createMultiLineString(
                    new LineString[] {
                      (LineString) combinedGeoms[2], (LineString) combinedGeoms[3]
                    }),
                GEOMETRY_FACTORY.createMultiPolygon(
                    new Polygon[] {(Polygon) combinedGeoms[4], (Polygon) combinedGeoms[5]})
              })
        };
    int resolution = 7;
    // the expected results
    Set<Long> expects = new HashSet<>();
    expects.addAll(
        new HashSet<>(
            Collections.singletonList(
                H3Utils.coordinateToCell(targets[0].getCoordinate(), resolution))));
    expects.addAll(new HashSet<>(H3Utils.polygonToCells((Polygon) targets[1], resolution, true)));
    expects.addAll(
        new HashSet<>(H3Utils.lineStringToCells((LineString) targets[2], resolution, true)));
    // for GeometryCollection, generate separately for the underlying geoms
    expects.add(H3Utils.coordinateToCell(combinedGeoms[0].getCoordinate(), resolution));
    expects.add(H3Utils.coordinateToCell(combinedGeoms[1].getCoordinate(), resolution));
    expects.addAll(H3Utils.lineStringToCells((LineString) combinedGeoms[2], resolution, true));
    expects.addAll(H3Utils.lineStringToCells((LineString) combinedGeoms[3], resolution, true));
    expects.addAll(H3Utils.polygonToCells((Polygon) combinedGeoms[4], resolution, true));
    expects.addAll(H3Utils.polygonToCells((Polygon) combinedGeoms[5], resolution, true));
    // generate exact
    Set<Long> exacts =
        new HashSet<>(
            Arrays.asList(
                Functions.h3CellIDs(
                    GEOMETRY_FACTORY.createGeometryCollection(targets), resolution, true)));
    assert exacts.equals(expects);
  }

  /** Test H3CellDistance */
  @Test
  public void h3CellDistance() {
    LineString pentagonLine =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                58.174758948493505, 10.427371502467615, 58.1388817207103, 10.469490838693966));
    // normal line
    LineString line =
        GEOMETRY_FACTORY.createLineString(
            coordArray(
                -4.414062499999996, 19.790494005157534, 5.781250000000004, 13.734595619093557));
    long pentagonDist =
        Functions.h3CellDistance(
            H3Utils.coordinateToCell(pentagonLine.getCoordinateN(0), 11),
            H3Utils.coordinateToCell(pentagonLine.getCoordinateN(1), 11));
    long lineDist =
        Functions.h3CellDistance(
            H3Utils.coordinateToCell(line.getCoordinateN(0), 10),
            H3Utils.coordinateToCell(line.getCoordinateN(1), 10));
    assertEquals(
        H3Utils.approxPathCells(
                    pentagonLine.getCoordinateN(0), pentagonLine.getCoordinateN(1), 11, true)
                .size()
            - 1,
        pentagonDist);

    assertEquals(
        H3Utils.h3.gridDistance(
            H3Utils.coordinateToCell(line.getCoordinateN(0), 10),
            H3Utils.coordinateToCell(line.getCoordinateN(1), 10)),
        lineDist);
  }

  /** Test h3kRing */
  @Test
  public void h3KRing() {
    Point[] points =
        new Point[] {
          // pentagon
          GEOMETRY_FACTORY.createPoint(new Coordinate(10.53619907546767, 64.70000012793487)),
          // 7th neighbor of pentagon
          GEOMETRY_FACTORY.createPoint(new Coordinate(10.536630883471666, 64.69944253201858)),
          // normal point
          GEOMETRY_FACTORY.createPoint(new Coordinate(-166.093005914, 61.61964122848931)),
        };
    for (Point point : points) {
      long cell = H3Utils.coordinateToCell(point.getCoordinate(), 12);
      Set<Long> allNeighbors = new HashSet<>(Arrays.asList(Functions.h3KRing(cell, 10, false)));
      Set<Long> kthNeighbors = new HashSet<>(Arrays.asList(Functions.h3KRing(cell, 10, true)));
      assert allNeighbors.containsAll(kthNeighbors);
      kthNeighbors.addAll(Arrays.asList(Functions.h3KRing(cell, 9, false)));
      assert allNeighbors.equals(kthNeighbors);
    }
  }

  @Test
  public void geometricMedian() throws Exception {
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(1480, 0, 620, 0));
    Geometry actual = Functions.geometricMedian(multiPoint);
    Geometry expected = wktReader.read("POINT (1050 0)");
    assertEquals(0, expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
  }

  @Test
  public void testForcePolygonCW() throws ParseException {
    Geometry polyCCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))", 0);
    String actual = Functions.asWKT(Functions.forcePolygonCW(polyCCW));
    String expected =
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))";
    assertEquals(expected, actual);

    polyCCW = Constructors.geomFromWKT("POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35))", 0);
    actual = Functions.asWKT(Functions.forcePolygonCCW(polyCCW));
    expected = "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35))";
    assertEquals(expected, actual);

    // both exterior ring and interior ring are counter-clockwise
    polyCCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 25, 20 15, 30 20))", 0);
    actual = Functions.asWKT(Functions.forcePolygonCW(polyCCW));
    expected = "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))";
    assertEquals(expected, actual);

    Geometry mPoly =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 25, 20 15, 30 20)), ((40 40, 20 45, 45 30, 40 40)))",
            0);
    actual = Functions.asWKT(Functions.forcePolygonCW(mPoly));
    expected =
        "MULTIPOLYGON (((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20)), ((40 40, 45 30, 20 45, 40 40)))";
    assertEquals(expected, actual);

    mPoly =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (4 4, 4 6, 6 6, 6 4, 4 4), (6 6, 6 8, 8 8, 8 6, 6 6), (3 3, 3 4, 4 4, 4 3, 3 3), (5 5, 5 6, 6 6, 6 5, 5 5), (7 7, 7 8, 8 8, 8 7, 7 7)))",
            0);
    actual = Functions.asWKT(Functions.forcePolygonCW(mPoly));
    expected =
        "MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2), (4 4, 6 4, 6 6, 4 6, 4 4), (6 6, 8 6, 8 8, 6 8, 6 6), (3 3, 4 3, 4 4, 3 4, 3 3), (5 5, 6 5, 6 6, 5 6, 5 5), (7 7, 8 7, 8 8, 7 8, 7 7)))";
    assertEquals(expected, actual);

    Geometry nonPoly = Constructors.geomFromWKT("POINT (45 20)", 0);
    actual = Functions.asWKT(Functions.forcePolygonCW(nonPoly));
    expected = Functions.asWKT(nonPoly);
    assertEquals(expected, actual);
  }

  @Test
  public void testForcePolygonCCW() throws ParseException {
    Geometry polyCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))", 0);
    String actual = Functions.asWKT(Functions.forcePolygonCCW(polyCW));
    String expected =
        "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))";
    assertEquals(expected, actual);

    polyCW = Constructors.geomFromWKT("POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35))", 0);
    actual = Functions.asWKT(Functions.forcePolygonCCW(polyCW));
    expected = "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35))";
    assertEquals(expected, actual);

    // both exterior ring and interior ring are counter-clockwise
    polyCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))", 0);
    actual = Functions.asWKT(Functions.forcePolygonCCW(polyCW));
    expected = "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))";
    assertEquals(expected, actual);

    Geometry mPoly =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20)), ((40 40, 45 30, 20 45, 40 40)))",
            0);
    actual = Functions.asWKT(Functions.forcePolygonCCW(mPoly));
    expected =
        "MULTIPOLYGON (((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)), ((40 40, 20 45, 45 30, 40 40)))";
    assertEquals(expected, actual);

    mPoly =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2), (4 4, 6 4, 6 6, 4 6, 4 4), (6 6, 8 6, 8 8, 6 8, 6 6), (3 3, 4 3, 4 4, 3 4, 3 3), (5 5, 6 5, 6 6, 5 6, 5 5), (7 7, 8 7, 8 8, 7 8, 7 7)))",
            0);
    actual = Functions.asWKT(Functions.forcePolygonCCW(mPoly));
    expected =
        "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (4 4, 4 6, 6 6, 6 4, 4 4), (6 6, 6 8, 8 8, 8 6, 6 6), (3 3, 3 4, 4 4, 4 3, 3 3), (5 5, 5 6, 6 6, 6 5, 5 5), (7 7, 7 8, 8 8, 8 7, 7 7)))";
    assertEquals(expected, actual);

    Geometry nonPoly = Constructors.geomFromWKT("POINT (45 20)", 0);
    actual = Functions.asWKT(Functions.forcePolygonCCW(nonPoly));
    expected = Functions.asWKT(nonPoly);
    assertEquals(expected, actual);
  }

  @Test
  public void testIsPolygonCW() throws ParseException {
    Geometry polyCCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))", 0);
    assertFalse(Functions.isPolygonCW(polyCCW));

    polyCCW = Constructors.geomFromWKT("POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35))", 0);
    assertFalse(Functions.isPolygonCW(polyCCW));

    Geometry polyCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))", 0);
    assertTrue(Functions.isPolygonCW(polyCW));

    Geometry mPoly =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2), (4 4, 6 4, 6 6, 4 6, 4 4), (6 6, 8 6, 8 8, 6 8, 6 6), (3 3, 4 3, 4 4, 3 4, 3 3), (5 5, 6 5, 6 6, 5 6, 5 5), (7 7, 8 7, 8 8, 7 8, 7 7)))",
            0);
    assertTrue(Functions.isPolygonCW(mPoly));

    Geometry point = Constructors.geomFromWKT("POINT (45 20)", 0);
    assertFalse(Functions.isPolygonCW(point));

    Geometry lineClosed = Constructors.geomFromWKT("LINESTRING (30 20, 20 25, 20 15, 30 20)", 0);
    assertFalse(Functions.isPolygonCW(lineClosed));
  }

  @Test
  public void testIsPolygonCCW() throws ParseException {
    Geometry polyCCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))", 0);
    assertTrue(Functions.isPolygonCCW(polyCCW));

    polyCCW = Constructors.geomFromWKT("POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35))", 0);
    assertTrue(Functions.isPolygonCCW(polyCCW));

    Geometry polyCW =
        Constructors.geomFromWKT(
            "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))", 0);
    assertFalse(Functions.isPolygonCCW(polyCW));

    Geometry mPoly =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (4 4, 4 6, 6 6, 6 4, 4 4), (6 6, 6 8, 8 8, 8 6, 6 6), (3 3, 3 4, 4 4, 4 3, 3 3), (5 5, 5 6, 6 6, 6 5, 5 5), (7 7, 7 8, 8 8, 8 7, 7 7)))",
            0);
    assertTrue(Functions.isPolygonCCW(mPoly));

    Geometry point = Constructors.geomFromWKT("POINT (45 20)", 0);
    assertFalse(Functions.isPolygonCCW(point));

    Geometry lineClosed = Constructors.geomFromWKT("LINESTRING (30 20, 20 25, 20 15, 30 20)", 0);
    assertFalse(Functions.isPolygonCCW(lineClosed));
  }

  @Test
  public void testTriangulatePolygon() throws ParseException {
    Geometry geom =
        Constructors.geomFromWKT(
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))", 0);
    String actual = Functions.asWKT(Functions.triangulatePolygon(geom));
    String expected =
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((4 4, 4 6, 6 6, 6 4, 4 4)), ((6 6, 6 8, 8 8, 8 6, 6 6)), ((3 3, 3 4, 4 4, 4 3, 3 3)), ((5 5, 5 6, 6 6, 6 5, 5 5)), ((7 7, 7 8, 8 8, 8 7, 7 7)))",
            0);
    actual = Functions.asWKT(Functions.triangulatePolygon(geom));
    expected =
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 2 2, 0 0)), POLYGON ((2 8, 2 2, 0 10, 2 8)), POLYGON ((10 0, 0 0, 2 2, 10 0)), POLYGON ((8 8, 2 8, 0 10, 8 8)), POLYGON ((10 0, 2 2, 8 2, 10 0)), POLYGON ((8 8, 0 10, 10 10, 8 8)), POLYGON ((10 10, 10 0, 8 2, 10 10)), POLYGON ((8 2, 8 8, 10 10, 8 2)), POLYGON ((4 4, 4 6, 6 6, 4 4)), POLYGON ((6 6, 6 4, 4 4, 6 6)), POLYGON ((6 6, 6 8, 8 8, 6 6)), POLYGON ((8 8, 8 6, 6 6, 8 8)), POLYGON ((3 3, 3 4, 4 4, 3 3)), POLYGON ((4 4, 4 3, 3 3, 4 4)), POLYGON ((5 5, 5 6, 6 6, 5 5)), POLYGON ((6 6, 6 5, 5 5, 6 6)), POLYGON ((7 7, 7 8, 8 8, 7 7)), POLYGON ((8 8, 8 7, 7 7, 8 8)))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("POINT(0 1)", 0);
    actual = Functions.asWKT(Functions.triangulatePolygon(geom));
    expected = "GEOMETRYCOLLECTION EMPTY";
    assertEquals(expected, actual);
  }

  @Test
  public void geometricMedianTolerance() throws Exception {
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0, 0, 10, 1, 5, 1, 20, 20));
    Geometry actual = Functions.geometricMedian(multiPoint, 1e-15);
    Geometry expected = wktReader.read("POINT (5 1)");
    assertEquals(0, expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
  }

  @Test
  public void geometricMedianUnsupported() {
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1480, 0, 620, 0));
    Exception e = assertThrows(Exception.class, () -> Functions.geometricMedian(lineString));
    assertEquals("Unsupported geometry type: LineString", e.getMessage());
  }

  @Test
  public void geometricMedianFailConverge() {
    MultiPoint multiPoint =
        GEOMETRY_FACTORY.createMultiPointFromCoords(
            coordArray(12, 5, 62, 7, 100, -1, 100, -5, 10, 20, 105, -5));
    Exception e =
        assertThrows(Exception.class, () -> Functions.geometricMedian(multiPoint, 1e-6, 5, true));
    assertEquals("Median failed to converge within 1.0E-06 after 5 iterations.", e.getMessage());
  }

  @Test
  public void longestLine() throws ParseException {
    Geometry geom1 = Constructors.geomFromWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 0);
    Geometry geom2 =
        Functions.buffer(Constructors.geomFromWKT("POINT (10.123456 -20.654321)", 0), 30);
    String actual = Functions.asWKT(Functions.longestLine(geom1, geom2));
    String expected = "LINESTRING (40 40, -1.3570469709526929 -48.37070697533861)";
    assertEquals(expected, actual);

    geom1 = Constructors.geomFromWKT("POLYGON ((190 150, 20 10, 160 70, 190 150))", 0);
    geom2 = Constructors.geomFromWKT("POINT(80 160)", 0);
    actual =
        Functions.asWKT(
            Functions.reducePrecision(
                Functions.longestLine(geom1, Functions.buffer(geom2, 30)), 8));
    expected = "LINESTRING (20 10, 91.48050297 187.71638598)";
    assertEquals(expected, actual);

    geom1 = Constructors.geomFromWKT("POINT (160 40)", 0);
    geom2 =
        Constructors.geomFromWKT("LINESTRING (10 30, 50 50, 30 110, 70 90, 180 140, 130 190)", 0);
    actual = Functions.asWKT(Functions.longestLine(geom1, geom2));
    expected = "LINESTRING (160 40, 130 190)";
    assertEquals(expected, actual);

    geom1 =
        Constructors.geomFromWKT(
            "POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))",
            0);
    actual = Functions.asWKT(Functions.normalize(Functions.longestLine(geom1, geom1)));
    expected = "LINESTRING (20 50, 180 180)";
    assertEquals(expected, actual);

    geom1 = Constructors.geomFromWKT("POINT Z (10 20 5)", 0);
    geom2 = Constructors.geomFromWKT("POLYGON Z ((30 40 10, 40 50 15, 50 60 20, 30 40 10))", 0);
    actual = Functions.asWKT(Functions.longestLine(geom1, geom2));
    expected = "LINESTRING Z(10 20 5, 50 60 20)";
    assertEquals(expected, actual);

    geom1 = Constructors.geomFromWKT("POINT (0 0)", 0);
    actual = Functions.asWKT(Functions.longestLine(geom1, geom1));
    expected = "LINESTRING (0 0, 0 0)";
    assertEquals(expected, actual);
  }

  @Test
  public void makepolygonWithSRID() {
    Geometry lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1, 1, 0, 0, 0));
    Geometry actual1 = Functions.makepolygonWithSRID(lineString1, 4326);
    Geometry expected1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 1, 1, 1, 0, 0, 0));
    assertEquals(expected1.toText(), actual1.toText());
    assertEquals(4326, actual1.getSRID());

    Geometry lineString2 =
        GEOMETRY_FACTORY.createLineString(coordArray3d(75, 29, 1, 77, 29, 2, 77, 29, 3, 75, 29, 1));
    Geometry actual2 = Functions.makepolygonWithSRID(lineString2, 123);
    Geometry expected2 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(75, 29, 1, 77, 29, 2, 77, 29, 3, 75, 29, 1));
    assertEquals(expected2.toText(), actual2.toText());
    assertEquals(123, actual2.getSRID());
  }

  @Test
  public void haversineDistance() {
    // Basic check
    Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
    Point p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    assertEquals(1.00075559643809E7, Haversine.distance(p1, p2), 0.1);

    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-0.56, 51.3168));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-3.1883, 55.9533));
    assertEquals(543796.9506134904, Haversine.distance(p1, p2), 0.1);

    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(8.570556, 50.033333));
    assertEquals(299073.03416817175, Haversine.distance(p1, p2), 0.1);

    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(13.287778, 52.559722));
    assertEquals(479569.4558072244, Haversine.distance(p1, p2), 0.1);

    LineString l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
    LineString l2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 0, 0));
    assertEquals(4948180.449055, Haversine.distance(l1, l2), 0.1);

    l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
    l2 = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 0, 0));
    assertEquals(4948180.449055, Haversine.distance(l1, l2), 0.1);

    // HK to Sydney
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(151.177222, -33.946111));
    assertEquals(7393893.072901942, Haversine.distance(p1, p2), 0.1);

    // HK to Toronto
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.630556, 43.677223));
    assertEquals(1.2548548944238186E7, Haversine.distance(p1, p2), 0.1);

    // Crossing the anti-meridian
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 0));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 0));
    assertTrue(Haversine.distance(p1, p2) < 300);
    assertTrue(Haversine.distance(p2, p1) < 300);
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 60));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 60));
    assertTrue(Haversine.distance(p1, p2) < 300);
    assertTrue(Haversine.distance(p2, p1) < 300);
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, -60));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, -60));
    assertTrue(Haversine.distance(p1, p2) < 300);
    assertTrue(Haversine.distance(p2, p1) < 300);

    // Crossing the North Pole
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, 89.999));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 89.999));
    assertTrue(Haversine.distance(p1, p2) < 300);
    assertTrue(Haversine.distance(p2, p1) < 300);

    // Crossing the South Pole
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, -89.999));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, -89.999));
    assertTrue(Haversine.distance(p1, p2) < 300);
    assertTrue(Haversine.distance(p2, p1) < 300);
  }

  @Test
  public void spheroidDistance() {
    // Basic check
    Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
    Point p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    assertEquals(1.0018754171394622E7, Spheroid.distance(p1, p2), 0.1);

    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-0.56, 51.3168));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-3.1883, 55.9533));
    assertEquals(544430.9411996203, Spheroid.distance(p1, p2), 0.1);

    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(8.570556, 50.033333));
    assertEquals(299648.07216251583, Spheroid.distance(p1, p2), 0.1);

    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(13.287778, 52.559722));
    assertEquals(479817.9049528187, Spheroid.distance(p1, p2), 0.1);

    LineString l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
    LineString l2 = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 0, 0));
    assertEquals(4953717.340300673, Spheroid.distance(l1, l2), 0.1);

    // HK to Sydney
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(151.177222, -33.946111));
    assertEquals(7371809.8295041, Spheroid.distance(p1, p2), 0.1);

    // HK to Toronto
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.630556, 43.677223));
    assertEquals(1.2568775317073349E7, Spheroid.distance(p1, p2), 0.1);

    // Crossing the anti-meridian
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 0));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 0));
    assertTrue(Spheroid.distance(p1, p2) < 300);
    assertTrue(Spheroid.distance(p2, p1) < 300);
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 60));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 60));
    assertTrue(Spheroid.distance(p1, p2) < 300);
    assertTrue(Spheroid.distance(p2, p1) < 300);
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, -60));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, -60));
    assertTrue(Spheroid.distance(p1, p2) < 300);
    assertTrue(Spheroid.distance(p2, p1) < 300);

    // Crossing the North Pole
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, 89.999));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 89.999));
    assertTrue(Spheroid.distance(p1, p2) < 300);
    assertTrue(Spheroid.distance(p2, p1) < 300);

    // Crossing the South Pole
    p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, -89.999));
    p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, -89.999));
    assertTrue(Spheroid.distance(p1, p2) < 300);
    assertTrue(Spheroid.distance(p2, p1) < 300);
  }

  @Test
  public void spheroidArea() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
    assertEquals(0, Spheroid.area(point), 0.1);

    LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
    assertEquals(0, Spheroid.area(line), 0.1);
    line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
    assertEquals(0, Spheroid.area(line), 0.1);

    Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
    assertEquals(0, Spheroid.area(polygon1), 0.1);
    polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 90, 0, 0, 0));
    assertEquals(0, Spheroid.area(polygon1), 0.1);

    Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(34, 35, 28, 30, 25, 34, 34, 35));
    assertEquals(2.0182485081176245E11, Spheroid.area(polygon2), 0.1);

    Polygon polygon3 = GEOMETRY_FACTORY.createPolygon(coordArray(34, 35, 25, 34, 28, 30, 34, 35));
    assertEquals(2.0182485081176245E11, Spheroid.area(polygon3), 0.1);

    MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPoint(new Point[] {point, point});
    assertEquals(0, Spheroid.area(multiPoint), 0.1);

    MultiLineString multiLineString =
        GEOMETRY_FACTORY.createMultiLineString(new LineString[] {line, line});
    assertEquals(0, Spheroid.area(multiLineString), 0.1);

    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon2, polygon3});
    assertEquals(4.036497016235249E11, Spheroid.area(multiPolygon), 0.1);

    GeometryCollection geometryCollection =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point, polygon2, polygon3});
    assertEquals(4.036497016235249E11, Spheroid.area(geometryCollection), 0.1);
  }

  @Test
  public void pologonize() throws ParseException {
    LineString line1 = GEOMETRY_FACTORY.createLineString(coordArray(180, 40, 30, 20, 20, 90));
    LineString line2 = GEOMETRY_FACTORY.createLineString(coordArray(180, 40, 160, 160));
    LineString line3 = GEOMETRY_FACTORY.createLineString(coordArray(80, 60, 120, 130, 150, 80));
    LineString line4 = GEOMETRY_FACTORY.createLineString(coordArray(80, 60, 150, 80));
    LineString line5 = GEOMETRY_FACTORY.createLineString(coordArray(20, 90, 70, 70, 80, 130));
    LineString line6 = GEOMETRY_FACTORY.createLineString(coordArray(80, 130, 160, 160));
    LineString line7 = GEOMETRY_FACTORY.createLineString(coordArray(20, 90, 20, 160, 70, 190));
    LineString line8 = GEOMETRY_FACTORY.createLineString(coordArray(70, 190, 80, 130));
    LineString line9 = GEOMETRY_FACTORY.createLineString(coordArray(70, 190, 160, 160));

    LineString line10 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 1, 0, 2));
    LineString line11 = GEOMETRY_FACTORY.createLineString(coordArray(4, 2, 4, 1, 4, 0));
    LineString line12 = GEOMETRY_FACTORY.createLineString(coordArray(4, 0, 3, 0, 2, 0, 1, 0, 0, 0));
    LineString line13 = GEOMETRY_FACTORY.createLineString(coordArray(2, 0, 2, 1, 2, 2));
    LineString line14 = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 2, 3, 2, 4));
    LineString line15 = GEOMETRY_FACTORY.createLineString(coordArray(0, 2, 1, 2, 2, 2));
    LineString line16 = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 2, 4, 2));
    LineString line17 = GEOMETRY_FACTORY.createLineString(coordArray(0, 2, 1, 3, 2, 4));
    LineString line18 = GEOMETRY_FACTORY.createLineString(coordArray(2, 4, 3, 3, 4, 2));

    GeometryCollection geometryCollection1 =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {line1, line2, line3, line4, line5, line6, line7, line8, line9});
    GeometryCollection geometryCollection2 =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              line10, line11, line12, line13, line14, line15, line16, line17, line18
            });
    GeometryCollection geometryCollection3 =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {line10, line11, line12, line13, line15, line16});
    GeometryCollection geometryCollection4 =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {line13, line14, line15, line16, line17, line18});

    Geometry expected1 =
        geomFromEWKT(
            "GEOMETRYCOLLECTION (POLYGON ((20 90, 20 160, 70 190, 80 130, 70 70, 20 90)), POLYGON ((20 90, 70 70, 80 130, 160 160, 180 40, 30 20, 20 90), (80 60, 150 80, 120 130, 80 60)), POLYGON ((70 190, 160 160, 80 130, 70 190)), POLYGON ((80 60, 120 130, 150 80, 80 60)))");
    Geometry result1 = Functions.polygonize(geometryCollection1);
    result1.normalize();
    assertEquals(expected1, result1);

    Geometry expected2 =
        geomFromEWKT(
            "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 0 2, 1 2, 2 2, 3 2, 4 2, 4 1, 4 0, 3 0, 2 0, 1 0, 0 0)), POLYGON ((0 2, 1 3, 2 4, 2 3, 2 2, 1 2, 0 2)), POLYGON ((2 2, 2 3, 2 4, 3 3, 4 2, 3 2, 2 2)))");
    Geometry result2 = Functions.polygonize(geometryCollection2);
    result2.normalize();
    assertEquals(expected2, result2);

    Geometry expected3 =
        geomFromEWKT(
            "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 0 2, 1 2, 2 2, 3 2, 4 2, 4 1, 4 0, 3 0, 2 0, 1 0, 0 0)))");
    Geometry result3 = Functions.polygonize(geometryCollection3);
    result3.normalize();
    assertEquals(expected3, result3);

    Geometry expected4 =
        geomFromEWKT(
            "GEOMETRYCOLLECTION (POLYGON ((0 2, 1 3, 2 4, 2 3, 2 2, 1 2, 0 2)), POLYGON ((2 2, 2 3, 2 4, 3 3, 4 2, 3 2, 2 2)))");
    Geometry result4 = Functions.polygonize(geometryCollection4);
    result4.normalize();
    assertEquals(expected4, result4);
  }

  @Test
  public void delaunayTriangles() throws ParseException {
    Geometry poly = Constructors.geomFromEWKT("POLYGON((175 150, 20 40, 50 60, 125 100, 175 150))");
    Geometry point = Constructors.geomFromEWKT("POINT (110 170)");
    Geometry combined = Functions.union(poly, point);
    String actual = Functions.delaunayTriangle(combined).toText();
    String expected =
        "GEOMETRYCOLLECTION (POLYGON ((20 40, 125 100, 50 60, 20 40)), POLYGON ((20 40, 50 60, 110 170, 20 40)), POLYGON ((110 170, 50 60, 125 100, 110 170)), POLYGON ((110 170, 125 100, 175 150, 110 170)))";
    assertEquals(expected, actual);

    poly =
        Constructors.geomFromEWKT(
            "MULTIPOLYGON (((10 10, 10 20, 20 20, 20 10, 10 10)),((25 10, 25 20, 35 20, 35 10, 25 10)))");
    actual = Functions.delaunayTriangle(poly, 20).toText();
    expected = "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 35 10, 10 20)))";
    assertEquals(expected, actual);

    actual = Functions.delaunayTriangle(poly, 0, 1).toText();
    expected =
        "MULTILINESTRING ((25 20, 35 20), (20 20, 25 20), (10 20, 20 20), (10 10, 10 20), (10 10, 20 10), (20 10, 25 10), (25 10, 35 10), (35 10, 35 20), (25 20, 35 10), (25 10, 25 20), (20 20, 25 10), (20 10, 20 20), (10 20, 20 10))";
    assertEquals(expected, actual);
  }

  @Test
  public void spheroidLength() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
    assertEquals(0, Spheroid.length(point), 0.1);

    LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
    assertEquals(1.0018754171394622E7, Spheroid.length(line), 0.1);

    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 90, 0, 0, 0));
    assertEquals(2.0037508342789244E7, Spheroid.length(polygon), 0.1);

    MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPoint(new Point[] {point, point});
    assertEquals(0, Spheroid.length(multiPoint), 0.1);

    MultiLineString multiLineString =
        GEOMETRY_FACTORY.createMultiLineString(new LineString[] {line, line});
    assertEquals(2.0037508342789244E7, Spheroid.length(multiLineString), 0.1);

    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon, polygon});
    assertEquals(4.007501668557849E7, Spheroid.length(multiPolygon), 0.1);

    GeometryCollection geometryCollection =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point, line, multiLineString});
    assertEquals(3.0056262514183864E7, Spheroid.length(geometryCollection), 0.1);
  }

  @Test
  public void numPoints() throws Exception {
    LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 1, 0, 2, 0));
    int expected = 3;
    int actual = Functions.numPoints(line);
    assertEquals(expected, actual);
  }

  @Test
  public void numPointsUnsupported() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
    String expected =
        "Unsupported geometry type: " + "Polygon" + ", only LineString geometry is supported.";
    Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.numPoints(polygon));
    assertEquals(expected, e.getMessage());
  }

  @Test
  public void simplifyVW() throws ParseException {
    Geometry geom = Constructors.geomFromEWKT("LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)");
    String actual = Functions.simplifyVW(geom, 30).toString();
    String expected = "LINESTRING (5 2, 7 25, 10 10)";
    assertEquals(expected, actual);

    actual = Functions.simplifyVW(geom, 10).toString();
    expected = "LINESTRING (5 2, 3 8, 7 25, 10 10)";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromEWKT(
            "POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))");
    actual = Functions.simplifyVW(geom, 10).toString();
    expected =
        "POLYGON ((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33, 47 44, 35 36, 45 33, 43 19, 29 21, 35 26, 24 39, 8 25))";
    assertEquals(expected, actual);

    actual = Functions.simplifyVW(geom, 80).toString();
    expected = "POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 43 19, 24 39, 8 25))";
    assertEquals(expected, actual);
  }

  @Test
  public void simplifyPolygonHull() throws ParseException {
    Geometry geom =
        Constructors.geomFromEWKT(
            "POLYGON ((131 158, 136 163, 161 165, 173 156, 179 148, 169 140, 186 144, 190 137, 185 131, 174 128, 174 124, 166 119, 158 121, 158 115, 165 107, 161 97, 166 88, 166 79, 158 57, 145 57, 112 53, 111 47, 93 43, 90 48, 88 40, 80 39, 68 32, 51 33, 40 31, 39 34, 49 38, 34 38, 25 34, 28 39, 36 40, 44 46, 24 41, 17 41, 14 46, 19 50, 33 54, 21 55, 13 52, 11 57, 22 60, 34 59, 41 68, 75 72, 62 77, 56 70, 46 72, 31 69, 46 76, 52 82, 47 84, 56 90, 66 90, 64 94, 56 91, 33 97, 36 100, 23 100, 22 107, 29 106, 31 112, 46 116, 36 118, 28 131, 53 132, 59 127, 62 131, 76 130, 80 135, 89 137, 87 143, 73 145, 80 150, 88 150, 85 157, 99 162, 116 158, 115 165, 123 165, 122 170, 134 164, 131 158))");
    String actual = Functions.asWKT(Functions.simplifyPolygonHull(geom, 0.3, true));
    String expected =
        "POLYGON ((161 165, 173 156, 186 144, 190 137, 185 131, 174 124, 166 119, 166 79, 158 57, 68 32, 40 31, 25 34, 17 41, 14 46, 11 57, 56 91, 33 97, 23 100, 22 107, 28 131, 80 135, 73 145, 85 157, 99 162, 122 170, 161 165))";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.simplifyPolygonHull(geom, 0.3));
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.simplifyPolygonHull(geom, 0.3, false));
    expected =
        "POLYGON ((131 158, 116 158, 99 162, 89 137, 76 130, 59 127, 28 131, 46 116, 36 100, 64 94, 75 72, 41 68, 33 54, 68 32, 90 48, 112 53, 145 57, 158 57, 161 97, 158 115, 158 121, 190 137, 169 140, 179 148, 161 165, 131 158))";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.simplifyPolygonHull(geom, 0.1, false));
    expected = "POLYGON ((89 137, 36 100, 64 94, 75 72, 33 54, 112 53, 145 57, 161 165, 89 137))";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.simplifyPolygonHull(geom, 0.1));
    expected =
        "POLYGON ((161 165, 173 156, 186 144, 190 137, 158 57, 68 32, 40 31, 25 34, 17 41, 14 46, 11 57, 22 107, 28 131, 85 157, 99 162, 122 170, 161 165))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromEWKT(
            "MULTIPOLYGON (((131 158, 136 163, 161 165, 173 156, 179 148, 169 140, 186 144, 190 137, 185 131, 174 128, 174 124, 166 119, 158 121, 158 115, 165 107, 161 97, 166 88, 166 79, 158 57, 145 57, 112 53, 111 47, 93 43, 90 48, 88 40, 80 39, 68 32, 51 33, 40 31, 39 34, 49 38, 34 38, 25 34, 28 39, 36 40, 44 46, 24 41, 17 41, 14 46, 19 50, 33 54, 21 55, 13 52, 11 57, 22 60, 34 59, 41 68, 75 72, 62 77, 56 70, 46 72, 31 69, 46 76, 52 82, 47 84, 56 90, 66 90, 64 94, 56 91, 33 97, 36 100, 23 100, 22 107, 29 106, 31 112, 46 116, 36 118, 28 131, 53 132, 59 127, 62 131, 76 130, 80 135, 89 137, 87 143, 73 145, 80 150, 88 150, 85 157, 99 162, 116 158, 115 165, 123 165, 122 170, 134 164, 131 158)))");
    actual = Functions.asWKT(Functions.simplifyPolygonHull(geom, 0.3, true));
    expected =
        "MULTIPOLYGON (((161 165, 173 156, 186 144, 190 137, 185 131, 174 124, 166 119, 166 79, 158 57, 68 32, 40 31, 25 34, 17 41, 14 46, 11 57, 56 91, 33 97, 23 100, 22 107, 28 131, 80 135, 73 145, 85 157, 99 162, 122 170, 161 165)))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromEWKT("LINESTRING (10 10, 20 20, 30 30)");
    Geometry finalGeom = geom;
    assertThrows(
        "Input geometry must be  polygonal",
        IllegalArgumentException.class,
        () -> {
          Functions.simplifyPolygonHull(finalGeom, 0.1);
        });
  }

  @Test
  public void force3DObject2D() {
    int expectedDims = 3;
    LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 1, 0, 2, 0));
    LineString expectedLine =
        GEOMETRY_FACTORY.createLineString(coordArray3d(0, 1, 1.1, 1, 0, 1.1, 2, 0, 1.1));
    Geometry forcedLine = Functions.force3D(line, 1.1);
    WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(expectedLine));
    assertEquals(wktWriter.write(expectedLine), wktWriter.write(forcedLine));
    assertEquals(expectedDims, Functions.nDims(forcedLine));
  }

  @Test
  public void force3DObject2DDefaultValue() {
    int expectedDims = 3;
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 90, 90, 0, 0));
    Polygon expectedPolygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(0, 0, 0, 0, 90, 0, 90, 90, 0, 0, 0, 0));
    Geometry forcedPolygon = Functions.force3D(polygon);
    WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(expectedPolygon));
    assertEquals(wktWriter.write(expectedPolygon), wktWriter.write(forcedPolygon));
    assertEquals(expectedDims, Functions.nDims(forcedPolygon));
  }

  @Test
  public void force3DObject3D() {
    int expectedDims = 3;
    LineString line3D = GEOMETRY_FACTORY.createLineString(coordArray3d(0, 1, 1, 1, 2, 1, 1, 2, 2));
    Geometry forcedLine3D = Functions.force3D(line3D, 2.0);
    WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(line3D));
    assertEquals(wktWriter.write(line3D), wktWriter.write(forcedLine3D));
    assertEquals(expectedDims, Functions.nDims(forcedLine3D));
  }

  @Test
  public void forceCollection() throws ParseException {
    Geometry geom =
        Constructors.geomFromWKT("MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)", 0);
    int actual = Functions.numGeometries(Functions.forceCollection(geom));
    int expected = 6;
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON(((0 0 0,0 1 0,1 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,1 0 1,0 0 1,0 0 0)),((1 1 0,1 1 1,1 0 1,1 0 0,1 1 0)),((0 1 0,0 1 1,1 1 1,1 1 0,0 1 0)),((0 0 1,1 0 1,1 1 1,0 1 1,0 0 1)))",
            0);
    actual = Functions.numGeometries(Functions.forceCollection(geom));
    expected = 5;
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTILINESTRING ((10 10, 20 20, 30 30), (15 15, 25 25, 35 35))", 0);
    actual = Functions.numGeometries(Functions.forceCollection(geom));
    expected = 2;
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 0);
    actual = Functions.numGeometries(Functions.forceCollection(geom));
    expected = 1;
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "GEOMETRYCOLLECTION(POLYGON((0 0 1,0 5 1,5 0 1,0 0 1),(1 1 1,3 1 1,1 3 1,1 1 1)))", 0);
    String actualWKT = Functions.asWKT(Functions.forceCollection(geom));
    String expectedWKT =
        "GEOMETRYCOLLECTION Z(POLYGON Z((0 0 1, 0 5 1, 5 0 1, 0 0 1), (1 1 1, 3 1 1, 1 3 1, 1 1 1)))";
    assertEquals(expectedWKT, actualWKT);
  }

  @Test
  public void testForce3DM() throws ParseException {
    Geometry geom = Constructors.geomFromWKT("POINT (1 2)", 0);
    String actual = Functions.asWKT(Functions.force3DM(geom, 5));
    String expected = "POINT M(1 2 5)";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("MULTIPOINT ((1 2), (2 3))", 0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected = "MULTIPOINT M((1 2 5), (2 3 5))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("LINESTRING (1 2, 2 3, 3 4)", 0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected = "LINESTRING M(1 2 5, 2 3 5, 3 4 5)";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTILINESTRING ((10 10, 20 20, 30 30), (15 15, 25 25, 35 35))", 0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected = "MULTILINESTRING M((10 10 5, 20 20 5, 30 30 5), (15 15 5, 25 25 5, 35 35 5))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("LINEARRING (30 10, 40 40, 20 40, 10 20, 30 10)", 0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected = "LINEARRING M(30 10 5, 40 40 5, 20 40 5, 10 20 5, 30 10 5)";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))", 0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected =
        "POLYGON M((0 0 5, 10 0 5, 10 10 5, 0 10 5, 0 0 5), (4 4 5, 4 6 5, 6 6 5, 6 4 5, 4 4 5))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)), ((15 5, 10 20, 20 30, 15 5)))",
            0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected =
        "MULTIPOLYGON M(((30 10 5, 40 40 5, 20 40 5, 10 20 5, 30 10 5)), ((15 5 5, 10 20 5, 20 30 5, 15 5 5)))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (15 15, 25 25, 35 35), POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)))",
            0);
    actual = Functions.asWKT(Functions.force3DM(geom, 5));
    expected =
        "GEOMETRYCOLLECTION M(POINT M(10 10 5), LINESTRING M(15 15 5, 25 25 5, 35 35 5), POLYGON M((30 10 5, 40 40 5, 20 40 5, 10 20 5, 30 10 5)))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "POLYGON M((0 0 3, 0 5 3, 5 0 3, 0 0 3), (1 1 3, 3 1 3, 1 3 3, 1 1 3))", 0);
    Geometry actualGeom = Functions.force3DM(geom, 10);
    assertTrue(Predicates.equals(geom, actualGeom));
  }

  @Test
  public void force4D() throws ParseException {
    // testing all geom types
    Geometry geom = Constructors.geomFromWKT("POINT (1 2)", 0);
    String actual = Functions.asWKT(Functions.force4D(geom, 2, 5));
    String expected = "POINT ZM(1 2 2 5)";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("MULTIPOINT ((1 2), (2 3))", 0);
    actual = Functions.asWKT(Functions.force4D(geom, 2, 5));
    expected = "MULTIPOINT ZM((1 2 2 5), (2 3 2 5))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("LINESTRING (1 2, 2 3, 3 4)", 0);
    actual = Functions.asWKT(Functions.force4D(geom));
    expected = "LINESTRING ZM(1 2 0 0, 2 3 0 0, 3 4 0 0)";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTILINESTRING ((10 10, 20 20, 30 30), (15 15, 25 25, 35 35))", 0);
    actual = Functions.asWKT(Functions.force4D(geom, 3, 5));
    expected =
        "MULTILINESTRING ZM((10 10 3 5, 20 20 3 5, 30 30 3 5), (15 15 3 5, 25 25 3 5, 35 35 3 5))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromWKT("LINEARRING (30 10, 40 40, 20 40, 10 20, 30 10)", 0);
    actual = Functions.asWKT(Functions.force4D(geom, 5, 5));
    expected = "LINEARRING ZM(30 10 5 5, 40 40 5 5, 20 40 5 5, 10 20 5 5, 30 10 5 5)";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))", 0);
    actual = Functions.asWKT(Functions.force4D(geom));
    expected =
        "POLYGON ZM((0 0 0 0, 10 0 0 0, 10 10 0 0, 0 10 0 0, 0 0 0 0), (4 4 0 0, 4 6 0 0, 6 6 0 0, 6 4 0 0, 4 4 0 0))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)), ((15 5, 10 20, 20 30, 15 5)))",
            0);
    actual = Functions.asWKT(Functions.force4D(geom, 2, 5));
    expected =
        "MULTIPOLYGON ZM(((30 10 2 5, 40 40 2 5, 20 40 2 5, 10 20 2 5, 30 10 2 5)), ((15 5 2 5, 10 20 2 5, 20 30 2 5, 15 5 2 5)))";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromWKT(
            "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (15 15, 25 25, 35 35), POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)))",
            0);
    actual = Functions.asWKT(Functions.force4D(geom, 2, 5));
    expected =
        "GEOMETRYCOLLECTION ZM(POINT ZM(10 10 2 5), LINESTRING ZM(15 15 2 5, 25 25 2 5, 35 35 2 5), POLYGON ZM((30 10 2 5, 40 40 2 5, 20 40 2 5, 10 20 2 5, 30 10 2 5)))";
    assertEquals(expected, actual);

    // return 4D input geom as is
    geom =
        Constructors.geomFromWKT(
            "POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))", 0);
    Geometry actualGeom = Functions.force4D(geom, 10, 10);
    assertTrue(Predicates.equals(geom, actualGeom));

    // if input geom has z value, keep it and add m
    geom = Constructors.geomFromWKT("LINESTRING Z(0 1 3, 1 0 3, 2 0 3)", 0);
    actual = Functions.asWKT(Functions.force4D(geom, 10, 10));
    expected = "LINESTRING ZM(0 1 3 10, 1 0 3 10, 2 0 3 10)";
    assertEquals(expected, actual);

    // if input geom has m value, keep it and add z
    geom = Constructors.geomFromWKT("LINESTRING M(0 1 3, 1 0 3, 2 0 3)", 0);
    actual = Functions.asWKT(Functions.force4D(geom, 10, 10));
    expected = "LINESTRING ZM(0 1 10 3, 1 0 10 3, 2 0 10 3)";
    assertEquals(expected, actual);
  }

  @Test
  public void force3DObject3DDefaultValue() {
    int expectedDims = 3;
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(0, 0, 0, 90, 0, 0, 90, 90, 0, 0, 0, 0));
    Geometry forcedPolygon = Functions.force3D(polygon);
    WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(polygon));
    assertEquals(wktWriter.write(polygon), wktWriter.write(forcedPolygon));
    assertEquals(expectedDims, Functions.nDims(forcedPolygon));
  }

  @Test
  public void force3DEmptyObject() {
    LineString emptyLine = GEOMETRY_FACTORY.createLineString();
    Geometry forcedEmptyLine = Functions.force3D(emptyLine, 1.2);
    assertEquals(emptyLine.isEmpty(), forcedEmptyLine.isEmpty());
  }

  @Test
  public void force3DEmptyObjectDefaultValue() {
    LineString emptyLine = GEOMETRY_FACTORY.createLineString();
    Geometry forcedEmptyLine = Functions.force3D(emptyLine);
    assertEquals(emptyLine.isEmpty(), forcedEmptyLine.isEmpty());
  }

  @Test
  public void force3DHybridGeomCollection() {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Polygon polygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
    Geometry geomCollection =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createGeometryCollection(
                  new Geometry[] {multiPolygon, point3D, lineString})
            });
    Polygon expectedPolygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 2, 1, 1, 2, 2, 1, 2, 2, 0, 2, 1, 0, 2));
    LineString expectedLineString3D =
        GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 2, 1, 1, 2, 1, 2, 2));
    Geometry actualGeometryCollection = Functions.force3D(geomCollection, 2);
    WKTWriter wktWriter3D = new WKTWriter(3);
    assertEquals(
        wktWriter3D.write(polygon3D),
        wktWriter3D.write(
            actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
    assertEquals(
        wktWriter3D.write(expectedPolygon3D),
        wktWriter3D.write(
            actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(1)));
    assertEquals(
        wktWriter3D.write(point3D),
        wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(1)));
    assertEquals(
        wktWriter3D.write(expectedLineString3D),
        wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(2)));
  }

  @Test
  public void force3DHybridGeomCollectionDefaultValue() {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Polygon polygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
    Geometry geomCollection =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createGeometryCollection(
                  new Geometry[] {multiPolygon, point3D, lineString})
            });
    Polygon expectedPolygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 0, 1, 1, 0, 2, 1, 0, 2, 0, 0, 1, 0, 0));
    LineString expectedLineString3D =
        GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 0, 1, 1, 0, 1, 2, 0));
    Geometry actualGeometryCollection = Functions.force3D(geomCollection);
    WKTWriter wktWriter3D = new WKTWriter(3);
    assertEquals(
        wktWriter3D.write(polygon3D),
        wktWriter3D.write(
            actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
    assertEquals(
        wktWriter3D.write(expectedPolygon3D),
        wktWriter3D.write(
            actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(1)));
    assertEquals(
        wktWriter3D.write(point3D),
        wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(1)));
    assertEquals(
        wktWriter3D.write(expectedLineString3D),
        wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(2)));
  }

  @Test
  public void makeLine() {
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    String actual = Functions.makeLine(point1, point2).toText();
    assertEquals("LINESTRING (0 0, 1 1)", actual);

    MultiPoint multiPoint1 =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0));
    MultiPoint multiPoint2 =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 2, 2));
    String actual2 = Functions.makeLine(multiPoint1, multiPoint2).toText();
    assertEquals("LINESTRING (0.5 0.5, 1 1, 0.5 0.5, 2 2)", actual2);

    LineString line1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1));
    LineString line2 = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 3));
    Geometry[] geoms = new Geometry[] {line1, line2};
    String actual3 = Functions.makeLine(geoms).toText();
    assertEquals("LINESTRING (0 0, 1 1, 2 2, 3 3)", actual3);
  }

  @Test
  public void makeLine3d() {
    WKTWriter wktWriter3D = new WKTWriter(3);
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2, 2));
    LineString actualLinestring = (LineString) Functions.makeLine(point1, point2);
    LineString expectedLineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(1, 1, 1, 2, 2, 2));
    assertEquals(wktWriter3D.write(actualLinestring), wktWriter3D.write(expectedLineString));

    MultiPoint multiPoint1 =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray3d(0.5, 0.5, 1, 1, 1, 1));
    MultiPoint multiPoint2 =
        GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray3d(0.5, 0.5, 2, 2, 2, 2));
    actualLinestring = (LineString) Functions.makeLine(multiPoint1, multiPoint2);
    expectedLineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(0.5, 0.5, 1, 1, 1, 1, 0.5, 0.5, 2, 2, 2, 2));
    assertEquals(wktWriter3D.write(actualLinestring), wktWriter3D.write(expectedLineString));

    LineString line1 = GEOMETRY_FACTORY.createLineString(coordArray3d(0, 0, 1, 1, 1, 1));
    LineString line2 = GEOMETRY_FACTORY.createLineString(coordArray3d(2, 2, 2, 2, 3, 3));
    Geometry[] geoms = new Geometry[] {line1, line2};
    actualLinestring = (LineString) Functions.makeLine(geoms);
    expectedLineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3));
    assertEquals(wktWriter3D.write(actualLinestring), wktWriter3D.write(expectedLineString));
  }

  @Test
  public void makeLineWithWrongType() {
    Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
    Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 10, 10, 10, 10, 0, 0, 0));

    Exception e =
        assertThrows(IllegalArgumentException.class, () -> Functions.makeLine(polygon1, polygon2));
    assertEquals(
        "ST_MakeLine only supports Point, MultiPoint and LineString geometries", e.getMessage());
  }

  @Test
  public void minimumBoundingRadius() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    assertEquals("POINT (0 0)", Functions.minimumBoundingRadius(point).getLeft().toString());
    assertEquals(0, Functions.minimumBoundingRadius(point).getRight(), 1e-6);

    LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 10));
    assertEquals("POINT (0 5)", Functions.minimumBoundingRadius(line).getLeft().toString());
    assertEquals(5, Functions.minimumBoundingRadius(line).getRight(), 1e-6);

    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 10, 10, 10, 10, 0, 0, 0));
    assertEquals("POINT (5 5)", Functions.minimumBoundingRadius(polygon).getLeft().toString());
    assertEquals(7.071067, Functions.minimumBoundingRadius(polygon).getRight(), 1e-6);
  }

  @Test
  public void minimumClearance() throws ParseException {
    Geometry geometry = Constructors.geomFromEWKT("POLYGON ((0 0, 1 0, 1 1, 0.5 3.2e-4, 0 0))");
    double actual = Functions.minimumClearance(geometry);
    double expected = 0.00032;
    assertEquals(expected, actual, FP_TOLERANCE);

    geometry =
        Constructors.geomFromEWKT(
            "POLYGON ((10 10, 20 20, 20.1 20.1, 30 25, 40 30, 50 40, 40 50, 30 45, 20 40, 10 30, 10 10))");
    actual = Functions.minimumClearance(geometry);
    expected = 0.14142135623731153;
    assertEquals(expected, actual, FP_TOLERANCE);

    geometry = Constructors.geomFromEWKT("POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))");
    actual = Functions.minimumClearance(geometry);
    expected = 0.5;
    assertEquals(expected, actual, FP_TOLERANCE2);

    geometry =
        Constructors.geomFromEWKT(
            "POLYGON ((65.10498 18.625425, 62.182617 16.36231, 64.863281 16.40447, 62.006836 14.157882, 65.522461 14.008696, 65.10498 18.625425))");
    actual = Functions.minimumClearance(geometry);
    expected = 0.4407369162202361;
    assertEquals(expected, actual, FP_TOLERANCE);

    geometry = Constructors.geomFromEWKT("MULTIPOINT(10 10, 20 20)");
    actual = Functions.minimumClearance(geometry);
    expected = 14.142135623730951;
    assertEquals(expected, actual, FP_TOLERANCE);

    geometry = Constructors.geomFromEWKT("POINT(10 10)");
    actual = Functions.minimumClearance(geometry);
    expected = Double.MAX_VALUE;
    assertEquals(expected, actual, FP_TOLERANCE2);
  }

  @Test
  public void minimumClearanceLine() throws ParseException {
    Geometry geometry = Constructors.geomFromEWKT("POLYGON ((0 0, 1 0, 1 1, 0.5 3.2e-4, 0 0))");
    String actual = Functions.minimumClearanceLine(geometry).toText();
    String expected = "LINESTRING (0.5 0.00032, 0.5 0)";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromEWKT(
            "POLYGON ((10 10, 20 20, 20.1 20.1, 30 25, 40 30, 50 40, 40 50, 30 45, 20 40, 10 30, 10 10))");
    actual = Functions.minimumClearanceLine(geometry).toText();
    expected = "LINESTRING (20 20, 20.1 20.1)";
    assertEquals(expected, actual);

    geometry = Constructors.geomFromEWKT("POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))");
    actual = Functions.minimumClearanceLine(geometry).toText();
    expected = "LINESTRING (64.5 16, 65 16)";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromEWKT(
            "POLYGON ((65.10498 18.625425, 62.182617 16.36231, 64.863281 16.40447, 62.006836 14.157882, 65.522461 14.008696, 65.10498 18.625425))");
    actual = Functions.minimumClearanceLine(geometry).toText();
    expected = "LINESTRING (64.863281 16.40447, 65.30222689577225 16.44416294526772)";
    assertEquals(expected, actual);

    geometry = Constructors.geomFromEWKT("MULTIPOINT(10 10, 20 20)");
    actual = Functions.minimumClearanceLine(geometry).toText();
    expected = "LINESTRING (20 20, 10 10)";
    assertEquals(expected, actual);

    geometry = Constructors.geomFromEWKT("POINT(10 10)");
    actual = Functions.minimumClearanceLine(geometry).toText();
    expected = "LINESTRING EMPTY";
    assertEquals(expected, actual);
  }

  @Test
  public void generatePoints() throws ParseException {
    Geometry geom = Constructors.geomFromWKT("LINESTRING(50 50,10 10,10 50)", 4326);

    Geometry actual =
        Functions.generatePoints(Functions.buffer(geom, 10, false, "endcap=round join=round"), 12);
    assertEquals(actual.getNumGeometries(), 12);

    actual =
        Functions.reducePrecision(
            Functions.generatePoints(
                Functions.buffer(geom, 10, false, "endcap=round join=round"), 5, 100),
            5);
    String expected =
        "MULTIPOINT ((40.02957 46.70645), (37.11646 37.38582), (14.2051 29.23363), (40.82533 31.47273), (28.16839 34.16338))";
    assertEquals(expected, actual.toString());
    assertEquals(4326, actual.getSRID());

    geom =
        Constructors.geomFromEWKT(
            "MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))");
    actual = Functions.generatePoints(geom, 30);
    assertEquals(actual.getNumGeometries(), 30);

    // Deterministic when using the same seed
    Geometry first = Functions.generatePoints(geom, 10, 100);
    Geometry second = Functions.generatePoints(geom, 10, 100);
    assertEquals(first, second);

    // Deterministic when using the same random number generator
    geom = geom.buffer(10, 48);
    Random rand = new Random(100);
    Random rand2 = new Random(100);
    first = Functions.generatePoints(geom, 100, rand);
    second = Functions.generatePoints(geom, 100, rand);
    Geometry first2 = Functions.generatePoints(geom, 100, rand2);
    Geometry second2 = Functions.generatePoints(geom, 100, rand2);
    assertNotEquals(first, second);
    assertEquals(first, first2);
    assertEquals(second, second2);
  }

  @Test
  public void nRingsPolygonOnlyExternal() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Integer expected = 1;
    Integer actual = Functions.nRings(polygon);
    assertEquals(expected, actual);
  }

  @Test
  public void nRingsPolygonWithHoles() throws Exception {
    LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordArray(1, 0, 1, 6, 6, 6, 6, 0, 1, 0));
    LinearRing[] holes =
        new LinearRing[] {
          GEOMETRY_FACTORY.createLinearRing(coordArray(2, 1, 2, 2, 3, 2, 3, 1, 2, 1)),
          GEOMETRY_FACTORY.createLinearRing(coordArray(4, 1, 4, 2, 5, 2, 5, 1, 4, 1))
        };
    Polygon polygonWithHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);
    Integer expected = 3;
    Integer actual = Functions.nRings(polygonWithHoles);
    assertEquals(expected, actual);
  }

  @Test
  public void nRingsPolygonEmpty() throws Exception {
    Polygon emptyPolygon = GEOMETRY_FACTORY.createPolygon();
    Integer expected = 0;
    Integer actual = Functions.nRings(emptyPolygon);
    assertEquals(expected, actual);
  }

  @Test
  public void nRingsMultiPolygonOnlyExternal() throws Exception {
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(
            new Polygon[] {
              GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0)),
              GEOMETRY_FACTORY.createPolygon(coordArray(5, 0, 5, 1, 7, 1, 7, 0, 5, 0))
            });
    Integer expected = 2;
    Integer actual = Functions.nRings(multiPolygon);
    assertEquals(expected, actual);
  }

  @Test
  public void nRingsMultiPolygonOnlyWithHoles() throws Exception {
    LinearRing shell1 = GEOMETRY_FACTORY.createLinearRing(coordArray(1, 0, 1, 6, 6, 6, 6, 0, 1, 0));
    LinearRing[] holes1 =
        new LinearRing[] {
          GEOMETRY_FACTORY.createLinearRing(coordArray(2, 1, 2, 2, 3, 2, 3, 1, 2, 1)),
          GEOMETRY_FACTORY.createLinearRing(coordArray(4, 1, 4, 2, 5, 2, 5, 1, 4, 1))
        };
    Polygon polygonWithHoles1 = GEOMETRY_FACTORY.createPolygon(shell1, holes1);
    LinearRing shell2 =
        GEOMETRY_FACTORY.createLinearRing(coordArray(10, 0, 10, 6, 16, 6, 16, 0, 10, 0));
    LinearRing[] holes2 =
        new LinearRing[] {
          GEOMETRY_FACTORY.createLinearRing(coordArray(12, 1, 12, 2, 13, 2, 13, 1, 12, 1)),
          GEOMETRY_FACTORY.createLinearRing(coordArray(14, 1, 14, 2, 15, 2, 15, 1, 14, 1))
        };
    Polygon polygonWithHoles2 = GEOMETRY_FACTORY.createPolygon(shell2, holes2);
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygonWithHoles1, polygonWithHoles2});
    Integer expected = 6;
    Integer actual = Functions.nRings(multiPolygon);
    assertEquals(expected, actual);
  }

  @Test
  public void nRingsMultiPolygonEmpty() throws Exception {
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(
            new Polygon[] {GEOMETRY_FACTORY.createPolygon(), GEOMETRY_FACTORY.createPolygon()});
    Integer expected = 0;
    Integer actual = Functions.nRings(multiPolygon);
    assertEquals(expected, actual);
  }

  @Test
  public void nRingsMultiPolygonMixed() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordArray(1, 0, 1, 6, 6, 6, 6, 0, 1, 0));
    LinearRing[] holes =
        new LinearRing[] {
          GEOMETRY_FACTORY.createLinearRing(coordArray(2, 1, 2, 2, 3, 2, 3, 1, 2, 1)),
          GEOMETRY_FACTORY.createLinearRing(coordArray(4, 1, 4, 2, 5, 2, 5, 1, 4, 1))
        };
    Polygon polygonWithHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);
    Polygon emptyPolygon = GEOMETRY_FACTORY.createPolygon();
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(
            new Polygon[] {polygon, polygonWithHoles, emptyPolygon});
    Integer expected = 4;
    Integer actual = Functions.nRings(multiPolygon);
    assertEquals(expected, actual);
  }

  @Test
  public void testExpand() throws ParseException {
    Geometry geometry =
        GEOMETRY_FACTORY.createPolygon(coordArray(50, 50, 50, 80, 80, 80, 80, 50, 50, 50));
    String actual = Functions.asWKT(Functions.expand(geometry, 10, 0));
    String expected = "POLYGON ((40 50, 40 80, 90 80, 90 50, 40 50))";
    assertEquals(expected, actual);

    geometry = Constructors.geomFromWKT("POINT (10 20 1)", 4326);
    Geometry result = Functions.expand(geometry, 10);
    actual = Functions.asWKT(result);
    expected = "POLYGON Z((0 10 -9, 0 30 -9, 20 30 11, 20 10 11, 0 10 -9))";
    assertEquals(expected, actual);
    assertEquals(4326, result.getSRID());

    geometry = Constructors.geomFromWKT("LINESTRING (0 0, 1 1, 2 2)", 0);
    actual = Functions.asWKT(Functions.expand(geometry, 10, 10));
    expected = "POLYGON ((-10 -10, -10 12, 12 12, 12 -10, -10 -10))";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromWKT(
            "MULTIPOLYGON (((52 68 1, 42 64 1, 66 62 2, 88 64 2, 85 68 2, 72 70 1, 52 68 1)), ((50 50 2, 50 80 2, 80 80 3, 80 50 2, 50 50 2)))",
            4326);
    actual = Functions.asWKT(Functions.expand(geometry, 10.5, 2, 5));
    expected = "POLYGON Z((31.5 48 -4, 31.5 82 -4, 98.5 82 8, 98.5 48 8, 31.5 48 -4))";
    assertEquals(expected, actual);

    geometry = Constructors.geomFromWKT("MULTIPOINT((10 20 1), (20 30 2))", 0);
    actual = Functions.asWKT(Functions.expand(geometry, 9.5, 3.5));
    expected = "POLYGON Z((0.5 16.5 1, 0.5 33.5 1, 29.5 33.5 2, 29.5 16.5 2, 0.5 16.5 1))";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromWKT(
            "MULTILINESTRING ((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))", 0);
    actual = Functions.asWKT(Functions.expand(geometry, 0));
    expected = "POLYGON Z((1 0 4, 1 0 4, 4 0 4, 4 0 4, 1 0 4))";
    assertEquals(expected, actual);

    geometry =
        Constructors.geomFromWKT(
            "GEOMETRYCOLLECTION (POINT (10 10),LINESTRING (20 20, 30 30),POLYGON ((25 25, 35 35, 35 35, 25 25)),MULTIPOINT (30 30, 40 40),MULTILINESTRING ((40 40, 50 50), (45 45, 55 55)),MULTIPOLYGON (((50 50, 60 60, 60 60, 50 50)), ((55 55, 65 65, 65 65, 55 55))))",
            1234);
    result = Functions.expand(geometry, 10);
    actual = Functions.asWKT(result);
    expected = "POLYGON ((0 0, 0 75, 75 75, 75 0, 0 0))";
    assertEquals(expected, actual);
    assertEquals(1234, result.getSRID());

    // The function drops the M dimension
    geometry =
        Constructors.geomFromWKT("POLYGON M((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))", 0);
    actual = Functions.asWKT(Functions.expand(geometry, 0));
    expected = "POLYGON ((50 50, 50 80, 80 80, 80 50, 50 50))";
    assertEquals(expected, actual);
  }

  @Test
  public void testBuffer() {
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray(50, 50, 50, 150, 150, 150, 150, 50, 50, 50));
    String actual = Functions.asWKT(Functions.reducePrecision(Functions.buffer(polygon, 15), 4));
    String expected =
        "POLYGON ((47.0736 35.2882, 44.2597 36.1418, 41.6664 37.528, 39.3934 39.3934, 37.528 41.6664, 36.1418 44.2597, 35.2882 47.0736, 35 50, 35 150, 35.2882 152.9264, 36.1418 155.7403, 37.528 158.3336, 39.3934 160.6066, 41.6664 162.472, 44.2597 163.8582, 47.0736 164.7118, 50 165, 150 165, 152.9264 164.7118, 155.7403 163.8582, 158.3336 162.472, 160.6066 160.6066, 162.472 158.3336, 163.8582 155.7403, 164.7118 152.9264, 165 150, 165 50, 164.7118 47.0736, 163.8582 44.2597, 162.472 41.6664, 160.6066 39.3934, 158.3336 37.528, 155.7403 36.1418, 152.9264 35.2882, 150 35, 50 35, 47.0736 35.2882))";
    assertEquals(expected, actual);

    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 50, 70, 100, 100));
    actual =
        Functions.asWKT(
            Functions.reducePrecision(Functions.buffer(lineString, 10, false, "side=left"), 4));
    expected =
        "POLYGON ((50 70, 0 0, -8.1373 5.8124, 41.8627 75.8124, 43.2167 77.3476, 44.855 78.5749, 94.855 108.5749, 100 100, 50 70))";
    assertEquals(expected, actual);

    lineString = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 50, 70, 70, -3));
    actual =
        Functions.asWKT(
            Functions.reducePrecision(Functions.buffer(lineString, 10, false, "endcap=square"), 4));
    expected =
        "POLYGON ((43.2156 77.3465, 44.8523 78.5733, 46.7044 79.4413, 48.6944 79.9144, 50.739 79.9727, 52.7527 79.6137, 54.6512 78.8525, 56.3552 77.7209, 57.7932 76.2663, 58.9052 74.5495, 59.6446 72.6424, 79.6446 -0.3576, 82.2869 -10.0022, 62.9978 -15.2869, 45.9128 47.0733, 8.1373 -5.8124, 2.325 -13.9497, -13.9497 -2.325, 41.8627 75.8124, 43.2156 77.3465))";
    assertEquals(expected, actual);

    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(100, 90));
    actual =
        Functions.asWKT(
            Functions.reducePrecision(Functions.buffer(point, 10, false, "quad_segs=2"), 4));
    expected =
        "POLYGON ((107.0711 82.9289, 100 80, 92.9289 82.9289, 90 90, 92.9289 97.0711, 100 100, 107.0711 97.0711, 110 90, 107.0711 82.9289))";
    assertEquals(expected, actual);
  }

  @Test
  public void testBufferSRID() throws ParseException {
    Geometry geom = geomFromWKT("POINT (10 20)", 3857);
    Geometry buffered = Functions.buffer(geom, 1);
    assertEquals(3857, buffered.getSRID());
    assertEquals(3857, buffered.getFactory().getSRID());

    geom = geomFromWKT("POINT (10 20)", 4326);
    buffered = Functions.buffer(geom, 1, true);
    assertEquals(4326, buffered.getSRID());
    assertEquals(4326, buffered.getFactory().getSRID());
  }

  @Test
  public void testSnap() throws ParseException {
    Geometry poly =
        geomFromWKT("POLYGON ((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))", 0);
    Geometry line = geomFromWKT("LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)", 0);
    double distance = Functions.distance(poly, line);
    String actual = Functions.asWKT(Functions.snap(poly, line, distance * 1.01));
    String expected = "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.snap(poly, line, distance * 1.25));
    expected = "POLYGON ((0.5 10.7, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 5.4 8.4, 0.5 10.7))";
    assertEquals(expected, actual);

    // if the tolerance is less than distance between Geometries then input Geometry will be
    // returned as is.
    actual = Functions.asWKT(Functions.snap(poly, line, distance * 0.9));
    expected = Functions.asWKT(poly);
    assertEquals(expected, actual);
  }

  @Test
  public void testBufferSpheroidal() throws ParseException {
    Geometry polygon1 =
        GEOMETRY_FACTORY.createPolygon(
            coordArray(
                16.2500, 48.2500, 16.3500, 48.2500, 16.3500, 48.2000, 16.2500, 48.2000, 16.2500,
                48.2500));
    Geometry polygon2 = geomFromWKT("POLYGON((-120 30, -80 30, -80 50, -120 50, -120 30))", 4269);
    Geometry point1 = geomFromWKT("POINT(-180 60)", 4269);
    Geometry linestring1 =
        geomFromEWKT("LINESTRING(-91.185 30.4505, -91.187 30.452, -91.189 30.4535)");
    Geometry polygon3 = geomFromWKT("POLYGON((-120 30, -80 30, -80 50, -120 50, -120 30))", 4269);
    Geometry linestring2 = geomFromEWKT("LINESTRING(0.05 15, -0.05 15)");

    // Geometries crossing dateline
    Geometry linestring3 = geomFromEWKT("LINESTRING(179.95 15, -179.95 15)");
    Geometry polygon4 = geomFromEWKT("POLYGON((179 -15, 179 15, -179 15, -179 -15, 179 -15))");
    Geometry polygon5 = geomFromEWKT("POLYGON((178 -10, 178 10, -178 10, -178 -10, 178 -10))");

    Geometry result1 = Functions.buffer(polygon1, 100, true);
    Geometry result2 = Functions.buffer(polygon2, 1000, true);
    Geometry result3 = Functions.buffer(point1, 100, true);
    Geometry result4 = Functions.buffer(linestring1, 10, true);
    Geometry result5 = Functions.buffer(polygon3, 10000, true);
    Geometry result6 = Functions.buffer(linestring2, 10000, true);

    // Geometries crossing dateline
    Geometry result7 = Functions.buffer(linestring3, 1000000, true);
    Geometry result8 = Functions.buffer(polygon4, 100, true);
    Geometry result9 = Functions.buffer(polygon5, 1000000, true);

    Geometry expected1 =
        geomFromEWKT(
            "POLYGON ((16.248653057837092 48.24999999781262, 16.24867891451385 48.25017542568975, 16.24875549793627 48.25034411793438, 16.24887986793391 48.25049959710696, 16.249047249310912 48.25063589306856, 16.249251215157546 48.250747772240985, 16.249483933612066 48.25083093858835, 16.249736468600165 48.250882198599385, 16.249999123001867 48.25089958393145, 16.35000087730807 48.25089956809054, 16.35026352923699 48.250882182692266, 16.350516061632444 48.250830922668115, 16.35074877731957 48.25074775640202, 16.350952740192536 48.250635877460866, 16.351120118386593 48.25049958194538, 16.351244485020548 48.25034410350243, 16.35132106495956 48.25017541233756, 16.351346918125614 48.24999998594946, 16.351345606585333 48.199999998284284, 16.351319726848168 48.19982442741669, 16.351243088946557 48.19965560959, 16.351118640921797 48.19950003769327, 16.350951169518318 48.19936369511042, 16.350747116034892 48.19925182561274, 16.350514328568078 48.19916873170147, 16.350261760179283 48.199117609152964, 16.34999912459177 48.19910042412578, 16.25000087573864 48.19910040825405, 16.24973823767017 48.199117593214716, 16.249485666681007 48.199168715750886, 16.249252876439694 48.19925180974529, 16.24904881997628 48.199363679477166, 16.248881345384483 48.199500022510115, 16.248756893991796 48.199655595141486, 16.24868025260387 48.199824414053964, 16.2486543693546 48.19999998641755, 16.248653057837092 48.24999999781262))");
    Geometry expected2 =
        geomFromEWKT(
            "POLYGON ((-120.00898315284118 29.999999999999467, -120.00898315284118 49.999999999988724, -120.00881054407824 50.001129625613444, -120.0082993510474 50.002215815187945, -120.00746921861013 50.003216831381316, -120.00635204829044 50.00409421217581, -120.0049907723172 50.004814247955025, -120.00343770376273 50.00534927578318, -120.00175252618048 50.00567874131385, -119.99999999999999 50.005789987696524, -80 50.005789987696524, -79.9982474738195 50.00567874131385, -79.99656229623727 50.00534927578318, -79.99500922768277 50.004814247955025, -79.99364795170956 50.00409421217581, -79.99253078138989 50.003216831381316, -79.99170064895262 50.002215815187945, -79.99118945592176 50.001129625613444, -79.99101684715882 49.999999999988724, -79.99101684715882 29.999999999999467, -79.99118945592176 29.998474584401656, -79.99170064895262 29.997007767335376, -79.99253078138989 29.995655921596196, -79.99364795170956 29.994471003601635, -79.99500922768277 29.99349855586065, -79.99656229623727 29.99277595576926, -79.9982474738195 29.99233097818683, -80 29.992180727189663, -119.99999999999999 29.992180727189663, -120.00175252618048 29.99233097818683, -120.00343770376273 29.99277595576926, -120.0049907723172 29.99349855586065, -120.00635204829044 29.994471003601635, -120.00746921861013 29.995655921596196, -120.0082993510474 29.997007767335376, -120.00881054407824 29.998474584401656, -120.00898315284118 29.999999999999467))");
    Geometry expected3 =
        geomFromEWKT(
            "POLYGON ((-179.9982096288439 59.99995928994206, -179.9982598922349 59.99978513613418, -179.99837702566958 59.99961923978765, -179.99855652694714 59.99946797603053, -179.998791497433 59.99933715760413, -179.99907290724568 59.999231811518854, -179.9993899422849 59.99915598591007, -179.99973041976276 59.99911259450936, 179.9999187437241 59.999103304702594, 179.99957102955173 59.99912847347155, 179.99923979915093 59.999187133678575, 179.99893778069153 59.999277031220146, 179.99867658007233 59.99939471162435, 179.99846623498902 59.99953565276752, 179.9983148292063 59.99969443861621, 179.99822818185396 59.999864967323326, 179.9982096236965 60.00004068568767, 179.99825986898801 60.00021484097138, 179.99837698785996 60.00038074040053, 179.99855648032874 60.00053200837772, 179.99879144910062 60.00066283151926, 179.99907286455522 60.00076818209686, 179.9993899117333 60.00084401129125, 179.9997304059989 60.000887404824965, -179.99991873860708 60.00089669498742, -179.99957100633523 60.000871524742664, -179.9992397613717 60.000812861453035, -179.9989377341035 60.000722959691345, -179.99867653177037 60.00060527457202, -179.99846619232903 60.00046432893652, -179.99831479868513 60.000305539502236, -179.99822816812048 60.00013500866213, -179.9982096288439 59.99995928994206))");
    Geometry expected4 =
        geomFromEWKT(
            "POLYGON ((-91.18706814560424 30.451931798138848, -91.18906814731041 30.453431798551634, -91.18908219577726 30.453444627150773, -91.1890930855116 30.4534595836974, -91.18910039802604 30.453476093420573, -91.18910385230312 30.453493521861454, -91.18910331559482 30.453511199255043, -91.1890988085245 30.4535284462689, -91.18909050429437 30.453544600109474, -91.18907872203003 30.453559039992996, -91.18906391451668 30.453571211001968, -91.18904665079899 30.453580645410415, -91.18902759431288 30.453586980658557, -91.18900747739012 30.453589973285787, -91.18898707311482 30.453589508286903, -91.18896716561396 30.453585603531685, -91.18894851992349 30.453578409078172, -91.18893185258821 30.453568201405897, -91.18693185327697 30.45206820105564, -91.18493185405761 30.450568200774196, -91.18491780618592 30.450555372061554, -91.18490691698157 30.450540415431583, -91.18489960490953 30.450523905659963, -91.18489615096698 30.450506477208492, -91.18489668788492 30.450488799843015, -91.18490119502782 30.450471552894637, -91.18490949918679 30.450455399153434, -91.18492128123634 30.450440959397913, -91.18493608839827 30.450428788538883, -91.18495335164187 30.45041935429472, -91.1849724075514 30.45041301921735, -91.18499252382057 30.450410026759716, -91.18501292739447 30.450410491920046, -91.18503283417732 30.45041439682263, -91.18505147916453 30.450421591404723, -91.18506814584102 30.450431799183303, -91.18706814560424 30.451931798138848))");
    Geometry expected5 =
        geomFromEWKT(
            "POLYGON ((-120.08983152841193 29.999999999999467, -120.08983152841193 49.999999999988724, -120.08810544078257 50.011295055219406, -120.082993510474 50.02215353087995, -120.07469218610126 50.03215857448533, -120.06352048290445 50.040926345154375, -120.04990772317231 50.048120665845154, -120.03437703762728 50.05346582623596, -120.01752526180509 50.05675706193107, -119.99999999999999 50.057868324959486, -80 50.057868324959486, -79.98247473819491 50.05675706193107, -79.96562296237272 50.05346582623596, -79.95009227682765 50.048120665845154, -79.93647951709555 50.040926345154375, -79.92530781389875 50.03215857448533, -79.91700648952599 50.02215353087995, -79.91189455921744 50.011295055219406, -79.91016847158805 49.999999999988724, -79.91016847158805 29.999999999999467, -79.91189455921744 29.984744778413628, -79.91700648952599 29.970073573592575, -79.92530781389875 29.956550575939833, -79.93647951709555 29.944696041120597, -79.95009227682765 29.934966209460505, -79.96562296237272 29.927735669848445, -79.98247473819491 29.92328286155486, -80 29.92177928675603, -119.99999999999999 29.92177928675603, -120.01752526180509 29.92328286155486, -120.03437703762728 29.927735669848445, -120.04990772317231 29.934966209460505, -120.06352048290445 29.944696041120597, -120.07469218610126 29.956550575939833, -120.082993510474 29.970073573592575, -120.08810544078257 29.984744778413628, -120.08983152841193 29.999999999999467))");
    Geometry expected6 =
        geomFromEWKT(
            "POLYGON ((-0.0499827380800979 14.90970764615526, -0.0680979441919658 14.911439289332314, -0.0855181036092048 14.916572765433372, -0.1015745790932317 14.924910889089798, -0.1156509498611482 14.936133480337729, -0.1272066059871122 14.949809631917493, -0.1357974717923214 14.965414213210906, -0.1410930699533803 14.982347984759812, -0.1428892711961725 14.9999605602647, -0.1411162325232574 15.017575343788987, -0.1358412050267848 15.034515492689351, -0.1272660846886664 15.050129914661689, -0.1157197795798512 15.063818302233363, -0.101645667253286 15.07505424080832, -0.0855846090787039 15.083405496399754, -0.068154165764569 15.088550694439519, -0.0500248124892859 15.090291738028835, 0.0500174017959422 15.0902994910598, 0.0681488768495457 15.08856105434859, 0.0855824246963393 15.08341775106032, 0.1016472676386697 15.075067339333813, 0.1157254254859488 15.06383098366898, 0.1272755583302725 15.050140871793444, 0.1358538186573691 15.034523547105394, 0.1411309042551145 15.017579606456339, 0.1429046575925539 14.999960553897546, 0.1411077364057122 14.982343709853023, 0.1358100760363371 14.965406147948107, 0.1272160681615492 14.949798665976873, 0.1156565845591405 14.936120792425546, 0.1015761705812689 14.9248977863495, 0.0855159139181673 14.916560508451733, 0.0680926538252635 14.911428928455091, 0.0499753291173759 14.909699892939221, -0.0499827380800979 14.90970764615526))");
    Geometry expected7 =
        geomFromEWKT(
            "POLYGON ((-179.908607846266 24.025403116254306, -177.99876698499668 23.825660822056278, -176.1851748144233 23.267372220454533, -174.54862792786022 22.380090414241554, -173.15522797402008 21.20765453023379, -172.05367901642606 19.803908497939666, -171.27542809803106 18.22850022788734, -170.83670228698406 16.543486102335997, -170.7412636921777 14.811036901560918, -170.9829095910743 13.092142713348924, -171.5471495735382 11.445952123266512, -172.41190582467604 9.929276420592164, -173.54741832839647 8.595821727211286, -174.91577148479777 7.494852554680671, -176.47058207976102 6.669211334615649, -178.157376393358 6.152879352694568, -179.9150124655878 5.968498588196282, 179.9878642700213 5.967715967762297, 178.22454996382154 6.1243064565567416, 176.52190735292885 6.617190677478861, 174.94354089208682 7.427303247307942, 173.5475157235323 8.522934832546376, 172.38432738193248 9.861905503007021, 171.4959038990676 11.393986435009106, 170.9153886431351 13.063103234007537, 170.66721922346738 14.80905645479948, 170.76695775874103 16.568760623485606, 171.22043647400534 18.277247812024235, 172.02203612722943 19.86884329704546, 173.1522881340142 21.278955415388584, 174.5754355707626 22.446796923405344, 176.2379815998183 23.319060447498003, 178.06938085914476 23.854152382752126, 179.98566424517946 24.026184454399257, -179.908607846266 24.025403116254306))");
    Geometry expected8 =
        geomFromEWKT(
            "POLYGON ((179 -15.000873160293315, 178.999824747382 -15.000856382797105, 178.99965622962375 -15.000806695050459, 178.9995009227683 -15.000726006503347, 178.999364795171 -15.000617417938033, 178.99925307813902 -15.000485102312915, 178.9991700648953 -15.000334144403906, 178.99911894559222 -15.00017034540551, 178.9991016847159 -14.999999999999092, 178.9991016847159 14.99999999999908, 178.99911894559222 15.00017034540551, 178.9991700648953 15.000334144403906, 178.99925307813902 15.000485102312902, 178.999364795171 15.000617417938045, 178.9995009227683 15.000726006503333, 178.99965622962375 15.000806695050459, 178.999824747382 15.000856382797105, 179 15.000873160293315, -179.00000000000003 15.000873160293315, -178.99982474738198 15.000856382797105, -178.99965622962375 15.000806695050459, -178.9995009227683 15.000726006503333, -178.99936479517098 15.000617417938045, -178.99925307813902 15.000485102312902, -178.99917006489528 15.000334144403906, -178.9991189455922 15.00017034540551, -178.99910168471592 14.99999999999908, -178.99910168471592 -14.999999999999092, -178.9991189455922 -15.00017034540551, -178.99917006489528 -15.000334144403906, -178.99925307813902 -15.000485102312915, -178.99936479517098 -15.000617417938033, -178.9995009227683 -15.000726006503347, -178.99965622962375 -15.000806695050459, -178.99982474738198 -15.000856382797105, -179.00000000000003 -15.000873160293315, 179 -15.000873160293315))");
    Geometry expected9 =
        geomFromEWKT(
            "POLYGON ((178 -18.747248844374, 176.24747381949112 -18.582729101204464, 174.5622962372712 -18.0945531116537, 173.0092276827665 -17.29887379200776, 171.64795170955566 -16.222572896097414, 170.53078138987692 -14.903037762363958, 169.70064895259912 -13.387566659631126, 169.18945592174327 -11.73221950803158, 169.01684715880478 -9.999999999999265, 169.01684715880478 9.999999999999238, 169.18945592174327 11.73221950803157, 169.70064895259912 13.387566659631101, 170.53078138987692 14.903037762363944, 171.64795170955566 16.2225728960974, 173.0092276827665 17.298873792007747, 174.5622962372712 18.094553111653685, 176.24747381949112 18.582729101204453, 178 18.747248844373974, -178 18.747248844373974, -176.24747381949115 18.582729101204453, -174.56229623727123 18.094553111653685, -173.00922768276646 17.298873792007747, -171.64795170955566 16.2225728960974, -170.5307813898769 14.903037762363944, -169.70064895259912 13.387566659631101, -169.18945592174325 11.73221950803157, -169.0168471588048 9.999999999999238, -169.0168471588048 -9.999999999999265, -169.18945592174325 -11.73221950803158, -169.70064895259912 -13.387566659631126, -170.5307813898769 -14.903037762363972, -171.64795170955566 -16.222572896097414, -173.00922768276646 -17.29887379200776, -174.56229623727123 -18.0945531116537, -176.24747381949115 -18.582729101204464, -178 -18.747248844374, 178 -18.747248844374))");

    Geometry postgis_result1 =
        geomFromEWKT(
            "POLYGON((16.24865305783681 48.249999997812985,16.248678914513565 48.250175425690124,16.248755497935985 48.250344117934766,16.248879867933624 48.25049959710733,16.249047249310628 48.25063589306893,16.249251215157262 48.25074777224136,16.24948393361178 48.25083093858872,16.24973646859988 48.25088219859975,16.249999123001583 48.25089958393185,16.35000087730765 48.25089956809112,16.35026352923657 48.25088218269285,16.350516061632025 48.2508309226687,16.35074877731915 48.2507477564026,16.35095274019212 48.25063587746145,16.351120118386177 48.250499581945974,16.35124448502013 48.250344103503025,16.351321064959144 48.25017541233815,16.3513469181252 48.24999998595004,16.351345606584914 48.19999999828486,16.35131972684775 48.19982442741726,16.351243088946138 48.19965560959058,16.351118640921378 48.19950003769385,16.3509511695179 48.199363695110996,16.350747116034473 48.19925182561332,16.350514328567655 48.19916873170203,16.35026176017886 48.19911760915353,16.34999912459135 48.19910042412636,16.250000875738355 48.19910040825441,16.249738237669884 48.19911759321507,16.249485666680723 48.19916871575125,16.24925287643941 48.199251809745654,16.249048819975993 48.19936367947753,16.2488813453842 48.199500022510485,16.248756893991516 48.19965559514185,16.248680252603585 48.199824414054326,16.248654369354316 48.199999986417914,16.24865305783681 48.249999997812985))");
    Geometry postgis_result2 =
        geomFromEWKT(
            "POLYGON((-120.00898315284118 29.999999999999993,-120.00898315284118 49.99999999999999,-120.00881054407824 50.00112962562472,-120.0082993510474 50.00221581519921,-120.00746921861011 50.003216831392564,-120.00635204829044 50.004094212187034,-120.00499077231723 50.00481424796622,-120.00343770376273 50.00534927579437,-120.00175252618051 50.00567874132504,-119.99999999999999 50.00578998770771,-80 50.00578998770771,-79.99824747381949 50.00567874132504,-79.99656229623727 50.00534927579437,-79.99500922768276 50.00481424796622,-79.99364795170956 50.004094212187034,-79.99253078138987 50.003216831392564,-79.9917006489526 50.00221581519921,-79.99118945592174 50.00112962562472,-79.9910168471588 49.99999999999999,-79.9910168471588 29.999999999999993,-79.99118945592174 29.99847458440221,-79.9917006489526 29.997007767335962,-79.99253078138987 29.99565592159683,-79.99364795170956 29.99447100360229,-79.99500922768276 29.993498555861333,-79.99656229623727 29.992775955769954,-79.99824747381949 29.992330978187542,-80 29.992180727190387,-119.99999999999999 29.992180727190387,-120.00175252618051 29.992330978187542,-120.00343770376273 29.992775955769954,-120.00499077231723 29.993498555861333,-120.00635204829044 29.99447100360229,-120.00746921861011 29.99565592159683,-120.0082993510474 29.997007767335962,-120.00881054407824 29.99847458440221,-120.00898315284118 29.999999999999993))");
    Geometry postgis_result3 =
        geomFromEWKT(
            "POLYGON((-179.99820962883618 59.99995929000264,-179.99825989222717 59.999785136194745,-179.9983770256619 59.99961923984817,-179.99855652693947 59.99946797609105,-179.99879149742534 59.99933715766465,-179.999072907238 59.99923181157932,-179.99938994227725 59.9991559859705,-179.99973041975517 59.999112594569745,179.9999187437317 59.999103304762926,179.99957102955932 59.999128473531854,179.9992397991585 59.99918713373882,179.99893778069907 59.999277031280386,179.99867658007986 59.99939471168456,179.99846623499656 59.99953565282769,179.99831482921377 59.99969443867636,179.99822818186144 59.999864967383445,179.998209623704 60.00004068574784,179.9982598689955 60.00021484103153,179.9983769878675 60.000380740460706,179.99855648033625 60.00053200843791,179.99879144910815 60.000662831579476,179.99907286456278 60.00076818215714,179.99938991174088 60.00084401135155,179.99973040600648 60.00088740488531,-179.9999187385995 60.0008966950478,-179.99957100632759 60.00087152480308,-179.9992397613641 60.0008128615135,-179.99893773409582 60.000722959751855,-179.99867653176275 60.00060527463255,-179.99846619232136 60.00046432899707,-179.99831479867737 60.00030553956281,-179.99822816811277 60.000135008722694,-179.99820962883618 59.99995929000264))");
    Geometry postgis_result4 =
        geomFromEWKT(
            "POLYGON((-91.18706814560713 30.45193179814003,-91.1890681473133 30.453431798552806,-91.18908219578012 30.453444627151963,-91.18909308551447 30.453459583698564,-91.18910039802891 30.453476093421752,-91.18910385230598 30.453493521862622,-91.1891033155977 30.453511199256216,-91.18909880852735 30.453528446270074,-91.18909050429724 30.453544600110646,-91.18907872203292 30.453559039994175,-91.18906391451956 30.453571211003144,-91.18904665080186 30.453580645411602,-91.18902759431575 30.453586980659722,-91.189007477393 30.45358997328696,-91.1889870731177 30.453589508288072,-91.18896716561683 30.45358560353286,-91.18894851992636 30.453578409079352,-91.18893185259108 30.453568201407066,-91.18693185325601 30.45206820103894,-91.1849318540605 30.450568200775386,-91.18491780618884 30.450555372062745,-91.18490691698447 30.450540415432766,-91.18489960491243 30.45052390566114,-91.18489615096986 30.450506477209665,-91.18489668788781 30.450488799844198,-91.1849011950307 30.45047155289582,-91.18490949918969 30.45045539915463,-91.18492128123923 30.450440959399103,-91.18493608840117 30.45042878854006,-91.18495335164478 30.45041935429591,-91.18497240755428 30.450413019218534,-91.18499252382345 30.450410026760903,-91.18501292739734 30.450410491921236,-91.1850328341802 30.450414396823803,-91.18505147916741 30.450421591405906,-91.1850681458439 30.450431799184482,-91.18706814560713 30.45193179814003))");
    Geometry postgis_result5 =
        geomFromEWKT(
            "POLYGON((-120.08983152841193 29.999999999999993,-120.08983152841193 49.99999999999999,-120.08810544078257 50.011295055230505,-120.082993510474 50.02215353089088,-120.07469218610122 50.032158574496115,-120.06352048290445 50.040926345165026,-120.04990772317234 50.04812066585569,-120.03437703762728 50.0534658262464,-120.01752526180509 50.05675706194147,-119.99999999999999 50.05786832496988,-80 50.05786832496988,-79.98247473819491 50.05675706194147,-79.9656229623727 50.0534658262464,-79.95009227682766 50.04812066585569,-79.93647951709556 50.040926345165026,-79.92530781389875 50.032158574496115,-79.91700648952599 50.02215353089088,-79.91189455921743 50.011295055230505,-79.91016847158805 49.99999999999999,-79.91016847158805 29.999999999999993,-79.91189455921743 29.984744778414523,-79.91700648952599 29.970073573593833,-79.92530781389875 29.956550575941442,-79.93647951709556 29.944696041122494,-79.95009227682766 29.934966209462644,-79.9656229623727 29.927735669850765,-79.98247473819491 29.923282861557286,-80 29.921779286758486,-119.99999999999999 29.921779286758486,-120.01752526180509 29.923282861557286,-120.03437703762728 29.927735669850765,-120.04990772317234 29.934966209462644,-120.06352048290445 29.944696041122494,-120.07469218610122 29.956550575941442,-120.082993510474 29.970073573593833,-120.08810544078257 29.984744778414523,-120.08983152841193 29.999999999999993))");

    assertEquals(Functions.reducePrecision(expected1, 8), Functions.reducePrecision(result1, 8));
    assertEquals(4326, Functions.getSRID(result1));
    assertEquals(7.424558176442617E-8, comparePolygons(postgis_result1, result1), FP_TOLERANCE2);
    assertEquals(Spheroid.area(postgis_result1), Spheroid.area(result1), FP_TOLERANCE2);

    assertEquals(Functions.reducePrecision(expected2, 8), Functions.reducePrecision(result2, 8));
    assertEquals(4269, Functions.getSRID(result2));
    assertEquals(Spheroid.area(postgis_result2), Spheroid.area(result2), 10);
    assertEquals(7.424558176442617E-8, comparePolygons(postgis_result2, result2), FP_TOLERANCE2);

    assertEquals(Functions.reducePrecision(expected3, 8), Functions.reducePrecision(result3, 8));
    assertEquals(4269, Functions.getSRID(result3));
    assertEquals(Spheroid.area(postgis_result3), Spheroid.area(result3), FP_TOLERANCE2);
    assertEquals(7.424558176442617E-8, comparePolygons(postgis_result3, result3), FP_TOLERANCE2);

    assertEquals(Functions.reducePrecision(expected4, 8), Functions.reducePrecision(result4, 8));
    assertEquals(4326, Functions.getSRID(result4));
    assertEquals(Spheroid.area(postgis_result4), Spheroid.area(result4), FP_TOLERANCE2);
    assertEquals(7.424558176442617E-8, comparePolygons(postgis_result4, result4), FP_TOLERANCE2);

    assertEquals(Functions.reducePrecision(expected5, 8), Functions.reducePrecision(result5, 8));
    assertEquals(4269, Functions.getSRID(result5));
    assertEquals(Spheroid.area(postgis_result5), Spheroid.area(result5), 10);
    assertEquals(7.424558176442617E-8, comparePolygons(postgis_result5, result5), FP_TOLERANCE2);

    assertEquals(Functions.reducePrecision(expected6, 8), Functions.reducePrecision(result6, 8));
    assertEquals(4326, Functions.getSRID(result6));

    assertEquals(Functions.reducePrecision(expected7, 8), Functions.reducePrecision(result7, 8));
    assertEquals(4326, Functions.getSRID(result6));

    assertEquals(Functions.reducePrecision(expected8, 8), Functions.reducePrecision(result8, 8));
    assertEquals(4326, Functions.getSRID(result6));

    assertEquals(Functions.reducePrecision(expected9, 8), Functions.reducePrecision(result9, 8));
    assertEquals(4326, Functions.getSRID(result6));
  }

  @Test
  public void testShiftLongitude() throws ParseException {
    Geometry point = geomFromWKT("POINT(-175 10)", 4230);
    Geometry linestring1 = geomFromEWKT("LINESTRING(179 10, -179 10)");
    Geometry linestring2 = geomFromEWKT("LINESTRING(179 10, 181 10)");
    Geometry polygon = geomFromEWKT("POLYGON((179 10, -179 10, -179 20, 179 20, 179 10))");
    Geometry multiPoint = geomFromEWKT("MULTIPOINT((179 10), (-179 10))");
    Geometry multiLineString = geomFromEWKT("MULTILINESTRING((179 10, -179 10), (179 20, 181 20))");
    Geometry multiPolygon =
        geomFromEWKT(
            "MULTIPOLYGON(((179 10, -179 10, -179 20, 179 20, 179 10)), ((-185 10, -185 20, -175 20, -175 10, -185 10)))");
    Geometry geomCollection =
        geomFromEWKT("GEOMETRYCOLLECTION(POINT(190 10), LINESTRING(179 10, -179 10))");

    Geometry expected1 = geomFromWKT("POINT (185 10)", 4230);
    Geometry expected2 = geomFromEWKT("LINESTRING (179 10, 181 10)");
    Geometry expected3 = geomFromEWKT("LINESTRING (179 10, -179 10)");
    Geometry expected4 = geomFromEWKT("POLYGON ((179 10, 181 10, 181 20, 179 20, 179 10))");
    Geometry expected5 = geomFromEWKT("MULTIPOINT ((179 10), (181 10))");
    Geometry expected6 = geomFromEWKT("MULTILINESTRING ((179 10, 181 10), (179 20, -179 20))");
    Geometry expected7 =
        geomFromEWKT(
            "MULTIPOLYGON (((179 10, 181 10, 181 20, 179 20, 179 10)), ((175 10, 175 20, 185 20, 185 10, 175 10)))");
    Geometry expected8 =
        geomFromEWKT("GEOMETRYCOLLECTION (POINT (-170 10), LINESTRING (179 10, 181 10))");

    assertEquals(expected1, Functions.shiftLongitude(point));
    assertEquals(expected2, Functions.shiftLongitude(linestring1));
    assertEquals(expected3, Functions.shiftLongitude(linestring2));
    assertEquals(expected4, Functions.shiftLongitude(polygon));
    assertEquals(expected5, Functions.shiftLongitude(multiPoint));
    assertEquals(expected6, Functions.shiftLongitude(multiLineString));
    assertEquals(expected7, Functions.shiftLongitude(multiPolygon));
    assertEquals(expected8, Functions.shiftLongitude(geomCollection));
  }

  private static double comparePolygons(Geometry p1, Geometry p2) {
    Coordinate[] coords1 = p1.getCoordinates();
    Coordinate[] coords2 = p2.getCoordinates();

    double maxDistance = 0.0;
    for (int i = 0; i < Math.min(coords1.length, coords2.length); i++) {
      Geometry point1 = GEOMETRY_FACTORY.createPoint(coords1[i]);
      Geometry point2 = GEOMETRY_FACTORY.createPoint(coords2[i]);
      double distance = Spheroid.distance(point1, point2);
      maxDistance = Math.max(maxDistance, distance);
    }
    return maxDistance;
  }

  @Test
  public void testBestSRID() throws ParseException {
    int[][] testCases_special = {
      {0, -70, 3409}, // EPSG:3409 (Antarctic Polar Stereographic)
      {0, 70, 3574}, // EPSG:3575 (North Pole LAEA Alaska)
      // Special cases
      {-180, 60, 32660},
      {180, 60, 32660},
      {-180, -60, 32760},
      {180, -60, 32760},
    };

    // Number of UTM zones
    int numZones = (177 - (-177)) / 6 + 1;
    int numZonesSouth = (177 - (-177)) / 6 + 1;

    int[][] testCases_UTMNorth = new int[numZones][3];
    int[][] testCases_UTMSouth = new int[numZonesSouth][3];

    int indexNorth = 0;
    int northernLat = 60; // Latitude for Northern Hemisphere UTM zones
    for (int lon = -177, epsg = 32601; lon <= 177; lon += 6, epsg++) {
      testCases_UTMNorth[indexNorth][0] = lon; // Longitude
      testCases_UTMNorth[indexNorth][1] = northernLat; // Latitude
      testCases_UTMNorth[indexNorth][2] = epsg; // EPSG code
      indexNorth++;
    }

    int indexSouth = 0;
    int southernLat = -60; // Latitude for Southern Hemisphere UTM zones
    for (int lon = -177, epsg = 32701; lon <= 177; lon += 6, epsg++) {
      testCases_UTMSouth[indexSouth][0] = lon; // Longitude
      testCases_UTMSouth[indexSouth][1] = southernLat; // Latitude
      testCases_UTMSouth[indexSouth][2] = epsg; // EPSG code
      indexSouth++;
    }

    for (int[] testCase : testCases_special) {
      Geometry geom = GEOMETRY_FACTORY.createPoint(new Coordinate(testCase[0], testCase[1]));
      int actualEPSG = Functions.bestSRID(geom);
      int expectedEPSG = testCase[2];
      assertEquals(
          "Failed at coordinates (" + testCase[0] + ", " + testCase[1] + ")",
          expectedEPSG,
          actualEPSG);
    }

    for (int[] testCase : testCases_UTMNorth) {
      Geometry geom = GEOMETRY_FACTORY.createPoint(new Coordinate(testCase[0], testCase[1]));
      int actualEPSG = Functions.bestSRID(geom);
      int expectedEPSG = testCase[2];
      assertEquals(
          "Failed at coordinates (" + testCase[0] + ", " + testCase[1] + ")",
          expectedEPSG,
          actualEPSG);
    }

    for (int[] testCase : testCases_UTMSouth) {
      Geometry geom = GEOMETRY_FACTORY.createPoint(new Coordinate(testCase[0], testCase[1]));
      int actualEPSG = Functions.bestSRID(geom);
      int expectedEPSG = testCase[2];
      assertEquals(
          "Failed at coordinates (" + testCase[0] + ", " + testCase[1] + ")",
          expectedEPSG,
          actualEPSG);
    }

    // Large geometry that does not fit into UTM or polar categories
    Geometry geom =
        GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
              new Coordinate(-160, -40),
              new Coordinate(-160, 40),
              new Coordinate(160, 40),
              new Coordinate(160, -40),
              new Coordinate(-160, -40)
            });
    int expectedEPSG = 3395; // EPSG code for World Mercator
    int actualEPSG = Functions.bestSRID(geom);

    assertEquals(
        "Expected World Mercator projection for wide range geometry", expectedEPSG, actualEPSG);
  }

  @Test
  public void nRingsUnsupported() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(0, 1, 1, 1, 2, 1, 1, 2, 2));
    String expected =
        "Unsupported geometry type: "
            + "LineString"
            + ", only Polygon or MultiPolygon geometries are supported.";
    Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.nRings(lineString));
    assertEquals(expected, e.getMessage());
  }

  @Test
  public void translateEmptyObjectNoDeltaZ() {
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    String expected = emptyLineString.toText();
    String actual = Functions.translate(emptyLineString, 1, 1).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void translateEmptyObjectDeltaZ() {
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    String expected = emptyLineString.toText();
    String actual = Functions.translate(emptyLineString, 1, 3, 2).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void translate2DGeomNoDeltaZ() {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    String expected =
        GEOMETRY_FACTORY.createPolygon(coordArray(2, 4, 2, 5, 3, 5, 3, 4, 2, 4)).toText();
    String actual = Functions.translate(polygon, 1, 4).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void translate2DGeomDeltaZ() {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    String expected =
        GEOMETRY_FACTORY.createPolygon(coordArray(2, 3, 2, 4, 3, 4, 3, 3, 2, 3)).toText();
    String actual = Functions.translate(polygon, 1, 3, 2).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void translate3DGeomNoDeltaZ() {
    WKTWriter wktWriter = new WKTWriter(3);
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 1, 2, 1, 1, 2, 0, 1, 1, 0, 1));
    Polygon expectedPolygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 5, 1, 2, 6, 1, 3, 6, 1, 3, 5, 1, 2, 5, 1));
    assertEquals(
        wktWriter.write(expectedPolygon), wktWriter.write(Functions.translate(polygon, 1, 5)));
  }

  @Test
  public void translate3DGeomDeltaZ() {
    WKTWriter wktWriter = new WKTWriter(3);
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 1, 2, 1, 1, 2, 0, 1, 1, 0, 1));
    Polygon expectedPolygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 2, 4, 2, 3, 4, 3, 3, 4, 3, 2, 4, 2, 2, 4));
    assertEquals(
        wktWriter.write(expectedPolygon), wktWriter.write(Functions.translate(polygon, 1, 2, 3)));
  }

  @Test
  public void translateHybridGeomCollectionNoDeltaZ() {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Polygon polygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 2, 0, 2, 2, 1, 2, 1, 0, 1));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    Geometry geomCollection =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createGeometryCollection(
                  new Geometry[] {multiPolygon, point3D, emptyLineString})
            });
    Polygon expectedPolygon =
        GEOMETRY_FACTORY.createPolygon(coordArray(2, 2, 2, 3, 3, 3, 3, 2, 2, 2));
    Polygon expectedPolygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 2, 1, 3, 2, 2, 3, 3, 2, 2, 2, 1));
    Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 3, 1));
    WKTWriter wktWriter3D = new WKTWriter(3);
    GeometryCollection actualGeometry =
        (GeometryCollection) Functions.translate(geomCollection, 1, 2);
    assertEquals(
        wktWriter3D.write(expectedPolygon3D),
        wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
    assertEquals(
        expectedPolygon.toText(),
        actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(1).toText());
    assertEquals(
        wktWriter3D.write(expectedPoint3D),
        wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(1)));
    assertEquals(emptyLineString.toText(), actualGeometry.getGeometryN(0).getGeometryN(2).toText());
  }

  @Test
  public void translateHybridGeomCollectionDeltaZ() {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Polygon polygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 2, 0, 2, 2, 1, 2, 1, 0, 1));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    Geometry geomCollection =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createGeometryCollection(
                  new Geometry[] {multiPolygon, point3D, emptyLineString})
            });
    Polygon expectedPolygon =
        GEOMETRY_FACTORY.createPolygon(coordArray(2, 3, 2, 4, 3, 4, 3, 3, 2, 3));
    Polygon expectedPolygon3D =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 3, 6, 3, 3, 7, 3, 4, 7, 2, 3, 6));
    Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 4, 6));
    WKTWriter wktWriter3D = new WKTWriter(3);
    GeometryCollection actualGeometry =
        (GeometryCollection) Functions.translate(geomCollection, 1, 3, 5);

    assertEquals(
        wktWriter3D.write(expectedPolygon3D),
        wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
    assertEquals(
        expectedPolygon.toText(),
        actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(1).toText());
    assertEquals(
        wktWriter3D.write(expectedPoint3D),
        wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(1)));
    assertEquals(emptyLineString.toText(), actualGeometry.getGeometryN(0).getGeometryN(2).toText());
  }

  @Test
  public void testFrechetGeom2D() {
    LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 100, 0));
    LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 50, 50, 100, 0));
    double expected = 70.7106781186548;
    double actual = Functions.frechetDistance(lineString1, lineString2);
    assertEquals(expected, actual, 1e-9);
  }

  @Test
  public void testFrechetGeom3D() {
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 2, 2, 2, 3, 3, 3));
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 0, 1, 1, 1, 2, 1, 1, 1, 0, 0));
    double expected = 3.605551275463989;
    double actual = Functions.frechetDistance(lineString, polygon);
    assertEquals(expected, actual, 1e-9);
  }

  @Test
  public void testFrechetGeomCollection() {
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
    Geometry lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 3, 4, 4));
    Geometry lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(-1, -1, -4, -4, -10, -10));
    Geometry geometryCollection =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point, lineString1, lineString2});
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    double expected = 14.866068747318506;
    double actual = Functions.frechetDistance(polygon, geometryCollection);
    assertEquals(expected, actual, 1e-9);
  }

  @Test
  public void testFrechetGeomEmpty() {
    Polygon p1 = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    LineString emptyPoint = GEOMETRY_FACTORY.createLineString();
    double expected = 0.0;
    double actual = Functions.frechetDistance(p1, emptyPoint);
    assertEquals(expected, actual, 1e-9);
  }

  @Test
  public void boundingDiagonalGeom2D() {
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 2, 2, 0, 1, 0));
    String expected = "LINESTRING (1 0, 2 2)";
    String actual = Functions.boundingDiagonal(polygon).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void boundingDiagonalGeom3D() {
    Polygon polygon =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 3, 2, 2, 2, 4, 5, 1, 1, 1, 1, 0, 1));
    WKTWriter wktWriter = new WKTWriter(3);
    String expected = "LINESTRING Z(1 0 1, 3 4 5)";
    String actual = wktWriter.write(Functions.boundingDiagonal(polygon));
    assertEquals(expected, actual);
  }

  @Test
  public void boundingDiagonalGeomEmpty() {
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    String expected = "LINESTRING EMPTY";
    String actual = Functions.boundingDiagonal(emptyLineString).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void boundingDiagonalGeomCollection2D() {
    Polygon polygon1 =
        GEOMETRY_FACTORY.createPolygon(coordArray(1, 1, 1, -1, 2, 2, 2, 9, 9, 1, 1, 1));
    Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(5, 5, 4, 4, 2, 2, 5, 5));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 3));
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-1, 0));
    Geometry geometryCollection =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, lineString, point});
    String expected = "LINESTRING (-1 -1, 9 9)";
    String actual = Functions.boundingDiagonal(geometryCollection).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void boundingDiagonalGeomCollection3D() {
    Polygon polygon1 =
        GEOMETRY_FACTORY.createPolygon(
            coordArray3d(1, 1, 4, 1, -1, 6, 2, 2, 4, 2, 9, 4, 9, 1, 0, 1, 1, 4));
    Polygon polygon2 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(5, 5, 1, 4, 4, 1, 2, 2, 2, 5, 5, 1));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray3d(2, 2, 9, 3, 3, -5));
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-1, 9, 1));
    Geometry geometryCollection =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, lineString, point});
    String expected = "LINESTRING Z(-1 -1 -5, 9 9 9)";
    WKTWriter wktWriter = new WKTWriter(3);
    String actual = wktWriter.write(Functions.boundingDiagonal(geometryCollection));
    assertEquals(expected, actual);
  }

  @Test
  public void boundingDiagonalSingleVertex() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5));
    String expected = "LINESTRING (10 5, 10 5)";
    String actual = Functions.boundingDiagonal(point).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void angleFourPoints() {
    Point start1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    Point end1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    Point start2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 0));
    Point end2 = GEOMETRY_FACTORY.createPoint(new Coordinate(6, 2));

    double expected = 0.4048917862850834;
    double expectedDegrees = 23.198590513648185;
    double reverseExpectedDegrees = 336.8014094863518;
    double reverseExpected = 5.878293520894503;

    double actualPointsFour = Functions.angle(start1, end1, start2, end2);
    double actualPointsFourDegrees = Functions.degrees(actualPointsFour);
    double actualPointsFourReverse = Functions.angle(start2, end2, start1, end1);
    double actualPointsFourReverseDegrees = Functions.degrees(actualPointsFourReverse);

    assertEquals(expected, actualPointsFour, 1e-9);
    assertEquals(expectedDegrees, actualPointsFourDegrees, 1e-9);
    assertEquals(reverseExpected, actualPointsFourReverse, 1e-9);
    assertEquals(reverseExpectedDegrees, actualPointsFourReverseDegrees, 1e-9);
  }

  @Test
  public void angleFourPoints3D() {
    Point start1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0, 4));
    Point end1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 5));
    Point start2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 0, 9));
    Point end2 = GEOMETRY_FACTORY.createPoint(new Coordinate(6, 2, 2));

    double expected = 0.4048917862850834;
    double expectedDegrees = 23.198590513648185;
    double reverseExpectedDegrees = 336.8014094863518;
    double reverseExpected = 5.878293520894503;

    double actualPointsFour = Functions.angle(start1, end1, start2, end2);
    double actualPointsFourDegrees = Functions.degrees(actualPointsFour);
    double actualPointsFourReverse = Functions.angle(start2, end2, start1, end1);
    double actualPointsFourReverseDegrees = Functions.degrees(actualPointsFourReverse);

    assertEquals(expected, actualPointsFour, 1e-9);
    assertEquals(expectedDegrees, actualPointsFourDegrees, 1e-9);
    assertEquals(reverseExpected, actualPointsFourReverse, 1e-9);
    assertEquals(reverseExpectedDegrees, actualPointsFourReverseDegrees, 1e-9);
  }

  @Test
  public void angleThreePoints() {
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
    Point point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(3, 2));

    double expected = 0.19739555984988044;
    double expectedDegrees = 11.309932474020195;

    double actualPointsThree = Functions.angle(point1, point2, point3);
    double actualPointsThreeDegrees = Functions.degrees(actualPointsThree);

    assertEquals(expected, actualPointsThree, 1e-9);
    assertEquals(expectedDegrees, actualPointsThreeDegrees, 1e-9);
  }

  @Test
  public void angleTwoLineStrings() {
    LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1));
    LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 3, 2));

    double expected = 0.19739555984988044;
    double expectedDegrees = 11.309932474020195;
    double reverseExpected = 6.085789747329706;
    double reverseExpectedDegrees = 348.69006752597977;

    double actualLineString = Functions.angle(lineString1, lineString2);
    double actualLineStringReverse = Functions.angle(lineString2, lineString1);
    double actualLineStringDegrees = Functions.degrees(actualLineString);
    double actualLineStringReverseDegrees = Functions.degrees(actualLineStringReverse);

    assertEquals(expected, actualLineString, 1e-9);
    assertEquals(reverseExpected, actualLineStringReverse, 1e-9);
    assertEquals(expectedDegrees, actualLineStringDegrees, 1e-9);
    assertEquals(reverseExpectedDegrees, actualLineStringReverseDegrees, 1e-9);
  }

  @Test
  public void angleInvalidEmptyGeom() {
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(3, 5));
    Point point2 = GEOMETRY_FACTORY.createPoint();
    Point point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));

    Exception e =
        assertThrows(IllegalArgumentException.class, () -> Functions.angle(point1, point2, point3));
    assertEquals("ST_Angle cannot support empty geometries.", e.getMessage());
  }

  @Test
  public void angleInvalidUnsupportedGeom() {
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(3, 5));
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Point point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));

    Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> Functions.angle(point1, polygon, point3));
    assertEquals(
        "ST_Angle supports either only POINT or only LINESTRING geometries.", e.getMessage());
  }

  @Test
  public void isCollectionWithCollection() {
    Point[] points =
        new Point[] {
          GEOMETRY_FACTORY.createPoint(new Coordinate(5.0, 6.0)),
          GEOMETRY_FACTORY.createPoint(new Coordinate(7.0, 8.0))
        };
    GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(points);

    boolean actualResult = Functions.isCollection(geometryCollection);
    boolean expectedResult = true;
    assertEquals(actualResult, expectedResult);
  }

  @Test
  public void isCollectionWithOutCollection() {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(6.0, 6.0));

    boolean actualResult = Functions.isCollection(point);
    boolean expectedResult = false;
    assertEquals(actualResult, expectedResult);
  }

  public void affineEmpty3D() {
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    String expected = emptyLineString.toText();
    String actual =
        Functions.affine(
                emptyLineString, 1.0, 1.0, 4.0, 2.0, 2.0, 4.0, 5.0, 5.0, 6.0, 3.0, 3.0, 6.0)
            .toText();
    assertEquals(expected, actual);
  }

  @Test
  public void affineEmpty2D() {
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    String expected = emptyLineString.toText();
    String actual = Functions.affine(emptyLineString, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void affine3DGeom2D() {
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
    String expected = GEOMETRY_FACTORY.createLineString(coordArray(4, 5, 5, 7, 6, 9)).toText();
    String actual =
        Functions.affine(lineString, 1.0, 1.0, 4.0, 2.0, 2.0, 4.0, 5.0, 5.0, 6.0, 3.0, 3.0, 6.0)
            .toText();
    assertEquals(expected, actual);
  }

  @Test
  public void affine3DGeom3D() {
    WKTWriter wktWriter = new WKTWriter(3);
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 1, 1, 2, 1, 2, 2));
    String expected =
        wktWriter.write(
            GEOMETRY_FACTORY.createLineString(coordArray3d(8, 9, 17, 13, 15, 28, 14, 17, 33)));
    String actual =
        wktWriter.write(
            Functions.affine(
                lineString, 1.0, 1.0, 4.0, 2.0, 2.0, 4.0, 5.0, 5.0, 6.0, 3.0, 3.0, 6.0));
    assertEquals(expected, actual);
  }

  @Test
  public void affine3DHybridGeomCollection() {
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
    Polygon polygon1 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 2, 1, 1, 2, 2, 1, 2, 2, 0, 2, 1, 0, 2));
    Polygon polygon2 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 1, 2, 2, 2, 1, 0, 1));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
    Geometry geomCollection =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point3D, multiPolygon})
            });
    Geometry actualGeomCollection =
        Functions.affine(
            geomCollection, 1.0, 2.0, 1.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 2.0, 3.0, 3.0);
    WKTWriter wktWriter3D = new WKTWriter(3);
    Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(6, 9, 9));
    Polygon expectedPolygon1 =
        GEOMETRY_FACTORY.createPolygon(
            coordArray3d(5, 10, 10, 7, 11, 11, 8, 14, 14, 6, 13, 13, 5, 10, 10));
    Polygon expectedPolygon2 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(4, 8, 8, 6, 9, 9, 10, 15, 15, 4, 8, 8));
    assertEquals(
        wktWriter3D.write(expectedPoint3D),
        wktWriter3D.write(actualGeomCollection.getGeometryN(0).getGeometryN(0)));
    assertEquals(
        wktWriter3D.write(expectedPolygon1),
        wktWriter3D.write(actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(0)));
    assertEquals(
        wktWriter3D.write(expectedPolygon2),
        wktWriter3D.write(actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(1)));
  }

  @Test
  public void affine2DGeom3D() {
    WKTWriter wktWriter = new WKTWriter(3);
    LineString lineString =
        GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 1, 1, 2, 1, 2, 2));
    String expected =
        wktWriter.write(
            GEOMETRY_FACTORY.createLineString(coordArray3d(6, 8, 1, 7, 11, 2, 8, 14, 2)));
    String actual = wktWriter.write(Functions.affine(lineString, 1d, 1d, 2d, 3d, 5d, 6d));
    assertEquals(expected, actual);
  }

  @Test
  public void affine2DGeom2D() {
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
    String expected = GEOMETRY_FACTORY.createLineString(coordArray(6, 8, 7, 11, 8, 14)).toText();
    String actual = Functions.affine(lineString, 1.0, 1.0, 2.0, 3.0, 5.0, 6.0).toText();
    assertEquals(expected, actual);
  }

  @Test
  public void affine2DHybridGeomCollection() {
    Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 4, 3, 5, 3, 7, 10, 7, 3, 4));
    MultiPolygon multiPolygon =
        GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
    Geometry geomCollection =
        GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
              GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point3D, multiPolygon})
            });
    Geometry actualGeomCollection = Functions.affine(geomCollection, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0);
    Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(4, 5));
    Polygon expectedPolygon1 =
        GEOMETRY_FACTORY.createPolygon(coordArray(2, 3, 4, 5, 5, 6, 3, 4, 2, 3));
    Polygon expectedPolygon2 =
        GEOMETRY_FACTORY.createPolygon(coordArray(12, 13, 14, 15, 18, 19, 25, 26, 12, 13));
    assertEquals(
        expectedPoint3D.toText(), actualGeomCollection.getGeometryN(0).getGeometryN(0).toText());
    assertEquals(
        expectedPolygon1.toText(),
        actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(0).toText());
    assertEquals(
        expectedPolygon2.toText(),
        actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(1).toText());
  }

  @Test
  public void geometryTypeWithMeasured2D() {
    String expected1 = "POINT";
    String actual1 =
        Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5)));
    assertEquals(expected1, actual1);

    // Create a point with measure value
    CoordinateXYM coords = new CoordinateXYM(2, 3, 4);
    Point measuredPoint = GEOMETRY_FACTORY.createPoint(coords);
    String expected2 = "POINTM";
    String actual2 = Functions.geometryTypeWithMeasured(measuredPoint);
    assertEquals(expected2, actual2);

    // Create a linestring with measure value
    CoordinateXYM[] coordsLineString =
        new CoordinateXYM[] {new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6)};
    LineString measuredLineString = GEOMETRY_FACTORY.createLineString(coordsLineString);
    String expected3 = "LINESTRINGM";
    String actual3 = Functions.geometryTypeWithMeasured(measuredLineString);
    assertEquals(expected3, actual3);

    // Create a polygon with measure value
    CoordinateXYM[] coordsPolygon =
        new CoordinateXYM[] {
          new CoordinateXYM(0, 0, 0),
          new CoordinateXYM(1, 1, 0),
          new CoordinateXYM(0, 1, 0),
          new CoordinateXYM(0, 0, 0)
        };
    Polygon measuredPolygon = GEOMETRY_FACTORY.createPolygon(coordsPolygon);
    String expected4 = "POLYGONM";
    String actual4 = Functions.geometryTypeWithMeasured(measuredPolygon);
    assertEquals(expected4, actual4);
  }

  @Test
  public void geometryTypeWithMeasured3D() {
    String expected1 = "POINT";
    String actual1 =
        Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5, 1)));
    assertEquals(expected1, actual1);

    // Create a point with measure value
    CoordinateXYZM coordsPoint = new CoordinateXYZM(2, 3, 4, 0);
    Point measuredPoint = GEOMETRY_FACTORY.createPoint(coordsPoint);
    String expected2 = "POINTM";
    String actual2 = Functions.geometryTypeWithMeasured(measuredPoint);
    assertEquals(expected2, actual2);

    // Create a linestring with measure value
    CoordinateXYZM[] coordsLineString =
        new CoordinateXYZM[] {new CoordinateXYZM(1, 2, 3, 0), new CoordinateXYZM(4, 5, 6, 0)};
    LineString measuredLineString = GEOMETRY_FACTORY.createLineString(coordsLineString);
    String expected3 = "LINESTRINGM";
    String actual3 = Functions.geometryTypeWithMeasured(measuredLineString);
    assertEquals(expected3, actual3);

    // Create a polygon with measure value
    CoordinateXYZM[] coordsPolygon =
        new CoordinateXYZM[] {
          new CoordinateXYZM(0, 0, 0, 0),
          new CoordinateXYZM(1, 1, 0, 0),
          new CoordinateXYZM(0, 1, 0, 0),
          new CoordinateXYZM(0, 0, 0, 0)
        };
    Polygon measuredPolygon = GEOMETRY_FACTORY.createPolygon(coordsPolygon);
    String expected4 = "POLYGONM";
    String actual4 = Functions.geometryTypeWithMeasured(measuredPolygon);
    assertEquals(expected4, actual4);
  }

  @Test
  public void geometryTypeWithMeasuredCollection() {
    String expected1 = "GEOMETRYCOLLECTION";
    String actual1 =
        Functions.geometryTypeWithMeasured(
            GEOMETRY_FACTORY.createGeometryCollection(
                new Geometry[] {GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5))}));
    assertEquals(expected1, actual1);

    // Create a geometrycollection with measure value
    CoordinateXYM coords = new CoordinateXYM(2, 3, 4);
    Point measuredPoint = GEOMETRY_FACTORY.createPoint(coords);
    String expected2 = "GEOMETRYCOLLECTIONM";
    String actual2 =
        Functions.geometryTypeWithMeasured(
            GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {measuredPoint}));
    assertEquals(expected2, actual2);

    // Create a geometrycollection with measure value
    CoordinateXYM[] coordsLineString =
        new CoordinateXYM[] {new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6)};
    LineString measuredLineString = GEOMETRY_FACTORY.createLineString(coordsLineString);
    String expected3 = "GEOMETRYCOLLECTIONM";
    String actual3 =
        Functions.geometryTypeWithMeasured(
            GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {measuredLineString}));
    assertEquals(expected3, actual3);

    // Create a geometrycollection with measure value
    CoordinateXYM[] coordsPolygon =
        new CoordinateXYM[] {
          new CoordinateXYM(0, 0, 0),
          new CoordinateXYM(1, 1, 0),
          new CoordinateXYM(0, 1, 0),
          new CoordinateXYM(0, 0, 0)
        };
    Polygon measuredPolygon = GEOMETRY_FACTORY.createPolygon(coordsPolygon);
    String expected4 = "GEOMETRYCOLLECTIONM";
    String actual4 =
        Functions.geometryTypeWithMeasured(
            GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {measuredPolygon}));
    assertEquals(expected4, actual4);
  }

  @Test
  public void closestPoint() {
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    LineString lineString1 =
        GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    String expected1 = "POINT (1 1)";
    String actual1 = Functions.closestPoint(point1, lineString1).toText();
    assertEquals(expected1, actual1);

    Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(160, 40));
    LineString lineString2 =
        GEOMETRY_FACTORY.createLineString(
            coordArray(10, 30, 50, 50, 30, 110, 70, 90, 180, 140, 130, 190));
    String expected2 = "POINT (160 40)";
    String actual2 = Functions.closestPoint(point2, lineString2).toText();
    assertEquals(expected2, actual2);
    Point expectedPoint3 =
        GEOMETRY_FACTORY.createPoint(new Coordinate(125.75342465753425, 115.34246575342466));
    Double expected3 = Functions.closestPoint(lineString2, point2).distance(expectedPoint3);
    assertEquals(expected3, 0, 1e-6);

    Point point4 = GEOMETRY_FACTORY.createPoint(new Coordinate(80, 160));
    Polygon polygonA =
        GEOMETRY_FACTORY.createPolygon(coordArray(190, 150, 20, 10, 160, 70, 190, 150));
    Geometry polygonB = Functions.buffer(point4, 30);
    Point expectedPoint4 =
        GEOMETRY_FACTORY.createPoint(new Coordinate(131.59149149528952, 101.89887534906197));
    Double expected4 = Functions.closestPoint(polygonA, polygonB).distance(expectedPoint4);
    assertEquals(expected4, 0, 1e-6);
  }

  @Test
  public void closestPoint3d() {
    // One of the object is 3D
    Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 10000));
    LineString lineString1 =
        GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
    String expected1 = "POINT (1 1)";
    String actual1 = Functions.closestPoint(point1, lineString1).toText();
    assertEquals(expected1, actual1);

    // Both of the object are 3D
    LineString lineString3D =
        GEOMETRY_FACTORY.createLineString(
            coordArray3d(1, 0, 100, 1, 1, 20, 2, 1, 40, 2, 0, 60, 1, 0, 70));
    String expected2 = "POINT (1 1)";
    String actual2 = Functions.closestPoint(point1, lineString3D).toText();
    assertEquals(expected2, actual2);
  }

  @Test
  public void closestPointGeometryCollection() {
    LineString line = GEOMETRY_FACTORY.createLineString(coordArray(2, 0, 0, 2));
    Geometry[] geometry =
        new Geometry[] {
          GEOMETRY_FACTORY.createLineString(coordArray(2, 0, 2, 1)),
          GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
        };
    GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(geometry);
    String actual1 = Functions.closestPoint(line, geometryCollection).toText();
    String expected1 = "POINT (2 0)";
    assertEquals(actual1, expected1);
  }

  @Test
  public void closestPointEmpty() {
    // One of the object is empty
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    String expected = "ST_ClosestPoint doesn't support empty geometry object.";
    Exception e1 =
        assertThrows(
            IllegalArgumentException.class, () -> Functions.closestPoint(point, emptyLineString));
    assertEquals(expected, e1.getMessage());

    // Both objects are empty
    Polygon emptyPolygon = GEOMETRY_FACTORY.createPolygon();
    Exception e2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> Functions.closestPoint(emptyPolygon, emptyLineString));
    assertEquals(expected, e2.getMessage());
  }

  @Test
  public void testZmFlag() throws ParseException {
    int _2D = 0, _3DM = 1, _3DZ = 2, _4D = 3;
    Geometry geom = Constructors.geomFromWKT("POINT (1 2)", 0);
    assertEquals(_2D, Functions.zmFlag(geom));

    geom = Constructors.geomFromWKT("LINESTRING (1 2 3, 4 5 6)", 0);
    assertEquals(_3DZ, Functions.zmFlag(geom));

    geom = Constructors.geomFromWKT("POLYGON M((1 2 3, 3 4 3, 5 6 3, 3 4 3, 1 2 3))", 0);
    assertEquals(_3DM, Functions.zmFlag(geom));

    geom =
        Constructors.geomFromWKT(
            "MULTIPOLYGON ZM (((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1)), ((15 5 3 1, 20 10 6 2, 10 10 7 3, 15 5 3 1)))",
            0);
    assertEquals(_4D, Functions.zmFlag(geom));
  }

  @Test
  public void hausdorffDistanceDefaultGeom2D() throws Exception {
    Polygon polygon1 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 2, 2, 1, 5, 2, 0, 1, 1, 0, 1));
    Polygon polygon2 =
        GEOMETRY_FACTORY.createPolygon(coordArray3d(4, 0, 4, 6, 1, 4, 6, 4, 9, 6, 1, 3, 4, 0, 4));
    Double expected = 5.0;
    Double actual = Functions.hausdorffDistance(polygon1, polygon2);
    assertEquals(expected, actual);
  }

  @Test
  public void hausdorffDistanceGeom2D() throws Exception {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(10, 34));
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 2, 1, 5, 2, 6, 1, 2));
    Double expected = 33.24154027718932;
    Double actual = Functions.hausdorffDistance(point, lineString, 0.33);
    assertEquals(expected, actual);
  }

  @Test
  public void hausdorffDistanceInvalidDensityFrac() throws Exception {
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(10, 34));
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 2, 1, 5, 2, 6, 1, 2));
    Exception e =
        assertThrows(
            IllegalArgumentException.class,
            () -> Functions.hausdorffDistance(point, lineString, 3));
    String expected = "Fraction is not in range (0.0 - 1.0]";
    String actual = e.getMessage();
    assertEquals(expected, actual);
  }

  @Test
  public void hausdorffDistanceDefaultGeomCollection() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
    Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 0));
    Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(40, 10));
    Geometry point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(-10, -40));
    GeometryCollection multiPoint =
        GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point1, point2, point3});
    Double actual = Functions.hausdorffDistance(polygon, multiPoint);
    Double expected = 41.7612260356422;
    assertEquals(expected, actual);
  }

  @Test
  public void hausdorffDistanceGeomCollection() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
    LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(1, 1, 2, 1, 4, 4, 5, 5));
    LineString lineString2 =
        GEOMETRY_FACTORY.createLineString(coordArray(10, 10, 11, 11, 12, 12, 14, 14));
    LineString lineString3 =
        GEOMETRY_FACTORY.createLineString(coordArray(-11, -20, -11, -21, -15, -19));
    MultiLineString multiLineString =
        GEOMETRY_FACTORY.createMultiLineString(
            new LineString[] {lineString1, lineString2, lineString3});
    Double actual = Functions.hausdorffDistance(polygon, multiLineString, 0.0000001);
    Double expected = 25.495097567963924;
    assertEquals(expected, actual);
  }

  @Test
  public void hausdorffDistanceEmptyGeom() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    Double expected = 0.0;
    Double actual = Functions.hausdorffDistance(polygon, emptyLineString, 0.00001);
    assertEquals(expected, actual);
  }

  @Test
  public void hausdorffDistanceDefaultEmptyGeom() throws Exception {
    Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
    LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
    Double expected = 0.0;
    Double actual = Functions.hausdorffDistance(polygon, emptyLineString);
    assertEquals(expected, actual);
  }

  @Test
  public void transform() throws FactoryException, TransformException {
    // The source and target CRS are the same
    Point geomExpected = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 60));
    Geometry geomActual = FunctionsGeoTools.transform(geomExpected, "EPSG:4326", "EPSG:4326");
    assertEquals(geomExpected.getCoordinate().x, geomActual.getCoordinate().x, FP_TOLERANCE);
    assertEquals(geomExpected.getCoordinate().y, geomActual.getCoordinate().y, FP_TOLERANCE);
    assertEquals(4326, geomActual.getSRID());
    assertEquals(4326, geomActual.getFactory().getSRID());

    // The source and target CRS are different
    geomActual = FunctionsGeoTools.transform(geomExpected, "EPSG:4326", "EPSG:3857");
    assertEquals(1.3358338895192828E7, geomActual.getCoordinate().x, FP_TOLERANCE);
    assertEquals(8399737.889818355, geomActual.getCoordinate().y, FP_TOLERANCE);
    assertEquals(3857, geomActual.getSRID());
    assertEquals(3857, geomActual.getFactory().getSRID());

    // The source CRS is not specified and the geometry has no SRID
    Exception e =
        assertThrows(
            IllegalArgumentException.class,
            () -> FunctionsGeoTools.transform(geomExpected, "EPSG:3857"));
    assertEquals("Source CRS must be specified. No SRID found on geometry.", e.getMessage());

    // The source CRS is an invalid SRID
    e =
        assertThrows(
            FactoryException.class,
            () -> FunctionsGeoTools.transform(geomExpected, "abcde", "EPSG:3857"));
    assertTrue(e.getMessage().contains("First failed to read as a well-known CRS code"));

    // The source CRS is a WKT CRS string
    String crsWkt = CRS.decode("EPSG:4326", true).toWKT();
    geomActual = FunctionsGeoTools.transform(geomExpected, crsWkt, "EPSG:3857");
    assertEquals(1.3358338895192828E7, geomActual.getCoordinate().x, FP_TOLERANCE);
    assertEquals(8399737.889818355, geomActual.getCoordinate().y, FP_TOLERANCE);
    assertEquals(3857, geomActual.getSRID());
    assertEquals(3857, geomActual.getFactory().getSRID());

    // The source CRS is not specified but the geometry has a valid SRID
    geomExpected.setSRID(4326);
    geomActual = FunctionsGeoTools.transform(geomExpected, "EPSG:3857");
    assertEquals(1.3358338895192828E7, geomActual.getCoordinate().x, FP_TOLERANCE);
    assertEquals(8399737.889818355, geomActual.getCoordinate().y, FP_TOLERANCE);
    assertEquals(3857, geomActual.getSRID());
    assertEquals(3857, geomActual.getFactory().getSRID());

    // The source and target CRS are different, and latitude is out of range
    Point geometryWrong = GEOMETRY_FACTORY.createPoint(new Coordinate(60, 120));
    assertThrows(
        ProjectionException.class,
        () -> FunctionsGeoTools.transform(geometryWrong, "EPSG:4326", "EPSG:3857"));
  }

  @Test
  public void testAddMeasure() throws ParseException {
    Geometry geom = Constructors.geomFromEWKT("LINESTRING (1 1, 2 2, 2 2, 3 3)");
    String actual = Functions.asWKT(Functions.addMeasure(geom, 1, 70));
    String expected = "LINESTRING M(1 1 1, 2 2 35.5, 2 2 35.5, 3 3 70)";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.addMeasure(geom, 1, 2));
    expected = "LINESTRING M(1 1 1, 2 2 1.5, 2 2 1.5, 3 3 2)";
    assertEquals(expected, actual);

    geom =
        Constructors.geomFromEWKT("MULTILINESTRING M((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))");
    actual = Functions.asWKT(Functions.addMeasure(geom, 1, 70));
    expected = "MULTILINESTRING M((1 0 1, 2 0 12.5, 4 0 35.5), (1 0 35.5, 2 0 47, 4 0 70))";
    assertEquals(expected, actual);
  }

  @Test
  public void voronoiPolygons() {
    MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0, 0, 2, 2));
    Geometry actual1 = FunctionsGeoTools.voronoiPolygons(multiPoint, 0, null);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 -2, -2 -2)), POLYGON ((-2 4, 4 4, 4 -2, -2 4)))",
        actual1.toText());

    Geometry actual2 = FunctionsGeoTools.voronoiPolygons(multiPoint, 30, null);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 4, 4 -2, -2 -2)))", actual2.toText());

    Geometry buf = Functions.buffer(GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1)), 10);
    Geometry actual3 = FunctionsGeoTools.voronoiPolygons(multiPoint, 0, buf);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 -9, -9 -9)), POLYGON ((-9 11, 11 11, 11 -9, -9 11)))",
        actual3.toText());

    Geometry actual4 = FunctionsGeoTools.voronoiPolygons(multiPoint, 30, buf);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 11, 11 -9, -9 -9)))", actual4.toText());

    Geometry actual5 = FunctionsGeoTools.voronoiPolygons(null, 0, null);
    assertEquals(null, actual5);
  }

  @Test
  public void lineLocatePoint() {
    LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1, 2, 2));
    Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-1, 1));
    Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 2));
    Geometry point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 3));

    Double actual1 = Functions.lineLocatePoint(lineString, point1);
    Double actual2 = Functions.lineLocatePoint(lineString, point2);
    Double actual3 = Functions.lineLocatePoint(lineString, point3);

    Double expectedResult1 = 0.0;
    Double expectedResult2 = 0.5;
    Double expectedResult3 = 1.0;

    assertEquals(expectedResult1, actual1, FP_TOLERANCE);
    assertEquals(expectedResult2, actual2, FP_TOLERANCE);
    assertEquals(expectedResult3, actual3, FP_TOLERANCE);
  }

  @Test
  public void locateAlong() throws ParseException {
    Geometry geom =
        Constructors.geomFromEWKT("MULTIPOINT M(1 2 3, 3 4 3, 9 4 3, 3 2 1, 1 2 3, 5 4 2)");
    String actual = Functions.asWKT(Functions.locateAlong(geom, 3, 0));
    String expected = "MULTIPOINT M((1 2 3), (3 4 3), (9 4 3), (1 2 3))";
    assertEquals(expected, actual);

    // offset doesn't affect Point or MultiPoint
    actual = Functions.asWKT(Functions.locateAlong(geom, 3, 4));
    assertEquals(expected, actual);

    geom = Constructors.geomFromEWKT("LINESTRING M(1 2 3, 3 4 3, 5 4 2, 9 4 3)");
    actual = Functions.asWKT(Functions.locateAlong(geom, 3, 0));
    expected = "MULTIPOINT M((2 3 3), (3 4 3), (9 4 3))";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.locateAlong(geom, 3, 2));
    expected = "MULTIPOINT M((0.5857864376269051 4.414213562373095 3), (3 6 3), (9 6 3))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromEWKT("LINESTRING M(1 2 3, 3 4 3, 5 4 2, 5 4 2, 9 4 3)");
    actual = Functions.asWKT(Functions.locateAlong(geom, 2, 0));
    expected = "MULTIPOINT M((5 4 2))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromEWKT("MULTILINESTRING M((1 2 3, 3 4 2, 9 4 3),(1 2 3, 5 4 5))");
    actual = Functions.asWKT(Functions.locateAlong(geom, 2, 0));
    expected = "MULTIPOINT M((3 4 2))";
    assertEquals(expected, actual);

    actual = Functions.asWKT(Functions.locateAlong(geom, 2, -3));
    expected = "MULTIPOINT M((5.121320343559642 1.8786796564403572 2), (3 1 2))";
    assertEquals(expected, actual);

    geom = Constructors.geomFromEWKT("POLYGON M((0 0 1, 1 1 1, 5 1 1, 5 0 1, 1 0 1, 0 0 1))");
    Geometry finalGeom = geom;
    Exception e =
        assertThrows(IllegalArgumentException.class, () -> Functions.locateAlong(finalGeom, 1));
    assertEquals(
        "Polygon geometry type not supported, supported types are: (Multi)Point and (Multi)LineString.",
        e.getMessage());
  }

  @Test
  public void maximumInscribedCircle() throws ParseException {
    Geometry geom =
        Constructors.geomFromEWKT(
            "POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))");
    InscribedCircle actual = Functions.maximumInscribedCircle(geom);
    InscribedCircle expected =
        new InscribedCircle(
            Constructors.geomFromEWKT("POINT (96.953125 76.328125)"),
            Constructors.geomFromEWKT("POINT (140 90)"),
            45.165846);
    assertTrue(expected.equals(actual));

    geom =
        Constructors.geomFromEWKT(
            "MULTILINESTRING ((59.5 17, 65 17), (60 16.5, 66 12), (65 12, 69 17))");
    actual = Functions.maximumInscribedCircle(geom);
    expected =
        new InscribedCircle(
            Constructors.geomFromEWKT("POINT (65.0419921875 15.1005859375)"),
            Constructors.geomFromEWKT("POINT (65 17)"),
            1.8998781);
    assertTrue(expected.equals(actual));

    geom =
        Constructors.geomFromEWKT(
            "MULTIPOINT ((60.8 15.5), (63.2 16.3), (63 14), (67.4 14.8), (66.3 18.4), (65. 13.), (67.5 16.9), (64.2 18))");
    actual = Functions.maximumInscribedCircle(geom);
    expected =
        new InscribedCircle(
            Constructors.geomFromEWKT("POINT (65.44062499999998 15.953124999999998)"),
            Constructors.geomFromEWKT("POINT (67.5 16.9)"),
            2.2666269);
    assertTrue(expected.equals(actual));

    geom = Constructors.geomFromEWKT("MULTIPOINT ((60.8 15.5), (63.2 16.3))");
    actual = Functions.maximumInscribedCircle(geom);
    expected =
        new InscribedCircle(
            Constructors.geomFromEWKT("POINT (60.8 15.5)"),
            Constructors.geomFromEWKT("POINT (60.8 15.5)"),
            0.0);
    assertTrue(expected.equals(actual));

    geom =
        Constructors.geomFromEWKT(
            "GEOMETRYCOLLECTION (POINT (60.8 15.5), POINT (63.2 16.3), POINT (63 14), POINT (67.4 14.8), POINT (66.3 18.4), POINT (65 13), POINT (67.5 16.9), POINT (64.2 18))");
    actual = Functions.maximumInscribedCircle(geom);
    expected =
        new InscribedCircle(
            Constructors.geomFromEWKT("POINT (65.44062499999998 15.953124999999998)"),
            Constructors.geomFromEWKT("POINT (67.5 16.9)"),
            2.2666269);
    assertTrue(expected.equals(actual));
  }

  @Test
  public void test() throws ParseException {
    Geometry geom = Constructors.geomFromEWKT("MULTILINESTRING M ((0 0 1,0 1 2), (0 0 1,0 1 2))");
    boolean actual = Functions.isValidTrajectory(geom);
    assertFalse(actual);

    geom = Constructors.geomFromEWKT("LINESTRING M (0 0 1, 0 1 2)");
    actual = Functions.isValidTrajectory(geom);
    assertTrue(actual);

    geom = Constructors.geomFromEWKT("LINESTRING M (0 0 1, 0 1 1)");
    actual = Functions.isValidTrajectory(geom);
    assertFalse(actual);
  }

  @Test
  public void isValidDetail() throws ParseException {
    // Valid geometry
    Geometry validGeom =
        GEOMETRY_FACTORY.createPolygon(coordArray(30, 10, 40, 40, 20, 40, 10, 20, 30, 10));
    ValidDetail actualValidDetail = Functions.isValidDetail(validGeom);
    ValidDetail expectedValidDetail = new ValidDetail(true, null, null);
    assertTrue(expectedValidDetail.equals(actualValidDetail));

    Integer OGC_SFS_VALIDITY = 0;
    Integer ESRI_VALIDITY = 1;

    actualValidDetail = Functions.isValidDetail(validGeom, OGC_SFS_VALIDITY);
    assertTrue(expectedValidDetail.equals(actualValidDetail));

    actualValidDetail = Functions.isValidDetail(validGeom, ESRI_VALIDITY);
    assertTrue(expectedValidDetail.equals(actualValidDetail));

    // Invalid geometry (self-intersection)
    Geometry invalidGeom =
        GEOMETRY_FACTORY.createPolygon(coordArray(30, 10, 40, 40, 20, 40, 30, 10, 10, 20, 30, 10));
    actualValidDetail = Functions.isValidDetail(invalidGeom);
    expectedValidDetail =
        new ValidDetail(
            false,
            "Ring Self-intersection at or near point (30.0, 10.0, NaN)",
            Constructors.geomFromEWKT("POINT (30 10)"));
    assertTrue(expectedValidDetail.equals(actualValidDetail));

    actualValidDetail = Functions.isValidDetail(invalidGeom, OGC_SFS_VALIDITY);
    expectedValidDetail =
        new ValidDetail(
            false,
            "Ring Self-intersection at or near point (30.0, 10.0, NaN)",
            Constructors.geomFromEWKT("POINT (30 10)"));
    assertTrue(expectedValidDetail.equals(actualValidDetail));

    actualValidDetail = Functions.isValidDetail(invalidGeom, ESRI_VALIDITY);
    expectedValidDetail =
        new ValidDetail(
            false,
            "Self-intersection at or near point (10.0, 20.0, NaN)",
            Constructors.geomFromEWKT("POINT (10 20)"));
    assertTrue(expectedValidDetail.equals(actualValidDetail));
  }

  @Test
  public void isValidReason() {
    // Valid geometry
    Geometry validGeom =
        GEOMETRY_FACTORY.createPolygon(coordArray(30, 10, 40, 40, 20, 40, 10, 20, 30, 10));
    String validReasonDefault = Functions.isValidReason(validGeom);
    assertEquals("Valid Geometry", validReasonDefault);

    Integer OGC_SFS_VALIDITY = 0;
    Integer ESRI_VALIDITY = 1;

    String validReasonOGC = Functions.isValidReason(validGeom, OGC_SFS_VALIDITY);
    assertEquals("Valid Geometry", validReasonOGC);

    String validReasonESRI = Functions.isValidReason(validGeom, ESRI_VALIDITY);
    assertEquals("Valid Geometry", validReasonESRI);

    // Invalid geometry (self-intersection)
    Geometry invalidGeom =
        GEOMETRY_FACTORY.createPolygon(coordArray(30, 10, 40, 40, 20, 40, 30, 10, 10, 20, 30, 10));
    String invalidReasonDefault = Functions.isValidReason(invalidGeom);
    assertEquals("Ring Self-intersection at or near point (30.0, 10.0, NaN)", invalidReasonDefault);

    String invalidReasonOGC = Functions.isValidReason(invalidGeom, OGC_SFS_VALIDITY);
    assertEquals("Ring Self-intersection at or near point (30.0, 10.0, NaN)", invalidReasonOGC);

    String invalidReasonESRI = Functions.isValidReason(invalidGeom, ESRI_VALIDITY);
    assertEquals("Self-intersection at or near point (10.0, 20.0, NaN)", invalidReasonESRI);
  }

  @Test
  public void points() throws ParseException {
    Geometry polygon = Constructors.geomFromEWKT("POLYGON ((0 0, 1 1, 5 1, 5 0, 1 0, 0 0))");
    Geometry lineString = Constructors.geomFromEWKT("LINESTRING (0 0, 1 1, 2 2)");
    Geometry point = Constructors.geomFromEWKT("POINT (0 0)");
    Geometry multiPoint = Constructors.geomFromEWKT("MULTIPOINT ((0 0), (1 1), (2 2))");
    Geometry multiLineString =
        Constructors.geomFromEWKT("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))");
    Geometry multiPolygon =
        Constructors.geomFromEWKT("MULTIPOLYGON (((0 0, 1 1, 1 0, 0 0)), ((2 2, 3 3, 3 2, 2 2)))");
    Geometry geometry3D = Constructors.geomFromEWKT("POLYGON Z ((0 0 1, 1 1 2, 2 2 3, 0 0 1))");

    String result = Functions.asEWKT(Functions.points(polygon));
    assertEquals("MULTIPOINT ((0 0), (1 1), (5 1), (5 0), (1 0), (0 0))", result);
    result = Functions.asEWKT(Functions.points(lineString));
    assertEquals("MULTIPOINT ((0 0), (1 1), (2 2))", result);
    result = Functions.asEWKT(Functions.points(point));
    assertEquals("MULTIPOINT ((0 0))", result);
    result = Functions.asEWKT(Functions.points(multiPoint));
    assertEquals("MULTIPOINT ((0 0), (1 1), (2 2))", result);
    result = Functions.asEWKT(Functions.points(multiLineString));
    assertEquals("MULTIPOINT ((0 0), (1 1), (2 2), (3 3))", result);
    result = Functions.asEWKT(Functions.points(multiPolygon));
    assertEquals("MULTIPOINT ((0 0), (1 1), (1 0), (0 0), (2 2), (3 3), (3 2), (2 2))", result);
    String result1 = Functions.asEWKT(Functions.points(geometry3D));
    assertEquals("MULTIPOINT Z((0 0 1), (1 1 2), (2 2 3), (0 0 1))", result1);
  }

  @Test
  public void rotateX() throws ParseException {
    Geometry lineString = Constructors.geomFromEWKT("LINESTRING (50 160, 50 50, 100 50)");
    String actual = Functions.asEWKT(Functions.rotateX(lineString, Math.PI));
    String expected = "LINESTRING (50 -160, 50 -50, 100 -50)";
    assertEquals(expected, actual);

    lineString = Constructors.geomFromWKT("LINESTRING(1 2 3, 1 1 1)", 1234);
    Geometry geomActual = Functions.rotateX(lineString, Math.PI / 2);
    actual = Functions.asWKT(geomActual);
    expected = "LINESTRING Z(1 -3 2, 1 -0.9999999999999999 1)";
    assertEquals(1234, geomActual.getSRID());
    assertEquals(expected, actual);
  }

  @Test
  public void rotate() throws ParseException {
    Geometry lineString = Constructors.geomFromEWKT("LINESTRING (50 160, 50 50, 100 50)");
    String result = Functions.asEWKT(Functions.rotate(lineString, Math.PI));
    assertEquals(
        "LINESTRING (-50.00000000000002 -160, -50.00000000000001 -49.99999999999999, -100 -49.999999999999986)",
        result);

    lineString = Constructors.geomFromEWKT("LINESTRING (0 0, 1 0, 1 1, 0 0)");
    Geometry pointOrigin = Constructors.geomFromEWKT("POINT (0 0)");
    result = Functions.asEWKT(Functions.rotate(lineString, 10, pointOrigin));
    assertEquals(
        "LINESTRING (0 0, -0.8390715290764524 -0.5440211108893698, -0.2950504181870827 -1.383092639965822, 0 0)",
        result);

    lineString = Constructors.geomFromEWKT("LINESTRING (0 0, 1 0, 1 1, 0 0)");
    result = Functions.asEWKT(Functions.rotate(lineString, 10, 0, 0));
    assertEquals(
        "LINESTRING (0 0, -0.8390715290764524 -0.5440211108893698, -0.2950504181870827 -1.383092639965822, 0 0)",
        result);
  }
}
