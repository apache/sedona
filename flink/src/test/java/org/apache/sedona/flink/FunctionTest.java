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
package org.apache.sedona.flink;

import static junit.framework.TestCase.assertNull;
import static org.apache.flink.table.api.Expressions.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.Table;
import org.apache.sedona.flink.expressions.Functions;
import org.apache.sedona.flink.expressions.FunctionsGeoTools;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class FunctionTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  @Test
  public void testArea() {
    Table polygonTable = createPolygonTable(1);
    Table ResultTable =
        polygonTable.select(call(Functions.ST_Area.class.getSimpleName(), $(polygonColNames[0])));
    assertNotNull(first(ResultTable).getField(0));
    double result = (double) first(ResultTable).getField(0);
    assertEquals(1.0, result, 0);
  }

  @Test
  public void testAreaSpheroid() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_AreaSpheroid(ST_GeomFromWKT('Polygon ((34 35, 28 30, 25 34, 34 35))'))");
    Double expected = 201824850811.76245;
    Double actual = (Double) first(tbl).getField(0);
    assertEquals(expected, actual, 0.1);
  }

  @Test
  public void testAzimuth() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_Azimuth(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 1)'))");
    assertEquals(45, ((double) first(pointTable).getField(0)) / (Math.PI * 2) * 360, 0);
  }

  @Test
  public void testBoundary() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POLYGON ((1 1, 0 0, -1 1, 1 1))') AS geom");
    Table boundaryTable =
        polygonTable.select(call(Functions.ST_Boundary.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(boundaryTable).getField(0);
    assertEquals("LINESTRING (1 1, 0 0, -1 1, 1 1)", result.toString());
  }

  @Test
  public void testBuffer() {
    Table pointTable = createPointTable_real(testDataSize);
    Table bufferTable =
        pointTable.select(call(Functions.ST_Buffer.class.getSimpleName(), $(pointColNames[0]), 1));
    Geometry result = (Geometry) first(bufferTable).getField(0);
    assert (result instanceof Polygon);

    String actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ReducePrecision(ST_Buffer(ST_GeomFromWKT('LINESTRING(0 0, 50 70, 100 100)'), 10, false, 'side=left'), 4))"))
                .getField(0);
    String expected =
        "POLYGON ((50 70, 0 0, -8.1373 5.8124, 41.8627 75.8124, 43.2167 77.3476, 44.855 78.5749, 94.855 108.5749, 100 100, 50 70))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ReducePrecision(ST_Buffer(ST_GeomFromWKT('LINESTRING(0 0, 50 70, 70 -3)'), 10, false, 'endcap=square'), 4))"))
                .getField(0);
    expected =
        "POLYGON ((43.2156 77.3465, 44.8523 78.5733, 46.7044 79.4413, 48.6944 79.9144, 50.739 79.9727, 52.7527 79.6137, 54.6512 78.8525, 56.3552 77.7209, 57.7932 76.2663, 58.9052 74.5495, 59.6446 72.6424, 79.6446 -0.3576, 82.2869 -10.0022, 62.9978 -15.2869, 45.9128 47.0733, 8.1373 -5.8124, 2.325 -13.9497, -13.9497 -2.325, 41.8627 75.8124, 43.2156 77.3465))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ReducePrecision(ST_Buffer(ST_Point(100, 90), 200, false, 'quad_segs=4'), 4))"))
                .getField(0);
    expected =
        "POLYGON ((284.7759 13.4633, 241.4214 -51.4214, 176.5367 -94.7759, 100 -110, 23.4633 -94.7759, -41.4214 -51.4214, -84.7759 13.4633, -100 90, -84.7759 166.5367, -41.4214 231.4214, 23.4633 274.7759, 100 290, 176.5367 274.7759, 241.4214 231.4214, 284.7759 166.5367, 300 90, 284.7759 13.4633))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ReducePrecision(ST_Buffer(ST_GeomFromWKT('LINESTRING(0 0, 50 70, 70 -3)'), 10, true, 'endcap=square'), 4))"))
                .getField(0);
    expected =
        "POLYGON ((50 70, 50.0001 70, 70.0001 -3, 70.0001 -3.0001, 69.9999 -3.0001, 50 69.9999, 0.0001 0, 0 -0.0001, -0.0001 0, 49.9999 70, 50 70))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ReducePrecision(ST_Buffer(ST_GeomFromWKT('POLYGON((-120 30, -80 30, -80 50, -120 50, -120 30))'), 200, true, 'quad_segs=4'), 4))"))
                .getField(0);
    expected =
        "POLYGON ((-120.0018 50, -120.0017 50.0004, -120.0013 50.0008, -120.0007 50.0011, -120 50.0012, -80 50.0012, -79.9993 50.0011, -79.9987 50.0008, -79.9983 50.0004, -79.9982 50, -79.9982 30, -79.9983 29.9994, -79.9987 29.9989, -79.9993 29.9986, -80 29.9984, -120 29.9984, -120.0007 29.9986, -120.0013 29.9989, -120.0017 29.9994, -120.0018 30, -120.0018 50))";
    assertEquals(expected, actual);
  }

  @Test
  public void testBestSRID() {
    Table table1 = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (160 40)') AS geom");
    table1 = table1.select(call(Functions.ST_BestSRID.class.getSimpleName(), $("geom")));
    int result = (int) first(table1).getField(0);
    assertEquals(32657, result);

    Table table2 =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING(-91.185 30.4505, -91.187 30.452, -91.189 30.4535)') AS geom");
    table2 = table2.select(call(Functions.ST_BestSRID.class.getSimpleName(), $("geom")));
    result = (int) first(table2).getField(0);
    assertEquals(32615, result);

    Table table3 =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON((-120 30, -80 30, -80 50, -120 50, -120 30))') AS geom");
    table3 = table3.select(call(Functions.ST_BestSRID.class.getSimpleName(), $("geom")));
    result = (int) first(table3).getField(0);
    assertEquals(3395, result);
  }

  @Test
  public void testShiftLongitude() {
    String actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ShiftLongitude(ST_GeomFromWKT('POLYGON((179 10, -179 10, -179 20, 179 20, 179 10))')))"))
                .getField(0);
    String expected = "POLYGON ((179 10, 181 10, 181 20, 179 20, 179 10))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ShiftLongitude(ST_GeomFromWKT('MULTIPOLYGON(((179 10, -179 10, -179 20, 179 20, 179 10)), ((-185 10, -185 20, -175 20, -175 10, -185 10)))')))"))
                .getField(0);
    expected =
        "MULTIPOLYGON (((179 10, 181 10, 181 20, 179 20, 179 10)), ((175 10, 175 20, 185 20, 185 10, 175 10)))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tableEnv.sqlQuery(
                        "SELECT ST_AsText(ST_ShiftLongitude(ST_GeomFromWKT('LINESTRING(179 10, 181 10)')))"))
                .getField(0);
    expected = "LINESTRING (179 10, -179 10)";
    assertEquals(expected, actual);
  }

  @Test
  public void testClosestPoint() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (160 40)') AS g1, ST_GeomFromWKT('POINT (10 10)') as g2");
    table = table.select(call(Functions.ST_ClosestPoint.class.getSimpleName(), $("g1"), $("g2")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POINT (160 40)", result.toString());
  }

  @Test
  public void testCentroid() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromText('POLYGON ((2 2, 0 0, 2 0, 0 2, 2 2))') as geom");
    Table resultTable =
        polygonTable.select(call(Functions.ST_Centroid.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(resultTable).getField(0);
    assertEquals("POINT (1 1)", result.toString());
  }

  @Test
  public void testCollectWithTwoInputs() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (1 2)') AS g1, ST_GeomFromWKT('POINT (-2 3)') as g2");
    Table resultTable =
        pointTable.select(call(Functions.ST_Collect.class.getSimpleName(), $("g1"), $("g2")));
    Geometry result1 = (Geometry) first(resultTable).getField(0);
    assertEquals("MULTIPOINT ((1 2), (-2 3))", result1.toString());

    Table collectionTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (1 2)') AS g1, ST_GeomFromWKT('LINESTRING(1 2, 3 4)') as g2");
    resultTable =
        collectionTable.select(call(Functions.ST_Collect.class.getSimpleName(), $("g1"), $("g2")));
    Geometry result2 = (Geometry) first(resultTable).getField(0);
    assertEquals("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4))", result2.toString());
  }

  @Test
  public void testCollectWithArray() {
    Table lineTable =
        tableEnv.sqlQuery(
            "SELECT array[ST_GeomFromText('LINESTRING(1 2, 3 4)'), ST_GeomFromText('LINESTRING(3 4, 4 5)')] as lines");
    Table resultTable =
        lineTable.select(call(Functions.ST_Collect.class.getSimpleName(), $("lines")));
    Geometry result1 = (Geometry) first(resultTable).getField(0);
    assertEquals("MULTILINESTRING ((1 2, 3 4), (3 4, 4 5))", result1.toString());

    Table collectionTable =
        tableEnv.sqlQuery(
            "SELECT array[ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING(3 4, 4 5)')] as lines");
    resultTable =
        collectionTable.select(call(Functions.ST_Collect.class.getSimpleName(), $("lines")));
    Geometry result2 = (Geometry) first(resultTable).getField(0);
    assertEquals("GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (3 4, 4 5))", result2.toString());
  }

  @Test
  public void testCollectionExtract() {
    Table collectionTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 1, 2 2))') as collection");
    Table resultTable =
        collectionTable.select(
            call(Functions.ST_CollectionExtract.class.getSimpleName(), $("collection")));
    Geometry result = (Geometry) first(resultTable).getField(0);
    assertEquals("MULTILINESTRING ((1 1, 2 2))", result.toString());
  }

  @Test
  public void testConcaveHull() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as geom");
    Table concaveHullPolygonTable =
        polygonTable.select(
            call(Functions.ST_ConcaveHull.class.getSimpleName(), $("geom"), 1.0, true));
    Geometry result = (Geometry) first(concaveHullPolygonTable).getField(0);
    assertEquals("POLYGON ((1 2, 2 2, 3 2, 5 0, 4 0, 1 0, 0 0, 1 2))", result.toString());

    Table polygonTable2 =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') as geom");
    Table concaveHullPolygonTable2 =
        polygonTable2.select(call(Functions.ST_ConcaveHull.class.getSimpleName(), $("geom"), 1.0));
    Geometry result2 = (Geometry) first(concaveHullPolygonTable2).getField(0);
    assertEquals("POLYGON ((0 0, 1 1, 1 0, 0 0))", result2.toString());
  }

  @Test
  public void testConvexHull() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as geom");
    Table concaveHullPolygonTable =
        polygonTable.select(call(Functions.ST_ConvexHull.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(concaveHullPolygonTable).getField(0);
    assertEquals("POLYGON ((0 0, 1 2, 3 2, 5 0, 0 0))", result.toString());
  }

  @Test
  public void testCrossesDateLine() {
    // Test line crossing the Date Line
    Table table1 =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(170 30, -170 30)') AS geom");
    table1 = table1.select(call("ST_CrossesDateLine", $("geom")));
    Boolean actual1 = (Boolean) first(table1).getField(0);
    assertEquals(true, actual1);

    // Test line not crossing the Date Line
    Table table2 =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(-120 30, -130 40)') AS geom");
    table2 = table2.select(call("ST_CrossesDateLine", $("geom")));
    Boolean actual2 = (Boolean) first(table2).getField(0);
    assertEquals(false, actual2);

    // Test polygon crossing the Date Line
    Table table3 =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))') AS geom");
    table3 = table3.select(call("ST_CrossesDateLine", $("geom")));
    Boolean actual3 = (Boolean) first(table3).getField(0);
    assertEquals(true, actual3);

    // Test polygon not crossing the Date Line
    Table table4 =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON((-120 10, -130 10, -130 -10, -120 -10, -120 10))') AS geom");
    table4 = table4.select(call("ST_CrossesDateLine", $("geom")));
    Boolean actual4 = (Boolean) first(table4).getField(0);
    assertEquals(false, actual4);
  }

  @Test
  public void testDifference() {
    Table lineTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING(50 100, 50 200)') AS g1, ST_GeomFromWKT('LINESTRING(50 50, 50 150)') as g2");
    Table resultTable =
        lineTable.select(call(Functions.ST_Difference.class.getSimpleName(), $("g1"), $("g2")));
    Geometry result = (Geometry) first(resultTable).getField(0);
    assertEquals("LINESTRING (50 150, 50 200)", result.toString());
  }

  @Test
  public void testDump() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom");
    table = table.select(call(Functions.ST_Dump.class.getSimpleName(), $("geom")));
    Geometry[] result = (Geometry[]) first(table).getField(0);
    assertEquals("POINT (0 0)", result[0].toString());
    assertEquals("POINT (1 1)", result[1].toString());
  }

  @Test
  public void testDumpPoints() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom");
    table = table.select(call(Functions.ST_DumpPoints.class.getSimpleName(), $("geom")));
    Geometry[] result = (Geometry[]) first(table).getField(0);
    assertEquals("POINT (0 0)", result[0].toString());
    assertEquals("POINT (1 0)", result[1].toString());
  }

  @Test
  public void testEndPoint() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3)') AS geom");
    table = table.select(call(Functions.ST_EndPoint.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POINT (3 3)", result.toString());
  }

  @Test
  public void testEnvelope() {
    Table linestringTable = createLineStringTable(1);
    linestringTable =
        linestringTable.select(
            call(Functions.ST_Envelope.class.getSimpleName(), $(linestringColNames[0])));
    assertEquals(
        "POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))",
        first(linestringTable).getField(0).toString());
  }

  @Test
  public void testExpand() {
    Table baseTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))') as geom");
    String actual =
        (String)
            first(
                    baseTable
                        .select(call(Functions.ST_Expand.class, $("geom"), 10))
                        .as("geom")
                        .select(call(Functions.ST_AsText.class, $("geom"))))
                .getField(0);
    String expected = "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    baseTable
                        .select(call(Functions.ST_Expand.class, $("geom"), 5, 6))
                        .as("geom")
                        .select(call(Functions.ST_AsText.class, $("geom"))))
                .getField(0);
    expected = "POLYGON Z((45 44 1, 45 86 1, 85 86 3, 85 44 3, 45 44 1))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    baseTable
                        .select(call(Functions.ST_Expand.class, $("geom"), 6, 5, -3))
                        .as("geom")
                        .select(call(Functions.ST_AsText.class, $("geom"))))
                .getField(0);
    expected = "POLYGON Z((44 45 4, 44 85 4, 86 85 0, 86 45 0, 44 45 4))";
    assertEquals(expected, actual);
  }

  @Test
  public void testFlipCoordinates() {
    Table pointTable = createPointTable_real(testDataSize);
    Table flippedTable =
        pointTable.select(
            call(Functions.ST_FlipCoordinates.class.getSimpleName(), $(pointColNames[0])));
    Geometry result = (Geometry) first(flippedTable).getField(0);
    assertEquals("POINT (32.01 -117.99)", result.toString());
  }

  @Test
  public void testSTGeometryType() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3)') AS geom");
    table = table.select(call(Functions.ST_GeometryType.class.getSimpleName(), $("geom")));
    String result = (String) first(table).getField(0);
    assertEquals("ST_LineString", result);
  }

  @Test
  public void testTransform() {
    Table pointTable = createPointTable_real(testDataSize);
    Table transformedTable =
        pointTable.select(
            call(
                FunctionsGeoTools.ST_Transform.class.getSimpleName(),
                $(pointColNames[0]),
                "epsg:4326",
                "epsg:3857"));
    String result = first(transformedTable).getField(0).toString();
    assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result);

    pointTable =
        pointTable
            .select(call(Functions.ST_SetSRID.class.getSimpleName(), $(pointColNames[0]), 4326))
            .as(pointColNames[0]);
    transformedTable =
        pointTable
            .select(
                call(
                    FunctionsGeoTools.ST_Transform.class.getSimpleName(),
                    $(pointColNames[0]),
                    "epsg:3857"))
            .as(pointColNames[0])
            .select(
                call(Functions.ST_ReducePrecision.class.getSimpleName(), $(pointColNames[0]), 2));
    result = first(transformedTable).getField(0).toString();
    assertEquals("POINT (-13134586.72 3764623.35)", result);
  }

  @Test
  public void testUnion() {
    Table polyTable =
        tableEnv.sqlQuery(
            "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') as b");
    String actual =
        first(polyTable.select(call(Functions.ST_Union.class.getSimpleName(), $("a"), $("b"))))
            .getField(0)
            .toString();
    String expected = "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))";
    assertEquals(expected, actual);
  }

  @Test
  public void testUnaryUnion() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))') AS geom");
    String actual =
        first(table.select(call(Functions.ST_UnaryUnion.class.getSimpleName(), $("geom"))))
            .getField(0)
            .toString();
    String expected = "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))";
    assertEquals(expected, actual);
  }

  @Test
  public void testUnionArrayVariant() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ARRAY[ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))')] as polys");
    String actual =
        first(polyTable.select(call(Functions.ST_Union.class.getSimpleName(), $("polys"))))
            .getField(0)
            .toString();
    String expected = "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))";
    assertEquals(expected, actual);
  }

  @Test
  public void testTransformWKT() throws FactoryException {
    Table pointTable = createPointTable_real(testDataSize);

    CoordinateReferenceSystem CRS_SRC = CRS.decode("epsg:4326", true);
    CoordinateReferenceSystem CRS_TGT = CRS.decode("epsg:3857", true);

    String SRC_WKT = CRS_SRC.toWKT();
    String TGT_WKT = CRS_TGT.toWKT();

    Table transformedTable_SRC =
        pointTable.select(
            call(
                FunctionsGeoTools.ST_Transform.class.getSimpleName(),
                $(pointColNames[0]),
                SRC_WKT,
                "epsg:3857"));
    String result_SRC = first(transformedTable_SRC).getField(0).toString();
    assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_SRC);

    Table transformedTable_TGT =
        pointTable.select(
            call(
                FunctionsGeoTools.ST_Transform.class.getSimpleName(),
                $(pointColNames[0]),
                "epsg:4326",
                TGT_WKT));
    String result_TGT = first(transformedTable_TGT).getField(0).toString();
    assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_TGT);

    Table transformedTable_SRC_TGT =
        pointTable.select(
            call(
                FunctionsGeoTools.ST_Transform.class.getSimpleName(),
                $(pointColNames[0]),
                SRC_WKT,
                TGT_WKT));
    String result_SRC_TGT = first(transformedTable_SRC_TGT).getField(0).toString();
    assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_SRC_TGT);

    Table transformedTable_SRC_TGT_lenient =
        pointTable.select(
            call(
                FunctionsGeoTools.ST_Transform.class.getSimpleName(),
                $(pointColNames[0]),
                SRC_WKT,
                TGT_WKT,
                false));
    String result_SRC_TGT_lenient = first(transformedTable_SRC_TGT_lenient).getField(0).toString();
    assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_SRC_TGT_lenient);
  }

  @Test
  public void testDimension() {
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_Dimension(ST_GeomFromWKT('GEOMETRYCOLLECTION EMPTY'))");
    assertEquals(0, first(pointTable).getField(0));

    pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_Dimension(ST_GeomFromWKT('GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2))), MULTIPOINT(6 6, 7 7, 8 8))'))");
    assertEquals(2, first(pointTable).getField(0));
  }

  @Test
  public void testDistance() {
    Table pointTable = createPointTable(testDataSize);
    pointTable =
        pointTable.select(
            call(
                Functions.ST_Distance.class.getSimpleName(),
                $(pointColNames[0]),
                call("ST_GeomFromWKT", "POINT (0 0)")));
    assertEquals(0.0, first(pointTable).getField(0));
  }

  @Test
  public void testDistanceSpheroid() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_DistanceSpheroid(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'))");
    Double expected = 544430.9411996207;
    Double actual = (Double) first(tbl).getField(0);
    assertEquals(expected, actual, 0.1);
  }

  @Test
  public void testDistanceSphere() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_DistanceSphere(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'))");
    Double expected = 543796.9506134904;
    Double actual = (Double) first(tbl).getField(0);
    assertEquals(expected, actual, 0.1);
  }

  @Test
  public void testDistanceSphereWithRadius() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_DistanceSphere(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'), 6378137.0)");
    Double expected = 544405.4459192449;
    Double actual = (Double) first(tbl).getField(0);
    assertEquals(expected, actual, 0.1);
  }

  @Test
  public void test3dDistance() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_3DDistance(ST_GeomFromWKT('POINT (0 0 0)'), ST_GeomFromWKT('POINT (1 1 1)'))");
    assertEquals(Math.sqrt(3), first(pointTable).getField(0));
  }

  @Test
  public void testIntersection() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (0 0)') AS g1, ST_GeomFromWKT('LINESTRING ( 0 0, 0 2 )') as g2");
    table = table.select(call(Functions.ST_Intersection.class.getSimpleName(), $("g1"), $("g2")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POINT (0 0)", result.toString());
  }

  @Test
  public void testLength() {
    Table polygonTable = createPolygonTable(1);
    Table resultTable =
        polygonTable.select(call(Functions.ST_Length.class.getSimpleName(), $(polygonColNames[0])));
    assertNotNull(first(resultTable).getField(0));
    double result = (double) first(resultTable).getField(0);
    assertEquals(4, result, 0);
  }

  @Test
  public void testLength2D() {
    Table polygonTable = createPolygonTable(1);
    Table resultTable =
        polygonTable.select(
            call(Functions.ST_Length2D.class.getSimpleName(), $(polygonColNames[0])));
    assertNotNull(first(resultTable).getField(0));
    double result = (double) first(resultTable).getField(0);
    assertEquals(4, result, 0);
  }

  @Test
  public void testLengthSpheroid() {
    Table tbl =
        tableEnv.sqlQuery("SELECT ST_LengthSpheroid(ST_GeomFromWKT('Polygon ((0 0, 90 0, 0 0))'))");
    Double expected = 20037508.342789244;
    Double actual = (Double) first(tbl).getField(0);
    assertEquals(expected, actual, 0.1);
  }

  @Test
  public void testLocateAlong() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTILINESTRING M((1 2 3, 3 4 2, 9 4 3),(1 2 3, 5 4 5))') AS geom");
    String actual =
        (String)
            first(
                    tbl.select(call(Functions.ST_LocateAlong.class.getSimpleName(), $("geom"), 2))
                        .as("geom")
                        .select(call(Functions.ST_AsEWKT.class.getSimpleName(), $("geom"))))
                .getField(0);
    String expected = "MULTIPOINT M((3 4 2))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tbl.select(
                            call(Functions.ST_LocateAlong.class.getSimpleName(), $("geom"), 2, -3))
                        .as("geom")
                        .select(call(Functions.ST_AsEWKT.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected = "MULTIPOINT M((5.121320343559642 1.8786796564403572 2), (3 1 2))";
    assertEquals(expected, actual);
  }

  @Test
  public void testLongestLine() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom1");
    String actual =
        (String)
            first(
                    tbl.select(
                            call(
                                Functions.ST_LongestLine.class.getSimpleName(),
                                $("geom1"),
                                $("geom1")))
                        .as("geom")
                        .select(call(Functions.ST_AsText.class.getSimpleName(), $("geom"))))
                .getField(0);
    String expected = "LINESTRING (180 180, 20 50)";
    assertEquals(expected, actual);
  }

  @Test
  public void testLineInterpolatePoint() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line");
    table =
        table.select(call(Functions.ST_LineInterpolatePoint.class.getSimpleName(), $("line"), 0.5));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POINT (1 0)", result.toString());
  }

  @Test
  public void testLineLocatePoint() {
    Table resultTable =
        tableEnv.sqlQuery(
            "SELECT ST_LineLocatePoint(ST_GeomFromWKT('LINESTRING (0 2, 1 1, 2 0)'), ST_GeomFromWKT('POINT (0 0)'))");
    Double result = (Double) first(resultTable).getField(0);
    Double expectedResult = 0.5;
    assertEquals(expectedResult, result, 0.1);
  }

  @Test
  public void testYMax() {
    Table polygonTable = createPolygonTable(1);
    Table ResultTable =
        polygonTable.select(call(Functions.ST_YMax.class.getSimpleName(), $(polygonColNames[0])));
    assertNotNull(first(ResultTable).getField(0));
    double result = (double) first(ResultTable).getField(0);
    assertEquals(0.5, result, 0);
  }

  @Test
  public void testYMin() {
    Table polygonTable = createPolygonTable(1);
    Table ResultTable =
        polygonTable.select(call(Functions.ST_YMin.class.getSimpleName(), $(polygonColNames[0])));
    assertNotNull(first(ResultTable).getField(0));
    double result = (double) first(ResultTable).getField(0);
    assertEquals(-0.5, result, 0);
  }

  @Test
  public void testGeomToGeoHash() {
    Table pointTable = createPointTable(testDataSize);
    pointTable = pointTable.select(call("ST_GeoHash", $(pointColNames[0]), 5));
    assertEquals(first(pointTable).getField(0), "s0000");
  }

  @Test
  public void testGeometryType() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))");
    assertEquals("LINESTRING", first(pointTable).getField(0));

    pointTable = tableEnv.sqlQuery("SELECT GeometryType(ST_GeomFromText('POINTM(2.0 3.5 10.2)'))");
    assertEquals("POINTM", first(pointTable).getField(0));
  }

  @Test
  public void testPointOnSurface() {
    Table pointTable = createPointTable_real(testDataSize);
    Table surfaceTable =
        pointTable.select(
            call(Functions.ST_PointOnSurface.class.getSimpleName(), $(pointColNames[0])));
    Geometry result = (Geometry) first(surfaceTable).getField(0);
    assertEquals("POINT (-117.99 32.01)", result.toString());
  }

  @Test
  public void testReducePrecision() {
    Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromText('POINT(0.12 0.23)') AS geom");
    Table resultTable =
        polygonTable.select(call(Functions.ST_ReducePrecision.class.getSimpleName(), $("geom"), 1));
    Geometry point = (Geometry) first(resultTable).getField(0);
    assertEquals("POINT (0.1 0.2)", point.toString());
  }

  @Test
  public void testReverse() {
    Table polygonTable = createPolygonTable(1);
    Table ReversedTable =
        polygonTable.select(
            call(Functions.ST_Reverse.class.getSimpleName(), $(polygonColNames[0])));
    Geometry result = (Geometry) first(ReversedTable).getField(0);
    assertEquals(
        "POLYGON ((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))", result.toString());
  }

  @Test
  public void testGeometryN() {
    Table collectionTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))') AS collection");
    Table resultTable =
        collectionTable.select(
            call(Functions.ST_GeometryN.class.getSimpleName(), $("collection"), 1));
    Point point = (Point) first(resultTable).getField(0);
    assertEquals("POINT (30 30)", point.toString());
  }

  @Test
  public void testInteriorRingN() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('POLYGON((7 9,8 7,11 6,15 8,16 6,17 7,17 10,18 12,17 14,15 15,11 15,10 13,9 12,7 9),(9 9,10 10,11 11,11 10,10 8,9 9),(12 14,15 14,13 11,12 14))') AS polygon");
    Table resultTable =
        polygonTable.select(
            call(Functions.ST_InteriorRingN.class.getSimpleName(), $("polygon"), 1));
    LineString lineString = (LineString) first(resultTable).getField(0);
    assertEquals("LINESTRING (12 14, 15 14, 13 11, 12 14)", lineString.toString());
  }

  @Test
  public void testPointN_positiveN() {
    int n = 1;
    Table polygonTable = createPolygonTable(1);
    Table linestringTable =
        polygonTable.select(
            call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
    Table pointTable =
        linestringTable.select(call(Functions.ST_PointN.class.getSimpleName(), $("_c0"), n));
    Point point = (Point) first(pointTable).getField(0);
    assertNotNull(point);
    assertEquals("POINT (-0.5 -0.5)", point.toString());
  }

  @Test
  public void testPointN_negativeN() {
    int n = -3;
    Table polygonTable = createPolygonTable(1);
    Table linestringTable =
        polygonTable.select(
            call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
    Table pointTable =
        linestringTable.select(call(Functions.ST_PointN.class.getSimpleName(), $("_c0"), n));
    Point point = (Point) first(pointTable).getField(0);
    assertNotNull(point);
    assertEquals("POINT (0.5 0.5)", point.toString());
  }

  @Test
  public void testNPoints() {
    Table polygonTable = createPolygonTable(1);
    Table resultTable =
        polygonTable.select(
            call(Functions.ST_NPoints.class.getSimpleName(), $(polygonColNames[0])));
    assertEquals(5, first(resultTable).getField(0));
  }

  @Test
  public void testNumGeometries() {
    Table collectionTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))') AS collection");
    Table resultTable =
        collectionTable.select(
            call(Functions.ST_NumGeometries.class.getSimpleName(), $("collection")));
    assertEquals(3, first(resultTable).getField(0));
  }

  @Test
  public void testNumInteriorRings() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('POLYGON((7 9,8 7,11 6,15 8,16 6,17 7,17 10,18 12,17 14,15 15,11 15,10 13,9 12,7 9),(9 9,10 10,11 11,11 10,10 8,9 9),(12 14,15 14,13 11,12 14))') AS polygon");
    Table resultTable =
        polygonTable.select(
            call(Functions.ST_NumInteriorRings.class.getSimpleName(), $("polygon")));
    assertEquals(2, first(resultTable).getField(0));
  }

  @Test
  public void testNumInteriorRing() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('POLYGON((7 9,8 7,11 6,15 8,16 6,17 7,17 10,18 12,17 14,15 15,11 15,10 13,9 12,7 9),(9 9,10 10,11 11,11 10,10 8,9 9),(12 14,15 14,13 11,12 14))') AS polygon");
    Table resultTable =
        polygonTable.select(call(Functions.ST_NumInteriorRing.class.getSimpleName(), $("polygon")));
    assertEquals(2, first(resultTable).getField(0));
  }

  @Test
  public void testExteriorRing() {
    Table polygonTable = createPolygonTable(1);
    Table linearRingTable =
        polygonTable.select(
            call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
    LineString lineString = (LineString) first(linearRingTable).getField(0);
    assertNotNull(lineString);
    Assert.assertEquals(
        "LINESTRING (-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5)", lineString.toString());
  }

  @Test
  public void testAsEWKT() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(call(Functions.ST_AsEWKT.class.getSimpleName(), $(polygonColNames[0])));
    String result = (String) first(polygonTable).getField(0);
    assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result);
  }

  @Test
  public void testAsText() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(call(Functions.ST_AsText.class.getSimpleName(), $(polygonColNames[0])));
    String result = (String) first(polygonTable).getField(0);
    assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result);
  }

  @Test
  public void testAsEWKB() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(call(Functions.ST_AsEWKB.class.getSimpleName(), $(polygonColNames[0])));
    String result = Hex.encodeHexString((byte[]) first(polygonTable).getField(0));
    assertEquals(
        "01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf",
        result);
  }

  @Test
  public void testAsHEXEWKB() {
    Table pointTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1 2)') AS point");
    String result =
        (String)
            first(
                    pointTable.select(
                        call(Functions.ST_AsHEXEWKB.class.getSimpleName(), $("point"), "XDR")))
                .getField(0);
    assertEquals("00000000013FF00000000000004000000000000000", result);

    result =
        (String)
            first(pointTable.select(call(Functions.ST_AsHEXEWKB.class.getSimpleName(), $("point"))))
                .getField(0);
    assertEquals("0101000000000000000000F03F0000000000000040", result);
  }

  @Test
  public void testAsBinary() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_AsBinary.class.getSimpleName(), $(polygonColNames[0])));
    String result = Hex.encodeHexString((byte[]) first(polygonTable).getField(0));
    assertEquals(
        "01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf",
        result);
  }

  @Test
  public void testAsGML() throws Exception {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(call(Functions.ST_AsGML.class.getSimpleName(), $(polygonColNames[0])));
    String result = (String) first(polygonTable).getField(0);
    String expected =
        "<gml:Polygon>\n"
            + "  <gml:outerBoundaryIs>\n"
            + "    <gml:LinearRing>\n"
            + "      <gml:coordinates>\n"
            + "        -0.5,-0.5 -0.5,0.5 0.5,0.5 0.5,-0.5 -0.5,-0.5 \n"
            + "      </gml:coordinates>\n"
            + "    </gml:LinearRing>\n"
            + "  </gml:outerBoundaryIs>\n"
            + "</gml:Polygon>\n";
    assertEquals(expected, result);
  }

  @Test
  public void testAsKML() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(call(Functions.ST_AsKML.class.getSimpleName(), $(polygonColNames[0])));
    String result = (String) first(polygonTable).getField(0);
    String expected =
        "<Polygon>\n"
            + "  <outerBoundaryIs>\n"
            + "  <LinearRing>\n"
            + "    <coordinates>-0.5,-0.5 -0.5,0.5 0.5,0.5 0.5,-0.5 -0.5,-0.5</coordinates>\n"
            + "  </LinearRing>\n"
            + "  </outerBoundaryIs>\n"
            + "</Polygon>\n";
    assertEquals(expected, result);
  }

  @Test
  public void testGeoJSON() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_AsGeoJSON.class.getSimpleName(), $(polygonColNames[0])));
    String result = (String) first(polygonTable).getField(0);
    assertEquals(
        "{\"type\":\"Polygon\",\"coordinates\":[[[-0.5,-0.5],[-0.5,0.5],[0.5,0.5],[0.5,-0.5],[-0.5,-0.5]]]}",
        result);

    polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_AsGeoJSON.class.getSimpleName(), $(polygonColNames[0]), "Feature"));
    result = (String) first(polygonTable).getField(0);
    assertEquals(
        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-0.5,-0.5],[-0.5,0.5],[0.5,0.5],[0.5,-0.5],[-0.5,-0.5]]]},\"properties\":{}}",
        result);

    polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(
            call(
                Functions.ST_AsGeoJSON.class.getSimpleName(),
                $(polygonColNames[0]),
                "FeatureCollection"));
    result = (String) first(polygonTable).getField(0);
    assertEquals(
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-0.5,-0.5],[-0.5,0.5],[0.5,0.5],[0.5,-0.5],[-0.5,-0.5]]]},\"properties\":{}}]}",
        result);
  }

  @Test
  public void testForce2D() {
    Table polygonTable = createPolygonTable(1);
    Table Forced2DTable =
        polygonTable.select(
            call(Functions.ST_Force_2D.class.getSimpleName(), $(polygonColNames[0])));
    Geometry result = (Geometry) first(Forced2DTable).getField(0);
    assertEquals(
        "POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result.toString());
  }

  @Test
  public void testIsEmpty() {
    Table polygonTable = createPolygonTable(testDataSize);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_IsEmpty.class.getSimpleName(), $(polygonColNames[0])));
    boolean result = (boolean) first(polygonTable).getField(0);
    assertEquals(false, result);
  }

  @Test
  public void testX() {
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1.23 4.56 7.89)') AS " + pointColNames[0]);
    pointTable = pointTable.select(call(Functions.ST_X.class.getSimpleName(), $(pointColNames[0])));
    assertEquals(1.23, first(pointTable).getField(0));
  }

  @Test
  public void testY() {
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1.23 4.56 7.89)') AS " + pointColNames[0]);
    pointTable = pointTable.select(call(Functions.ST_Y.class.getSimpleName(), $(pointColNames[0])));
    assertEquals(4.56, first(pointTable).getField(0));
  }

  @Test
  public void testZ() {
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1.23 4.56 7.89)') AS " + pointColNames[0]);
    pointTable = pointTable.select(call(Functions.ST_Z.class.getSimpleName(), $(pointColNames[0])));
    assertEquals(7.89, first(pointTable).getField(0));
  }

  @Test
  public void testZmflag() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1 2)') AS geom");
    int actual =
        (int)
            first(table.select(call(Functions.ST_Zmflag.class.getSimpleName(), $("geom"))))
                .getField(0);
    assertEquals(0, actual);

    table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (1 2 3, 4 5 6)') AS geom");
    actual =
        (int)
            first(table.select(call(Functions.ST_Zmflag.class.getSimpleName(), $("geom"))))
                .getField(0);
    assertEquals(2, actual);

    table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON M((1 2 3, 3 4 3, 5 6 3, 3 4 3, 1 2 3))') AS geom");
    actual =
        (int)
            first(table.select(call(Functions.ST_Zmflag.class.getSimpleName(), $("geom"))))
                .getField(0);
    assertEquals(1, actual);

    table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTIPOLYGON ZM (((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1)), ((15 5 3 1, 20 10 6 2, 10 10 7 3, 15 5 3 1)))') AS geom");
    actual =
        (int)
            first(table.select(call(Functions.ST_Zmflag.class.getSimpleName(), $("geom"))))
                .getField(0);
    assertEquals(3, actual);
  }

  @Test
  public void testHasZ() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))') as poly");
    boolean actual =
        (boolean)
            first(polyTable.select(call(Functions.ST_HasZ.class.getSimpleName(), $("poly"))))
                .getField(0);
    assertTrue(actual);
  }

  @Test
  public void testHasM() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))') as poly");
    boolean actual =
        (boolean)
            first(polyTable.select(call(Functions.ST_HasM.class.getSimpleName(), $("poly"))))
                .getField(0);
    assertTrue(actual);
  }

  @Test
  public void testM() {
    Table pointTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT ZM(1 2 3 4)') AS point");
    double actual =
        (double)
            first(pointTable.select(call(Functions.ST_M.class.getSimpleName(), $("point"))))
                .getField(0);
    assertEquals(4, actual, FP_TOLERANCE);
  }

  @Test
  public void testMMin() {
    Table lineTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') AS line");
    double actual =
        (double)
            first(lineTable.select(call(Functions.ST_MMin.class.getSimpleName(), $("line"))))
                .getField(0);
    assertEquals(-1.0, actual, FP_TOLERANCE);

    lineTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line");
    Double actualNull =
        (Double)
            first(lineTable.select(call(Functions.ST_MMin.class.getSimpleName(), $("line"))))
                .getField(0);
    assertNull(actualNull);
  }

  @Test
  public void testMMax() {
    Table lineTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') AS line");
    double actual =
        (double)
            first(lineTable.select(call(Functions.ST_MMax.class.getSimpleName(), $("line"))))
                .getField(0);
    assertEquals(3, actual, FP_TOLERANCE);

    lineTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line");
    Double actualNull =
        (Double)
            first(lineTable.select(call(Functions.ST_MMax.class.getSimpleName(), $("line"))))
                .getField(0);
    assertNull(actualNull);
  }

  @Test
  public void testZMax() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING(1 3 4, 5 6 7)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(call(Functions.ST_ZMax.class.getSimpleName(), $(polygonColNames[0])));
    double result = (double) first(polygonTable).getField(0);
    assertEquals(7.0, result, 0);
  }

  @Test
  public void testZMaxWithNoZCoordinate() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 3, 5 6)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(call(Functions.ST_ZMax.class.getSimpleName(), $(polygonColNames[0])));
    assertNull(first(polygonTable).getField(0));
  }

  @Test
  public void testZMin() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING(1 3 4, 5 6 7)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(call(Functions.ST_ZMin.class.getSimpleName(), $(polygonColNames[0])));
    double result = (double) first(polygonTable).getField(0);
    assertEquals(4.0, result, 0);
  }

  @Test
  public void testZMinWithNoZCoordinate() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 3, 5 6)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(call(Functions.ST_ZMin.class.getSimpleName(), $(polygonColNames[0])));
    assertNull(first(polygonTable).getField(0));
  }

  @Test
  public void testNDimsFor2D() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(1 1)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0])));
    int result = (int) first(polygonTable).getField(0);
    assertEquals(2, result, 0);
  }

  @Test
  public void testNDims() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(1 1 2)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0])));
    int result = (int) first(polygonTable).getField(0);
    assertEquals(3, result, 0);
  }

  @Test
  public void testNDimsForMCoordinate() {
    Object result =
        first(tableEnv.sqlQuery("SELECT ST_NDims(ST_GeomFromWKT('POINT M (1 2 3)'))")).getField(0);
    assertEquals(result, 3);
    result =
        first(tableEnv.sqlQuery("SELECT ST_NDims(ST_GeomFromWKT('POINT ZM (1 2 3 4)'))"))
            .getField(0);
    assertEquals(result, 4);
  }

  @Test
  public void testXMax() {
    Table polygonTable = createPolygonTable(1);
    Table MaxTable =
        polygonTable.select(call(Functions.ST_XMax.class.getSimpleName(), $(polygonColNames[0])));
    double result = (double) first(MaxTable).getField(0);
    assertEquals(0.5, result, 0);
  }

  @Test
  public void testXMin() {
    Table polygonTable = createPolygonTable(1);
    Table MinTable =
        polygonTable.select(call(Functions.ST_XMin.class.getSimpleName(), $(polygonColNames[0])));
    double result = (double) first(MinTable).getField(0);
    assertEquals(-0.5, result, 0);
  }

  @Test
  public void testBuildArea() {
    Table polygonTable = createPolygonTable(1);
    Table arealGeomTable =
        polygonTable.select(
            call(Functions.ST_BuildArea.class.getSimpleName(), $(polygonColNames[0])));
    Geometry result = (Geometry) first(arealGeomTable).getField(0);
    assertEquals(
        "POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result.toString());
  }

  @Test
  public void testSetSRID() {
    Table polygonTable = createPolygonTable(1);
    polygonTable =
        polygonTable
            .select(call(Functions.ST_SetSRID.class.getSimpleName(), $(polygonColNames[0]), 3021))
            .select(call(Functions.ST_SRID.class.getSimpleName(), $("_c0")));
    int result = (int) first(polygonTable).getField(0);
    assertEquals(3021, result);
  }

  @Test
  public void testSRID() {
    Table polygonTable = createPolygonTable(1);
    polygonTable =
        polygonTable.select(call(Functions.ST_SRID.class.getSimpleName(), $(polygonColNames[0])));
    int result = (int) first(polygonTable).getField(0);
    assertEquals(0, result);
  }

  @Test
  public void testIsClosedForOpen() {
    Table linestringTable = createLineStringTable(1);
    linestringTable =
        linestringTable.select(
            call(Functions.ST_IsClosed.class.getSimpleName(), $(linestringColNames[0])));
    assertFalse((boolean) first(linestringTable).getField(0));
  }

  @Test
  public void testIsClosedForClosed() {
    Table polygonTable = createPolygonTable(1);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_IsClosed.class.getSimpleName(), $(polygonColNames[0])));
    assertTrue((boolean) first(polygonTable).getField(0));
  }

  @Test
  public void testIsRingForRing() {
    Table polygonTable = createPolygonTable(1);
    Table linestringTable =
        polygonTable.select(
            call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
    linestringTable =
        linestringTable.select(call(Functions.ST_IsRing.class.getSimpleName(), $("_c0")));
    assertTrue((boolean) first(linestringTable).getField(0));
  }

  @Test
  public void testIsRingForNonRing() {
    Table linestringTable = createLineStringTable(1);
    linestringTable =
        linestringTable.select(
            call(Functions.ST_IsClosed.class.getSimpleName(), $(linestringColNames[0])));
    assertFalse((boolean) first(linestringTable).getField(0));
  }

  @Test
  public void testIsSimple() {
    Table polygonTable = createPolygonTable(1);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_IsSimple.class.getSimpleName(), $(polygonColNames[0])));
    assertTrue((boolean) first(polygonTable).getField(0));
  }

  @Test
  public void testIsValid() {
    Table polygonTable = createPolygonTable(1);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_IsValid.class.getSimpleName(), $(polygonColNames[0])));
    assertTrue((boolean) first(polygonTable).getField(0));

    final int OGC_SFS_VALIDITY = 0;
    final int ESRI_VALIDITY = 1;

    // Geometry that is invalid under both OGC and ESRI standards
    String selfTouchingWKT = "POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))";
    Table specialCaseTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromText('" + selfTouchingWKT + "') AS geom");

    // Test with OGC flag
    Table ogcValidityTable =
        specialCaseTable.select(call("ST_IsValid", $("geom"), OGC_SFS_VALIDITY));
    java.lang.Boolean ogcValidity = (java.lang.Boolean) first(ogcValidityTable).getField(0);
    assertEquals(false, ogcValidity); // Expecting a self-intersection error as per OGC standards

    // Test with ESRI flag
    Table esriValidityTable = specialCaseTable.select(call("ST_IsValid", $("geom"), ESRI_VALIDITY));
    java.lang.Boolean esriValidity = (java.lang.Boolean) first(esriValidityTable).getField(0);
    assertEquals(
        false,
        esriValidity); // Expecting an error related to interior disconnection as per ESRI standards
  }

  @Test
  public void testNormalize() {
    Table polygonTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('POLYGON((0 1, 1 1, 1 0, 0 0, 0 1))') AS polygon");
    polygonTable =
        polygonTable.select(call(Functions.ST_Normalize.class.getSimpleName(), $("polygon")));
    Geometry result = (Geometry) first(polygonTable).getField(0);
    assertEquals("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", result.toString());
  }

  @Test
  public void testAddMeasure() {
    Table baseTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING (1 1, 2 2, 2 2, 3 3)') as line, "
                + "ST_GeomFromWKT('MULTILINESTRING M((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))') as mline");
    String actual =
        (String)
            first(
                    baseTable
                        .select(
                            call(
                                Functions.ST_AddMeasure.class.getSimpleName(),
                                $("line"),
                                1.0,
                                70.0))
                        .as("geom")
                        .select(call(Functions.ST_AsText.class.getSimpleName(), $("geom"))))
                .getField(0);
    String expected = "LINESTRING M(1 1 1, 2 2 35.5, 2 2 35.5, 3 3 70)";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    baseTable
                        .select(
                            call(
                                Functions.ST_AddMeasure.class.getSimpleName(),
                                $("mline"),
                                10.0,
                                70.0))
                        .as("geom")
                        .select(call(Functions.ST_AsText.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected = "MULTILINESTRING M((1 0 10, 2 0 20, 4 0 40), (1 0 40, 2 0 50, 4 0 70))";
    assertEquals(expected, actual);
  }

  @Test
  public void testAddPoint() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_AddPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('POINT (2 2)'))");
    assertEquals("LINESTRING (0 0, 1 1, 2 2)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testAddPointWithIndex() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_AddPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('POINT (2 2)'), 1)");
    assertEquals("LINESTRING (0 0, 2 2, 1 1)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testRemovePoint() {
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_RemovePoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'))");
    assertEquals("LINESTRING (0 0, 1 1)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testRemovePointWithIndex() {
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_RemovePoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), 1)");
    assertEquals("LINESTRING (0 0, 2 2)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testSetPoint() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_SetPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), 0, ST_GeomFromWKT('POINT (3 3)'))");
    assertEquals("LINESTRING (3 3, 1 1, 2 2)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testSetPointWithNegativeIndex() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_SetPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), -1, ST_GeomFromWKT('POINT (3 3)'))");
    assertEquals("LINESTRING (0 0, 1 1, 3 3)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testLineFromMultiPoint() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_LineFromMultiPoint(ST_GeomFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))'))");
    assertEquals(
        "LINESTRING (10 40, 40 30, 20 20, 30 10)", first(pointTable).getField(0).toString());
  }

  @Test
  public void testLineMerge() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTILINESTRING((10 160, 60 120), (120 140, 60 120), (120 140, 180 120))') AS multiline");
    table = table.select(call(Functions.ST_LineMerge.class.getSimpleName(), $("multiline")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("LINESTRING (10 160, 60 120, 120 140, 180 120)", result.toString());
  }

  @Test
  public void testLineSubString() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line");
    table =
        table.select(call(Functions.ST_LineSubstring.class.getSimpleName(), $("line"), 0.5, 1.0));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("LINESTRING (1 0, 2 0)", result.toString());
  }

  @Test
  public void testMakeLine() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (0 0)') AS point1, ST_GeomFromWKT('POINT (1 1)') AS point2");
    table =
        table.select(call(Functions.ST_MakeLine.class.getSimpleName(), $("point1"), $("point2")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("LINESTRING (0 0, 1 1)", result.toString());

    table = tableEnv.sqlQuery("SELECT ST_MakeLine(ARRAY[ST_Point(2, 2), ST_Point(3, 3)]) AS line");
    result = (Geometry) first(table).getField(0);
    assertEquals("LINESTRING (2 2, 3 3)", result.toString());
  }

  @Test
  public void testPoints() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 1, 5 1, 5 0, 1 0, 0 0))') AS polygon");
    table = table.select(call(Functions.ST_Points.class.getSimpleName(), $("polygon")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("MULTIPOINT ((0 0), (1 1), (5 1), (5 0), (1 0), (0 0))", result.toString());
  }

  @Test
  public void testPolygon() {
    Table table =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS line");
    table = table.select(call(Functions.ST_Polygon.class.getSimpleName(), $("line"), 4236));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 0))", result.toString());
    assertEquals(4236, result.getSRID());
  }

  @Test
  public void testPolygonize() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromEWKT('GEOMETRYCOLLECTION (LINESTRING (180 40, 30 20, 20 90), LINESTRING (180 40, 160 160), LINESTRING (80 60, 120 130, 150 80), LINESTRING (80 60, 150 80), LINESTRING (20 90, 70 70, 80 130), LINESTRING (80 130, 160 160), LINESTRING (20 90, 20 160, 70 190), LINESTRING (70 190, 80 130), LINESTRING (70 190, 160 160))') AS geom");
    table = table.select(call(Functions.ST_Polygonize.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(table).getField(0);
    result.normalize();
    String expected =
        "GEOMETRYCOLLECTION (POLYGON ((20 90, 20 160, 70 190, 80 130, 70 70, 20 90)), POLYGON ((20 90, 70 70, 80 130, 160 160, 180 40, 30 20, 20 90), (80 60, 150 80, 120 130, 80 60)), POLYGON ((70 190, 160 160, 80 130, 70 190)), POLYGON ((80 60, 120 130, 150 80, 80 60)))";
    assertEquals(expected, result.toString());
  }

  @Test
  public void testMakePolygon() {
    Table table =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS line");
    table = table.select(call(Functions.ST_MakePolygon.class.getSimpleName(), $("line")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 0))", result.toString());
  }

  @Test
  public void testMakePolygonWithHoles() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS line,"
                + "array[ST_GeomFromText('LINESTRING (0.5 0.1, 0.7 0.1, 0.7 0.3, 0.5 0.1)')] AS holes");
    table =
        table.select(call(Functions.ST_MakePolygon.class.getSimpleName(), $("line"), $("holes")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals(
        "POLYGON ((0 0, 1 0, 1 1, 0 0), (0.5 0.1, 0.7 0.1, 0.7 0.3, 0.5 0.1))", result.toString());
  }

  @Test
  public void testMakeValid() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS polygon");
    table = table.select(call(Functions.ST_MakeValid.class.getSimpleName(), $("polygon")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals(
        "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))", result.toString());
  }

  @Test
  public void testMaxDistance() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom");
    Double actual =
        (Double)
            first(
                    tbl.select(
                        call(Functions.ST_MaxDistance.class.getSimpleName(), $("geom"), $("geom"))))
                .getField(0);
    Double expected = 206.15528128088303;
    assertEquals(expected, actual);
  }

  @Test
  public void testMinimumClearance() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom");
    Double actual =
        (Double)
            first(
                    table.select(
                        call(Functions.ST_MinimumClearance.class.getSimpleName(), $("geom"))))
                .getField(0);
    Double expected = 0.5;
    assertEquals(expected, actual);
  }

  @Test
  public void testMinimumClearanceLine() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom");
    String actual =
        ((Geometry)
                first(
                        table.select(
                            call(
                                Functions.ST_MinimumClearanceLine.class.getSimpleName(),
                                $("geom"))))
                    .getField(0))
            .toText();
    String expected = "LINESTRING (64.5 16, 65 16)";
    assertEquals(expected, actual);
  }

  @Test
  public void testMinimumBoundingCircle() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom");
    table = table.select(call(Functions.ST_MinimumBoundingCircle.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(table).getField(0);
    Integer actual = result.getCoordinates().length;
    Integer expected = BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6 * 4 + 1;
    assertEquals(actual, expected);
  }

  @Test
  public void testMinimumBoundingCircleWithQuadrantSegments() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom");
    table =
        table.select(call(Functions.ST_MinimumBoundingCircle.class.getSimpleName(), $("geom"), 2));
    Geometry result = (Geometry) first(table).getField(0);
    Integer actual = result.getCoordinates().length;
    Integer expected = 2 * 4 + 1;
    assertEquals(actual, expected);
  }

  @Test
  public void testMinimumBoundingRadius() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom");
    table = table.select(call(Functions.ST_MinimumBoundingRadius.class.getSimpleName(), $("geom")));
    Pair<Geometry, Double> result = (Pair<Geometry, Double>) first(table).getField(0);
    assertEquals("POINT (0.5 0)", result.getLeft().toString());
    assertEquals(0.5, result.getRight(), 1e-6);
  }

  @Test
  public void testSnap() {
    Table base =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))') AS poly, ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)') AS line");
    Table table =
        base.select(
            call(Functions.ST_Snap.class.getSimpleName(), $("poly"), $("line"), 2.525)
                .as("result"));
    String actual =
        (String)
            first(table.select(call(Functions.ST_AsText.class.getSimpleName(), $("result"))))
                .getField(0);
    String expected = "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))";
    assertEquals(expected, actual);

    table =
        base.select(
            call(Functions.ST_Snap.class.getSimpleName(), $("poly"), $("line"), 3.125)
                .as("result"));
    actual =
        (String)
            first(table.select(call(Functions.ST_AsText.class.getSimpleName(), $("result"))))
                .getField(0);
    expected = "POLYGON ((0.5 10.7, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 5.4 8.4, 0.5 10.7))";
    assertEquals(expected, actual);
  }

  @Test
  public void testMulti() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (0 0)') AS geom");
    table = table.select(call(Functions.ST_Multi.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("MULTIPOINT ((0 0))", result.toString());
  }

  @Test
  public void testStartPoint() {
    Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom");
    table = table.select(call(Functions.ST_StartPoint.class.getSimpleName(), $("geom")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POINT (0 0)", result.toString());
  }

  @Test
  public void testSimplifyPreserveTopology() {
    Table table =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom");
    table =
        table.select(
            call(Functions.ST_SimplifyPreserveTopology.class.getSimpleName(), $("geom"), 0.2));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 0))", result.toString());
  }

  @Test
  public void testSimplifyVW() {
    Table table =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom");
    table =
        table.select(
            call(Functions.ST_SimplifyPreserveTopology.class.getSimpleName(), $("geom"), 2));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals("POLYGON ((0 0, 1 0, 1 1, 0 0))", result.toString());
  }

  @Test
  public void testSimplifyPolygonHull() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))') AS geom");
    String actual =
        first(
                table.select(
                    call(
                        Functions.ST_SimplifyPolygonHull.class.getSimpleName(),
                        $("geom"),
                        0.3,
                        false)))
            .getField(0)
            .toString();
    String expected = "POLYGON ((30 10, 40 40, 10 20, 30 10))";
    assertEquals(expected, actual);

    actual =
        first(
                table.select(
                    call(Functions.ST_SimplifyPolygonHull.class.getSimpleName(), $("geom"), 0.3)))
            .getField(0)
            .toString();
    expected = "POLYGON ((30 10, 15 15, 10 20, 20 40, 45 45, 30 10))";
    assertEquals(expected, actual);
  }

  @Test
  public void testSplit() {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_Split(ST_GeomFromWKT('LINESTRING (0 0, 1.5 1.5, 2 2)'), ST_GeomFromWKT('MULTIPOINT (0.5 0.5, 1 1)'))");
    assertEquals(
        "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))",
        ((Geometry) first(pointTable).getField(0)).norm().toText());
  }

  @Test
  public void testSubdivide() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom");
    table = table.select(call(Functions.ST_Subdivide.class.getSimpleName(), $("geom"), 5));
    Geometry[] result = (Geometry[]) first(table).getField(0);
    assertEquals("LINESTRING (0 0, 2.5 0)", result[0].toString());
    assertEquals("LINESTRING (2.5 0, 5 0)", result[1].toString());
  }

  @Test
  public void testSymDifference() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') AS a, ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') AS b");
    table = table.select(call(Functions.ST_SymDifference.class.getSimpleName(), $("a"), $("b")));
    Geometry result = (Geometry) first(table).getField(0);
    assertEquals(
        "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))",
        result.toString());
  }

  @Test
  public void testS2CellIDs() {
    String initExplodeQuery =
        "SELECT id, geom, cell_tbl.cell from (VALUES %s) as raw_tbl(id, geom, cells) CROSS JOIN UNNEST(raw_tbl.cells) AS cell_tbl (cell)";
    // left is a polygon
    tableEnv.createTemporaryView(
        "lefts",
        tableEnv.sqlQuery(
            String.format(
                initExplodeQuery,
                "(1, ST_GeomFromWKT('POLYGON ((0 0, 0.2 0, 0.2 0.2, 0 0.2, 0 0))'), ST_S2CellIDs(ST_GeomFromWKT('POLYGON ((0 0, 0.2 0, 0.2 0.2, 0 0.2, 0 0))'), 10))")));
    // points for test
    String points =
        String.join(
            ", ",
            new String[] {
              "(2, ST_GeomFromWKT('POINT (0.1 0.1)'), ST_S2CellIDs(ST_GeomFromWKT('POINT (0.1 0.1)'), 10))", // points within polygon
              "(3, ST_GeomFromWKT('POINT (0.25 0.1)'), ST_S2CellIDs(ST_GeomFromWKT('POINT (0.25 0.1)'), 10))", // points outside of polygon
              "(4, ST_GeomFromWKT('POINT (0.2005 0.1)'), ST_S2CellIDs(ST_GeomFromWKT('POINT (0.2005 0.1)'), 10))" // points outside of polygon, but very close to border
            });
    tableEnv.createTemporaryView(
        "rights", tableEnv.sqlQuery(String.format(initExplodeQuery, points)));
    Table joinTable =
        tableEnv.sqlQuery(
            "select lefts.id, rights.id from lefts join rights on lefts.cell = rights.cell group by (lefts.id, rights.id)");
    assertEquals(2, count(joinTable));
    ;
    assert take(joinTable, 2).stream()
        .map(r -> Objects.requireNonNull(r.getField(1)).toString())
        .collect(Collectors.toSet())
        .containsAll(Arrays.asList("2", "4"));
    // This is due to under level = 10, point id = 4 fall into same cell as the boarder of polygon
    // id = 1
    // join and filter by st_intersects to exclude the wrong join
    Table joinCleanedTable =
        tableEnv.sqlQuery(
            "select lefts.id, rights.id from lefts join rights on lefts.cell = rights.cell where ST_Intersects(lefts.geom, rights.geom) is true group by (lefts.id, rights.id)");
    // after filter by ST_Intersects, only id =2 point
    assertEquals(1, count(joinCleanedTable));
    assertEquals(2, first(joinCleanedTable).getField(1));
  }

  @Test
  public void testS2ToGeom() {
    Table pointTable =
        tableEnv.sqlQuery(
            "select ST_S2ToGeom(ST_S2CellIDs(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), 10))");
    Geometry target =
        (Geometry)
            first(
                    tableEnv.sqlQuery(
                        "select ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))')"))
                .getField(0);
    Geometry[] actual = (Geometry[]) Objects.requireNonNull(first(pointTable).getField(0));
    assertTrue(actual[0].intersects(target));
    assertTrue(actual[20].intersects(target));
    assertTrue(actual[100].intersects(target));
  }

  @Test
  public void testH3CellIDs() {
    String initExplodeQuery =
        "SELECT id, geom, cell_tbl.cell from (VALUES %s) as raw_tbl(id, geom, cells) CROSS JOIN UNNEST(raw_tbl.cells) AS cell_tbl (cell)";
    // left is a polygon
    tableEnv.createTemporaryView(
        "lefts_h3",
        tableEnv.sqlQuery(
            String.format(
                initExplodeQuery,
                "(1, ST_GeomFromWKT('POLYGON ((0 0, 0.2 0, 0.2 0.2, 0 0.2, 0 0))'), ST_H3CellIDs(ST_GeomFromWKT('POLYGON ((0 0, 0.2 0, 0.2 0.2, 0 0.2, 0 0))'), 8, true))")));
    // points for test
    String points =
        String.join(
            ", ",
            new String[] {
              "(2, ST_GeomFromWKT('POINT (0.1 0.1)'), ST_H3CellIDs(ST_GeomFromWKT('POINT (0.1 0.1)'), 8, true))", // points within polygon
              "(3, ST_GeomFromWKT('POINT (0.25 0.1)'), ST_H3CellIDs(ST_GeomFromWKT('POINT (0.25 0.1)'), 8, true))", // points outside of polygon
              "(4, ST_GeomFromWKT('POINT (0.2005 0.1)'), ST_H3CellIDs(ST_GeomFromWKT('POINT (0.2005 0.1)'), 8, true))" // points outside of polygon, but very close to border
            });
    tableEnv.createTemporaryView(
        "rights_h3", tableEnv.sqlQuery(String.format(initExplodeQuery, points)));
    Table joinTable =
        tableEnv.sqlQuery(
            "select lefts_h3.id, rights_h3.id from lefts_h3 join rights_h3 on lefts_h3.cell = rights_h3.cell group by (lefts_h3.id, rights_h3.id)");
    assertEquals(2, count(joinTable));
    ;
    assert take(joinTable, 2).stream()
        .map(r -> Objects.requireNonNull(r.getField(1)).toString())
        .collect(Collectors.toSet())
        .containsAll(Arrays.asList("2", "4"));
    // This is due to under level = 10, point id = 4 fall into same cell as the boarder of polygon
    // id = 1
    // join and filter by st_intersects to exclude the wrong join
    Table joinCleanedTable =
        tableEnv.sqlQuery(
            "select lefts_h3.id, rights_h3.id from lefts_h3 join rights_h3 on lefts_h3.cell = rights_h3.cell where ST_Intersects(lefts_h3.geom, rights_h3.geom) is true group by (lefts_h3.id, rights_h3.id)");
    // after filter by ST_Intersects, only id =2 point
    assertEquals(1, count(joinCleanedTable));
    assertEquals(2, first(joinCleanedTable).getField(1));
  }

  @Test
  public void testH3CellDistance() {
    Table pointTable =
        tableEnv.sqlQuery(
            "select ST_H3CellDistance(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[1], ST_H3CellIDs(ST_GeomFromWKT('POINT(1.23 1.59)'), 8, true)[1])");
    long exact = Long.parseLong(Objects.requireNonNull(first(pointTable).getField(0)).toString());
    assertEquals(exact, 78);
  }

  @Test
  public void testH3KRing() {
    Table pointTable =
        tableEnv.sqlQuery(
            "select ST_H3KRing(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[1], 3, false), ST_H3KRing(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[1], 3, true)");
    List<Long> full = Arrays.asList((Long[]) Objects.requireNonNull(first(pointTable).getField(0)));
    List<Long> exactRing =
        Arrays.asList((Long[]) Objects.requireNonNull(first(pointTable).getField(0)));
    assert full.containsAll(exactRing);
  }

  @Test
  public void testH3ToGeom() {
    Table pointTable =
        tableEnv.sqlQuery(
            "select ST_H3ToGeom(ST_H3CellIDs(ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))'), 4, true))");
    Geometry target =
        (Geometry)
            first(
                    tableEnv.sqlQuery(
                        "select ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))')"))
                .getField(0);
    Geometry[] actual = (Geometry[]) first(pointTable).getField(0);
    assertTrue(actual[0].intersects(target));
    assertTrue(actual[11].intersects(target));
    assertTrue(actual[20].intersects(target));
  }

  @Test
  public void testGeometricMedian() throws ParseException {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeometricMedian(ST_GeomFromWKT('MULTIPOINT((0 0), (1 1), (2 2), (200 200))'))");
    Geometry expected = wktReader.read("POINT (1.9761550281255005 1.9761550281255005)");
    Geometry actual = (Geometry) first(pointTable).getField(0);
    assertEquals(
        String.format(
            "expected: %s was %s", expected.toText(), actual != null ? actual.toText() : "null"),
        0,
        expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
  }

  @Test
  public void testGeometricMedianParamsTolerance() throws ParseException {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeometricMedian(ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1), (0 1), (2 2))'), 1e-5)");
    Geometry expected = wktReader.read("POINT (0.996230268436779 0.9999899629155288)");
    Geometry actual = (Geometry) first(pointTable).getField(0);
    assertEquals(
        String.format(
            "expected: %s was %s", expected.toText(), actual != null ? actual.toText() : "null"),
        0,
        expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
  }

  @Test
  public void testGeometricMedianParamsFull() throws ParseException {
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeometricMedian(ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1), (0 1), (2 2))'), 1e-5, 10, false)");
    Geometry expected = wktReader.read("POINT (0.8844442206215307 0.9912184073718183)");
    Geometry actual = (Geometry) first(pointTable).getField(0);
    assertEquals(
        String.format(
            "expected: %s was %s", expected.toText(), actual != null ? actual.toText() : "null"),
        0,
        expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
  }

  @Test
  public void testNumPoints() {
    Integer expected = 3;
    Table pointTable =
        tableEnv.sqlQuery("SELECT ST_NumPoints(ST_GeomFromWKT('LINESTRING(0 1, 1 0, 2 0)'))");
    Integer actual = (Integer) first(pointTable).getField(0);
    assertEquals(expected, actual);
  }

  @Test
  public void testForce3D() {
    Integer expectedDims = 3;
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force3D(ST_GeomFromWKT('LINESTRING(0 1, 1 0, 2 0)'), 1.2) "
                + "AS "
                + polygonColNames[0]);
    pointTable =
        pointTable.select(call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0])));
    Integer actual = (Integer) first(pointTable).getField(0);
    assertEquals(expectedDims, actual);
  }

  @Test
  public void testForce3DDefaultValue() {
    Integer expectedDims = 3;
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force3D(ST_GeomFromWKT('LINESTRING(0 1, 1 0, 2 0)')) "
                + "AS "
                + polygonColNames[0]);
    pointTable =
        pointTable.select(call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0])));
    Integer actual = (Integer) first(pointTable).getField(0);
    assertEquals(expectedDims, actual);
  }

  @Test
  public void testForce3DZ() {
    Integer expectedDims = 3;
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force3DZ(ST_GeomFromWKT('LINESTRING(0 1, 1 0, 2 0)'), 1.2) "
                + "AS "
                + polygonColNames[0]);
    Integer actual =
        (Integer)
            first(
                    polyTable.select(
                        call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0]))))
                .getField(0);
    assertEquals(expectedDims, actual);

    polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force3DZ(ST_GeomFromWKT('LINESTRING(0 1, 1 0, 2 0)')) "
                + "AS "
                + polygonColNames[0]);
    actual =
        (Integer)
            first(
                    polyTable.select(
                        call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0]))))
                .getField(0);
    assertEquals(expectedDims, actual);
  }

  @Test
  public void testForce3DM() {
    Table geomTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force3DM(ST_GeomFromText('LINESTRING (1 2, 2 3, 3 4)')) AS geom");
    Boolean actual =
        (Boolean)
            first(geomTable.select(call(Functions.ST_HasM.class.getSimpleName(), $("geom"))))
                .getField(0);
    assertEquals(Boolean.TRUE, actual);

    geomTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force3DM(ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))')) AS geom");
    actual =
        (Boolean)
            first(geomTable.select(call(Functions.ST_HasM.class.getSimpleName(), $("geom"))))
                .getField(0);
    assertEquals(Boolean.TRUE, actual);
  }

  @Test
  public void testForce4D() {
    Table geomTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force4D(ST_GeomFromText('LINESTRING (1 2, 2 3, 3 4)')) AS geom");
    String actual =
        (String)
            first(geomTable.select(call(Functions.ST_AsText.class.getSimpleName(), $("geom"))))
                .getField(0);
    String expected = "LINESTRING ZM(1 2 0 0, 2 3 0 0, 3 4 0 0)";
    assertEquals(expected, actual);

    geomTable =
        tableEnv.sqlQuery(
            "SELECT ST_Force4D(ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))')) AS geom");
    actual =
        (String)
            first(geomTable.select(call(Functions.ST_AsText.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected =
        "POLYGON ZM((0 0 0 0, 10 0 0 0, 10 10 0 0, 0 10 0 0, 0 0 0 0), (4 4 0 0, 4 6 0 0, 6 6 0 0, 6 4 0 0, 4 4 0 0))";
    assertEquals(expected, actual);
  }

  @Test
  public void testForceCollection() {
    int actual =
        (int)
            first(
                    tableEnv
                        .sqlQuery(
                            "SELECT ST_GeomFromWKT('MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)') AS geom")
                        .select(call(Functions.ST_ForceCollection.class.getSimpleName(), $("geom")))
                        .as("geom")
                        .select(call(Functions.ST_NumGeometries.class.getSimpleName(), $("geom"))))
                .getField(0);
    int expected = 6;
    assertEquals(expected, actual);

    actual =
        (int)
            first(
                    tableEnv
                        .sqlQuery(
                            "SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 0 0,0 1 0,1 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,1 0 1,0 0 1,0 0 0)),((1 1 0,1 1 1,1 0 1,1 0 0,1 1 0)),((0 1 0,0 1 1,1 1 1,1 1 0,0 1 0)),((0 0 1,1 0 1,1 1 1,0 1 1,0 0 1)))') AS geom")
                        .select(call(Functions.ST_ForceCollection.class.getSimpleName(), $("geom")))
                        .as("geom")
                        .select(call(Functions.ST_NumGeometries.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected = 5;
    assertEquals(expected, actual);

    actual =
        (int)
            first(
                    tableEnv
                        .sqlQuery(
                            "SELECT ST_GeomFromWKT('MULTILINESTRING ((10 10, 20 20, 30 30), (15 15, 25 25, 35 35))') AS geom")
                        .select(call(Functions.ST_ForceCollection.class.getSimpleName(), $("geom")))
                        .as("geom")
                        .select(call(Functions.ST_NumGeometries.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected = 2;
    assertEquals(expected, actual);

    actual =
        (int)
            first(
                    tableEnv
                        .sqlQuery(
                            "SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS geom")
                        .select(call(Functions.ST_ForceCollection.class.getSimpleName(), $("geom")))
                        .as("geom")
                        .select(call(Functions.ST_NumGeometries.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected = 1;
    assertEquals(expected, actual);
  }

  @Test
  public void testTriangulatePolygon() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_TriangulatePolygon(ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))')) as poly");
    String actual =
        (String)
            first(polyTable.select(call(Functions.ST_AsText.class.getSimpleName(), $("poly"))))
                .getField(0);
    String expected =
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))";
    assertEquals(expected, actual);
  }

  @Test
  public void testForceRHR() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_ForceRHR(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')) AS polyCW");
    String actual =
        (String)
            first(polyTable.select(call(Functions.ST_AsText.class.getSimpleName(), $("polyCW"))))
                .getField(0);
    String expected =
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))";
    assertEquals(expected, actual);
  }

  @Test
  public void testForcePolygonCW() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_ForcePolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')) AS polyCW");
    String actual =
        (String)
            first(polyTable.select(call(Functions.ST_AsText.class.getSimpleName(), $("polyCW"))))
                .getField(0);
    String expected =
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))";
    assertEquals(expected, actual);
  }

  @Test
  public void testIsPolygonCW() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') AS polyCCW");
    boolean actual =
        (boolean)
            first(
                    polyTable.select(
                        call(Functions.ST_IsPolygonCW.class.getSimpleName(), $("polyCCW"))))
                .getField(0);
    assertFalse(actual);

    polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))') AS polyCW");
    actual =
        (boolean)
            first(
                    polyTable.select(
                        call(Functions.ST_IsPolygonCW.class.getSimpleName(), $("polyCW"))))
                .getField(0);
    assertTrue(actual);
  }

  @Test
  public void testGeneratePoints() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_Buffer(ST_GeomFromWKT('LINESTRING(50 50,10 10,10 50)'), 10, false, 'endcap=round join=round') AS geom");
    Geometry actual =
        (Geometry)
            first(
                    polyTable.select(
                        call(Functions.ST_GeneratePoints.class.getSimpleName(), $("geom"), 15)))
                .getField(0);
    assertEquals(actual.getNumGeometries(), 15);

    actual =
        (Geometry)
            first(
                    polyTable
                        .select(
                            call(
                                Functions.ST_GeneratePoints.class.getSimpleName(),
                                $("geom"),
                                5,
                                100L))
                        .as("geom")
                        .select(
                            call(Functions.ST_ReducePrecision.class.getSimpleName(), $("geom"), 5)))
                .getField(0);
    String expected =
        "MULTIPOINT ((40.02957 46.70645), (37.11646 37.38582), (14.2051 29.23363), (40.82533 31.47273), (28.16839 34.16338))";
    assertEquals(expected, actual.toString());

    polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))') AS geom");
    actual =
        (Geometry)
            first(
                    polyTable.select(
                        call(Functions.ST_GeneratePoints.class.getSimpleName(), $("geom"), 30)))
                .getField(0);
    assertEquals(actual.getNumGeometries(), 30);
  }

  @Test
  public void testNRings() {
    Integer expected = 1;
    Table pointTable =
        tableEnv.sqlQuery(
            "SELECT ST_NRings(ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'))");
    Integer actual = (Integer) first(pointTable).getField(0);
    assertEquals(expected, actual);
  }

  @Test
  public void testForcePolygonCCW() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_ForcePolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))')) AS polyCW");
    String actual =
        (String)
            first(polyTable.select(call(Functions.ST_AsText.class.getSimpleName(), $("polyCW"))))
                .getField(0);
    String expected =
        "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))";
    assertEquals(expected, actual);
  }

  @Test
  public void testIsPolygonCCW() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') AS polyCCW");
    boolean actual =
        (boolean)
            first(
                    polyTable.select(
                        call(Functions.ST_IsPolygonCCW.class.getSimpleName(), $("polyCCW"))))
                .getField(0);
    assertTrue(actual);

    polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))') AS polyCW");
    actual =
        (boolean)
            first(
                    polyTable.select(
                        call(Functions.ST_IsPolygonCCW.class.getSimpleName(), $("polyCW"))))
                .getField(0);
    assertFalse(actual);
  }

  @Test
  public void testTranslate() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_Translate(ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'), 2, 5)"
                + "AS "
                + polygonColNames[0]);
    polyTable =
        polyTable.select(call(Functions.ST_AsText.class.getSimpleName(), $(polygonColNames[0])));
    String expected = "POLYGON ((3 5, 3 6, 4 6, 4 5, 3 5))";
    String actual = (String) first(polyTable).getField(0);
    assertEquals(expected, actual);
  }

  @Test
  public void testVoronoiPolygons() {
    Table polyTable1 =
        tableEnv.sqlQuery("SELECT ST_VoronoiPolygons(ST_GeomFromWKT('MULTIPOINT ((0 0), (2 2))'))");
    Geometry result = (Geometry) first(polyTable1).getField(0);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 -2, -2 -2)), POLYGON ((-2 4, 4 4, 4 -2, -2 4)))",
        result.toString());

    Table polyTable2 =
        tableEnv.sqlQuery(
            "SELECT ST_VoronoiPolygons(ST_GeomFromWKT('MULTIPOINT ((0 0), (2 2))'), 0, ST_Buffer(ST_GeomFromWKT('POINT(1 1)'), 10.0) )");
    result = (Geometry) first(polyTable2).getField(0);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 -9, -9 -9)), POLYGON ((-9 11, 11 11, 11 -9, -9 11)))",
        result.toString());

    Table polyTable3 =
        tableEnv.sqlQuery(
            "SELECT ST_VoronoiPolygons(ST_GeomFromWKT('MULTIPOINT ((0 0), (2 2))'), 30)");
    result = (Geometry) first(polyTable3).getField(0);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 4, 4 -2, -2 -2)))", result.toString());

    Table polyTable4 =
        tableEnv.sqlQuery(
            "SELECT ST_VoronoiPolygons(ST_GeomFromWKT('MULTIPOINT ((0 0), (2 2))'), 30, ST_Buffer(ST_GeomFromWKT('POINT(1 1)'), 10) )");
    result = (Geometry) first(polyTable4).getField(0);
    assertEquals(
        "GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 11, 11 -9, -9 -9)))", result.toString());

    Table polyTable5 =
        tableEnv.sqlQuery(
            "SELECT ST_VoronoiPolygons(null, 30, ST_Buffer(ST_GeomFromWKT('POINT(1 1)'), 10))");
    result = (Geometry) first(polyTable5).getField(0);
    assertEquals(null, result);
  }

  @Test
  public void testFrechet() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (1 2)') AS g1, ST_GeomFromWKT('POINT (10 10)') as g2");
    polyTable =
        polyTable.select(
            call(Functions.ST_FrechetDistance.class.getSimpleName(), $("g1"), $("g2")));
    Double expected = 12.041594578792296;
    Double actual = (Double) first(polyTable).getField(0);
    assertEquals(expected, actual);
  }

  @Test
  public void testAffine() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POLYGON ((1 0 1, 1 1 1, 2 2 2, 1 0 1))')"
                + " AS "
                + polygonColNames[0]);
    Table polyTableDefault =
        polyTable
            .select(
                call(
                    Functions.ST_Affine.class.getSimpleName(),
                    $(polygonColNames[0]),
                    1,
                    2,
                    1,
                    2,
                    1,
                    2))
            .as(polygonColNames[0])
            .select(call(Functions.ST_AsText.class.getSimpleName(), $(polygonColNames[0])));
    polyTable =
        polyTable
            .select(
                call(
                    Functions.ST_Affine.class.getSimpleName(),
                    $(polygonColNames[0]),
                    1,
                    2,
                    4,
                    1,
                    1,
                    2,
                    3,
                    2,
                    5,
                    4,
                    8,
                    3))
            .as(polygonColNames[0])
            .select(call(Functions.ST_AsText.class.getSimpleName(), $(polygonColNames[0])));
    String expectedDefault = "POLYGON Z((2 3 1, 4 5 1, 7 8 2, 2 3 1))";
    String actualDefault = (String) first(polyTableDefault).getField(0);
    String expected = "POLYGON Z((9 11 11, 11 12 13, 18 16 23, 9 11 11))";
    String actual = (String) first(polyTable).getField(0);
    assertEquals(expected, actual);
    assertEquals(expectedDefault, actualDefault);
  }

  @Test
  public void testBoundingDiagonal() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_BoundingDiagonal(ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'))"
                + " AS "
                + polygonColNames[0]);
    polyTable =
        polyTable.select(call(Functions.ST_AsText.class.getSimpleName(), $(polygonColNames[0])));
    String expected = "LINESTRING (1 0, 2 1)";
    String actual = (String) first(polyTable).getField(0);
    assertEquals(expected, actual);
  }

  @Test
  public void testAngle() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_Angle(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('LINESTRING (0 0, 3 2)'))"
                + " AS "
                + polygonColNames[0]);
    polyTable =
        polyTable.select(call(Functions.ST_Degrees.class.getSimpleName(), $(polygonColNames[0])));
    Double expected = 11.309932474020195;
    Double actual = (Double) first(polyTable).getField(0);
    assertEquals(expected, actual, 1e-9);
  }

  @Test
  public void testDelaunayTriangle() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTIPOLYGON (((10 10, 10 20, 20 20, 20 10, 10 10)),((25 10, 25 20, 35 20, 35 10, 25 10)))') AS geom");
    String actual =
        ((Geometry)
                first(
                        polyTable.select(
                            call(Functions.ST_DelaunayTriangles.class.getSimpleName(), $("geom"))))
                    .getField(0))
            .toText();
    String expected =
        "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 20 10, 10 20)), POLYGON ((10 20, 20 10, 20 20, 10 20)), POLYGON ((20 20, 20 10, 25 10, 20 20)), POLYGON ((20 20, 25 10, 25 20, 20 20)), POLYGON ((25 20, 25 10, 35 10, 25 20)), POLYGON ((25 20, 35 10, 35 20, 25 20)))";
    assertEquals(expected, actual);

    actual =
        ((Geometry)
                first(
                        polyTable.select(
                            call(
                                Functions.ST_DelaunayTriangles.class.getSimpleName(),
                                $("geom"),
                                20)))
                    .getField(0))
            .toText();
    expected = "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 35 10, 10 20)))";
    assertEquals(expected, actual);

    actual =
        ((Geometry)
                first(
                        polyTable.select(
                            call(
                                Functions.ST_DelaunayTriangles.class.getSimpleName(),
                                $("geom"),
                                20,
                                1)))
                    .getField(0))
            .toText();
    expected = "MULTILINESTRING ((10 20, 35 10), (10 10, 10 20), (10 10, 35 10))";
    assertEquals(expected, actual);
  }

  @Test
  public void testHausdorffDistance() {
    Table polyTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (0.0 1.0)') AS g1, ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS g2");
    Table actualTable =
        polyTable.select(
            call(Functions.ST_HausdorffDistance.class.getSimpleName(), $("g1"), $("g2"), 0.4));
    Table actualTableDefault =
        polyTable.select(
            call(Functions.ST_HausdorffDistance.class.getSimpleName(), $("g1"), $("g2")));
    Double expected = 5.0990195135927845;
    Double expectedDefault = 5.0990195135927845;
    Double actual = (Double) first(actualTable).getField(0);
    Double actualDefault = (Double) first(actualTableDefault).getField(0);
    assertEquals(expected, actual);
    assertEquals(expectedDefault, actualDefault);
  }

  @Test
  public void testIsCollectionForCollection() {
    Table collectionTable =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(2 3), POINT(4 6), LINESTRING(15 15, 20 20))') AS collection");
    Table resultTable =
        collectionTable.select(
            call(Functions.ST_IsCollection.class.getSimpleName(), $("collection")));
    boolean result = (boolean) first(resultTable).getField(0);
    assertTrue(result);
  }

  @Test
  public void testIsCollectionForNotCollection() {
    Table collectionTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(10 10)') AS collection");
    Table resultTable =
        collectionTable.select(
            call(Functions.ST_IsCollection.class.getSimpleName(), $("collection")));
    boolean result = (boolean) first(resultTable).getField(0);
    assertFalse(result);
  }

  @Test
  public void testCoordDimFor2D() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(3 7)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_CoordDim.class.getSimpleName(), $(polygonColNames[0])));
    int result = (int) first(polygonTable).getField(0);
    assertEquals(2, result, 0);
  }

  @Test
  public void testCoordDimFor3D() {
    Table polygonTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(1 2 1)') AS " + polygonColNames[0]);
    polygonTable =
        polygonTable.select(
            call(Functions.ST_CoordDim.class.getSimpleName(), $(polygonColNames[0])));
    int result = (int) first(polygonTable).getField(0);
    assertEquals(3, result, 0);
  }

  @Test
  public void testIsValidTrajectory() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromText('LINESTRING M (0 0 1, 0 1 2)') as geom1, ST_GeomFromText('LINESTRING M (0 0 1, 0 1 1)') as geom2");
    boolean result =
        (boolean)
            first(
                    table.select(
                        call(Functions.ST_IsValidTrajectory.class.getSimpleName(), $("geom1"))))
                .getField(0);
    assertTrue(result);

    result =
        (boolean)
            first(
                    table.select(
                        call(Functions.ST_IsValidTrajectory.class.getSimpleName(), $("geom2"))))
                .getField(0);
    assertFalse(result);
  }

  @Test
  public void testIsValidReason() {
    // Test with an invalid geometry (bow-tie polygon)
    String bowTieWKT = "POLYGON ((100 200, 100 100, 200 200, 200 100, 100 200))";
    Table bowTieTable = tableEnv.sqlQuery("SELECT ST_GeomFromText('" + bowTieWKT + "') AS geom");
    Table bowTieValidityTable = bowTieTable.select(call("ST_IsValidReason", $("geom")));
    String bowTieValidityReason = (String) first(bowTieValidityTable).getField(0);
    System.out.println(bowTieValidityReason);
    assertTrue(bowTieValidityReason.contains("Self-intersection"));

    // Test with a valid geometry (simple linestring)
    String lineWKT = "LINESTRING (220227 150406, 2220227 150407, 222020 150410)";
    Table lineTable = tableEnv.sqlQuery("SELECT ST_GeomFromText('" + lineWKT + "') AS geom");
    Table lineValidityTable = lineTable.select(call("ST_IsValidReason", $("geom")));
    String lineValidityReason = (String) first(lineValidityTable).getField(0);
    assertEquals("Valid Geometry", lineValidityReason);

    final int OGC_SFS_VALIDITY = 0;
    final int ESRI_VALIDITY = 1;

    // Geometry that is invalid under both OGC and ESRI standards, but with different reasons
    String selfTouchingWKT = "POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))";
    Table specialCaseTable =
        tableEnv.sqlQuery("SELECT ST_GeomFromText('" + selfTouchingWKT + "') AS geom");

    // Test with OGC flag
    Table ogcValidityTable =
        specialCaseTable.select(call("ST_IsValidReason", $("geom"), OGC_SFS_VALIDITY));
    String ogcValidityReason = (String) first(ogcValidityTable).getField(0);
    assertEquals(
        "Ring Self-intersection at or near point (1.0, 1.0, NaN)",
        ogcValidityReason); // Expecting a self-intersection error as per OGC standards

    // Test with ESRI flag
    Table esriValidityTable =
        specialCaseTable.select(call("ST_IsValidReason", $("geom"), ESRI_VALIDITY));
    String esriValidityReason = (String) first(esriValidityTable).getField(0);
    assertEquals(
        "Interior is disconnected at or near point (1.0, 1.0, NaN)",
        esriValidityReason); // Expecting an error related to interior disconnection as per ESRI
    // standards
  }

  @Test
  public void testRotateX() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromEWKT('POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))') AS geom");
    String actual =
        (String)
            first(
                    tbl.select(call(Functions.ST_RotateX.class.getSimpleName(), $("geom"), Math.PI))
                        .as("geom")
                        .select(call(Functions.ST_AsEWKT.class.getSimpleName(), $("geom"))))
                .getField(0);
    String expected = "POLYGON ((0 0, 2 0, 1 -1, 2 -2, 0 -2, 1 -1, 0 0))";
    assertEquals(expected, actual);
  }

  @Test
  public void testRotate() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromEWKT('POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))') AS geom1, ST_GeomFromEWKT('POINT (2 0)') AS geom2");
    String actual =
        (String)
            first(
                    tbl.select(call(Functions.ST_Rotate.class.getSimpleName(), $("geom1"), Math.PI))
                        .as("geom")
                        .select(call(Functions.ST_AsEWKT.class.getSimpleName(), $("geom"))))
                .getField(0);
    String expected =
        "POLYGON ((0 0, -2 0.0000000000000002, -1.0000000000000002 -0.9999999999999999, -2.0000000000000004 -1.9999999999999998, -0.0000000000000002 -2, -1.0000000000000002 -0.9999999999999999, 0 0))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tbl.select(
                            call(
                                Functions.ST_Rotate.class.getSimpleName(),
                                $("geom1"),
                                50,
                                $("geom2")))
                        .as("geom")
                        .select(call(Functions.ST_AsEWKT.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected =
        "POLYGON ((0.0700679430157733 0.5247497074078575, 2 0, 1.2974088252118154 1.227340882196042, 2.5247497074078575 1.9299320569842267, 0.5948176504236309 2.454681764392084, 1.2974088252118154 1.227340882196042, 0.0700679430157733 0.5247497074078575))";
    assertEquals(expected, actual);

    actual =
        (String)
            first(
                    tbl.select(
                            call(Functions.ST_Rotate.class.getSimpleName(), $("geom1"), 50, 2, 0))
                        .as("geom")
                        .select(call(Functions.ST_AsEWKT.class.getSimpleName(), $("geom"))))
                .getField(0);
    expected =
        "POLYGON ((0.0700679430157733 0.5247497074078575, 2 0, 1.2974088252118154 1.227340882196042, 2.5247497074078575 1.9299320569842267, 0.5948176504236309 2.454681764392084, 1.2974088252118154 1.227340882196042, 0.0700679430157733 0.5247497074078575))";
    assertEquals(expected, actual);
  }
}
