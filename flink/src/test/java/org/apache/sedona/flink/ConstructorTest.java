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

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Constructors;
import org.apache.sedona.flink.expressions.Functions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wololo.jts2geojson.GeoJSONReader;

public class ConstructorTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  @Test
  public void test2DPoint() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of(1.0, 2.0, "point"));
    String[] colNames = new String[] {"x", "y", "name_point"};

    TypeInformation<?>[] colTypes = {
      BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    Table pointTable = tableEnv.fromDataStream(ds);

    Table geomTable =
        pointTable.select(
            call(Constructors.ST_Point.class.getSimpleName(), $(colNames[0]), $(colNames[1]))
                .as(colNames[2]));

    String result = first(geomTable).getFieldAs(colNames[2]).toString();

    String expected = "POINT (1 2)";

    assertEquals(expected, result);
  }

  @Test
  public void testPointZ() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of(2.0, 2.0, 5.0, "point"));
    String[] colNames = new String[] {"x", "y", "z", "name_point"};

    TypeInformation<?>[] colTypes = {
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    Table pointTable = tableEnv.fromDataStream(ds);

    Table geomTable =
        pointTable.select(
            call(
                    Constructors.ST_PointZ.class.getSimpleName(),
                    $(colNames[0]),
                    $(colNames[1]),
                    $(colNames[2]))
                .as(colNames[3]));

    Point result = first(geomTable).getFieldAs(colNames[3]);

    assertEquals(5.0, result.getCoordinate().getZ(), 1e-6);
  }

  @Test
  public void testPointM() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of(2.0, 2.0, 5.0, "point"));
    String[] colNames = new String[] {"x", "y", "m", "name_point"};

    TypeInformation<?>[] colTypes = {
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    Table pointTable = tableEnv.fromDataStream(ds);

    Table geomTable =
        pointTable.select(
            call(
                    Constructors.ST_PointM.class.getSimpleName(),
                    $(colNames[0]),
                    $(colNames[1]),
                    $(colNames[2]))
                .as(colNames[3]));

    Point result = first(geomTable).getFieldAs(colNames[3]);

    assertEquals(5.0, result.getCoordinate().getM(), 1e-6);
  }

  @Test
  public void testPointZM() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of(2.0, 2.0, 5.0, 100.0, "point"));
    String[] colNames = new String[] {"x", "y", "z", "m", "name_point"};

    TypeInformation<?>[] colTypes = {
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    Table pointTable = tableEnv.fromDataStream(ds);

    Table geomTable =
        pointTable.select(
            call(
                    Constructors.ST_PointZM.class.getSimpleName(),
                    $(colNames[0]),
                    $(colNames[1]),
                    $(colNames[2]),
                    $(colNames[3]))
                .as(colNames[4]));

    Point result = first(geomTable).getFieldAs(colNames[4]);

    assertEquals(5.0, result.getCoordinate().getZ(), 1e-6);
    assertEquals(100.0, result.getCoordinate().getM(), 1e-6);
  }

  @Test
  public void testMakePointM() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of(1.0, 2.0, 3.0, "pointM"));
    String[] colNames = new String[] {"x", "y", "m", "name_point"};

    TypeInformation<?>[] colTypes = {
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    Table pointTable = tableEnv.fromDataStream(ds);

    Table geomTable =
        pointTable
            .select(
                call(
                    Constructors.ST_MakePointM.class.getSimpleName(),
                    $(colNames[0]),
                    $(colNames[1]),
                    $(colNames[2])))
            .as(colNames[3]);

    String result =
        (String)
            first(geomTable.select(call(Functions.ST_AsText.class.getSimpleName(), $(colNames[3]))))
                .getField(0);

    String expected = "POINT M(1 2 3)";
    assertEquals(expected, result);
  }

  @Test
  public void testMakePoint() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of(1.0, 2.0, "point"));
    String[] colNames = new String[] {"x", "y", "name_point"};

    TypeInformation<?>[] colTypes = {
      BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    Table pointTable = tableEnv.fromDataStream(ds);

    Table geomTable =
        pointTable.select(
            call(Constructors.ST_MakePoint.class.getSimpleName(), $(colNames[0]), $(colNames[1]))
                .as(colNames[2]));

    String result = first(geomTable).getFieldAs(colNames[2]).toString();

    String expected = "POINT (1 2)";

    assertEquals(expected, result);
  }

  @Test
  public void testPointFromText() {
    List<Row> data = createPointWKT(testDataSize);
    Row result = last(createPointTable(testDataSize));
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
  }

  @Test
  public void testLineFromText() {
    List<Row> data = createLineStringWKT(testDataSize);

    Table lineStringTable =
        createLineStringTextTable(testDataSize)
            .select(
                call(Constructors.ST_LineFromText.class.getSimpleName(), $(linestringColNames[0]))
                    .as(linestringColNames[0]),
                $(linestringColNames[1]));
    Row result = last(lineStringTable);

    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
  }

  @Test
  public void testLineStringFromText() {
    List<Row> data = createLineStringWKT(testDataSize);

    Table lineStringTable =
        createLineStringTextTable(testDataSize)
            .select(
                call(
                        Constructors.ST_LineStringFromText.class.getSimpleName(),
                        $(linestringColNames[0]))
                    .as(linestringColNames[0]),
                $(linestringColNames[1]));
    Row result = last(lineStringTable);

    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
  }

  @Test
  public void testPolygonFromText() {
    List<Row> data = createPolygonWKT(testDataSize);
    Row result = last(createPolygonTable(testDataSize));
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
  }

  @Test
  public void testGeomFromWKT() {
    List<Row> data = createPolygonWKT(testDataSize);
    Table wktTable = createTextTable(data, polygonColNames);
    Table geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromWKT.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    Row result = last(geomTable);
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());

    geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromWKT.class.getSimpleName(), $(polygonColNames[0]), 1111)
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    int sridActual =
        (int)
            last(geomTable.select(
                    call(Functions.ST_SRID.class.getSimpleName(), $(polygonColNames[0]))))
                .getField(0);
    assertEquals(1111, sridActual);
  }

  @Test
  public void testGeomFromEWKT() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of("SRID=123;MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", "multiline", 0L));

    Table geomTable = createTextTable(data, multilinestringColNames);
    geomTable =
        geomTable.select(
            call(Constructors.ST_GeomFromEWKT.class.getSimpleName(), $(multilinestringColNames[0]))
                .as(multilinestringColNames[0]),
            $(multilinestringColNames[1]));
    String result = first(geomTable).getFieldAs(0).toString();
    String expectedGeom = "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))";
    assertEquals(expectedGeom, result);
  }

  @Test
  public void testGeomFromText() {
    List<Row> data = createPolygonWKT(testDataSize);
    Table wktTable = createTextTable(data, polygonColNames);
    Table geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromText.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    Row result = last(geomTable);
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());

    geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromText.class.getSimpleName(), $(polygonColNames[0]), 1111)
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    int sridActual =
        (int)
            last(geomTable.select(
                    call(Functions.ST_SRID.class.getSimpleName(), $(polygonColNames[0]))))
                .getField(0);
    assertEquals(1111, sridActual);
  }

  @Test
  public void testGeometryFromText() {
    List<Row> data = createPolygonWKT(testDataSize);
    Table wktTable = createTextTable(data, polygonColNames);
    Table geomTable =
        wktTable.select(
            call(Constructors.ST_GeometryFromText.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    Row result = last(geomTable);
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());

    geomTable =
        wktTable.select(
            call(
                Constructors.ST_GeometryFromText.class.getSimpleName(),
                $(polygonColNames[0]),
                4326));
    int actual =
        (int)
            last(geomTable.select(call(Functions.ST_SRID.class.getSimpleName(), $("_c0"))))
                .getField(0);
    assertEquals(4326, actual);
  }

  @Test
  public void testPolygonFromEnvelope() {
    Double minX = 1.0;
    Double minY = 100.0;
    Double maxX = 2.0;
    Double maxY = 200.0;
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(minX, minY);
    coordinates[1] = new Coordinate(minX, maxY);
    coordinates[2] = new Coordinate(maxX, maxY);
    coordinates[3] = new Coordinate(maxX, minY);
    coordinates[4] = coordinates[0];
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry geom = geometryFactory.createPolygon(coordinates);
    assertEquals(
        last(tableEnv.sqlQuery("SELECT ST_PolygonFromEnvelope(1, 100, 2, 200)"))
            .getField(0)
            .toString(),
        geom.toString());
    assertEquals(
        last(tableEnv.sqlQuery("SELECT ST_PolygonFromEnvelope(1.0, 100.0, 2.0, 200.0)"))
            .getField(0)
            .toString(),
        geom.toString());
  }

  @Test
  public void testGeomFromGeoJSON() {
    List<Row> data = createPolygonGeoJSON(testDataSize);
    Table geojsonTable = createTextTable(data, polygonColNames);
    Table geomTable =
        geojsonTable.select(
            call(Constructors.ST_GeomFromGeoJSON.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    String result = last(geomTable).getFieldAs(0).toString();

    GeoJSONReader reader = new GeoJSONReader();
    String expectedGeoJSON = data.get(data.size() - 1).getFieldAs(0);
    String expectedGeom = reader.read(expectedGeoJSON).toText();

    assertEquals(expectedGeom, result);
  }

  @Test
  public void testGeomFromWKBBytes() {
    byte[] wkb =
        new byte[] {
          1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75, -42, -65,
          0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65
        };
    List<Row> data = new ArrayList<>();
    data.add(Row.of(wkb, "polygon"));
    TypeInformation<?>[] colTypes = {
      PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(polygonColNames, 0, 2));
    DataStream<Row> wkbDS = env.fromCollection(data).returns(typeInfo);
    Table wkbTable = tableEnv.fromDataStream(wkbDS, $(polygonColNames[0]), $(polygonColNames[1]));

    Table geomTable =
        wkbTable.select(
            call(Constructors.ST_GeomFromWKB.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    String result = first(geomTable).getFieldAs(0).toString();

    String expectedGeom =
        "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)";

    assertEquals(expectedGeom, result);
  }

  @Test
  public void testPointFromWKB() throws Exception {
    String hexWkb1 = "010100000000000000000000000000000000000000";
    byte[] wkbPoint1 = Hex.decodeHex(hexWkb1);
    String hexWkb2 = "010100000000000000000024400000000000002e40";
    byte[] wkbPoint2 = Hex.decodeHex(hexWkb2);
    String hexWkb3 =
        "01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf";
    byte[] wkbPolygon = Hex.decodeHex(hexWkb3);

    List<Row> data1 = new ArrayList<>();
    data1.add(Row.of(wkbPoint1));
    data1.add(Row.of(wkbPoint2));
    data1.add(Row.of(wkbPolygon));

    List<Row> data2 = new ArrayList<>();
    data2.add(Row.of(hexWkb1));
    data2.add(Row.of(hexWkb2));
    data2.add(Row.of(hexWkb3));

    TypeInformation<?>[] colTypes1 = {PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO};
    TypeInformation<?>[] colTypes2 = {BasicTypeInfo.STRING_TYPE_INFO};
    RowTypeInfo typeInfo1 = new RowTypeInfo(colTypes1, new String[] {"wkb"});
    RowTypeInfo typeInfo2 = new RowTypeInfo(colTypes2, new String[] {"wkb"});

    DataStream<Row> wkbDS1 = env.fromCollection(data1).returns(typeInfo1);
    DataStream<Row> wkbDS2 = env.fromCollection(data2).returns(typeInfo2);
    Table wkbTable1 = tableEnv.fromDataStream(wkbDS1, $("wkb"));
    Table wkbTable2 = tableEnv.fromDataStream(wkbDS2, $("wkb"));

    Table pointTable1 =
        wkbTable1.select(
            call(Constructors.ST_PointFromWKB.class.getSimpleName(), $("wkb")).as("point"));
    Table pointTable2 =
        wkbTable2.select(
            call(Constructors.ST_PointFromWKB.class.getSimpleName(), $("wkb")).as("point"));

    List<Row> results1 = TestBase.take(pointTable1, 3);
    assertEquals("POINT (0 0)", results1.get(0).getField(0).toString());
    assertEquals("POINT (10 15)", results1.get(1).getField(0).toString());
    assertNull(results1.get(2).getField(0));

    List<Row> results2 = TestBase.take(pointTable2, 3);
    assertEquals("POINT (0 0)", results2.get(0).getField(0).toString());
    assertEquals("POINT (10 15)", results2.get(1).getField(0).toString());
    assertNull(results2.get(2).getField(0));
  }

  @Test
  public void testLineFromWKB() throws DecoderException {
    String hexWkb1 =
        "010200000003000000000000000000000000000000000000000000000000000040000000000000004000000000000010400000000000001040";
    byte[] wkbLine1 = Hex.decodeHex(hexWkb1);
    String hexWkb2 =
        "010200000003000000000000000000000000000000000000000000000000407f400000000000407f400000000000407f4000000000000059c0";
    byte[] wkbLine2 = Hex.decodeHex(hexWkb2);
    String hexWkb3 =
        "01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf";
    byte[] wkbPolygon = Hex.decodeHex(hexWkb3);

    List<Row> data1 = Arrays.asList(Row.of(wkbLine1), Row.of(wkbLine2), Row.of(wkbPolygon));
    List<Row> data2 = Arrays.asList(Row.of(hexWkb1), Row.of(hexWkb2), Row.of(hexWkb3));

    TypeInformation<?>[] colTypes1 = {PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO};
    TypeInformation<?>[] colTypes2 = {BasicTypeInfo.STRING_TYPE_INFO};
    RowTypeInfo typeInfo1 = new RowTypeInfo(colTypes1, new String[] {"wkb"});
    RowTypeInfo typeInfo2 = new RowTypeInfo(colTypes2, new String[] {"wkb"});

    DataStream<Row> wkbDS1 = env.fromCollection(data1).returns(typeInfo1);
    Table wkbTable1 = tableEnv.fromDataStream(wkbDS1, $("wkb"));

    DataStream<Row> wkbDS2 = env.fromCollection(data2).returns(typeInfo2);
    Table wkbTable2 = tableEnv.fromDataStream(wkbDS2, $("wkb"));

    Table lineTable1 =
        wkbTable1.select(
            call(Constructors.ST_LineFromWKB.class.getSimpleName(), $("wkb")).as("point"));
    Table lineTable2 =
        wkbTable2.select(
            call(Constructors.ST_LineFromWKB.class.getSimpleName(), $("wkb")).as("point"));

    // Test with byte array
    List<Row> results1 = TestBase.take(lineTable1, 3);
    assertEquals("LINESTRING (0 0, 2 2, 4 4)", results1.get(0).getField(0).toString());
    assertEquals("LINESTRING (0 0, 500 500, 500 -100)", results1.get(1).getField(0).toString());
    assertNull(results1.get(2).getField(0));

    // Test with hex string
    List<Row> results2 = TestBase.take(lineTable2, 3);
    assertEquals("LINESTRING (0 0, 2 2, 4 4)", results2.get(0).getField(0).toString());
    assertEquals("LINESTRING (0 0, 500 500, 500 -100)", results2.get(1).getField(0).toString());
    assertNull(results2.get(2).getField(0));
  }

  @Test
  public void testLinestringFromWKB() throws DecoderException {
    String hexWkb1 =
        "010200000003000000000000000000000000000000000000000000000000000040000000000000004000000000000010400000000000001040";
    byte[] wkbLine1 = Hex.decodeHex(hexWkb1);
    String hexWkb2 =
        "010200000003000000000000000000000000000000000000000000000000407f400000000000407f400000000000407f4000000000000059c0";
    byte[] wkbLine2 = Hex.decodeHex(hexWkb2);
    String hexWkb3 =
        "01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf";
    byte[] wkbPolygon = Hex.decodeHex(hexWkb3);

    List<Row> data1 = Arrays.asList(Row.of(wkbLine1), Row.of(wkbLine2), Row.of(wkbPolygon));
    List<Row> data2 = Arrays.asList(Row.of(hexWkb1), Row.of(hexWkb2), Row.of(hexWkb3));

    TypeInformation<?>[] colTypes1 = {PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO};
    TypeInformation<?>[] colTypes2 = {BasicTypeInfo.STRING_TYPE_INFO};
    RowTypeInfo typeInfo1 = new RowTypeInfo(colTypes1, new String[] {"wkb"});
    RowTypeInfo typeInfo2 = new RowTypeInfo(colTypes2, new String[] {"wkb"});

    DataStream<Row> wkbDS1 = env.fromCollection(data1).returns(typeInfo1);
    Table wkbTable1 = tableEnv.fromDataStream(wkbDS1, $("wkb"));

    DataStream<Row> wkbDS2 = env.fromCollection(data2).returns(typeInfo2);
    Table wkbTable2 = tableEnv.fromDataStream(wkbDS2, $("wkb"));

    Table lineTable1 =
        wkbTable1.select(
            call(Constructors.ST_LinestringFromWKB.class.getSimpleName(), $("wkb")).as("point"));
    Table lineTable2 =
        wkbTable2.select(
            call(Constructors.ST_LinestringFromWKB.class.getSimpleName(), $("wkb")).as("point"));

    // Test with byte array
    List<Row> results1 = TestBase.take(lineTable1, 3);
    assertEquals("LINESTRING (0 0, 2 2, 4 4)", results1.get(0).getField(0).toString());
    assertEquals("LINESTRING (0 0, 500 500, 500 -100)", results1.get(1).getField(0).toString());
    assertNull(results1.get(2).getField(0));

    // Test with hex string
    List<Row> results2 = TestBase.take(lineTable2, 3);
    assertEquals("LINESTRING (0 0, 2 2, 4 4)", results2.get(0).getField(0).toString());
    assertEquals("LINESTRING (0 0, 500 500, 500 -100)", results2.get(1).getField(0).toString());
    assertNull(results2.get(2).getField(0));
  }

  @Test
  public void testGeomFromEWKB() {
    byte[] wkb =
        new byte[] {
          1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75, -42, -65,
          0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65
        };
    List<Row> data = new ArrayList<>();
    data.add(Row.of(wkb, "polygon"));
    TypeInformation<?>[] colTypes = {
      PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(polygonColNames, 0, 2));
    DataStream<Row> wkbDS = env.fromCollection(data).returns(typeInfo);
    Table wkbTable = tableEnv.fromDataStream(wkbDS, $(polygonColNames[0]), $(polygonColNames[1]));

    Table geomTable =
        wkbTable.select(
            call(Constructors.ST_GeomFromEWKB.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    String result = first(geomTable).getFieldAs(0).toString();

    String expectedGeom =
        "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)";

    assertEquals(expectedGeom, result);
  }

  @Test
  public void testGeomFromGeoHash() {
    Integer precision = 2;
    List<Row> data = new ArrayList<>();
    data.add(Row.of("2131s12fd", "polygon", 0L));

    Table geohashTable = createTextTable(data, polygonColNames);
    Table geomTable =
        geohashTable.select(
            call(
                    Constructors.ST_GeomFromGeoHash.class.getSimpleName(),
                    $(polygonColNames[0]),
                    precision)
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    String result = first(geomTable).getFieldAs(0).toString();
    String expectedGeom =
        "POLYGON ((-180 -39.375, -180 -33.75, -168.75 -33.75, -168.75 -39.375, -180 -39.375))";

    assertEquals(expectedGeom, result);
  }

  @Test
  public void testPointFromGeoHash() {
    String actual =
        first(
                tableEnv
                    .sqlQuery("SELECT 's00twy01mt' as geohash")
                    .select(
                        call(
                            Constructors.ST_PointFromGeoHash.class.getSimpleName(),
                            $("geohash"),
                            4)))
            .getField(0)
            .toString();
    assertEquals("POINT (0.87890625 0.966796875)", actual);

    actual =
        first(
                tableEnv
                    .sqlQuery("SELECT 's00twy01mt' as geohash")
                    .select(
                        call(Constructors.ST_PointFromGeoHash.class.getSimpleName(), $("geohash"))))
            .getField(0)
            .toString();
    assertEquals("POINT (0.9999972581863403 0.9999999403953552)", actual);
  }

  @Test
  public void testGeomFromGeoHashNullPrecision() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of("2131s12fd", "polygon", 0L));

    Table geohashTable = createTextTable(data, polygonColNames);
    Table geomTable =
        geohashTable.select(
            call(Constructors.ST_GeomFromGeoHash.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    String result = first(geomTable).getFieldAs(0).toString();
    String expectedGeom =
        "POLYGON ((-178.4168529510498 -37.69778251647949, -178.4168529510498 -37.697739601135254, -178.41681003570557 -37.697739601135254, -178.41681003570557 -37.69778251647949, -178.4168529510498 -37.69778251647949))";

    assertEquals(expectedGeom, result);
  }

  @Test
  public void testGeomFromGML() {
    List<Row> data = new ArrayList<>();
    String gml =
        "<gml:Polygon>\n"
            + "  <gml:outerBoundaryIs>\n"
            + "    <gml:LinearRing>\n"
            + "      <gml:coordinates>\n"
            + "        0.0,0.0 0.0,1.0 1.0,1.0 1.0,0.0 0.0,0.0\n"
            + "      </gml:coordinates>\n"
            + "    </gml:LinearRing>\n"
            + "  </gml:outerBoundaryIs>\n"
            + "</gml:Polygon>";
    data.add(Row.of(gml, "polygon", 0L));

    Table wktTable = createTextTable(data, polygonColNames);
    Table geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromGML.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    assertTrue(first(geomTable).getField(0) instanceof Polygon);
  }

  @Test
  public void testGeomFromKML() {
    List<Row> data = new ArrayList<>();
    String kml =
        "<Polygon>\n"
            + "  <outerBoundaryIs>\n"
            + "    <LinearRing>\n"
            + "      <coordinates>0.0,0.0 0.0,1.0 1.0,1.0 1.0,0.0 0.0,0.0</coordinates>\n"
            + "    </LinearRing>\n"
            + "  </outerBoundaryIs>\n"
            + "</Polygon>";
    data.add(Row.of(kml, "polygon", 0L));

    Table wktTable = createTextTable(data, polygonColNames);
    Table geomTable =
        wktTable.select(
            call(Constructors.ST_GeomFromKML.class.getSimpleName(), $(polygonColNames[0]))
                .as(polygonColNames[0]),
            $(polygonColNames[1]));
    assertTrue(first(geomTable).getField(0) instanceof Polygon);
  }

  @Test
  public void testMPolygonFromText() {
    List<Row> data = createMultiPolygonText(testDataSize);
    Row result = last(createMultiPolygonTable(testDataSize));
    assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
  }

  @Test
  public void testMLineFromText() {
    List<Row> data = new ArrayList<>();
    data.add(Row.of("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", "multiline", 0L));

    Table geohashTable = createTextTable(data, multilinestringColNames);
    Table geomTable =
        geohashTable.select(
            call(Constructors.ST_MLineFromText.class.getSimpleName(), $(multilinestringColNames[0]))
                .as(multilinestringColNames[0]),
            $(multilinestringColNames[1]));
    String result = first(geomTable).getFieldAs(0).toString();
    String expectedGeom = "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))";
    assertEquals(expectedGeom, result);
  }

  @Test
  public void testMPointFromText() {
    Table tbl =
        tableEnv.sqlQuery("SELECT 'MULTIPOINT ((10 10), (20 20), (30 30))' AS coll, 4326 as srid");
    String actualColl =
        first(tbl.select(call(Constructors.ST_MPointFromText.class.getSimpleName(), $("coll"))))
            .getFieldAs(0)
            .toString();
    String expectedColl = "MULTIPOINT ((10 10), (20 20), (30 30))";
    assertEquals(expectedColl, actualColl);

    actualColl =
        first(
                tbl.select(
                    call(
                        Constructors.ST_MPointFromText.class.getSimpleName(),
                        $("coll"),
                        $("srid"))))
            .getFieldAs(0)
            .toString();
    assertEquals(expectedColl, actualColl);
    int actualSrid =
        first(
                tbl.select(
                        call(
                            Constructors.ST_MPointFromText.class.getSimpleName(),
                            $("coll"),
                            $("srid")))
                    .as("geom")
                    .select(call(Functions.ST_SRID.class.getSimpleName(), $("geom"))))
            .getFieldAs(0);
    assertEquals(4326, actualSrid);
  }

  @Test
  public void testGeomCollFromText() {
    Table tbl =
        tableEnv.sqlQuery(
            "SELECT 'GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90),POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))' AS coll, 4326 as srid");
    String actualColl =
        first(tbl.select(call(Constructors.ST_GeomCollFromText.class.getSimpleName(), $("coll"))))
            .getFieldAs(0)
            .toString();
    String expectedColl =
        "GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))";
    assertEquals(expectedColl, actualColl);

    actualColl =
        first(
                tbl.select(
                    call(
                        Constructors.ST_GeomCollFromText.class.getSimpleName(),
                        $("coll"),
                        $("srid"))))
            .getFieldAs(0)
            .toString();
    assertEquals(expectedColl, actualColl);
    int actualSrid =
        first(
                tbl.select(
                        call(
                            Constructors.ST_GeomCollFromText.class.getSimpleName(),
                            $("coll"),
                            $("srid")))
                    .as("geom")
                    .select(call(Functions.ST_SRID.class.getSimpleName(), $("geom"))))
            .getFieldAs(0);
    assertEquals(4326, actualSrid);
  }
}
