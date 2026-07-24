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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.flink.table.api.Table;
import org.apache.sedona.flink.expressions.Predicates;
import org.junit.BeforeClass;
import org.junit.Test;

public class PredicateTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  @Test
  public void testIntersectsOnBox2D() {
    Table t =
        tableEnv.sqlQuery(
            "WITH boxes AS ("
                + " SELECT ST_Box2D(ST_GeomFromWKT('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')) AS a,"
                + " ST_Box2D(ST_GeomFromWKT('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))')) AS overlap,"
                + " ST_Box2D(ST_GeomFromWKT('POLYGON((6 6, 6 7, 7 7, 7 6, 6 6))')) AS disjoint)"
                + " SELECT ST_Intersects(a, overlap), ST_Intersects(a, disjoint) FROM boxes");
    org.apache.flink.types.Row row = first(t);
    assertEquals(true, row.getField(0));
    assertEquals(false, row.getField(1));
  }

  @Test
  public void testContainsOnBox2D() {
    Table t =
        tableEnv.sqlQuery(
            "WITH boxes AS ("
                + " SELECT ST_Box2D(ST_GeomFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) AS outer_box,"
                + " ST_Box2D(ST_GeomFromWKT('POLYGON((2 2, 2 5, 5 5, 5 2, 2 2))')) AS inner_box,"
                + " ST_Box2D(ST_GeomFromWKT('POLYGON((5 5, 5 11, 11 11, 11 5, 5 5))')) AS overlap)"
                + " SELECT ST_Contains(outer_box, inner_box), ST_Contains(outer_box, overlap) FROM boxes");
    org.apache.flink.types.Row row = first(t);
    assertEquals(true, row.getField(0));
    assertEquals(false, row.getField(1));
  }

  @Test
  public void testIntersectsOnBox3D() {
    Table t =
        tableEnv.sqlQuery(
            "WITH boxes AS ("
                + " SELECT ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(5, 5, 5)) AS a,"
                + " ST_3DMakeBox(ST_PointZ(3, 3, 3), ST_PointZ(7, 7, 7)) AS overlap,"
                + " ST_3DMakeBox(ST_PointZ(0, 0, 10), ST_PointZ(5, 5, 11)) AS zdisjoint,"
                + " ST_3DMakeBox(ST_PointZ(6, 6, 6), ST_PointZ(7, 7, 7)) AS disjoint)"
                + " SELECT ST_Intersects(a, overlap), ST_Intersects(a, zdisjoint),"
                + " ST_Intersects(a, disjoint) FROM boxes");
    org.apache.flink.types.Row row = first(t);
    assertEquals(true, row.getField(0));
    // Overlaps in XY but separated in Z — Box3D intersection must reject it.
    assertEquals(false, row.getField(1));
    assertEquals(false, row.getField(2));
  }

  @Test
  public void testContainsOnBox3D() {
    Table t =
        tableEnv.sqlQuery(
            "WITH boxes AS ("
                + " SELECT ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(10, 10, 10)) AS outer_box,"
                + " ST_3DMakeBox(ST_PointZ(2, 2, 2), ST_PointZ(5, 5, 5)) AS inner_box,"
                + " ST_3DMakeBox(ST_PointZ(2, 2, 2), ST_PointZ(5, 5, 99)) AS zout)"
                + " SELECT ST_Contains(outer_box, inner_box), ST_Contains(outer_box, zout)"
                + " FROM boxes");
    org.apache.flink.types.Row row = first(t);
    assertEquals(true, row.getField(0));
    // Contained in XY but pokes out of the Z range — not contained in 3D.
    assertEquals(false, row.getField(1));
  }

  @Test
  public void test3DDWithin() {
    // Two POINT Z 3 units apart in Z only.
    Table within =
        tableEnv.sqlQuery(
            "SELECT ST_3DDWithin(ST_PointZ(0, 0, 0), ST_PointZ(0, 0, 3), 3.0),"
                + " ST_3DDWithin(ST_PointZ(0, 0, 0), ST_PointZ(0, 0, 3), 2.9)");
    org.apache.flink.types.Row row = first(within);
    assertEquals(true, row.getField(0));
    assertEquals(false, row.getField(1));

    // Box3D-on-Box3D: faces 4 units apart in Z.
    Table boxes =
        tableEnv.sqlQuery(
            "WITH b AS ("
                + " SELECT ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(1, 1, 1)) AS a,"
                + " ST_3DMakeBox(ST_PointZ(0, 0, 5), ST_PointZ(1, 1, 6)) AS far)"
                + " SELECT ST_3DDWithin(a, far, 4.0), ST_3DDWithin(a, far, 3.9) FROM b");
    org.apache.flink.types.Row boxRow = first(boxes);
    assertEquals(true, boxRow.getField(0));
    assertEquals(false, boxRow.getField(1));

    // NULL distance propagates to NULL rather than throwing on autounbox.
    Table nullDist =
        tableEnv.sqlQuery(
            "SELECT ST_3DDWithin(ST_PointZ(0, 0, 0), ST_PointZ(0, 0, 3),"
                + " CAST(NULL AS DOUBLE)) AS geom_null,"
                + " ST_3DDWithin(ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(1, 1, 1)),"
                + " ST_3DMakeBox(ST_PointZ(0, 0, 5), ST_PointZ(1, 1, 6)),"
                + " CAST(NULL AS DOUBLE)) AS box_null");
    org.apache.flink.types.Row nullRow = first(nullDist);
    assertNull(nullRow.getField(0));
    assertNull(nullRow.getField(1));
  }

  @Test
  public void testIntersects() {
    Table pointTable = createPointTable(testDataSize);
    String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        pointTable.filter(call("ST_Intersects", call("ST_GeomFromWkt", polygon), $("geom_point")));
    assertEquals(1, count(result));
  }

  @Test
  public void testDisjoint() {
    Table pointTable = createPointTable(testDataSize);
    String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        pointTable.filter(call("ST_Disjoint", call("ST_GeomFromWkt", polygon), $("geom_point")));
    assertEquals(999, count(result));
  }

  @Test
  public void testContains() {
    Table pointTable = createPointTable(testDataSize);
    String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        pointTable.filter(call("ST_Contains", call("ST_GeomFromWkt", polygon), $("geom_point")));
    assertEquals(1, count(result));
  }

  @Test
  public void testWithin() {
    Table pointTable = createPointTable(testDataSize);
    String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        pointTable.filter(call("ST_Within", $("geom_point"), call("ST_GeomFromWkt", polygon)));
    assertEquals(1, count(result));
  }

  @Test
  public void testCovers() {
    Table pointTable = createPointTable(testDataSize);
    String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        pointTable.filter(call("ST_Covers", call("ST_GeomFromWkt", polygon), $("geom_point")));
    assertEquals(1, count(result));
  }

  @Test
  public void testCoveredBy() {
    Table pointTable = createPointTable(testDataSize);
    String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        pointTable.filter(call("ST_CoveredBy", $("geom_point"), call("ST_GeomFromWkt", polygon)));
    assertEquals(1, count(result));
  }

  @Test
  public void testCrosses() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('MULTIPOINT((0 0), (2 2))') AS g1, ST_GeomFromWKT('LINESTRING(-1 -1, 1 1)') as g2");
    table = table.select(call(Predicates.ST_Crosses.class.getSimpleName(), $("g1"), $("g2")));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(true, actual);
  }

  @Test
  public void testEquals() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 2)') AS g1, ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)') as g2");
    table = table.select(call(Predicates.ST_Equals.class.getSimpleName(), $("g1"), $("g2")));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(true, actual);
  }

  @Test
  public void testEqualsExact() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT"
                + " ST_EqualsExact(ST_Point(0.0, 0.0), ST_Point(0.03, 0.04), 0.05)"
                + " AS within_tolerance,"
                + " ST_EqualsExact(ST_Point(0.0, 0.0), ST_Point(0.03, 0.04), 0.049)"
                + " AS outside_tolerance,"
                + " ST_EqualsExact(ST_GeomFromWKT(CAST(NULL AS STRING)),"
                + " ST_Point(0.0, 0.0), 0.0) AS null_left,"
                + " ST_EqualsExact(ST_Point(0.0, 0.0),"
                + " ST_GeomFromWKT(CAST(NULL AS STRING)), 0.0) AS null_right,"
                + " ST_EqualsExact(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0),"
                + " CAST(NULL AS DOUBLE)) AS null_tolerance");
    org.apache.flink.types.Row row = first(table);
    assertEquals(true, row.getField(0));
    assertEquals(false, row.getField(1));
    assertNull(row.getField(2));
    assertNull(row.getField(3));
    assertNull(row.getField(4));
  }

  @Test
  public void testOrderingEquals() {
    Table lineStringTable = createLineStringTable(testDataSize);
    String lineString = createLineStringWKT(testDataSize).get(0).getField(0).toString();
    Table result =
        lineStringTable.filter(
            call("ST_OrderingEquals", call("ST_GeomFromWkt", lineString), $("geom_linestring")));
    assertEquals(1, count(result));
  }

  @Test
  public void testOverlaps() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 2)') AS g1, ST_GeomFromWKT('LINESTRING (1 1, 3 3)') as g2");
    table = table.select(call(Predicates.ST_Overlaps.class.getSimpleName(), $("g1"), $("g2")));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(true, actual);
  }

  @Test
  public void testTouches() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS g1, ST_GeomFromWKT('LINESTRING (0 0, 1 1)') as g2");
    table = table.select(call(Predicates.ST_Touches.class.getSimpleName(), $("g1"), $("g2")));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(true, actual);
  }

  @Test
  public void testRelate() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('LINESTRING (1 1, 5 5)') AS g1, ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))') as g2, '1010F0212' as im");
    String actual =
        (String)
            first(table.select(call(Predicates.ST_Relate.class.getSimpleName(), $("g1"), $("g2"))))
                .getField(0);
    assertEquals("1010F0212", actual);

    Boolean actualBoolean =
        (Boolean)
            first(
                    table.select(
                        call(
                            Predicates.ST_Relate.class.getSimpleName(), $("g1"), $("g2"), $("im"))))
                .getField(0);
    assertEquals(true, actualBoolean);
  }

  @Test
  public void testRelateMatch() {
    Table table = tableEnv.sqlQuery("SELECT '101202FFF' as matrix1, 'TTTTTTFFF' as matrix2");
    Boolean actual =
        (Boolean)
            first(
                    table.select(
                        call(
                            Predicates.ST_RelateMatch.class.getSimpleName(),
                            $("matrix1"),
                            $("matrix2"))))
                .getField(0);
    assertEquals(true, actual);
  }

  @Test
  public void testDWithin() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (0 0)') as origin, ST_GeomFromWKT('POINT (1 0)') as p1");
    table =
        table.select(call(Predicates.ST_DWithin.class.getSimpleName(), $("origin"), $("p1"), 1));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(true, actual);
  }

  @Test
  public void testDWithinNullArguments() {
    // Any NULL argument propagates to NULL rather than NPE-ing on autounbox / null geometry.
    Table t =
        tableEnv.sqlQuery(
            "SELECT ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 0)'),"
                + " CAST(NULL AS DOUBLE)) AS null_dist,"
                + " ST_DWithin(ST_GeomFromWKT(CAST(NULL AS STRING)),"
                + " ST_GeomFromWKT('POINT (1 0)'), 1.0) AS null_geom,"
                + " ST_DWithin(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 0)'), 1.0,"
                + " CAST(NULL AS BOOLEAN)) AS null_sphere");
    org.apache.flink.types.Row row = first(t);
    assertNull(row.getField(0));
    assertNull(row.getField(1));
    assertNull(row.getField(2));
  }

  @Test
  public void testDWithinFailure() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (0 0)') as origin, ST_GeomFromWKT('POINT (5 0)') as p1");
    table =
        table.select(call(Predicates.ST_DWithin.class.getSimpleName(), $("origin"), $("p1"), 2));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(false, actual);
  }

  @Test
  public void testDWithinSphere() {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_GeomFromWKT('POINT (-122.335167 47.608013)') as seattle, ST_GeomFromWKT('POINT (-73.935242 40.730610)') as ny");
    table =
        table.select(
            call(
                Predicates.ST_DWithin.class.getSimpleName(),
                $("seattle"),
                $("ny"),
                3500 * 1e3,
                true));
    Boolean actual = (Boolean) first(table).getField(0);
    assertEquals(false, actual);
  }
}
