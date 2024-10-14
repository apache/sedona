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
