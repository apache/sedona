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

import static org.apache.flink.table.api.Expressions.*;
import static org.junit.Assert.assertEquals;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Functions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;

public class AggregatorTest extends TestBase {
  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  @Test
  public void testEnvelop_Aggr() {
    Table pointTable = createPointTable(testDataSize);
    Table result = pointTable.select(call("ST_Envelope_Aggr", $(pointColNames[0])));
    Row last = last(result);
    assertEquals(
        String.format(
            "POLYGON ((0 0, 0 %s, %s %s, %s 0, 0 0))",
            testDataSize - 1, testDataSize - 1, testDataSize - 1, testDataSize - 1),
        last.getField(0).toString());
  }

  @Test
  public void testKNN() {
    Table pointTable = createPointTable(testDataSize);
    pointTable =
        pointTable.select(
            $(pointColNames[0]),
            call(
                    Functions.ST_Distance.class.getSimpleName(),
                    $(pointColNames[0]),
                    call("ST_GeomFromWKT", "POINT (0 0)"))
                .as("distance"));
    tableEnv.createTemporaryView(pointTableName, pointTable);
    Table resultTable =
        tableEnv.sqlQuery(
            "SELECT distance, "
                + pointColNames[0]
                + " "
                + "FROM ("
                + "SELECT *, ROW_NUMBER() OVER (ORDER BY distance ASC) AS row_num "
                + "FROM "
                + pointTableName
                + ")"
                + "WHERE row_num <= 5");
    assertEquals(0.0, first(resultTable).getField(0));
    assertEquals(5.656854249492381, last(resultTable).getField(0));
  }

  @Test
  public void testIntersection_Aggr() {
    Table polygonTable = createPolygonOverlappingTable(testDataSize);
    Table result = polygonTable.select(call("ST_Intersection_Aggr", $(polygonColNames[0])));
    Row last = last(result);
    assertEquals("LINESTRING EMPTY", last.getField(0).toString());

    polygonTable = createPolygonOverlappingTable(3);
    result = polygonTable.select(call("ST_Intersection_Aggr", $(polygonColNames[0])));
    last = last(result);
    assertEquals("LINESTRING (1 1, 1 0)", last.getField(0).toString());
  }

  @Test
  public void testUnion_Aggr() {
    Table polygonTable = createPolygonOverlappingTable(testDataSize);
    Table result = polygonTable.select(call("ST_Union_Aggr", $(polygonColNames[0])));
    Row last = last(result);
    assertEquals(1001, ((Polygon) last.getField(0)).getArea(), 0);
  }
}
