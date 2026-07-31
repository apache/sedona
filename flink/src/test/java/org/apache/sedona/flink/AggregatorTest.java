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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geometryObjects.Box2D;
import org.apache.sedona.common.geometryObjects.Box3D;
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
  public void testExtent() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW extent_view AS "
            + "SELECT ST_GeomFromWKT(wkt) as geom FROM ("
            + "VALUES ('POINT (1 2)'), ('POINT (4 5)'), ('LINESTRING (-3 0, 0 0)')"
            + ") AS t(wkt)");
    Table result = tableEnv.sqlQuery("SELECT ST_Extent(geom) FROM extent_view");
    Row last = last(result);
    Box2D bbox = (Box2D) last.getField(0);
    assertEquals(-3.0, bbox.getXMin(), 0.0);
    assertEquals(0.0, bbox.getYMin(), 0.0);
    assertEquals(4.0, bbox.getXMax(), 0.0);
    assertEquals(5.0, bbox.getYMax(), 0.0);
  }

  @Test
  public void test3DExtent() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW extent3d_view AS "
            + "SELECT ST_GeomFromWKT(wkt) as geom FROM ("
            + "VALUES ('POINT Z (1 2 3)'), ('POINT Z (4 5 -1)'), ('LINESTRING (-3 0, 0 0)')"
            + ") AS t(wkt)");
    Table result = tableEnv.sqlQuery("SELECT ST_3DExtent(geom) FROM extent3d_view");
    Row last = last(result);
    Box3D bbox = (Box3D) last.getField(0);
    assertEquals(-3.0, bbox.getXMin(), 0.0);
    assertEquals(0.0, bbox.getYMin(), 0.0);
    // z-min is -1.0 from POINT Z (4 5 -1); the XY-only linestring folds to z = 0, which sits
    // between the -1 and 3 of the two POINT Z rows and so does not move either Z bound.
    assertEquals(-1.0, bbox.getZMin(), 0.0);
    assertEquals(4.0, bbox.getXMax(), 0.0);
    assertEquals(5.0, bbox.getYMax(), 0.0);
    assertEquals(3.0, bbox.getZMax(), 0.0);
  }

  @Test
  public void test3DExtent_EmptyAndNullGeometries() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW null_extent3d_view AS "
            + "SELECT ST_GeomFromWKT(wkt) as geom FROM ("
            + "VALUES (CAST(NULL AS STRING)), ('POINT EMPTY'), ('POLYGON EMPTY')"
            + ") AS t(wkt)");
    Table result = tableEnv.sqlQuery("SELECT ST_3DExtent(geom) FROM null_extent3d_view");
    Row last = last(result);
    assertNull(last.getField(0));
  }

  @Test
  public void testExtent_EmptyAndNullGeometries() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW null_extent_view AS "
            + "SELECT ST_GeomFromWKT(wkt) as geom FROM ("
            + "VALUES (CAST(NULL AS STRING)), ('POINT EMPTY'), ('POLYGON EMPTY')"
            + ") AS t(wkt)");
    Table result = tableEnv.sqlQuery("SELECT ST_Extent(geom) FROM null_extent_view");
    Row last = last(result);
    assertNull(last.getField(0));
  }

  @Test
  public void testEnvelope_Aggr_EmptyGeometries() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW empty_geom_view AS "
            + "SELECT ST_GeomFromWKT(wkt) as geom FROM ("
            + "VALUES ('POINT EMPTY'), ('LINESTRING EMPTY'), ('POLYGON EMPTY')"
            + ") AS t(wkt)");
    Table result = tableEnv.sqlQuery("SELECT ST_Envelope_Aggr(geom) FROM empty_geom_view");
    Row last = last(result);
    assertNull(last.getField(0));
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

  // Test aliases for *_Aggr functions with *_Agg suffix
  @Test
  public void testEnvelop_Agg_Alias() {
    Table pointTable = createPointTable(testDataSize);
    Table result = pointTable.select(call("ST_Envelope_Agg", $(pointColNames[0])));
    Row last = last(result);
    assertEquals(
        String.format(
            "POLYGON ((0 0, 0 %s, %s %s, %s 0, 0 0))",
            testDataSize - 1, testDataSize - 1, testDataSize - 1, testDataSize - 1),
        last.getField(0).toString());
  }

  @Test
  public void testIntersection_Agg_Alias() {
    Table polygonTable = createPolygonOverlappingTable(testDataSize);
    Table result = polygonTable.select(call("ST_Intersection_Agg", $(polygonColNames[0])));
    Row last = last(result);
    assertEquals("LINESTRING EMPTY", last.getField(0).toString());

    polygonTable = createPolygonOverlappingTable(3);
    result = polygonTable.select(call("ST_Intersection_Agg", $(polygonColNames[0])));
    last = last(result);
    assertEquals("LINESTRING (1 1, 1 0)", last.getField(0).toString());
  }

  @Test
  public void testUnion_Agg_Alias() {
    Table polygonTable = createPolygonOverlappingTable(testDataSize);
    Table result = polygonTable.select(call("ST_Union_Agg", $(polygonColNames[0])));
    Row last = last(result);
    assertEquals(1001, ((Polygon) last.getField(0)).getArea(), 0);
  }

  @Test
  public void testCollect_Aggr_GeometryAndAlias() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW collect_geom_view AS "
            + "SELECT ST_GeomFromWKT(wkt) AS geom FROM ("
            + "VALUES ('POINT (1 2)'), (CAST(NULL AS STRING)), "
            + "('POINT (1 2)'), ('POINT (3 4)')) AS t(wkt)");
    Row result =
        last(
            tableEnv.sqlQuery(
                "SELECT ST_Collect_Aggr(geom), ST_Collect_Agg(geom) FROM collect_geom_view"));
    assertEquals("MULTIPOINT ((1 2), (1 2), (3 4))", result.getField(0).toString());
    assertEquals(result.getField(0).toString(), result.getField(1).toString());
  }

  @Test
  public void testCollect_Agg_GeographyFeedsConvexHull() throws Exception {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW collect_geog_view AS "
            + "SELECT CASE WHEN wkt IS NULL THEN NULL "
            + "ELSE ST_GeogFromWKT(wkt, 4326) END AS geog FROM ("
            + "VALUES ('POINT (170 10)'), (CAST(NULL AS STRING)), "
            + "('POINT (-170 10)'), ('POINT (180 30)'), ('POINT (170 10)')) AS t(wkt)");

    Row result =
        last(
            tableEnv.sqlQuery(
                "SELECT ST_Collect_Aggr(geog), ST_Collect_Agg(geog), "
                    + "ST_ConvexHull(ST_Collect_Agg(geog)) FROM collect_geog_view"));
    Geography expectedCollection =
        org.apache.sedona.common.geography.Functions.createMultiGeography(
            new Geography[] {
              Constructors.geogFromWKT("POINT (170 10)", 4326),
              Constructors.geogFromWKT("POINT (-170 10)", 4326),
              Constructors.geogFromWKT("POINT (180 30)", 4326),
              Constructors.geogFromWKT("POINT (170 10)", 4326)
            });
    Geography expectedHull =
        org.apache.sedona.common.geography.Functions.convexHull(expectedCollection);

    assertTrue(
        Constructors.geogToGeometry(expectedCollection)
            .equalsNorm(Constructors.geogToGeometry((Geography) result.getField(0))));
    assertTrue(
        Constructors.geogToGeometry(expectedCollection)
            .equalsNorm(Constructors.geogToGeometry((Geography) result.getField(1))));
    assertEquals(expectedHull.toEWKT(), ((Geography) result.getField(2)).toEWKT());
  }

  @Test
  public void testCollect_Agg_AllNullReturnsNull() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW collect_null_geog_view AS "
            + "SELECT CASE WHEN wkt IS NULL THEN NULL "
            + "ELSE ST_GeogFromWKT(wkt, 4326) END AS geog FROM ("
            + "VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING))) AS t(wkt)");
    Row result = last(tableEnv.sqlQuery("SELECT ST_Collect_Agg(geog) FROM collect_null_geog_view"));
    assertNull(result.getField(0));
  }

  @Test
  public void testCollect_Agg_RejectsMixedGeographySrids() {
    tableEnv.executeSql(
        "CREATE OR REPLACE TEMPORARY VIEW collect_mixed_srid_view AS "
            + "SELECT ST_GeogFromWKT(wkt, srid) AS geog FROM ("
            + "VALUES ('POINT (1 2)', 4326), ('POINT (3 4)', 3857)) AS t(wkt, srid)");
    try {
      last(tableEnv.sqlQuery("SELECT ST_Collect_Agg(geog) FROM collect_mixed_srid_view"));
      fail("Expected ST_Collect_Agg to reject mixed Geography SRIDs");
    } catch (Exception e) {
      String messages = messageChain(e);
      assertTrue(
          "Expected a mixed-SRID error, got: " + messages,
          messages.contains("requires all Geography values to have the same SRID"));
    }
  }

  private static String messageChain(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null; c = c.getCause()) {
      sb.append(c.getMessage()).append(" | ");
    }
    return sb.toString();
  }
}
