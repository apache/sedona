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
import static org.apache.flink.table.api.Expressions.lit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.flink.expressions.Functions;
import org.apache.sedona.flink.expressions.geography.GeographyConstructors;
import org.junit.BeforeClass;
import org.junit.Test;

public class GeographyFunctionTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  /**
   * A single-row table whose column "geog" is the geography parsed from {@code wkt} (SRID 4326).
   */
  private Table geogTable(String wkt) {
    return geogTable(wkt, 4326);
  }

  private Table geogTable(String wkt, int srid) {
    RowTypeInfo ti =
        new RowTypeInfo(
            new TypeInformation<?>[] {BasicTypeInfo.STRING_TYPE_INFO}, new String[] {"v"});
    DataStream<Row> ds = env.fromCollection(Collections.singletonList(Row.of(wkt))).returns(ti);
    return tableEnv
        .fromDataStream(ds)
        .select(
            call(GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(), $("v"), lit(srid))
                .as("geog"));
  }

  private Object eval(String wkt, ApiExpression call) {
    return first(geogTable(wkt).select(call.as("o"))).getFieldAs("o");
  }

  private Object eval(String wkt, int srid, ApiExpression call) {
    return first(geogTable(wkt, srid).select(call.as("o"))).getFieldAs("o");
  }

  @Test
  public void testArea() throws Exception {
    String wkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))";
    Object out = eval(wkt, call(Functions.ST_Area.class.getSimpleName(), $("geog")));
    double expected =
        org.apache.sedona.common.geography.Functions.area(Constructors.geogFromWKT(wkt, 4326));
    assertEquals(expected, (Double) out, 1e-6);
    // sanity: geodesic area of a ~1deg box near the equator is on the order of 1e10 m^2
    assertTrue((Double) out > 1e9);
  }

  @Test
  public void testLength() throws Exception {
    String wkt = "LINESTRING (0 0, 0 1)";
    Object out = eval(wkt, call(Functions.ST_Length.class.getSimpleName(), $("geog")));
    double expected =
        org.apache.sedona.common.geography.Functions.length(Constructors.geogFromWKT(wkt, 4326));
    assertEquals(expected, (Double) out, 1e-6);
  }

  @Test
  public void testMakeLineDeduplicatesLineStringSeam() throws Exception {
    Table table =
        tableEnv.sqlQuery(
            "SELECT line, ST_AsEWKT(line) AS ewkt FROM ("
                + "SELECT ST_MakeLine("
                + "ST_GeogFromWKT('LINESTRING (0 0, 1 0)', 4326), "
                + "ST_GeogFromWKT('LINESTRING (1 0, 2 0)', 4326)) AS line)");

    Row row = first(table);
    Geography line = row.getFieldAs("line");
    assertEquals(
        "LINESTRING (0 0, 1 0, 2 0)", org.apache.sedona.common.geography.Functions.asText(line));
    assertEquals("SRID=4326; LINESTRING (0 0, 1 0, 2 0)", row.getFieldAs("ewkt"));
    assertEquals(3, org.apache.sedona.common.geography.Functions.nPoints(line));
    assertEquals(4326, line.getSRID());
  }

  @Test
  public void testMakeLinePreservesCoincidentPointsAndFirstSRID() throws Exception {
    Table table =
        tableEnv.sqlQuery(
            "SELECT line, ST_AsText(line) AS wkt, ST_AsEWKT(line) AS ewkt, "
                + "ST_Centroid(line) AS centroid, ST_Envelope(line, FALSE) AS envelope, "
                + "ST_Envelope(line, TRUE) AS split_envelope FROM ("
                + "SELECT ST_MakeLine("
                + "ST_GeogFromWKT('POINT (12 34)', 4326), "
                + "ST_GeogFromWKT('POINT (12 34)', 3857)) AS line)");

    Row row = first(table);
    Geography line = row.getFieldAs("line");
    assertEquals("LINESTRING (12 34, 12 34)", row.getFieldAs("wkt"));
    assertEquals("SRID=4326; LINESTRING (12 34, 12 34)", row.getFieldAs("ewkt"));
    Geography centroid = row.getFieldAs("centroid");
    assertEquals(4326, centroid.getSRID());
    assertEquals("POINT (12 34)", centroid.toString());
    assertEquals(2, org.apache.sedona.common.geography.Functions.nPoints(line));
    assertEquals(0.0, org.apache.sedona.common.geography.Functions.length(line), 0.0);
    assertEquals(4326, line.getSRID());
    assertEquals("LINESTRING (12 34, 12 34)", line.toString());
    assertEquals("SRID=4326; LINESTRING (12 34, 12 34)", line.toEWKT());
    for (String field : new String[] {"envelope", "split_envelope"}) {
      Geography envelope = row.getFieldAs(field);
      assertEquals(4326, envelope.getSRID());
      assertEquals("ST_Point", org.apache.sedona.common.geography.Functions.geometryType(envelope));
      assertEquals(12.0, org.apache.sedona.common.geography.Functions.x(envelope), 1e-12);
      assertEquals(34.0, org.apache.sedona.common.geography.Functions.y(envelope), 1e-12);
    }
  }

  @Test
  public void testMakeLineSkipsEmptyGeographyInputs() throws Exception {
    Table table =
        tableEnv.sqlQuery(
            "SELECT "
                + "ST_AsText(ST_MakeLine("
                + "ST_GeogFromWKT('POINT EMPTY', 3857), "
                + "ST_GeogFromWKT('POINT (12 34)', 4326))) AS empty_first, "
                + "ST_AsText(ST_MakeLine("
                + "ST_GeogFromWKT('POINT (12 34)', 4326), "
                + "ST_GeogFromWKT('LINESTRING EMPTY', 3857))) AS empty_second, "
                + "ST_AsEWKT(ST_MakeLine("
                + "ST_GeogFromWKT('POINT EMPTY', 3857), "
                + "ST_GeogFromWKT('LINESTRING EMPTY', 4326))) AS both_empty");

    Row row = first(table);
    assertEquals("LINESTRING (12 34, 12 34)", row.getFieldAs("empty_first"));
    assertEquals("LINESTRING (12 34, 12 34)", row.getFieldAs("empty_second"));
    assertEquals("SRID=3857; LINESTRING EMPTY", row.getFieldAs("both_empty"));
  }

  @Test
  public void testIntersectionReturnsGeographyAndPreservesFirstSRID() throws Exception {
    Table table =
        tableEnv.sqlQuery(
            "SELECT overlap, ST_GeometryType(overlap) AS overlap_type, "
                + "ST_Area(overlap) AS overlap_area FROM ("
                + "SELECT ST_Intersection("
                + "ST_GeogFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 3857), "
                + "ST_GeogFromWKT('POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))', 4326)) "
                + "AS overlap)");

    Row row = first(table);
    Geography overlap = row.getFieldAs("overlap");
    assertEquals(3857, overlap.getSRID());
    assertEquals("ST_Polygon", row.getFieldAs("overlap_type"));
    assertEquals(3.071055126726233e11, (Double) row.getFieldAs("overlap_area"), 1e5);
  }

  @Test
  public void testIntersectionUsesClosedSetBoundarySemantics() throws Exception {
    Table table =
        tableEnv.sqlQuery(
            "SELECT "
                + "ST_AsText(ST_Intersection("
                + "ST_GeogFromWKT('LINESTRING (0 -5, 0 5)', 4326), "
                + "ST_GeogFromWKT('LINESTRING (-5 0, 5 0)', 4326))) AS crossing, "
                + "ST_AsText(ST_Intersection("
                + "ST_GeogFromWKT('LINESTRING (0 0, 1 0)', 4326), "
                + "ST_GeogFromWKT('LINESTRING (0 1, 1 1)', 4326))) AS disjoint, "
                + "ST_GeometryType(ST_Intersection("
                + "ST_GeogFromWKT('LINESTRING (0 0, 0 20)', 4326), "
                + "ST_GeogFromWKT('LINESTRING (0 5, 0 15)', 4326))) AS partial_overlap_type, "
                + "ST_Length(ST_Intersection("
                + "ST_GeogFromWKT('LINESTRING (0 0, 0 20)', 4326), "
                + "ST_GeogFromWKT('LINESTRING (0 5, 0 15)', 4326))) AS partial_overlap_length");

    Row row = first(table);
    assertEquals("POINT (0 0)", row.getFieldAs("crossing"));
    assertEquals("LINESTRING EMPTY", row.getFieldAs("disjoint"));
    assertEquals("ST_LineString", row.getFieldAs("partial_overlap_type"));
    assertTrue((Double) row.getFieldAs("partial_overlap_length") > 1000000.0);
  }

  @Test
  public void testDistance() throws Exception {
    String wktA = "POINT (0 0)";
    String wktB = "POINT (0 1)";
    RowTypeInfo ti =
        new RowTypeInfo(
            new TypeInformation<?>[] {
              BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
            },
            new String[] {"a", "b"});
    DataStream<Row> ds =
        env.fromCollection(Collections.singletonList(Row.of(wktA, wktB))).returns(ti);
    Table t =
        tableEnv
            .fromDataStream(ds)
            .select(
                call(GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(), $("a"), lit(4326))
                    .as("ga"),
                call(GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(), $("b"), lit(4326))
                    .as("gb"));
    Object out =
        first(t.select(call(Functions.ST_Distance.class.getSimpleName(), $("ga"), $("gb")).as("o")))
            .getFieldAs("o");
    double expected =
        org.apache.sedona.common.geography.Functions.distance(
            Constructors.geogFromWKT(wktA, 4326), Constructors.geogFromWKT(wktB, 4326));
    assertEquals(expected, (Double) out, 1e-6);
  }

  @Test
  public void testNPoints() {
    Object out =
        eval(
            "LINESTRING (0 0, 1 1, 2 2)",
            call(Functions.ST_NPoints.class.getSimpleName(), $("geog")));
    assertEquals(3, ((Integer) out).intValue());
  }

  @Test
  public void testNumGeometries() {
    Object out =
        eval(
            "MULTIPOINT ((0 0), (1 1))",
            call(Functions.ST_NumGeometries.class.getSimpleName(), $("geog")));
    assertEquals(2, ((Integer) out).intValue());
  }

  @Test
  public void testGeometryType() {
    Object out =
        eval("POINT (1 2)", call(Functions.ST_GeometryType.class.getSimpleName(), $("geog")));
    assertEquals("ST_Point", out.toString());
  }

  @Test
  public void testXY() {
    String wkt = "POINT (-73.9857 40.7484)";
    Object x = eval(wkt, call(Functions.ST_X.class.getSimpleName(), $("geog")));
    Object y = eval(wkt, call(Functions.ST_Y.class.getSimpleName(), $("geog")));
    assertEquals(-73.9857, (Double) x, 1e-12);
    assertEquals(40.7484, (Double) y, 1e-12);
  }

  @Test
  public void testXYReturnNullForNonPointAndEmpty() {
    for (String wkt : new String[] {"LINESTRING (0 0, 1 1)", "POINT EMPTY"}) {
      assertNull(eval(wkt, call(Functions.ST_X.class.getSimpleName(), $("geog"))));
      assertNull(eval(wkt, call(Functions.ST_Y.class.getSimpleName(), $("geog"))));
    }
  }

  @Test
  public void testAsText() throws Exception {
    String wkt = "POINT (1 2)";
    Object out = eval(wkt, call(Functions.ST_AsText.class.getSimpleName(), $("geog")));
    assertEquals(
        org.apache.sedona.common.geography.Functions.asText(Constructors.geogFromWKT(wkt, 4326)),
        out.toString());
  }

  @Test
  public void testAsEWKT() throws Exception {
    String wkt = "POINT (-122.4194 37.7749)";
    Object out = eval(wkt, call(Functions.ST_AsEWKT.class.getSimpleName(), $("geog")));
    assertEquals("SRID=4326; POINT (-122.4194 37.7749)", out.toString());
  }

  @Test
  public void testCentroid() throws Exception {
    String wkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))";
    Object out = eval(wkt, call(Functions.ST_Centroid.class.getSimpleName(), $("geog")));
    Geography expected =
        org.apache.sedona.common.geography.Functions.centroid(Constructors.geogFromWKT(wkt, 4326));
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
  }

  @Test
  public void testEnvelope() throws Exception {
    String wkt = "LINESTRING (0 0, 2 3)";
    Object out =
        eval(wkt, call(Functions.ST_Envelope.class.getSimpleName(), $("geog"), lit(false)));
    Geography expected =
        org.apache.sedona.common.geography.Functions.getEnvelope(
            Constructors.geogFromWKT(wkt, 4326), false);
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
  }

  @Test
  public void testEnvelopeSplitAtAntiMeridian() throws Exception {
    // An antimeridian-crossing line: with splitAtAntiMeridian=true the envelope is split into a
    // MultiPolygon, exercising a different code path than the false branch above.
    String wkt = "LINESTRING (170 10, -170 20)";
    Object out = eval(wkt, call(Functions.ST_Envelope.class.getSimpleName(), $("geog"), lit(true)));
    Geography expected =
        org.apache.sedona.common.geography.Functions.getEnvelope(
            Constructors.geogFromWKT(wkt, 4326), true);
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
    assertTrue(expected.toEWKT().contains("MULTIPOLYGON"));
  }

  @Test
  public void testEnvelopePreservesEmptyGeography() {
    for (String wkt :
        new String[] {
          "POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY"
        }) {
      for (boolean splitAtAntiMeridian : new boolean[] {false, true}) {
        Geography out =
            (Geography)
                eval(
                    wkt,
                    3857,
                    call(
                        Functions.ST_Envelope.class.getSimpleName(),
                        $("geog"),
                        lit(splitAtAntiMeridian)));
        assertEquals(wkt, out.toString());
        assertEquals(3857, out.getSRID());
      }
    }
  }

  @Test
  public void testBuffer() throws Exception {
    String wkt = "POINT (0 0)";
    Object out = eval(wkt, call(Functions.ST_Buffer.class.getSimpleName(), $("geog"), lit(1000.0)));
    Geography expected =
        org.apache.sedona.common.geography.Functions.buffer(
            Constructors.geogFromWKT(wkt, 4326), 1000.0);
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
  }

  @Test
  public void testBufferWithParameters() throws Exception {
    String wkt = "POINT (0 0)";
    String params = "quad_segs=2";
    Object out =
        eval(
            wkt,
            call(Functions.ST_Buffer.class.getSimpleName(), $("geog"), lit(1000.0), lit(params)));
    Geography expected =
        org.apache.sedona.common.geography.Functions.buffer(
            Constructors.geogFromWKT(wkt, 4326), 1000.0, params);
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
  }

  @Test
  public void testBufferUseSpheroidThrows() {
    // The (Geography, radius, boolean) overload exists for parity with Spark and intentionally
    // throws a clear error: Geography is always spheroidal, so useSpheroid is not accepted. This
    // also confirms Flink resolves the boolean argument to this overload rather than coercing it to
    // the (Geography, radius, String) parameters overload.
    try {
      eval(
          "POINT (0 0)",
          call(Functions.ST_Buffer.class.getSimpleName(), $("geog"), lit(1000.0), lit(true)));
      fail("Expected ST_Buffer(geog, radius, useSpheroid) to throw");
    } catch (Exception e) {
      assertTrue(
          "Expected a clear useSpheroid error, got: " + messageChain(e),
          messageChain(e).contains("does not accept a useSpheroid argument"));
    }
  }

  /** Concatenate the messages of an exception and all its causes, for robust assertion. */
  private static String messageChain(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null; c = c.getCause()) {
      sb.append(c.getMessage()).append(" | ");
    }
    return sb.toString();
  }

  @Test
  public void testConvexHull() throws Exception {
    String wkt = "MULTIPOINT ((170 10), (-170 10), (180 30), (175 15))";
    Object out = eval(wkt, call(Functions.ST_ConvexHull.class.getSimpleName(), $("geog")));
    Geography expected =
        org.apache.sedona.common.geography.Functions.convexHull(
            Constructors.geogFromWKT(wkt, 4326));
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
    assertEquals(4326, ((Geography) out).getSRID());

    Object empty =
        eval("LINESTRING EMPTY", call(Functions.ST_ConvexHull.class.getSimpleName(), $("geog")));
    assertEquals("LINESTRING EMPTY", ((Geography) empty).toString());
    assertEquals(4326, ((Geography) empty).getSRID());
  }

  @Test
  public void testCollectWithTwoInputs() throws Exception {
    Table geographies =
        tableEnv.sqlQuery(
            "SELECT ST_GeogFromWKT('POINT (1 2)', 4326) AS g1, "
                + "ST_GeogFromWKT('POINT (-2 3)', 4326) AS g2");
    Geography actual =
        (Geography)
            first(
                    geographies.select(
                        call(Functions.ST_Collect.class.getSimpleName(), $("g1"), $("g2"))))
                .getField(0);
    Geography expected =
        org.apache.sedona.common.geography.Functions.createMultiGeography(
            new Geography[] {
              Constructors.geogFromWKT("POINT (1 2)", 4326),
              Constructors.geogFromWKT("POINT (-2 3)", 4326)
            });
    assertEquals(expected.toEWKT(), actual.toEWKT());
  }

  @Test
  public void testCollectWithArray() throws Exception {
    Table geographies =
        tableEnv.sqlQuery(
            "SELECT ARRAY["
                + "ST_GeogFromWKT('LINESTRING (1 2, 3 4)', 4326), "
                + "ST_GeogFromWKT('LINESTRING (3 4, 4 5)', 4326)] AS geogs");
    Geography actual =
        (Geography)
            first(geographies.select(call(Functions.ST_Collect.class.getSimpleName(), $("geogs"))))
                .getField(0);
    Geography expected =
        org.apache.sedona.common.geography.Functions.createMultiGeography(
            new Geography[] {
              Constructors.geogFromWKT("LINESTRING (1 2, 3 4)", 4326),
              Constructors.geogFromWKT("LINESTRING (3 4, 4 5)", 4326)
            });
    assertEquals(expected.toEWKT(), actual.toEWKT());

    Table withEmpty =
        tableEnv.sqlQuery(
            "SELECT ARRAY["
                + "ST_GeogFromWKT('LINESTRING EMPTY', 4326), "
                + "ST_GeogFromWKT('LINESTRING (3 4, 4 5)', 4326)] AS geogs");
    Geography actualWithEmpty =
        (Geography)
            first(withEmpty.select(call(Functions.ST_Collect.class.getSimpleName(), $("geogs"))))
                .getField(0);
    assertEquals(2, org.apache.sedona.common.geography.Functions.numGeometries(actualWithEmpty));
  }

  @Test
  public void testGeometryStillWorks() throws Exception {
    // The geometry overload must remain selectable on the same function.
    RowTypeInfo ti =
        new RowTypeInfo(
            new TypeInformation<?>[] {BasicTypeInfo.STRING_TYPE_INFO}, new String[] {"v"});
    DataStream<Row> ds =
        env.fromCollection(Collections.singletonList(Row.of("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")))
            .returns(ti);
    Table geom =
        tableEnv
            .fromDataStream(ds)
            .select(
                call(
                        org.apache.sedona.flink.expressions.Constructors.ST_GeomFromWKT.class
                            .getSimpleName(),
                        $("v"))
                    .as("g"));
    Object out =
        first(geom.select(call(Functions.ST_Area.class.getSimpleName(), $("g")).as("o")))
            .getFieldAs("o");
    assertEquals(1.0, (Double) out, 1e-9);
  }
}
