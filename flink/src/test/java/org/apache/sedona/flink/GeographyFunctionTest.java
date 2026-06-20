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
import static org.junit.Assert.assertTrue;

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
    RowTypeInfo ti =
        new RowTypeInfo(
            new TypeInformation<?>[] {BasicTypeInfo.STRING_TYPE_INFO}, new String[] {"v"});
    DataStream<Row> ds = env.fromCollection(Collections.singletonList(Row.of(wkt))).returns(ti);
    return tableEnv
        .fromDataStream(ds)
        .select(
            call(GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(), $("v"), lit(4326))
                .as("geog"));
  }

  private Object eval(String wkt, ApiExpression call) {
    return first(geogTable(wkt).select(call.as("o"))).getFieldAs("o");
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
  public void testAsText() throws Exception {
    String wkt = "POINT (1 2)";
    Object out = eval(wkt, call(Functions.ST_AsText.class.getSimpleName(), $("geog")));
    assertEquals(
        org.apache.sedona.common.geography.Functions.asText(Constructors.geogFromWKT(wkt, 4326)),
        out.toString());
  }

  @Test
  public void testAsEWKT() throws Exception {
    String wkt = "POINT (1 2)";
    Object out = eval(wkt, call(Functions.ST_AsEWKT.class.getSimpleName(), $("geog")));
    assertEquals(
        org.apache.sedona.common.geography.Functions.asEWKT(Constructors.geogFromWKT(wkt, 4326)),
        out.toString());
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
  public void testBuffer() throws Exception {
    String wkt = "POINT (0 0)";
    Object out = eval(wkt, call(Functions.ST_Buffer.class.getSimpleName(), $("geog"), lit(1000.0)));
    Geography expected =
        org.apache.sedona.common.geography.Functions.buffer(
            Constructors.geogFromWKT(wkt, 4326), 1000.0);
    assertEquals(expected.toEWKT(), ((Geography) out).toEWKT());
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
