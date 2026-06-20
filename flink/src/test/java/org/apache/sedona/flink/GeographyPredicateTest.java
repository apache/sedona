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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Predicates;
import org.apache.sedona.flink.expressions.geography.GeographyConstructors;
import org.junit.BeforeClass;
import org.junit.Test;

public class GeographyPredicateTest extends TestBase {

  // Geography polygons follow the spherical right-hand rule: a CCW ring's interior is the enclosed
  // region. The polygons below are CCW so containment matches the intuitive (planar) expectation; a
  // CW ring would denote the complementary region on the sphere.

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  /** Evaluate a predicate call over two geography columns built from {@code wktA}/{@code wktB}. */
  private Object eval(String wktA, String wktB, PredicateFactory factory) {
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
    return first(t.select(factory.build($("ga"), $("gb")).as("o"))).getFieldAs("o");
  }

  private interface PredicateFactory {
    ApiExpression build(ApiExpression a, ApiExpression b);
  }

  @Test
  public void testIntersects() {
    String poly = "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))";
    assertTrue(
        (Boolean)
            eval(
                poly,
                "POINT (1 1)",
                (a, b) -> call(Predicates.ST_Intersects.class.getSimpleName(), a, b)));
    assertFalse(
        (Boolean)
            eval(
                poly,
                "POINT (5 5)",
                (a, b) -> call(Predicates.ST_Intersects.class.getSimpleName(), a, b)));
  }

  @Test
  public void testContains() {
    assertTrue(
        (Boolean)
            eval(
                "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
                "POINT (1 1)",
                (a, b) -> call(Predicates.ST_Contains.class.getSimpleName(), a, b)));
  }

  @Test
  public void testWithin() {
    assertTrue(
        (Boolean)
            eval(
                "POINT (1 1)",
                "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
                (a, b) -> call(Predicates.ST_Within.class.getSimpleName(), a, b)));
  }

  @Test
  public void testEquals() {
    assertTrue(
        (Boolean)
            eval(
                "POINT (1 1)",
                "POINT (1 1)",
                (a, b) -> call(Predicates.ST_Equals.class.getSimpleName(), a, b)));
    assertFalse(
        (Boolean)
            eval(
                "POINT (1 1)",
                "POINT (2 2)",
                (a, b) -> call(Predicates.ST_Equals.class.getSimpleName(), a, b)));
  }

  @Test
  public void testDWithin() {
    // Two points ~111 km apart (1 degree of latitude near the equator).
    assertTrue(
        (Boolean)
            eval(
                "POINT (0 0)",
                "POINT (0 1)",
                (a, b) -> call(Predicates.ST_DWithin.class.getSimpleName(), a, b, lit(200000.0))));
    assertFalse(
        (Boolean)
            eval(
                "POINT (0 0)",
                "POINT (0 1)",
                (a, b) -> call(Predicates.ST_DWithin.class.getSimpleName(), a, b, lit(50000.0))));
  }

  @Test
  public void testGeometryStillWorks() {
    // The geometry overload must remain selectable on the same predicate.
    RowTypeInfo ti =
        new RowTypeInfo(
            new TypeInformation<?>[] {
              BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
            },
            new String[] {"a", "b"});
    DataStream<Row> ds =
        env.fromCollection(
                Collections.singletonList(
                    Row.of("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POINT (1 1)")))
            .returns(ti);
    String geom =
        org.apache.sedona.flink.expressions.Constructors.ST_GeomFromWKT.class.getSimpleName();
    Table t =
        tableEnv
            .fromDataStream(ds)
            .select(call(geom, $("a")).as("ga"), call(geom, $("b")).as("gb"));
    Object out =
        first(
                t.select(
                    call(Predicates.ST_Contains.class.getSimpleName(), $("ga"), $("gb")).as("o")))
            .getFieldAs("o");
    assertEquals(true, out);
  }
}
