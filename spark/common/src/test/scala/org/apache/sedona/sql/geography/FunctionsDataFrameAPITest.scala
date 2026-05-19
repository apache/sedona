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
package org.apache.sedona.sql.geography

import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Envelope, st_constructors, st_functions, st_predicates}
import org.junit.Assert.{assertEquals, assertTrue}

class FunctionsDataFrameAPITest extends TestBaseScala {
  import sparkSession.implicits._

  it("Passed ST_Envelope antarctica") {
    val antarctica =
      "POLYGON ((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90))"
    val df = sparkSession
      .sql(s"SELECT '$antarctica' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("geog"))
      .select(st_functions.ST_Envelope(col("geog"), split = true))
      .as("env")

    val env = df.first().get(0).asInstanceOf[Geography]
    val expectedWKT =
      "POLYGON ((-180 -63.3, 180 -63.3, 180 -90, -180 -90, -180 -63.3))";
    assertEquals(expectedWKT, env.toString)
  }

  it("Passed ST_Envelope Fiji") {
    val fiji =
      "MULTIPOLYGON (" + "((177.285 -18.28799, 180 -18.28799, 180 -16.02088, 177.285 -16.02088, 177.285 -18.28799))," +
        "((-180 -18.28799, -179.7933 -18.28799, -179.7933 -16.02088, -180 -16.02088, -180 -18.28799))" + ")"

    val df = sparkSession
      .sql(s"SELECT '$fiji' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("geog"))
      .select(st_functions.ST_Envelope(col("geog"), split = false))
      .as("env")

    val env = df.first().get(0).asInstanceOf[Geography]
    val expectedWKT =
      "POLYGON ((177.3 -18.3, -179.8 -18.3, -179.8 -16, 177.3 -16, 177.3 -18.3))";
    assertEquals(expectedWKT, env.toString)
  }

  it("Passed ST_AsEWKT") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val wktExpected = "SRID=4326; LINESTRING (1 2, 3 4, 5 6)"
    val df = sparkSession
      .sql(s"SELECT '$wkt' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("geog"))
      .select(st_functions.ST_AsEWKT(col("geog")))
    val geoStr = df.first().get(0)
    assert(geoStr == wktExpected)
  }

  it("Passed ST_Length via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'LINESTRING (0 0, 1 0)' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_Length(col("g")).as("l"))
    val len = df.first().getDouble(0)
    // 1° along the equator on a sphere of radius 6371008 m ≈ 111195 m
    assertEquals(111195.10, len, 1.0)
  }

  it("Passed ST_Area via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_Area(col("g")).as("a"))
    val area = df.first().getDouble(0)
    // 1°×1° box near equator on R=6371008m sphere ≈ 1.2364e10 m²
    assertEquals(1.2364e10, area, 1e7)
  }

  it("Passed ST_Centroid via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_Centroid(col("g")).as("c"))
    val centroid = df.first().get(0).asInstanceOf[Geography]
    assertTrue(centroid.toString.startsWith("POINT"))
  }

  it("Passed ST_NumGeometries via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'MULTIPOINT ((0 0), (1 1), (2 2))' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_NumGeometries(col("g")).as("n"))
    assertEquals(3, df.first().getInt(0))
  }

  it("Passed ST_GeometryType via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_GeometryType(col("g")).as("t"))
    assertEquals("ST_Polygon", df.first().getString(0))
  }

  it("Passed ST_AsText via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POINT (1 2)' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_AsText(col("g")).as("t"))
    val txt = df.first().getString(0)
    assertTrue(s"expected POINT prefix; got $txt", txt.startsWith("POINT"))
  }

  it("Passed ST_Intersects via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))' AS a, " +
        "'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))' AS b")
      .select(
        st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
        st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
      .select(st_predicates.ST_Intersects(col("a"), col("b")).as("r"))
    assertTrue(df.first().getBoolean(0))
  }

  it("Passed ST_Distance via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POINT (0 0)' AS a, 'POINT (1 1)' AS b")
      .select(
        st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
        st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
      .select(st_functions.ST_Distance(col("a"), col("b")).as("d"))
    val d = df.first().getDouble(0)
    // ~157km on a sphere; matches the Python TestGeographyFunctionsDataFrameAPI bound.
    assertTrue(s"expected 155000 < d < 160000; got $d", d > 155000 && d < 160000)
  }

  it("Passed ST_Length on POINT via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POINT (1 2)' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_Length(col("g")).as("l"))
    assertEquals(0.0, df.first().getDouble(0), 0.0)
  }

  it("Passed ST_Buffer via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POINT (0 0)' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_Buffer(col("g"), lit(1000.0)).as("b"))
      .select(st_functions.ST_AsText(col("b")).as("t"))
    val txt = df.first().getString(0)
    assertTrue(s"expected POLYGON prefix; got $txt", txt.startsWith("POLYGON"))
  }

  it("Passed ST_NPoints via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'LINESTRING (0 0, 1 1, 2 2)' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("g"))
      .select(st_functions.ST_NPoints(col("g")).as("n"))
    assertEquals(3, df.first().getInt(0))
  }

  it("Passed ST_Contains via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS a, " +
        "'POINT (0.5 0.5)' AS b")
      .select(
        st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
        st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
      .select(st_predicates.ST_Contains(col("a"), col("b")).as("r"))
    assertTrue(df.first().getBoolean(0))
  }

  it("Passed ST_Within via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POINT (0.5 0.5)' AS a, " +
        "'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS b")
      .select(
        st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
        st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
      .select(st_predicates.ST_Within(col("a"), col("b")).as("r"))
    assertTrue(df.first().getBoolean(0))
  }

  it("Passed ST_DWithin via DataFrame API") {
    // 1° of latitude ≈ 111 km, well within 200 km.
    val df = sparkSession
      .sql("SELECT 'POINT (0 0)' AS a, 'POINT (0 1)' AS b")
      .select(
        st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
        st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
      .select(st_predicates.ST_DWithin(col("a"), col("b"), lit(200000.0)).as("r"))
    assertTrue(df.first().getBoolean(0))
  }

  it("Passed ST_Equals via DataFrame API") {
    val df = sparkSession
      .sql("SELECT 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS a, " +
        "'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))' AS b")
      .select(
        st_constructors.ST_GeogFromWKT(col("a"), lit(4326)).as("a"),
        st_constructors.ST_GeogFromWKT(col("b"), lit(4326)).as("b"))
      .select(st_predicates.ST_Equals(col("a"), col("b")).as("r"))
    assertTrue(df.first().getBoolean(0))
  }

}
