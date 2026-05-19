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
package org.apache.sedona.sql

import org.apache.sedona.common.geometryObjects.Box2D
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.UDT.{Box2DUDT, GeometryUDT}

class Box2DCastSuite extends TestBaseScala {

  /**
   * SQL `CAST(... AS box2d)` / `CAST(... AS geometry)` parsing requires Sedona's
   * `SedonaSqlAstBuilder` to be active. The test base randomizes
   * `spark.sedona.enableParserExtension` across CI runs, and `SparkContext` is JVM-singleton so
   * the active value can differ from this suite's session-level config. Probe directly by parsing
   * a tiny CAST: this matches the behavior the SQL tests actually depend on, and caches the
   * answer for the rest of the suite. DataFrame `.cast(...)` tests run unconditionally because
   * the resolution rule is always injected.
   */
  private lazy val sqlCastSupported: Boolean = {
    try {
      sparkSession
        .sql("SELECT CAST(ST_GeomFromText('POINT (0 0)') AS box2d) AS b")
        .collect()
      true
    } catch {
      case _: org.apache.spark.sql.catalyst.parser.ParseException => false
    }
  }

  describe("Geometry ↔ Box2D Catalyst cast") {

    it("DataFrame .cast(Box2DUDT) rewrites to ST_Box2D") {
      import sparkSession.implicits._
      val df = Seq("LINESTRING (0 0, 10 20)").toDF("wkt")
      val box = df
        .select(expr("ST_GeomFromText(wkt)").alias("g"))
        .select(col("g").cast(Box2DUDT).alias("b"))
        .collect()
        .head
        .getAs[Box2D]("b")
      assert(box == new Box2D(0.0, 0.0, 10.0, 20.0))
    }

    it("DataFrame .cast(GeometryUDT) rewrites to ST_GeomFromBox2D") {
      val df =
        sparkSession.sql("SELECT ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(2.0, 4.0)) AS b")
      val wkt = df
        .select(col("b").cast(GeometryUDT()).alias("g"))
        .selectExpr("ST_AsText(g) AS wkt")
        .collect()
        .head
        .getString(0)
      assert(wkt == "POLYGON ((0 0, 0 4, 2 4, 2 0, 0 0))")
    }

    it("DataFrame round-trip Geometry → Box2D → Geometry yields the envelope polygon") {
      import sparkSession.implicits._
      val df = Seq("LINESTRING (0 0, 5 10)").toDF("wkt")
      val wkt = df
        .select(expr("ST_GeomFromText(wkt)").alias("g"))
        .select(col("g").cast(Box2DUDT).cast(GeometryUDT()).alias("env"))
        .selectExpr("ST_AsText(env) AS wkt")
        .collect()
        .head
        .getString(0)
      assert(wkt == "POLYGON ((0 0, 0 10, 5 10, 5 0, 0 0))")
    }

    it("DataFrame .cast(Box2DUDT) on NULL geometry returns null") {
      val box = sparkSession
        .sql("SELECT ST_GeomFromText(NULL) AS g")
        .select(col("g").cast(Box2DUDT).alias("b"))
        .collect()
        .head
        .getAs[Box2D]("b")
      assert(box == null)
    }

    it("SQL CAST(geom AS box2d) returns the planar bbox") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS box2d)` syntax")
      val box = sparkSession
        .sql("SELECT CAST(ST_GeomFromText('LINESTRING (0 0, 10 20)') AS box2d) AS b")
        .collect()
        .head
        .getAs[Box2D]("b")
      assert(box == new Box2D(0.0, 0.0, 10.0, 20.0))
    }

    it("SQL CAST(box AS geometry) returns the rectangular polygon") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS geometry)` syntax")
      val wkt = sparkSession
        .sql("SELECT ST_AsText(CAST(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(2.0, 4.0)) AS geometry)) AS w")
        .collect()
        .head
        .getString(0)
      assert(wkt == "POLYGON ((0 0, 0 4, 2 4, 2 0, 0 0))")
    }

    it("SQL round-trip Geometry → Box2D → Geometry yields the envelope polygon") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS ...)` between UDTs")
      val wkt = sparkSession
        .sql("SELECT ST_AsText(CAST(CAST(ST_GeomFromText('LINESTRING (0 0, 5 10)') AS box2d) AS geometry)) AS w")
        .collect()
        .head
        .getString(0)
      assert(wkt == "POLYGON ((0 0, 0 10, 5 10, 5 0, 0 0))")
    }

    it("SQL CAST(NULL geometry AS box2d) returns null") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS box2d)` syntax")
      val box = sparkSession
        .sql("SELECT CAST(ST_GeomFromText(NULL) AS box2d) AS b")
        .collect()
        .head
        .getAs[Box2D]("b")
      assert(box == null)
    }
  }
}
