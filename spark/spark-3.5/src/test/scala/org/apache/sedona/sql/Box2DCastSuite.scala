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

  describe("Geometry ↔ Box2D Catalyst cast") {

    it("SQL CAST(geom AS box2d) returns the planar bbox") {
      val row = sparkSession
        .sql(
          "SELECT CAST(ST_GeomFromText('LINESTRING (0 0, 10 20)') AS box2d) AS b")
        .collect()
        .head
      val box = row.getAs[Box2D]("b")
      assert(box == new Box2D(0.0, 0.0, 10.0, 20.0))
    }

    it("SQL CAST(box AS geometry) returns the rectangular polygon") {
      val wkt = sparkSession
        .sql(
          "SELECT ST_AsText(CAST(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(2.0, 4.0)) AS geometry)) AS w")
        .collect()
        .head
        .getString(0)
      assert(wkt == "POLYGON ((0 0, 2 0, 2 4, 0 4, 0 0))")
    }

    it("DataFrame API .cast(Box2DUDT) rewrites to ST_Box2D") {
      import sparkSession.implicits._
      val df = Seq("POINT (3 7)").toDF("wkt")
      val out = df
        .select(expr("ST_GeomFromText(wkt)").alias("g"))
        .select(col("g").cast(Box2DUDT).alias("b"))
        .collect()
      val box = out.head.getAs[Box2D]("b")
      assert(box == new Box2D(3.0, 7.0, 3.0, 7.0))
    }

    it("DataFrame API .cast(GeometryUDT) rewrites to ST_GeomFromBox2D") {
      import sparkSession.implicits._
      val df = sparkSession.sql(
        "SELECT ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)) AS b")
      val out = df
        .select(col("b").cast(GeometryUDT()).alias("g"))
        .selectExpr("ST_AsText(g) AS wkt")
        .collect()
      assert(out.head.getString(0) == "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
    }

    it("Round-trip Geometry → Box2D → Geometry yields the envelope polygon") {
      val wkt = sparkSession
        .sql(
          "SELECT ST_AsText(CAST(CAST(ST_GeomFromText('LINESTRING (0 0, 5 10)') AS box2d) AS geometry)) AS w")
        .collect()
        .head
        .getString(0)
      assert(wkt == "POLYGON ((0 0, 5 0, 5 10, 0 10, 0 0))")
    }

    it("CAST(NULL geometry AS box2d) returns null") {
      val box = sparkSession
        .sql("SELECT CAST(ST_GeomFromText(NULL) AS box2d) AS b")
        .collect()
        .head
        .getAs[Box2D]("b")
      assert(box == null)
    }
  }
}
