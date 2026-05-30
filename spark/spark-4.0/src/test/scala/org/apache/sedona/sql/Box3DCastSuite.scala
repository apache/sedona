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

import org.apache.sedona.common.geometryObjects.Box3D
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.UDT.Box3DUDT

class Box3DCastSuite extends TestBaseScala {

  /**
   * SQL `CAST(... AS box3d)` parsing requires Sedona's `SedonaSqlAstBuilder` to be active. The
   * test base randomizes `spark.sedona.enableParserExtension` across CI runs, and `SparkContext`
   * is JVM-singleton so the active value can differ from this suite's session-level config. Probe
   * directly by parsing a tiny CAST: this matches the behavior the SQL tests actually depend on,
   * and caches the answer for the rest of the suite. DataFrame `.cast(...)` tests run
   * unconditionally because the resolution rule is always injected.
   */
  private lazy val sqlCastSupported: Boolean = {
    try {
      sparkSession
        .sql("SELECT CAST(ST_GeomFromText('POINT (0 0)') AS box3d) AS b")
        .collect()
      true
    } catch {
      case _: org.apache.spark.sql.catalyst.parser.ParseException => false
    }
  }

  describe("Geometry → Box3D Catalyst cast") {

    it("DataFrame .cast(Box3DUDT) rewrites to ST_Box3D") {
      import sparkSession.implicits._
      val df = Seq("LINESTRING Z(0 0 -3, 5 10 7)").toDF("wkt")
      val box = df
        .select(expr("ST_GeomFromText(wkt)").alias("g"))
        .select(col("g").cast(Box3DUDT).alias("b"))
        .collect()
        .head
        .getAs[Box3D]("b")
      assert(box == new Box3D(0.0, 0.0, -3.0, 5.0, 10.0, 7.0))
    }

    it("DataFrame .cast(Box3DUDT) on XY geometry folds Z = 0") {
      import sparkSession.implicits._
      val df = Seq("LINESTRING (0 0, 5 10)").toDF("wkt")
      val box = df
        .select(expr("ST_GeomFromText(wkt)").alias("g"))
        .select(col("g").cast(Box3DUDT).alias("b"))
        .collect()
        .head
        .getAs[Box3D]("b")
      assert(box == new Box3D(0.0, 0.0, 0.0, 5.0, 10.0, 0.0))
    }

    it("DataFrame .cast(Box3DUDT) on NULL geometry returns null") {
      val box = sparkSession
        .sql("SELECT ST_GeomFromText(NULL) AS g")
        .select(col("g").cast(Box3DUDT).alias("b"))
        .collect()
        .head
        .getAs[Box3D]("b")
      assert(box == null)
    }

    it("SQL CAST(geom AS box3d) returns the 3D bbox") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS box3d)` syntax")
      val box = sparkSession
        .sql("SELECT CAST(ST_GeomFromText('LINESTRING Z(0 0 -3, 5 10 7)') AS box3d) AS b")
        .collect()
        .head
        .getAs[Box3D]("b")
      assert(box == new Box3D(0.0, 0.0, -3.0, 5.0, 10.0, 7.0))
    }

    it("SQL CAST(geom AS box3d) on XY geometry folds Z = 0") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS box3d)` syntax")
      val box = sparkSession
        .sql("SELECT CAST(ST_GeomFromText('LINESTRING (0 0, 5 10)') AS box3d) AS b")
        .collect()
        .head
        .getAs[Box3D]("b")
      assert(box == new Box3D(0.0, 0.0, 0.0, 5.0, 10.0, 0.0))
    }

    it("SQL CAST(NULL geometry AS box3d) returns null") {
      assume(
        sqlCastSupported,
        "Sedona SQL parser extension is required for `CAST(... AS box3d)` syntax")
      val box = sparkSession
        .sql("SELECT CAST(ST_GeomFromText(NULL) AS box3d) AS b")
        .collect()
        .head
        .getAs[Box3D]("b")
      assert(box == null)
    }
  }
}
