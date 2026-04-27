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

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.{broadcast, expr}
import org.apache.spark.sql.sedona_sql.strategy.join.BroadcastIndexJoinExec

class BroadcastIndexJoinGeographySuite extends TestBaseScala {

  // Three unit-square polygons centred at integer offsets.
  private lazy val polygonGeogDf = {
    val rows = (0 until 3).map { i =>
      (i, s"POLYGON((${i} ${i}, ${i + 1} ${i}, ${i + 1} ${i + 1}, ${i} ${i + 1}, ${i} ${i}))")
    }
    import sparkSession.implicits._
    rows
      .toDF("poly_id", "wkt")
      .selectExpr("poly_id", "ST_GeogFromWKT(wkt, 4326) AS poly_geog")
  }

  // One point inside each of the three polygons (3 hits) plus three points outside.
  private lazy val pointGeogDf = {
    val rows = Seq(
      (0, "POINT(0.5 0.5)"), // in polygon 0
      (1, "POINT(1.5 1.5)"), // in polygon 1
      (2, "POINT(2.5 2.5)"), // in polygon 2
      (3, "POINT(10 10)"), // outside
      (4, "POINT(20 20)"), // outside
      (5, "POINT(30 30)") // outside
    )
    import sparkSession.implicits._
    rows
      .toDF("pt_id", "wkt")
      .selectExpr("pt_id", "ST_GeogFromWKT(wkt, 4326) AS pt_geog")
  }

  private def planUsesBroadcastIndexJoin(df: org.apache.spark.sql.DataFrame): Boolean =
    df.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.nonEmpty

  describe("Geography broadcast spatial join (ST_Contains)") {

    it("plans BroadcastIndexJoinExec when the polygon side is broadcast") {
      val joined =
        pointGeogDf.join(broadcast(polygonGeogDf), expr("ST_Contains(poly_geog, pt_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 3)
    }

    it("plans BroadcastIndexJoinExec when the point side is broadcast") {
      val joined =
        polygonGeogDf.join(broadcast(pointGeogDf), expr("ST_Contains(poly_geog, pt_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 3)
    }

    it("returns the correct (poly_id, pt_id) pairs") {
      val rows = pointGeogDf
        .join(broadcast(polygonGeogDf), expr("ST_Contains(poly_geog, pt_geog)"))
        .selectExpr("poly_id", "pt_id")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(rows === Set((0, 0), (1, 1), (2, 2)))
    }

    it("handles antimeridian-spanning polygons correctly") {
      // Polygon spanning longitude 170 → -170 (5° wide across the antimeridian on each side).
      // The lat/lng rect is "inverted" so we expand to the full longitude range as a coarse
      // filter; the S2 refine step is what guarantees the correct answer here.
      import sparkSession.implicits._
      val polyDf = Seq((100, "POLYGON((170 -1, -170 -1, -170 1, 170 1, 170 -1))"))
        .toDF("poly_id", "wkt")
        .selectExpr("poly_id", "ST_GeogFromWKT(wkt, 4326) AS poly_geog")

      val ptDf = Seq(
        (1, "POINT(175 0)"), // inside on the +180 side
        (2, "POINT(-175 0)"), // inside on the −180 side
        (3, "POINT(0 0)") // far outside
      ).toDF("pt_id", "wkt")
        .selectExpr("pt_id", "ST_GeogFromWKT(wkt, 4326) AS pt_geog")

      val joined = ptDf.join(broadcast(polyDf), expr("ST_Contains(poly_geog, pt_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      val matched = joined.selectExpr("pt_id").collect().map(_.getInt(0)).toSet
      assert(matched === Set(1, 2))
    }

    it("auto-broadcasts the small side when sedona.join.autoBroadcastJoinThreshold permits") {
      // Bump the threshold so the small Geography frames qualify for auto-broadcast.
      withConf(Map("sedona.join.autoBroadcastJoinThreshold" -> "10485760")) {
        val joined = polygonGeogDf.join(pointGeogDf, expr("ST_Contains(poly_geog, pt_geog)"))
        assert(planUsesBroadcastIndexJoin(joined))
        assert(joined.count() === 3)
      }
    }

    it("does NOT plan BroadcastIndexJoinExec without a broadcast hint") {
      // autoBroadcastJoinThreshold = -1 in TestBaseScala, so neither side auto-broadcasts.
      // Geography ST_Contains has no partition/range-join path, so Spark falls back to a
      // row-by-row evaluation (BroadcastNestedLoopJoinExec). The result must still be
      // correct.
      val joined = polygonGeogDf.join(pointGeogDf, expr("ST_Contains(poly_geog, pt_geog)"))
      assert(!planUsesBroadcastIndexJoin(joined))
      val pairs = joined
        .selectExpr("poly_id", "pt_id")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(pairs === Set((0, 0), (1, 1), (2, 2)))
    }
  }
}
