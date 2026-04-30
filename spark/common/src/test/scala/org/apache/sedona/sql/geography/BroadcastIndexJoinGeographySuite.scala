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

    it("supports LEFT OUTER with the polygon side broadcast") {
      val joined = pointGeogDf
        .join(broadcast(polygonGeogDf), expr("ST_Contains(poly_geog, pt_geog)"), "left_outer")
      assert(planUsesBroadcastIndexJoin(joined))
      // 6 stream rows; 3 match a polygon, 3 are emitted with NULL polygon columns.
      assert(joined.count() === 6)
      val nullPolygonCount =
        joined.where("poly_id IS NULL").count()
      assert(nullPolygonCount === 3)
    }

    it("supports RIGHT OUTER with the polygon side broadcast (build = left)") {
      // For RIGHT OUTER the planner requires broadcastLeft, so we broadcast the polygon side
      // and stream the points. Every right-side (point) row must appear; unmatched points
      // come back with NULL polygon columns.
      val joined = broadcast(polygonGeogDf)
        .join(pointGeogDf, expr("ST_Contains(poly_geog, pt_geog)"), "right_outer")
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 6)
      val unmatchedPoints =
        joined.where("poly_id IS NULL").selectExpr("pt_id").collect().map(_.getInt(0)).toSet
      assert(unmatchedPoints === Set(3, 4, 5))
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

  describe("Geography broadcast spatial join (ST_Within)") {

    it("plans BroadcastIndexJoinExec when the polygon side is broadcast") {
      val joined =
        pointGeogDf.join(broadcast(polygonGeogDf), expr("ST_Within(pt_geog, poly_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 3)
    }

    it("plans BroadcastIndexJoinExec when the point side is broadcast") {
      val joined =
        polygonGeogDf.join(broadcast(pointGeogDf), expr("ST_Within(pt_geog, poly_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 3)
    }

    it("returns the correct (poly_id, pt_id) pairs") {
      val rows = pointGeogDf
        .join(broadcast(polygonGeogDf), expr("ST_Within(pt_geog, poly_geog)"))
        .selectExpr("poly_id", "pt_id")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(rows === Set((0, 0), (1, 1), (2, 2)))
    }

    it("supports LEFT OUTER with the polygon side broadcast") {
      val joined = pointGeogDf
        .join(broadcast(polygonGeogDf), expr("ST_Within(pt_geog, poly_geog)"), "left_outer")
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 6)
      assert(joined.where("poly_id IS NULL").count() === 3)
    }
  }

  describe("Geography broadcast spatial join (ST_Intersects)") {

    it("plans BroadcastIndexJoinExec when the polygon side is broadcast") {
      val joined =
        pointGeogDf.join(broadcast(polygonGeogDf), expr("ST_Intersects(poly_geog, pt_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      assert(joined.count() === 3)
    }

    it("returns the correct (poly_id, pt_id) pairs") {
      val rows = pointGeogDf
        .join(broadcast(polygonGeogDf), expr("ST_Intersects(poly_geog, pt_geog)"))
        .selectExpr("poly_id", "pt_id")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(rows === Set((0, 0), (1, 1), (2, 2)))
    }

    it("handles antimeridian-spanning polygons correctly") {
      import sparkSession.implicits._
      val polyDf = Seq((100, "POLYGON((170 -1, -170 -1, -170 1, 170 1, 170 -1))"))
        .toDF("poly_id", "wkt")
        .selectExpr("poly_id", "ST_GeogFromWKT(wkt, 4326) AS poly_geog")

      val ptDf = Seq((1, "POINT(175 0)"), (2, "POINT(-175 0)"), (3, "POINT(0 0)"))
        .toDF("pt_id", "wkt")
        .selectExpr("pt_id", "ST_GeogFromWKT(wkt, 4326) AS pt_geog")

      val joined = ptDf.join(broadcast(polyDf), expr("ST_Intersects(poly_geog, pt_geog)"))
      assert(planUsesBroadcastIndexJoin(joined))
      val matched = joined.selectExpr("pt_id").collect().map(_.getInt(0)).toSet
      assert(matched === Set(1, 2))
    }
  }

  private lazy val pointsLeftDf = {
    import sparkSession.implicits._
    Seq((0, "POINT(0 0)"), (1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(99 99)"))
      .toDF("id_l", "wkt")
      .selectExpr("id_l", "ST_GeogFromWKT(wkt, 4326) AS geog_l")
  }
  private lazy val pointsRightDf = {
    import sparkSession.implicits._
    Seq((10, "POINT(0 0)"), (11, "POINT(1 1)"), (12, "POINT(2 2)"), (13, "POINT(50 50)"))
      .toDF("id_r", "wkt")
      .selectExpr("id_r", "ST_GeogFromWKT(wkt, 4326) AS geog_r")
  }

  describe("Geography broadcast spatial join (ST_Equals)") {

    it("plans BroadcastIndexJoinExec and matches identical points") {
      val joined =
        pointsLeftDf.join(broadcast(pointsRightDf), expr("ST_Equals(geog_l, geog_r)"))
      assert(planUsesBroadcastIndexJoin(joined))
      val pairs = joined
        .selectExpr("id_l", "id_r")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(pairs === Set((0, 10), (1, 11), (2, 12)))
    }
  }

  private lazy val pointsADf = {
    import sparkSession.implicits._
    Seq((0, "POINT(0 0)"), (1, "POINT(1 0)"), (2, "POINT(2 0)"))
      .toDF("id_a", "wkt")
      .selectExpr("id_a", "ST_GeogFromWKT(wkt, 4326) AS geog_a")
  }
  private lazy val pointsBDf = {
    import sparkSession.implicits._
    Seq(
      (10, "POINT(0 0)"), // 0 m from (0,0)
      (11, "POINT(1 0)"), // 0 m from (1,0)
      (12, "POINT(0 1)") // ~111 km north of (0,0)
    ).toDF("id_b", "wkt")
      .selectExpr("id_b", "ST_GeogFromWKT(wkt, 4326) AS geog_b")
  }

  describe("Geography broadcast spatial join (ST_DWithin)") {

    it("plans BroadcastIndexJoinExec when the right side is broadcast") {
      val joined =
        pointsADf.join(broadcast(pointsBDf), expr("ST_DWithin(geog_a, geog_b, 1000.0)"))
      assert(planUsesBroadcastIndexJoin(joined))
    }

    it("returns only same-location pairs at a tight threshold (1 km)") {
      val pairs = pointsADf
        .join(broadcast(pointsBDf), expr("ST_DWithin(geog_a, geog_b, 1000.0)"))
        .selectExpr("id_a", "id_b")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(pairs === Set((0, 10), (1, 11)))
    }

    it("returns the additional cross-row pair at a wide threshold (200 km)") {
      // 200 km covers the ~111 km north neighbour from (0,0) -> (0,1) and the ~111 km
      // east-west neighbours.
      val pairs = pointsADf
        .join(broadcast(pointsBDf), expr("ST_DWithin(geog_a, geog_b, 200000.0)"))
        .selectExpr("id_a", "id_b")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      // (0,0)↔(0,0), (0,0)↔(1,0), (0,0)↔(0,1)
      // (1,0)↔(0,0), (1,0)↔(1,0), (1,0)↔(0,1)
      // (2,0)↔(1,0)  (only one within 200 km — (0,0) is ~222 km, (0,1) is ~244 km)
      assert(pairs.contains((0, 10)))
      assert(pairs.contains((0, 11)))
      assert(pairs.contains((0, 12)))
      assert(pairs.contains((1, 10)))
      assert(pairs.contains((1, 11)))
      assert(pairs.contains((1, 12)))
      assert(pairs.contains((2, 11)))
      assert(!pairs.contains((2, 10)))
    }

    it("supports a per-row column-distance threshold") {
      import sparkSession.implicits._
      val withRadius =
        Seq((0, "POINT(0 0)", 1000.0), (1, "POINT(1 0)", 1.0), (2, "POINT(2 0)", 200000.0))
          .toDF("id_a", "wkt", "radius_m")
          .selectExpr("id_a", "ST_GeogFromWKT(wkt, 4326) AS geog_a", "radius_m")

      val joined =
        withRadius.join(broadcast(pointsBDf), expr("ST_DWithin(geog_a, geog_b, radius_m)"))
      assert(planUsesBroadcastIndexJoin(joined))
      val pairs = joined
        .selectExpr("id_a", "id_b")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      // id_a=0 with 1 km: only (0,0) self-match — id_b=10
      // id_a=1 with 1 m: only (1,0) self-match — id_b=11
      // id_a=2 with 200 km: only (1,0) at ~111 km — id_b=11
      assert(pairs === Set((0, 10), (1, 11), (2, 11)))
    }

    it("supports LEFT OUTER with the right side broadcast") {
      val joined = pointsADf.join(
        broadcast(pointsBDf),
        expr("ST_DWithin(geog_a, geog_b, 1000.0)"),
        "left_outer")
      assert(planUsesBroadcastIndexJoin(joined))
      // id_a=2 has no match within 1km -> NULL right side. Counts: (0,10),(1,11),(2,NULL).
      assert(joined.count() === 3)
      assert(joined.where("id_b IS NULL").count() === 1)
    }

    it("rejects ST_DWithin(geog, geog, dist, useSpheroid) at planning time") {
      val ex = intercept[Throwable] {
        pointsADf
          .join(broadcast(pointsBDf), expr("ST_DWithin(geog_a, geog_b, 1000.0, true)"))
          .queryExecution
          .sparkPlan
      }
      val msg = Iterator
        .iterate[Throwable](ex)(t => if (t == null) null else t.getCause)
        .takeWhile(_ != null)
        .map(_.getMessage)
        .mkString(" | ")
      assert(
        msg.contains("useSpheroid") && msg.contains("Geography"),
        s"expected useSpheroid/Geography in error; got: $msg")
    }
  }
}
