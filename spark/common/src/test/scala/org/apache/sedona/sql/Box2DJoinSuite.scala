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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, expr}
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, DistanceJoinExec, RangeJoinExec}

class Box2DJoinSuite extends TestBaseScala {

  import Box2DJoinSuite.TestBox

  /**
   * Three left-side boxes and three right-side boxes wired so we can predict exact result sizes:
   *
   *   - L1=(0,0,10,10) R1=(5,5,15,15) — overlapping
   *   - L1=(0,0,10,10) R2=(2,2,8,8) — R2 fully inside L1
   *   - L2=(0,0,10,10) R1=(5,5,15,15) — overlapping
   *   - L2=(0,0,10,10) R2=(2,2,8,8) — R2 fully inside L2
   *   - L3 and R3 are disjoint from everything else; (L3,R3) is itself disjoint.
   *
   * Intersection-pair count: 4. Containment-pair count: 2 (L1⊇R2, L2⊇R2).
   */
  private def leftBoxes: DataFrame = {
    import sparkSession.implicits._
    Seq(TestBox(1, 0, 0, 10, 10), TestBox(2, 0, 0, 10, 10), TestBox(3, 20, 20, 30, 30))
      .toDF("id", "xmin", "ymin", "xmax", "ymax")
      .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
  }

  private def rightBoxes: DataFrame = {
    import sparkSession.implicits._
    Seq(TestBox(11, 5, 5, 15, 15), TestBox(12, 2, 2, 8, 8), TestBox(13, 40, 40, 50, 50))
      .toDF("id", "xmin", "ymin", "xmax", "ymax")
      .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
  }

  describe("Box2D spatial join") {

    it("ST_Intersects: broadcast index join produces correct pairs") {
      val df = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(L.box, R.box)"))
      val plan = df.queryExecution.sparkPlan
      assert(
        plan.collect { case b: BroadcastIndexJoinExec => b }.size == 1,
        "Expected BroadcastIndexJoinExec in the plan")
      assert(df.count() == 4)
    }

    it("ST_Intersects: argument order is symmetric") {
      val swapped = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(R.box, L.box)"))
      assert(swapped.count() == 4)
      assert(swapped.queryExecution.sparkPlan.collect { case b: BroadcastIndexJoinExec =>
        b
      }.size == 1)
    }

    it("ST_Contains: broadcast index join uses COVERS semantics") {
      val df = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Contains(L.box, R.box)"))
      assert(df.queryExecution.sparkPlan.collect { case b: BroadcastIndexJoinExec =>
        b
      }.size == 1)
      assert(df.count() == 2)
    }

    it("ST_Contains: edge-touching boxes count (closed-interval semantics)") {
      // R contained in L sharing an edge: ST_Contains is closed-interval, so this matches.
      // JTS Polygon.contains would reject (strict-interior), JTS Polygon.covers accepts; the
      // detector maps ST_Contains → SpatialPredicate.COVERS specifically for this case.
      import sparkSession.implicits._
      val outer = Seq(TestBox(1, 0, 0, 10, 10))
        .toDF("id", "xmin", "ymin", "xmax", "ymax")
        .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
      // edge-sharing box: same xmax, shares the right edge with outer.
      val inner = Seq(TestBox(11, 5, 5, 10, 10))
        .toDF("id", "xmin", "ymin", "xmax", "ymax")
        .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
      val df = outer
        .alias("O")
        .join(broadcast(inner.alias("I")), expr("ST_Contains(O.box, I.box)"))
      assert(df.count() == 1, "Closed-interval containment must include edge-touching boxes")
    }

    it("ST_Intersects: non-broadcast range join produces the same count") {
      val df = leftBoxes
        .alias("L")
        .join(rightBoxes.alias("R"), expr("ST_Intersects(L.box, R.box)"))
      assert(
        df.queryExecution.sparkPlan.collect { case r: RangeJoinExec => r }.size == 1,
        "Expected RangeJoinExec in the plan")
      assert(df.count() == 4)
    }

    it("Null Box2D rows are safe and produce no matches") {
      // A null shape on either side must not crash the executor and must not contribute matches
      // (mirrors the existing GeometrySerializer.deserialize(null) → empty-collection fallback).
      import sparkSession.implicits._
      val withNullLeft = leftBoxes
        .selectExpr("id", "box AS box")
        .union(Seq((99, null.asInstanceOf[org.apache.sedona.common.geometryObjects.Box2D]))
          .toDF("id", "box"))
      val df = withNullLeft
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(L.box, R.box)"))
      assert(df.count() == 4) // unchanged from the non-null fixture
      // Range join path (no broadcast) also tolerates nulls.
      val rangeDf = withNullLeft
        .alias("L")
        .join(rightBoxes.alias("R"), expr("ST_Intersects(L.box, R.box)"))
      assert(rangeDf.count() == 4)
    }

    it("Inverted Box2D bounds in a join throw IllegalArgumentException") {
      import sparkSession.implicits._
      // Construct an inverted Box2D directly via the Java constructor (the SQL ST_MakeBox2D
      // doesn't validate, so this is how a stored column with inverted bounds would look).
      val invertedLeft =
        Seq((1, new org.apache.sedona.common.geometryObjects.Box2D(10.0, 0.0, 0.0, 10.0)))
          .toDF("id", "box")
      val df = invertedLeft
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(L.box, R.box)"))
      // Confirm the join is actually planned as BroadcastIndexJoinExec so the throw originates
      // from the join-side `shapeToGeometry` validation, not from a row-by-row fallback that
      // also happens to throw via `Predicates.boxIntersects`.
      assert(
        df.queryExecution.sparkPlan.collect { case b: BroadcastIndexJoinExec => b }.size == 1,
        "Expected BroadcastIndexJoinExec — without it the test could pass via row-by-row " +
          "predicate evaluation, hiding a regression in join optimization")
      val ex = intercept[org.apache.spark.SparkException](df.collect())
      val cause = Iterator
        .iterate(ex: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .find(_.isInstanceOf[IllegalArgumentException])
      assert(cause.isDefined, s"Expected IllegalArgumentException in cause chain, got: $ex")
      assert(cause.get.getMessage.contains("inverted bounds"))
    }

    it("Result is equivalent to ST_Intersects on the Box2D-as-polygon envelopes") {
      val viaBox = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(L.box, R.box)"))
        .selectExpr("L.id AS l", "R.id AS r")
        .orderBy("l", "r")
        .collect()
        .toSeq

      // ST_GeomFromBox2D is the function-form equivalent of `CAST(box AS geometry)`. The cast
      // syntax requires the Sedona SQL parser extension; this suite runs under the common test
      // base, which doesn't wire that extension, so we go through the function form here.
      val asPolygons = leftBoxes
        .selectExpr("id", "ST_GeomFromBox2D(box) AS g")
        .alias("L")
        .join(
          broadcast(rightBoxes.selectExpr("id", "ST_GeomFromBox2D(box) AS g").alias("R")),
          expr("ST_Intersects(L.g, R.g)"))
        .selectExpr("L.id AS l", "R.id AS r")
        .orderBy("l", "r")
        .collect()
        .toSeq

      assert(viaBox == asPolygons)
    }
  }

  describe("Box2D distance join") {

    /**
     * Boxes wired so the L↔R distances are exactly predictable:
     *   - L1=(0,0,10,10), R1=(11,0,12,1) → distance 1.0 (separated by 1 on X)
     *   - L1=(0,0,10,10), R2=(5,15,8,18) → distance 5.0 (separated by 5 on Y)
     *   - L2=(0,0,1,1), R1=(11,0,12,1) → distance 10.0
     *   - L2=(0,0,1,1), R2=(5,15,8,18) → sqrt(16 + 196) ≈ 14.56
     */
    def distLeft: DataFrame = {
      import sparkSession.implicits._
      Seq(TestBox(1, 0.0, 0.0, 10.0, 10.0), TestBox(2, 0.0, 0.0, 1.0, 1.0))
        .toDF("id", "xmin", "ymin", "xmax", "ymax")
        .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
    }

    def distRight: DataFrame = {
      import sparkSession.implicits._
      Seq(TestBox(11, 11.0, 0.0, 12.0, 1.0), TestBox(12, 5.0, 15.0, 8.0, 18.0))
        .toDF("id", "xmin", "ymin", "xmax", "ymax")
        .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
    }

    it("ST_DWithin on Box2D inputs: broadcast index join, radius 1.0") {
      val df = distLeft
        .alias("L")
        .join(broadcast(distRight.alias("R")), expr("ST_DWithin(L.box, R.box, 1.0)"))
      assert(
        df.queryExecution.sparkPlan.collect { case b: BroadcastIndexJoinExec => b }.size == 1,
        "Expected BroadcastIndexJoinExec in the plan")
      // Only (L1, R1) is within 1.0.
      assert(df.count() == 1)
    }

    it("ST_DWithin on Box2D inputs: broadcast index join, radius 6.0") {
      val df = distLeft
        .alias("L")
        .join(broadcast(distRight.alias("R")), expr("ST_DWithin(L.box, R.box, 6.0)"))
      // (L1, R1) distance=1 ✓, (L1, R2) distance=5 ✓; (L2, R1) distance=10 ✗, (L2, R2) ≈14.56 ✗.
      assert(df.count() == 2)
    }

    it("ST_DWithin on Box2D inputs: distance join (non-broadcast) produces the same count") {
      val df = distLeft
        .alias("L")
        .join(distRight.alias("R"), expr("ST_DWithin(L.box, R.box, 6.0)"))
      assert(
        df.queryExecution.sparkPlan.collect { case d: DistanceJoinExec => d }.size == 1,
        "Expected DistanceJoinExec in the plan")
      assert(df.count() == 2)
    }

    it("ST_DWithin on Box2D inputs: zero radius matches only edge/corner-touching pairs") {
      import sparkSession.implicits._
      val touching = Seq(TestBox(1, 0.0, 0.0, 10.0, 10.0))
        .toDF("id", "xmin", "ymin", "xmax", "ymax")
        .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
      val adjacent = Seq(TestBox(11, 10.0, 0.0, 20.0, 10.0)) // shares edge x=10
        .toDF("id", "xmin", "ymin", "xmax", "ymax")
        .selectExpr("id", "ST_MakeBox2D(ST_Point(xmin, ymin), ST_Point(xmax, ymax)) AS box")
      val df = touching
        .alias("L")
        .join(broadcast(adjacent.alias("R")), expr("ST_DWithin(L.box, R.box, 0.0)"))
      assert(df.count() == 1)
    }
  }

}

object Box2DJoinSuite {
  // Top-level case class so Spark's encoder doesn't need an outer-class reference.
  case class TestBox(id: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double)
}
