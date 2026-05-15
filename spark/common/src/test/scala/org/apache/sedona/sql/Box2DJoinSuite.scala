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
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, RangeJoinExec}

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

    it("ST_BoxIntersects: broadcast index join produces correct pairs") {
      val df = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_BoxIntersects(L.box, R.box)"))
      val plan = df.queryExecution.sparkPlan
      assert(
        plan.collect { case b: BroadcastIndexJoinExec => b }.size == 1,
        "Expected BroadcastIndexJoinExec in the plan")
      assert(df.count() == 4)
    }

    it("ST_BoxIntersects: argument order is symmetric") {
      val swapped = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_BoxIntersects(R.box, L.box)"))
      assert(swapped.count() == 4)
      assert(swapped.queryExecution.sparkPlan.collect { case b: BroadcastIndexJoinExec =>
        b
      }.size == 1)
    }

    it("ST_BoxContains: broadcast index join uses COVERS semantics") {
      val df = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_BoxContains(L.box, R.box)"))
      assert(df.queryExecution.sparkPlan.collect { case b: BroadcastIndexJoinExec =>
        b
      }.size == 1)
      assert(df.count() == 2)
    }

    it("ST_BoxContains: edge-touching boxes count (closed-interval semantics)") {
      // R contained in L sharing an edge: ST_BoxContains is closed-interval, so this matches.
      // JTS Polygon.contains would reject (strict-interior), JTS Polygon.covers accepts; the
      // detector maps ST_BoxContains → SpatialPredicate.COVERS specifically for this case.
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
        .join(broadcast(inner.alias("I")), expr("ST_BoxContains(O.box, I.box)"))
      assert(df.count() == 1, "Closed-interval containment must include edge-touching boxes")
    }

    it("ST_BoxIntersects: non-broadcast range join produces the same count") {
      val df = leftBoxes
        .alias("L")
        .join(rightBoxes.alias("R"), expr("ST_BoxIntersects(L.box, R.box)"))
      assert(
        df.queryExecution.sparkPlan.collect { case r: RangeJoinExec => r }.size == 1,
        "Expected RangeJoinExec in the plan")
      assert(df.count() == 4)
    }

    it("Result is equivalent to ST_Intersects on the Box2D-as-polygon envelopes") {
      val viaBox = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_BoxIntersects(L.box, R.box)"))
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

}

object Box2DJoinSuite {
  // Top-level case class so Spark's encoder doesn't need an outer-class reference.
  case class TestBox(id: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double)
}
