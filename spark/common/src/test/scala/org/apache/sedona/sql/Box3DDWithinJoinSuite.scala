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
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, DistanceJoinExec}

class Box3DDWithinJoinSuite extends TestBaseScala {

  import Box3DDWithinJoinSuite.TestBox3D

  /**
   * Box3D fixtures designed so the 3D refine kills XY-expansion candidates that 2D distance alone
   * would accept.
   *
   *   - L1=(0..1, 0..1, 0..1).
   *   - L2=(0..1, 0..1, 0..1) — duplicate so we can count multiple matches.
   *   - L3=(50..51, 50..51, 0..1) — XY-disjoint outlier (over 49 units away).
   *
   *   - R1=(0.5..1.5, 0.5..1.5, 0.5..1.5) — overlaps every axis with L1/L2 → 3D distance 0.
   *   - R2=(10..11, 0..1, 0..1) — XY distance 9 in X, no Z offset. 3D distance 9.
   *   - R3=(0..1, 0..1, 100..101) — XY-overlapping with L1/L2 but Z far above. 3D distance 99
   *     (purely in Z). With XY-expansion the R-tree pairs (L1/L2, R3); the 3D refine rejects.
   */
  private def leftBoxes: DataFrame = {
    import sparkSession.implicits._
    Seq(
      TestBox3D(1, 0, 0, 0, 1, 1, 1),
      TestBox3D(2, 0, 0, 0, 1, 1, 1),
      TestBox3D(3, 50, 50, 0, 51, 51, 1))
      .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
      .selectExpr(
        "id",
        "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
  }

  private def rightBoxes: DataFrame = {
    import sparkSession.implicits._
    Seq(
      TestBox3D(11, 0.5, 0.5, 0.5, 1.5, 1.5, 1.5),
      TestBox3D(12, 10, 0, 0, 11, 1, 1),
      TestBox3D(13, 0, 0, 100, 1, 1, 101))
      .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
      .selectExpr(
        "id",
        "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
  }

  /**
   * Collect (L.id, R.id) pairs from a joined DataFrame and sort for stable equality assertions.
   */
  private def collectPairs(joined: DataFrame): Seq[(Int, Int)] =
    joined
      .selectExpr("L.id AS l", "R.id AS r")
      .collect()
      .toSeq
      .map(r => (r.getInt(0), r.getInt(1)))
      .sorted

  describe("Box3D distance join") {

    it("ST_3DDWithin: broadcast index distance join rejects Z-only-disjoint candidates") {
      // distance=5 → R1 matches L1/L2 (3D distance 0); R2 is 9 away in X (rejected); R3 is 99
      // away in Z (rejected even though the 2D expansion would pair it with L1/L2). The XY R-tree
      // pass would also surface (L1/L2, R3) as candidates; assert the pair set, not just the
      // count, so a wrong-pair-replacing-right-pair regression can't hide under the same count.
      val joined = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_3DDWithin(L.box, R.box, 5.0)"))
      assert(joined.queryExecution.executedPlan.collectFirst { case _: BroadcastIndexJoinExec =>
        true
      }.isDefined)
      assert(collectPairs(joined) == Seq((1, 11), (2, 11)))
    }

    it("ST_3DDWithin: threshold edge picks up the borderline pair") {
      // distance=9 → adds R2 to the match set (3D distance is exactly 9; closed interval). R3 is
      // still 99 away in Z and remains rejected.
      val joined = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_3DDWithin(L.box, R.box, 9.0)"))
      assert(collectPairs(joined) == Seq((1, 11), (1, 12), (2, 11), (2, 12)))
    }

    it("ST_3DDWithin: non-broadcast distance join produces the same pair set") {
      val joined = leftBoxes
        .alias("L")
        .join(rightBoxes.alias("R"), expr("ST_3DDWithin(L.box, R.box, 5.0)"))
      assert(joined.queryExecution.executedPlan.collectFirst { case _: DistanceJoinExec =>
        true
      }.isDefined)
      assert(collectPairs(joined) == Seq((1, 11), (2, 11)))
    }

    it("ST_3DDWithin: Geometry inputs route through the same indexed path") {
      // POINT Z points: (0,0,0) on the left side, (0,0,4) and (0,0,99) on the right. With
      // distance=5 only the (0,0,4) pair matches; the (0,0,99) pair would survive an XY-expansion
      // probe but the 3D refine kills it. Assert the surviving id is R1, not just the count.
      import sparkSession.implicits._
      val lp = Seq(("L1", 0.0, 0.0, 0.0))
        .toDF("id", "x", "y", "z")
        .selectExpr("id", "ST_PointZ(x, y, z) AS pt")
      val rp = Seq(("R1", 0.0, 0.0, 4.0), ("R2", 0.0, 0.0, 99.0))
        .toDF("id", "x", "y", "z")
        .selectExpr("id", "ST_PointZ(x, y, z) AS pt")
      val joined = lp
        .alias("L")
        .join(broadcast(rp.alias("R")), expr("ST_3DDWithin(L.pt, R.pt, 5.0)"))
      assert(joined.queryExecution.executedPlan.collectFirst { case _: BroadcastIndexJoinExec =>
        true
      }.isDefined)
      val pairs = joined
        .selectExpr("L.id AS l", "R.id AS r")
        .collect()
        .toSeq
        .map(r => (r.getString(0), r.getString(1)))
      assert(pairs == Seq(("L1", "R1")))
    }
  }
}

object Box3DDWithinJoinSuite {
  case class TestBox3D(
      id: Int,
      xmin: Double,
      ymin: Double,
      zmin: Double,
      xmax: Double,
      ymax: Double,
      zmax: Double)
}
