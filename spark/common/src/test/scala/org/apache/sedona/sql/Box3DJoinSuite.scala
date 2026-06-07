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

class Box3DJoinSuite extends TestBaseScala {

  import Box3DJoinSuite.TestBox3D

  /**
   * Left and right boxes wired so result counts are predictable across the XY filter and the
   * Z-axis refine step. L4 is the discriminating row: same XY footprint as L1/L2 so the 2D R-tree
   * pairs it with every right in the (0..10) XY cluster, but its Z range is disjoint from every
   * right's Z (above R1/R2's Z, below R4's Z) so the refine must reject all those candidates.
   *
   *   - L1=(0..10, 0..10, 0..10), L2=same — dense cluster on Z=(0..10).
   *   - L3=(20..30, 20..30, 0..10) — XY-disjoint outlier.
   *   - L4=(0..10, 0..10, 50..60) — XY in the cluster, Z disjoint from every right side. Every
   *     L4-R* candidate the R-tree emits has to be killed by the Z refine.
   *
   *   - R1=(5..15, 5..15, 5..15), R2=(2..8, 2..8, 2..8) — overlap L1/L2 in XY *and* Z.
   *   - R3=(40..50, 40..50, 0..10) — XY-disjoint.
   *   - R4=(2..8, 2..8, 100..200) — XY in the cluster, Z separated from every left.
   *
   * True intersection pairs: 4 — (L1,R1), (L1,R2), (L2,R1), (L2,R2). Containment pairs
   * (closed-interval, all three axes): 2 — (L1,R2), (L2,R2). R1 extends past L1/L2 on all three
   * axes; R4 and L4 are Z-disjoint from anything they could contain or be contained by.
   */
  private def leftBoxes: DataFrame = {
    import sparkSession.implicits._
    Seq(
      TestBox3D(1, 0, 0, 0, 10, 10, 10),
      TestBox3D(2, 0, 0, 0, 10, 10, 10),
      TestBox3D(3, 20, 20, 0, 30, 30, 10),
      TestBox3D(4, 0, 0, 50, 10, 10, 60))
      .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
      .selectExpr(
        "id",
        "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
  }

  private def rightBoxes: DataFrame = {
    import sparkSession.implicits._
    Seq(
      TestBox3D(11, 5, 5, 5, 15, 15, 15),
      TestBox3D(12, 2, 2, 2, 8, 8, 8),
      TestBox3D(13, 40, 40, 0, 50, 50, 10),
      TestBox3D(14, 2, 2, 100, 8, 8, 200))
      .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
      .selectExpr(
        "id",
        "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
  }

  describe("Box3D spatial join") {

    it("ST_Intersects: broadcast index join filters Z-disjoint candidates") {
      val joined = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(L.box, R.box)"))

      assert(joined.queryExecution.executedPlan.collectFirst { case _: BroadcastIndexJoinExec =>
        true
      }.isDefined)
      // 4 true intersections. The L4 row produces three XY-overlapping candidates (L4-R1,
      // L4-R2, L4-R4) that the 2D R-tree emits and the Z refine rejects.
      assert(joined.count() == 4)
    }

    it("ST_Intersects: argument order is symmetric") {
      val joined = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Intersects(R.box, L.box)"))
      assert(joined.count() == 4)
    }

    it("ST_Contains: broadcast index join uses Box3D containment semantics") {
      val joined = leftBoxes
        .alias("L")
        .join(broadcast(rightBoxes.alias("R")), expr("ST_Contains(L.box, R.box)"))

      assert(joined.queryExecution.executedPlan.collectFirst { case _: BroadcastIndexJoinExec =>
        true
      }.isDefined)
      // Only R2 is fully inside L1 / L2 on all three axes. R1 sticks out in X/Y/Z; R4 is
      // disjoint in Z.
      assert(joined.count() == 2)
    }

    it("ST_Contains: edge-touching boxes count (closed-interval semantics)") {
      import sparkSession.implicits._
      val outer = Seq(TestBox3D(1, 0, 0, 0, 10, 10, 10))
        .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
        .selectExpr(
          "id",
          "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
      // Inner shares all three faces with the outer on the high side.
      val inner = Seq(TestBox3D(2, 5, 5, 5, 10, 10, 10))
        .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
        .selectExpr(
          "id",
          "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
      val joined = outer
        .alias("O")
        .join(broadcast(inner.alias("I")), expr("ST_Contains(O.box, I.box)"))
      assert(joined.count() == 1)
    }

    it("ST_Intersects: non-broadcast range join produces the same count") {
      val joined = leftBoxes
        .alias("L")
        .join(rightBoxes.alias("R"), expr("ST_Intersects(L.box, R.box)"))

      assert(joined.queryExecution.executedPlan.collectFirst { case _: RangeJoinExec =>
        true
      }.isDefined)
      assert(joined.count() == 4)
    }

    it("Inverted Box3D bounds in a join throw IllegalArgumentException") {
      import sparkSession.implicits._
      val ordered = Seq(TestBox3D(1, 0, 0, 0, 10, 10, 10))
        .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
        .selectExpr(
          "id",
          "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
      // Z inverted on the right side.
      val invertedZ = Seq(TestBox3D(2, 0, 0, 10, 10, 10, 0))
        .toDF("id", "xmin", "ymin", "zmin", "xmax", "ymax", "zmax")
        .selectExpr(
          "id",
          "ST_3DMakeBox(ST_PointZ(xmin, ymin, zmin), ST_PointZ(xmax, ymax, zmax)) AS box")
      val ex = intercept[Exception] {
        ordered
          .alias("L")
          .join(broadcast(invertedZ.alias("R")), expr("ST_Intersects(L.box, R.box)"))
          .collect()
      }
      assert(
        Iterator
          .iterate(ex: Throwable)(_.getCause)
          .takeWhile(_ != null)
          .exists(_.isInstanceOf[IllegalArgumentException]))
    }
  }
}

object Box3DJoinSuite {
  case class TestBox3D(
      id: Int,
      xmin: Double,
      ymin: Double,
      zmin: Double,
      xmax: Double,
      ymax: Double,
      zmax: Double)
}
