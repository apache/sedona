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

class Box3DIntersectsContainsSuite extends TestBaseScala {

  describe("Box3D predicates") {

    it("ST_Intersects covers overlap, face-, edge- and corner-touching") {
      val row = sparkSession
        .sql("""
          WITH t AS (
            SELECT
              ST_3DMakeBox(ST_PointZ(0,0,0), ST_PointZ(5,5,5))    AS a,
              ST_3DMakeBox(ST_PointZ(1,1,1), ST_PointZ(2,2,2))    AS inside,
              ST_3DMakeBox(ST_PointZ(3,3,3), ST_PointZ(7,7,7))    AS overlap,
              ST_3DMakeBox(ST_PointZ(5,0,0), ST_PointZ(10,5,5))   AS face,
              ST_3DMakeBox(ST_PointZ(5,5,5), ST_PointZ(10,10,10)) AS corner,
              ST_3DMakeBox(ST_PointZ(6,6,6), ST_PointZ(7,7,7))    AS disjoint
          )
          SELECT
            ST_Intersects(a, inside),
            ST_Intersects(a, overlap),
            ST_Intersects(a, face),
            ST_Intersects(a, corner),
            ST_Intersects(a, disjoint)
          FROM t
        """)
        .collect()(0)
      assert(row.getBoolean(0))
      assert(row.getBoolean(1))
      assert(row.getBoolean(2))
      assert(row.getBoolean(3))
      assert(!row.getBoolean(4))
    }

    it("ST_Contains is closed-interval (equal boxes contain each other)") {
      val row = sparkSession
        .sql("""
          WITH t AS (
            SELECT
              ST_3DMakeBox(ST_PointZ(0,0,0), ST_PointZ(5,5,5))    AS a,
              ST_3DMakeBox(ST_PointZ(1,1,1), ST_PointZ(2,2,2))    AS inside,
              ST_3DMakeBox(ST_PointZ(3,3,3), ST_PointZ(7,7,7))    AS overlap,
              ST_3DMakeBox(ST_PointZ(0,0,0), ST_PointZ(5,5,5))    AS equal
          )
          SELECT
            ST_Contains(a, inside),
            ST_Contains(a, overlap),
            ST_Contains(a, equal)
          FROM t
        """)
        .collect()(0)
      assert(row.getBoolean(0))
      assert(!row.getBoolean(1))
      assert(row.getBoolean(2))
    }

    it("ST_Intersects rejects inverted bounds") {
      val ex = intercept[Exception] {
        sparkSession
          .sql(
            "SELECT ST_Intersects(" +
              "ST_3DMakeBox(ST_PointZ(5,0,0), ST_PointZ(0,5,5)), " +
              "ST_3DMakeBox(ST_PointZ(0,0,0), ST_PointZ(1,1,1)))")
          .collect()
      }
      assert(
        Iterator
          .iterate(ex: Throwable)(_.getCause)
          .takeWhile(_ != null)
          .exists(_.isInstanceOf[IllegalArgumentException]))
    }

    it("Predicates propagate NULL when either argument is NULL") {
      val row = sparkSession
        .sql("""
          WITH t AS (
            SELECT
              ST_3DMakeBox(ST_PointZ(0,0,0), ST_PointZ(5,5,5)) AS a,
              ST_3DMakeBox(ST_GeomFromText(NULL), ST_PointZ(1,1,1)) AS n
          )
          SELECT ST_Intersects(a, n), ST_Contains(a, n) FROM t
        """)
        .collect()(0)
      assert(row.isNullAt(0))
      assert(row.isNullAt(1))
    }

    // Regression for the JoinQueryDetector / TraitJoinQueryBase Box3D mishandling: prior to the
    // hasBox3DInput guard, a Box3D-on-Box3D join would fall into the generic shape path and try
    // to deserialise the Box3D struct as a Geometry binary, throwing at runtime. With the guard
    // in place, the join silently falls back to row-by-row evaluation and the scalar Box3D
    // overload of ST_Intersects / ST_Contains produces the correct boolean.
    it("Box3D-on-Box3D join falls back to row-by-row evaluation without a cast error") {
      import sparkSession.implicits._
      val left = Seq(("a1", 0.0, 0.0, 0.0, 5.0, 5.0, 5.0)).toDF(
        "id",
        "xmin",
        "ymin",
        "zmin",
        "xmax",
        "ymax",
        "zmax")
      val right = Seq(("b1", 1.0, 1.0, 1.0, 2.0, 2.0, 2.0)).toDF(
        "id",
        "xmin",
        "ymin",
        "zmin",
        "xmax",
        "ymax",
        "zmax")
      left.createOrReplaceTempView("left_box3d")
      right.createOrReplaceTempView("right_box3d")
      val df = sparkSession.sql("""
        SELECT L.id AS l_id, R.id AS r_id
        FROM left_box3d L
        JOIN right_box3d R ON ST_Intersects(
          ST_3DMakeBox(ST_PointZ(L.xmin, L.ymin, L.zmin), ST_PointZ(L.xmax, L.ymax, L.zmax)),
          ST_3DMakeBox(ST_PointZ(R.xmin, R.ymin, R.zmin), ST_PointZ(R.xmax, R.ymax, R.zmax)))
      """)
      val rows = df.collect()
      assert(rows.length == 1)
      assert(rows(0).getString(0) == "a1" && rows(0).getString(1) == "b1")
    }
  }
}
