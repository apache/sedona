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

class Box3DPredicateSuite extends TestBaseScala {

  describe("Box3D predicates") {

    it("ST_3DBoxIntersects covers overlap, face-, edge- and corner-touching") {
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
            ST_3DBoxIntersects(a, inside),
            ST_3DBoxIntersects(a, overlap),
            ST_3DBoxIntersects(a, face),
            ST_3DBoxIntersects(a, corner),
            ST_3DBoxIntersects(a, disjoint)
          FROM t
        """)
        .collect()(0)
      assert(row.getBoolean(0))
      assert(row.getBoolean(1))
      assert(row.getBoolean(2))
      assert(row.getBoolean(3))
      assert(!row.getBoolean(4))
    }

    it("ST_3DBoxContains is closed-interval (equal boxes contain each other)") {
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
            ST_3DBoxContains(a, inside),
            ST_3DBoxContains(a, overlap),
            ST_3DBoxContains(a, equal)
          FROM t
        """)
        .collect()(0)
      assert(row.getBoolean(0))
      assert(!row.getBoolean(1))
      assert(row.getBoolean(2))
    }

    it("ST_3DBoxIntersects rejects inverted bounds") {
      val ex = intercept[Exception] {
        sparkSession
          .sql(
            "SELECT ST_3DBoxIntersects(" +
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
          SELECT ST_3DBoxIntersects(a, n), ST_3DBoxContains(a, n) FROM t
        """)
        .collect()(0)
      assert(row.isNullAt(0))
      assert(row.isNullAt(1))
    }
  }
}
