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

class Box3DDWithinSuite extends TestBaseScala {

  describe("ST_3DDWithin") {

    it("returns true for two POINT Z values within the supplied 3D distance") {
      // 3D distance from (0,0,0) to (1,1,1) is sqrt(3) ≈ 1.7320508075688772.
      val row = sparkSession
        .sql("SELECT ST_3DDWithin(ST_PointZ(0, 0, 0), ST_PointZ(1, 1, 1), 2.0) AS within")
        .first()
      assert(row.getBoolean(0))
    }

    it("returns false when the 3D distance exceeds the supplied threshold") {
      val row = sparkSession
        .sql("SELECT ST_3DDWithin(ST_PointZ(0, 0, 0), ST_PointZ(1, 1, 1), 1.7) AS within")
        .first()
      assert(!row.getBoolean(0))
    }

    it("handles XY LineString and Polygon inputs without NaN errors") {
      // Distance3DOp throws on NaN Z for non-point inputs; the implementation folds missing
      // Z to 0 before dispatching, so XY LineString and Polygon arguments work as documented.
      val row = sparkSession
        .sql(
          "SELECT ST_3DDWithin(ST_GeomFromText('LINESTRING(0 0, 3 4)'), ST_Point(0, 0), 1.0)" +
            " AS line_point, " +
            "ST_3DDWithin(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), " +
            "ST_Point(5, 5), 0.0) AS polygon_point")
        .first()
      assert(row.getBoolean(0))
      assert(row.getBoolean(1))
    }

    it("folds Z = 0 for XY-only geometry inputs") {
      val row = sparkSession
        .sql("SELECT ST_3DDWithin(ST_Point(0, 0), ST_Point(3, 4), 5.0) AS edge, " +
          "ST_3DDWithin(ST_Point(0, 0), ST_Point(3, 4), 4.999) AS just_outside")
        .first()
      assert(row.getBoolean(0))
      assert(!row.getBoolean(1))
    }

    it("returns true for overlapping Box3D values") {
      val row = sparkSession
        .sql(
          "SELECT ST_3DDWithin(" +
            "ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(5, 5, 5)), " +
            "ST_3DMakeBox(ST_PointZ(4, 4, 4), ST_PointZ(6, 6, 6)), 0.0) AS within")
        .first()
      assert(row.getBoolean(0))
    }

    it("returns true at the threshold edge for Box3D inputs (3-4-12 diagonal)") {
      val row = sparkSession
        .sql(
          "SELECT ST_3DDWithin(" +
            "ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(1, 1, 1)), " +
            "ST_3DMakeBox(ST_PointZ(4, 5, 13), ST_PointZ(5, 6, 14)), 13.0) AS edge")
        .first()
      assert(row.getBoolean(0))
    }

    it("throws on inverted Box3D bounds") {
      val ex = intercept[Exception] {
        sparkSession
          .sql(
            "SELECT ST_3DDWithin(" +
              "ST_3DMakeBox(ST_PointZ(0, 0, 5), ST_PointZ(5, 5, 0)), " +
              "ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(5, 5, 5)), 1.0)")
          .collect()
      }
      val messages =
        Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage)
      assert(messages.exists(m => m != null && m.contains("inverted bounds")))
    }

    it("returns NULL on null input") {
      val row = sparkSession
        .sql("SELECT ST_3DDWithin(ST_GeomFromText(NULL), ST_PointZ(0, 0, 0), 1.0) AS within")
        .first()
      assert(row.isNullAt(0))
    }
  }
}
