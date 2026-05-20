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

class Box3DConstructorSuite extends TestBaseScala {

  describe("Box3D constructors") {

    it("ST_Box3D returns the 3D bbox, defaulting Z=0 for XY input") {
      val row = sparkSession
        .sql("SELECT ST_Box3D(ST_GeomFromText('LINESTRING(0 0, 10 20)')) AS box3d_xy, " +
          "ST_Box3D(ST_GeomFromWKT('LINESTRING Z(0 0 -3, 5 10 7)')) AS box3d_xyz")
        .collect()(0)
      assert(row.getAs[Box3D]("box3d_xy") == new Box3D(0, 0, 0, 10, 20, 0))
      assert(row.getAs[Box3D]("box3d_xyz") == new Box3D(0, 0, -3, 5, 10, 7))
    }

    it("ST_Box3D returns NULL for null and empty input") {
      val row = sparkSession
        .sql("SELECT ST_Box3D(ST_GeomFromText(NULL)) AS b_null, " +
          "ST_Box3D(ST_GeomFromText('LINESTRING EMPTY')) AS b_empty")
        .collect()(0)
      assert(row.isNullAt(0))
      assert(row.isNullAt(1))
    }

    it("ST_3DMakeBox builds a Box3D from two POINTZ corners") {
      val row = sparkSession
        .sql("SELECT ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(2, 4, 6)) AS b")
        .collect()(0)
      assert(row.getAs[Box3D]("b") == new Box3D(0, 0, 0, 2, 4, 6))
    }

    it("ST_3DMakeBox treats missing Z as 0") {
      val row = sparkSession
        .sql("SELECT ST_3DMakeBox(ST_Point(0, 0), ST_Point(2, 4)) AS b")
        .collect()(0)
      assert(row.getAs[Box3D]("b") == new Box3D(0, 0, 0, 2, 4, 0))
    }

    it("ST_3DMakeBox returns NULL for null point input") {
      val row = sparkSession
        .sql("SELECT ST_3DMakeBox(ST_GeomFromText(NULL), ST_PointZ(2, 4, 6)) AS b")
        .collect()(0)
      assert(row.isNullAt(0))
    }

    it("ST_3DMakeBox returns NULL for empty point input") {
      val row = sparkSession
        .sql("SELECT ST_3DMakeBox(ST_GeomFromText('POINT EMPTY'), ST_PointZ(2, 4, 6)) AS b")
        .collect()(0)
      assert(row.isNullAt(0))
    }

    it("ST_3DMakeBox throws for non-POINT input") {
      val thrown = intercept[Exception] {
        sparkSession
          .sql("SELECT ST_3DMakeBox(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_PointZ(2, 4, 6))")
          .collect()
      }
      val messages =
        Iterator.iterate[Throwable](thrown)(_.getCause).takeWhile(_ != null).map(_.getMessage)
      assert(messages.exists(m => m != null && m.contains("requires two POINT geometries")))
    }
  }
}
