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

class Box3DAccessorSuite extends TestBaseScala {

  describe("Box3D accessors + ST_AsText") {

    it("ST_XMin/YMin/ZMin/XMax/YMax/ZMax accept Box3D") {
      val row = sparkSession
        .sql("""
          WITH t AS (
            SELECT ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)) AS b
          )
          SELECT
            ST_XMin(b), ST_YMin(b), ST_ZMin(b),
            ST_XMax(b), ST_YMax(b), ST_ZMax(b)
          FROM t
        """)
        .collect()(0)
      assert(row.getDouble(0) == 1.0)
      assert(row.getDouble(1) == 2.0)
      assert(row.getDouble(2) == 3.0)
      assert(row.getDouble(3) == 4.0)
      assert(row.getDouble(4) == 5.0)
      assert(row.getDouble(5) == 6.0)
    }

    it("Accessors propagate NULL on a NULL Box3D input") {
      val row = sparkSession
        .sql(
          "SELECT ST_XMin(b), ST_YMin(b), ST_ZMin(b), " +
            "ST_XMax(b), ST_YMax(b), ST_ZMax(b) " +
            "FROM (SELECT ST_3DMakeBox(ST_GeomFromText(NULL), ST_PointZ(1,1,1)) AS b)")
        .collect()(0)
      (0 until 6).foreach(i => assert(row.isNullAt(i), s"column $i should be NULL"))
    }

    it("ST_AsText(box3d) returns BOX3D(...) text") {
      val str = sparkSession
        .sql("SELECT ST_AsText(ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(10, 20, 30))) AS s")
        .collect()(0)
        .getString(0)
      assert(str == "BOX3D(0.0 0.0 0.0, 10.0 20.0 30.0)")
    }

    it("ST_AsText(box3d) returns NULL for NULL input") {
      val row = sparkSession
        .sql("SELECT ST_AsText(ST_3DMakeBox(ST_GeomFromText(NULL), ST_PointZ(1,1,1))) AS s")
        .collect()(0)
      assert(row.isNullAt(0))
    }
  }
}
