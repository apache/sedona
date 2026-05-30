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

class Box3DExtentSuite extends TestBaseScala {

  describe("ST_3DExtent aggregate") {

    it("aggregates 3D bbox over geometry rows, treating XY as Z=0") {
      val row = sparkSession
        .sql("""
          WITH t AS (
            SELECT ST_GeomFromText('POINT(1 1)') AS g UNION ALL
            SELECT ST_GeomFromWKT('POINT Z(5 7 -2)') UNION ALL
            SELECT ST_GeomFromWKT('LINESTRING Z(3 2 4, 6 4 9)')
          )
          SELECT ST_AsText(ST_3DExtent(g)) AS s FROM t
        """)
        .collect()(0)
      assert(row.getString(0) == "BOX3D(1.0 1.0 -2.0, 6.0 7.0 9.0)")
    }

    it("returns NULL on empty input") {
      val v = sparkSession
        .sql("SELECT ST_3DExtent(g) FROM (SELECT ST_GeomFromText(NULL) AS g) WHERE false")
        .collect()
      assert(v.isEmpty || v(0).isNullAt(0))
    }

    it("skips NULL and empty geometry rows") {
      val row = sparkSession
        .sql("""
          WITH t AS (
            SELECT ST_GeomFromWKT('POINT Z(5 7 -2)') AS g UNION ALL
            SELECT ST_GeomFromText(NULL) UNION ALL
            SELECT ST_GeomFromText('LINESTRING EMPTY') UNION ALL
            SELECT ST_GeomFromWKT('POINT Z(1 1 1)')
          )
          SELECT ST_AsText(ST_3DExtent(g)) AS s FROM t
        """)
        .collect()(0)
      assert(row.getString(0) == "BOX3D(1.0 1.0 -2.0, 5.0 7.0 1.0)")
    }
  }
}
