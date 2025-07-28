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

import com.google.common.geometry.{S2LatLng, S2Polyline}
import org.apache.sedona.common.S2Geography.{PolylineGeography, S2Geography, WKTReader}

class S2distanceTestScala extends TestBaseScala {

  describe("Sedona-SQL Distance Test") {
    it("Passed ST_S2Distance between two points (0,0) and (90,0)") {
      val df = sparkSession.sql("""
          |SELECT ST_S2Distance(
          |  ST_S2PointFromText('POINT (0 0)', 'wkt'),
          |  ST_S2PointFromText('POINT (0 90)', 'wkt')
          |) AS dist
          |""".stripMargin)

      val dist = df.first().getDouble(0)
      assert(math.abs(dist - (Math.PI / 2)) < 1e-9, s"Expected π/2 but got $dist")
    }

  }
  it("Passed ST_S2MaxDistance between two points (0,0) and (90,0)") {
    val df = sparkSession.sql("""
        |SELECT ST_S2MaxDistance(
        |  ST_S2PointFromText('POINT (0 0)', 'wkt'),
        |  ST_S2PointFromText('POINT (0 90)', 'wkt')
        |) AS maxdist
        |""".stripMargin)

    val maxdist = df.first().getDouble(0)
    assert(math.abs(maxdist - (Math.PI / 2)) < 1e-9, s"Expected π/2 but got $maxdist")
  }

  it("Passed ST_S2MinimumClearanceLineBetween from polygon to point") {
    val polygonWKT = "POLYGON ((-64 45, 0 45, 0 0, -64 45))"
    val pointWKT = "POINT (0 45)"

    val df = sparkSession.sql(s"""
         |SELECT ST_AsTextGeog(ST_S2MinimumClearanceLineBetween(
         |  ST_S2PolygonFromText('$polygonWKT', 'wkt'),
         |  ST_S2PointFromText('$pointWKT', 'wkt')
         |)) AS line
         |""".stripMargin)

    val lineWKT = df.first().getString(0)
    val expected = "LINESTRING (0.000000 45.000000, 0.000000 45.000000)"
    assert(lineWKT.equals(expected))
  }

}
