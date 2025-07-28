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

import com.google.common.geometry.S2LatLng
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Literal}
import org.apache.spark.sql.sedona_sql.expressions._

class S2predicateTestScala extends TestBaseScala {

  describe("Sedona-SQL Predicate Test") {

    it("Passed ST_S2Contains") {
      val pointDf =
        sparkSession.sql("SELECT 'POINT (24 20)' AS geom, 4326 AS srid")
      val actualPointDf = pointDf.selectExpr("ST_S2PointFromText(geom, 'wkt') AS arealandmark")
      actualPointDf.createOrReplaceTempView("pointdf")

      val polygonDF =
        sparkSession.sql(
          "SELECT 'POLYGON ((1 100, 1000 100, 1000 1100, 1 1100, 1 100))' AS poly, 4326 AS srid")
      val actualPolyDF = polygonDF.selectExpr("ST_S2PolygonFromText(poly, 'wkt') AS aoi")
      actualPolyDF.createOrReplaceTempView("polydf")

      // Cross join to bring both into the same row context
      val resultDF = sparkSession.sql("""
          |SELECT * FROM pointdf, polydf
          |WHERE ST_S2Contains(polydf.aoi, pointdf.arealandmark)
          |""".stripMargin)

      assert(resultDF.count() == 1)
    }

    it("Passed ST_S2Intersects") {
      val pointDf =
        sparkSession.sql("SELECT 'POINT (24 20)' AS geom, 4326 AS srid")
      val actualPointDf = pointDf.selectExpr("ST_S2PointFromText(geom, 'wkt') AS arealandmark")
      actualPointDf.createOrReplaceTempView("pointdf")

      val polygonDF =
        sparkSession.sql(
          "SELECT 'POLYGON ((1 100, 1000 100, 1000 1100, 1 1100, 1 100))' AS poly, 4326 AS srid")
      val actualPolyDF = polygonDF.selectExpr("ST_S2PolygonFromText(poly, 'wkt') AS aoi")
      actualPolyDF.createOrReplaceTempView("polydf")

      val resultDF = sparkSession.sql("""
          |SELECT * FROM pointdf, polydf
          |WHERE ST_S2Intersects(polydf.aoi, pointdf.arealandmark)
          |""".stripMargin)

      assert(resultDF.count() == 1)
    }

    it("Passed ST_S2Equals for ST_Point") {
      val pointDf =
        sparkSession.sql("SELECT 'POINT (24 20)' AS geom, 4326 AS srid")
      val actualPointDf = pointDf.selectExpr("ST_S2PointFromText(geom, 'wkt') AS point")
      actualPointDf.createOrReplaceTempView("pointdf")

      val pointDf2 =
        sparkSession.sql("SELECT 'POINT (24 20)' AS geom2, 4326 AS srid")
      val actualPointDf2 = pointDf2.selectExpr("ST_S2PointFromText(geom2, 'wkt') AS point2")
      actualPointDf2.createOrReplaceTempView("pointdf2")

      val equaldf = sparkSession.sql("""
          |SELECT * FROM pointdf, pointdf2
          |WHERE ST_S2Equals(pointdf.point, pointdf2.point2)
          |""".stripMargin)

      assert(equaldf.count() == 1, s"Expected 1 match but got ${equaldf.count()}")

    }

    it("Passed ST_S2Equals for ST_PolygonFromEnvelope and ST_PolygonFromText") {
      // WKT of target polygon
      val wkt =
        "POLYGON ((100.01 200.01, 100.5 200.01, 100.5 200.5, 100.01 200.5, 100.01 200.01))"

      // Create two DataFrames with the same WKT polygon
      val df1 = sparkSession.sql(s"SELECT ST_S2PolygonFromText('$wkt', 'wkt') AS poly1")
      val df2 = sparkSession.sql(s"SELECT ST_S2PolygonFromText('$wkt', 'wkt') AS poly2")

      df1.createOrReplaceTempView("df1")
      df2.createOrReplaceTempView("df2")

      // Compare using ST_S2Equals
      val equaldf = sparkSession.sql("""
          |SELECT * FROM df1, df2
          |WHERE ST_S2Equals(df1.poly1, df2.poly2)
          |""".stripMargin)

      assert(equaldf.count() == 1, s"Expected 1 match but got ${equaldf.count()}")

    }

    it("Passed ST_S2IntersectsBox") {
      def latLngRectToWKT(lo: S2LatLng, hi: S2LatLng): String = {
        s"POLYGON ((${lo.lng().degrees()} ${lo.lat().degrees()}, ${hi.lng().degrees()} ${lo
            .lat()
            .degrees()}, ${hi.lng().degrees()} ${hi.lat().degrees()}, ${lo.lng().degrees()} ${hi
            .lat()
            .degrees()}, ${lo.lng().degrees()} ${lo.lat().degrees()}))"
      }

      // Create square polygon covering (0,0) to (1,1)
      val squareLo = S2LatLng.fromDegrees(0, 0)
      val squareHi = S2LatLng.fromDegrees(1, 1)
      val squareWKT = latLngRectToWKT(squareLo, squareHi)
      val squareDF = sparkSession.sql(s"SELECT ST_S2PolygonFromText('$squareWKT', 'wkt') AS poly")
      squareDF.createOrReplaceTempView("square")

      // Create intersecting box from (0.5,0.5) to (1.5,1.5)
      val hitLo = S2LatLng.fromDegrees(0.5, 0.5)
      val hitHi = S2LatLng.fromDegrees(1.5, 1.5)
      val hitWKT = latLngRectToWKT(hitLo, hitHi)
      val hitDF = sparkSession.sql(s"SELECT ST_S2PolygonFromText('$hitWKT', 'wkt') AS bbox")
      hitDF.createOrReplaceTempView("bbox")

      // Create non-overlapping box from (2,2) to (3,3)
      val missLo = S2LatLng.fromDegrees(2, 2)
      val missHi = S2LatLng.fromDegrees(3, 3)
      val missWKT = latLngRectToWKT(missLo, missHi)
      val missDF = sparkSession.sql(s"SELECT ST_S2PolygonFromText('$missWKT', 'wkt') AS bbox")
      missDF.createOrReplaceTempView("missbox")

      // Test no intersection
      val miss = sparkSession.sql("""
          |SELECT * FROM square, missbox
          |WHERE ST_S2IntersectsBox(square.poly, missbox.bbox, 0.1D)
          |""".stripMargin)
      assert(miss.count() == 0, "Expected no intersection for distant box")

      // Test intersection
      val result = sparkSession.sql("""
          |SELECT * FROM square, bbox
          |WHERE ST_S2IntersectsBox(square.poly, bbox.bbox, 0.1D)
          |""".stripMargin)
      assert(result.count() == 1, "Expected intersection with partial overlap")
    }
  }
}
