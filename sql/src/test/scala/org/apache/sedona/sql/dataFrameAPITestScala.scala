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

import scala.collection.mutable.WrappedArray
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.functions.{col, lit}
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.st_constructors._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.sedona_sql.expressions.st_predicates._
import org.apache.spark.sql.sedona_sql.expressions.st_aggregates._

import scala.collection.mutable

class dataFrameAPITestScala extends TestBaseScala {

  import sparkSession.implicits._

  describe("Sedona DataFrame API Test") {
    // constructors
    it("passed st_point") {
      val df = sparkSession.sql("SELECT 0.0 AS x, 1.0 AS y").select(ST_Point("x", "y"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_pointz") {
      val df = sparkSession.sql("SELECT 0.0 AS x, 1.0 AS y, 2.0 AS z").select(ST_AsText(ST_PointZ("x", "y", "z")))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "POINT Z(0 1 2)"
      assert(actualResult == expectedResult)
    }

    it("passed st_pointfromtext") {
      val df = sparkSession.sql("SELECT '0.0,1.0' AS c").select(ST_PointFromText($"c", lit(',')))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_polygonfromtext") {
      val df = sparkSession.sql("SELECT '0.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0' AS c").select(ST_PolygonFromText($"c", lit(',')))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("passed st_linefromtext") {
      val df = sparkSession.sql("SELECT 'Linestring(1 2, 3 4)' AS wkt").select(ST_LineFromText("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 2, 3 4)"
      assert(actualResult == expectedResult)
    }

    it("passed st_linestringfromtext") {
      val df = sparkSession.sql("SELECT '0.0,0.0,1.0,0.0' AS c").select(ST_LineStringFromText($"c", lit(',')))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromwkt") {
      val df = sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromWKT("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromwkt with srid") {
      val df = sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromWKT("wkt", 4326))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      assert(actualResult.toText == "POINT (0 1)")
      assert(actualResult.getSRID == 4326)
    }

    it("passed st_geomfromtext") {
      val df = sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromText("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromtext with srid") {
      val df = sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromText("wkt", 4326))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      assert(actualResult.toText == "POINT (0 1)")
      assert(actualResult.getSRID == 4326)
    }

    it("passed st_geomfromwkb") {
      val wkbSeq = Seq[Array[Byte]](Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
      val df = wkbSeq.toDF("wkb").select(ST_GeomFromWKB("wkb"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromgeojson") {
      val geojson = "{ \"type\": \"Feature\", \"properties\": { \"prop\": \"01\" }, \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 0.0, 1.0 ] }},"
      val df = Seq[String](geojson).toDF("geojson").select(ST_GeomFromGeoJSON("geojson"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_polygonfromenvelope with column values") {
      val df = sparkSession.sql("SELECT 0.0 AS minx, 1.0 AS miny, 2.0 AS maxx, 3.0 AS maxy")
      val actualResult = df.select(ST_PolygonFromEnvelope("minx", "miny", "maxx", "maxy")).take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"
      assert(actualResult == expectedResult)
    }

    it("passed st_polygonfromenvelope with literal values") {
      val df = sparkSession.sql("SELECT null AS c").select(ST_PolygonFromEnvelope(0.0, 1.0, 2.0, 3.0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromgeohash") {
      val df = sparkSession.sql("SELECT 's00twy01mt' AS geohash").select(ST_GeomFromGeoHash("geohash", 4))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0.703125 0.87890625, 0.703125 1.0546875, 1.0546875 1.0546875, 1.0546875 0.87890625, 0.703125 0.87890625))"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromgml") {
      val gmlString = "<gml:LineString srsName=\"EPSG:4269\"><gml:coordinates>-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932</gml:coordinates></gml:LineString>"
      val df = sparkSession.sql(s"SELECT '$gmlString' AS gml").select(ST_GeomFromGML("gml"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (-71.16028 42.258729, -71.160837 42.259112, -71.161143 42.25932)"
      assert(actualResult == expectedResult)
    }

    it ("passed st_geomfromkml") {
      val kmlString = "<LineString><coordinates>-71.1663,42.2614 -71.1667,42.2616</coordinates></LineString>"
      val df = sparkSession.sql(s"SELECT '$kmlString' as kml").select(ST_GeomFromKML("kml"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (-71.1663 42.2614, -71.1667 42.2616)"
      assert(actualResult == expectedResult)
    }

    // functions
      it("Passed ST_ConcaveHull"){
        val baseDF = sparkSession.sql("SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as mline")
        val df = baseDF.select(ST_ConcaveHull("mline", 1, true))
        val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
        assert(actualResult == "POLYGON ((1 2, 2 2, 3 2, 5 0, 4 0, 1 0, 0 0, 1 2))")
      }

    it("Passed ST_ConvexHull") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_ConvexHull("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualResult == "POLYGON ((0 0, 1 1, 1 0, 0 0))")
    }

    it("Passed ST_Buffer") {
      val polygonDf = sparkSession.sql("SELECT ST_Point(1.0, 1.0) AS geom")
      val df = polygonDf.select(ST_Buffer("geom", 1.0).as("geom")).selectExpr("ST_PrecisionReduce(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((1.98 0.8, 1.92 0.62, 1.83 0.44, 1.71 0.29, 1.56 0.17, 1.38 0.08, 1.2 0.02, 1 0, 0.8 0.02, 0.62 0.08, 0.44 0.17, 0.29 0.29, 0.17 0.44, 0.08 0.62, 0.02 0.8, 0 1, 0.02 1.2, 0.08 1.38, 0.17 1.56, 0.29 1.71, 0.44 1.83, 0.62 1.92, 0.8 1.98, 1 2, 1.2 1.98, 1.38 1.92, 1.56 1.83, 1.71 1.71, 1.83 1.56, 1.92 1.38, 1.98 1.2, 2 1, 1.98 0.8))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Envelope") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_Envelope("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_YMax") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_YMax("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_YMin") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_YMin("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 0.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Centroid") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom")
      val df = polygonDf.select(ST_Centroid("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Length") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = lineDf.select(ST_Length("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Area") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_Area("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 0.5
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Distance") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 0.0) as b")
      val df = pointDf.select(ST_Distance("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_3DDistance") {
      val pointDf = sparkSession.sql("SELECT ST_PointZ(0.0, 0.0, 0.0) AS a, ST_PointZ(3.0, 0.0, 4.0) as b")
      val df = pointDf.select(ST_3DDistance("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 5.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Transform") {
      val pointDf = sparkSession.sql("SELECT ST_Point(1.0, 1.0) AS geom")
      val df = pointDf.select(ST_Transform($"geom", lit("EPSG:4326"), lit("EPSG:32649")).as("geom")).selectExpr("ST_PrecisionReduce(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (-33741810.95 1823994.03)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Intersection") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS a, ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') AS b")
      val df = polygonDf.select(ST_Intersection("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsValid") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS geom")
      val df = polygonDf.select(ST_IsValid("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Boolean]
      assert(actualResult)
    }

    it("Passed ST_PrecisionReduce") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.12, 0.23) AS geom")
      val df = pointDf.select(ST_PrecisionReduce("geom", 1))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0.1 0.2)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsSimple") {
      val triangleDf = sparkSession.sql("SELECT ST_GeomFromWkt('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = triangleDf.select(ST_IsSimple("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Boolean]
      assert(actualResult)
    }

    it("Passed ST_MakeValid On Invalid Polygon") {
      val invalidDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS geom")
      val df = invalidDf.select(ST_MakeValid("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MakePolygon") {
      val invalidDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = invalidDf.select(ST_MakePolygon("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MakePolygon with holes") {
      val invalidDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom, array(ST_GeomFromWKT('LINESTRING (0.5 0.1, 0.7 0.1, 0.7 0.3, 0.5 0.1)')) AS holes")
      val df = invalidDf.select(ST_MakePolygon("geom", "holes"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0), (0.5 0.1, 0.7 0.1, 0.7 0.3, 0.5 0.1))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SimplifyPreserveTopology") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_SimplifyPreserveTopology("geom", 0.2))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AsText") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_AsText("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AsGeoJSON") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_AsGeoJSON("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AsBinary") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_AsBinary("geom"))
      val actualResult = Hex.encodeHexString(df.take(1)(0).get(0).asInstanceOf[Array[Byte]])
      val expectedResult = "010100000000000000000000000000000000000000"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AsGML") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_AsGML("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "<gml:Point>\n  <gml:coordinates>\n    0.0,0.0 \n  </gml:coordinates>\n</gml:Point>\n"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AsKML") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_AsKML("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "<Point>\n  <coordinates>0.0,0.0</coordinates>\n</Point>\n"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SRID") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_SRID("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SetSRID") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_SetSRID("geom", 3021))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].getSRID()
      val expectedResult = 3021
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AsEWKB") {
      val sridPointDf = sparkSession.sql("SELECT ST_SetSRID(ST_Point(0.0, 0.0), 3021) AS geom")
      val df = sridPointDf.select(ST_AsEWKB("geom"))
      val actualResult = Hex.encodeHexString(df.take(1)(0).get(0).asInstanceOf[Array[Byte]])
      val expectedResult = "0101000020cd0b000000000000000000000000000000000000"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_NPoints") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS geom")
      val df = lineDf.select(ST_NPoints("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 2
      assert(actualResult == expectedResult)
    }

    it("Passed ST_GeometryType") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_GeometryType("geom"))
      val actualResult = df.take(1)(0).getString(0)
      val expectedResult = "ST_Point"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Difference") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') AS a,ST_GeomFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))') AS b")
      val df = polygonDf.select(ST_Difference("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SymDifference") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') AS a, ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') AS b")
      val df = polygonDf.select(ST_SymDifference("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Union") {
      val polygonDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') AS a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') AS b")
      val df = polygonDf.select(ST_Union("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Azimuth") {
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 1.0) AS b")
      val df = baseDf.select(ST_Azimuth("a", "b"))
      val actualResult = df.take(1)(0).getDouble(0) * 180 / math.Pi
      val expectedResult = 45.0
      assert(actualResult == expectedResult)
    }

    it("Should pass ST_X") {
      val baseDf = sparkSession.sql("SELECT ST_PointZ(0.0, 1.0, 2.0) AS geom")
      val df = baseDf.select(ST_X("geom"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 0.0
      assert(actualResult == expectedResult)
    }

    it("Should pass ST_Y") {
      val baseDf = sparkSession.sql("SELECT ST_PointZ(0.0, 1.0, 2.0) AS geom")
      val df = baseDf.select(ST_Y("geom"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Should pass ST_Z") {
      val baseDf = sparkSession.sql("SELECT ST_PointZ(0.0, 1.0, 2.0) AS geom")
      val df = baseDf.select(ST_Z("geom"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 2.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_StartPoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_StartPoint("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Boundary") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = baseDf.select(ST_Boundary("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 1 1, 0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_EndPoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_EndPoint("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_ExteriorRing") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = baseDf.select(ST_ExteriorRing("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 1 1, 0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_GeometryN") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0))') AS geom")
      val df = baseDf.select(ST_GeometryN("geom", 0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_InteriorRingN") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
      val df = baseDf.select(ST_InteriorRingN("geom", 0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 1, 2 2, 2 1, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Dump") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
      val df = baseDf.select(ST_Dump("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[WrappedArray[Any]].map(_.asInstanceOf[Geometry].toText)
      val expectedResult = Array("POINT (0 0)", "POINT (1 1)")
      assert(actualResult(0) == expectedResult(0))
      assert(actualResult(1) == expectedResult(1))
    }

    it("Passed ST_DumpPoints") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_DumpPoints("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[WrappedArray[Any]].map(_.asInstanceOf[Geometry].toText)
      val expectedResult = Array("POINT (0 0)", "POINT (1 0)")
      assert(actualResult(0) == expectedResult(0))
      assert(actualResult(1) == expectedResult(1))
    }

    it("Passed ST_IsClosed") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = baseDf.select(ST_IsClosed("geom"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_NumInteriorRings") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
      val df = baseDf.select(ST_NumInteriorRings("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 1
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AddPoint without index") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
      val df = baseDf.select(ST_AddPoint("line", "point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AddPoint with index") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
      val df = baseDf.select(ST_AddPoint("line", "point", 1))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 1, 1 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_RemovePoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1)') AS geom")
      val df = baseDf.select(ST_RemovePoint("geom", 1))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SetPoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
      val df = baseDf.select(ST_SetPoint("line", 1, "point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsRing") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = baseDf.select(ST_IsRing("geom"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Subdivide") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom")
      val df = baseDf.select(ST_SubDivide("geom", 5))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[WrappedArray[Any]].map(_.asInstanceOf[Geometry].toText)
      val expectedResult = Array[String]("LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)")
      assert(actualResult(0) == expectedResult(0))
      assert(actualResult(1) == expectedResult(1))
    }

    it("Passed ST_SubdivideExplode") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom")
      val df = baseDf.select(ST_SubDivideExplode("geom", 5))
      val actualResults = df.take(2).map(_.get(0).asInstanceOf[Geometry].toText).sortWith(_ < _)
      val expectedResult = Array[String]("LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)")
      assert(actualResults(0) == expectedResult(0))
      assert(actualResults(1) == expectedResult(1))
    }

    it("Passed ST_NumGeometries") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
      val df = baseDf.select(ST_NumGeometries("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 2
      assert(actualResult == expectedResult)
    }

    it("Passed ST_LineMerge") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 2 0))') AS geom")
      val df = baseDf.select(ST_LineMerge("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 2 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_FlipCoordinates") {
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 1.0) AS geom")
      val df = baseDf.select(ST_FlipCoordinates("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MinimumBoundingCircle with default quadrantSegments") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_MinimumBoundingCircle("geom").as("geom")).selectExpr("ST_PrecisionReduce(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0.99 -0.1, 0.96 -0.19, 0.92 -0.28, 0.85 -0.35, 0.78 -0.42, 0.69 -0.46, 0.6 -0.49, 0.5 -0.5, 0.4 -0.49, 0.31 -0.46, 0.22 -0.42, 0.15 -0.35, 0.08 -0.28, 0.04 -0.19, 0.01 -0.1, 0 0, 0.01 0.1, 0.04 0.19, 0.08 0.28, 0.15 0.35, 0.22 0.42, 0.31 0.46, 0.4 0.49, 0.5 0.5, 0.6 0.49, 0.69 0.46, 0.78 0.42, 0.85 0.35, 0.92 0.28, 0.96 0.19, 0.99 0.1, 1 0, 0.99 -0.1))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MinimumBoundingCircle with specified quadrantSegments") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_MinimumBoundingCircle("geom", 2).as("geom")).selectExpr("ST_PrecisionReduce(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0.85 -0.35, 0.5 -0.5, 0.15 -0.35, 0 0, 0.15 0.35, 0.5 0.5, 0.85 0.35, 1 0, 0.85 -0.35))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MinimumBoundingRadius") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_MinimumBoundingRadius("geom").as("c")).select("c.center", "c.radius")
      val rowResult = df.take(1)(0)

      val actualCenter = rowResult.get(0).asInstanceOf[Geometry].toText()
      val actualRadius = rowResult.getDouble(1)

      val expectedCenter = "POINT (0.5 0)"
      val expectedRadius = 0.5

      assert(actualCenter == expectedCenter)
      assert(actualRadius == expectedRadius)
    }

    it ("Passed ST_LineSubstring") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line")
      val df = baseDf.select(ST_LineSubstring("line", 0.5, 1.0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 0, 2 0)"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_LineInterpolatePoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line")
      val df = baseDf.select(ST_LineInterpolatePoint("line", 0.5))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_Multi"){
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS point")
      val df = baseDf.select(ST_Multi("point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0))"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_PointOnSurface") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
      val df = baseDf.select(ST_PointOnSurface("line"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_Reverse") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
      val df = baseDf.select(ST_Reverse("line"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 0, 0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_PointN") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
      val df = baseDf.select(ST_PointN("line", 2))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_AsEWKT") {
      val baseDf = sparkSession.sql("SELECT ST_SetSRID(ST_Point(0.0, 0.0), 4326) AS point")
      val df = baseDf.select(ST_AsEWKT("point"))
      val actualResult = df.take(1)(0).getString(0)
      val expectedResult = "SRID=4326;POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_Force_2D") {
      val baseDf = sparkSession.sql("SELECT ST_PointZ(0.0, 0.0, 1.0) AS point")
      val df = baseDf.select(ST_Force_2D("point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_IsEmpty") {
      val baseDf = sparkSession.sql("SELECT ST_Difference(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0)) AS empty_geom")
      val df = baseDf.select(ST_IsEmpty("empty_geom"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_XMax") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
      val df = baseDf.select(ST_XMax("line"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_XMin") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
      val df = baseDf.select(ST_XMin("line"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 0.0
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_BuildArea") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 0))') AS multiline")
      val df = baseDf.select(ST_BuildArea("multiline").as("geom")).selectExpr("ST_Normalize(geom)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_Collect with array argument") {
      val baseDf = sparkSession.sql("SELECT array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)) as points")
      val df = baseDf.select(ST_Collect("points"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0), (1 1))"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_Collect with variable arguments") {
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 1.0) AS b")
      val df = baseDf.select(ST_Collect("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0), (1 1))"
      assert(actualResult == expectedResult)
    }

    it ("Passed St_CollectionExtract with default geomType") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
      val df = baseDf.select(ST_CollectionExtract("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTILINESTRING ((0 0, 1 0))"
      assert(actualResult == expectedResult)
    }

    it ("Passed St_CollectionExtract with specified geomType") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
      val df = baseDf.select(ST_CollectionExtract("geom", 1))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Normalize") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 0))') AS polygon")
      val df = baseDf.select(ST_Normalize("polygon"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Split") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1.5 1.5, 2 2)') AS input, ST_GeomFromWKT('MULTIPOINT (0.5 0.5, 1 1)') AS blade")
      var df = baseDf.select(ST_Split("input", "blade"))
      var actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))"
      assert(actualResult == expectedResult)

      df = baseDf.select(ST_Split($"input", $"blade"))
      actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualResult == expectedResult)
    }

    // predicates
    it("Passed ST_Contains") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(0.5, 0.25) AS b")
      val df = baseDf.select(ST_Contains("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }
    it("Passed ST_Intersects") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS a, ST_GeomFromWKT('LINESTRING (0 1, 1 0)') AS b")
      val df = baseDf.select(ST_Intersects("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Within") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(0.5, 0.25) AS b")
      val df = baseDf.select(ST_Within("b", "a"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Equals for ST_Point") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS a, ST_GeomFromWKT('LINESTRING (1 0, 0 0)') AS b")
      val df = baseDf.select(ST_Equals("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Crosses") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') AS a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') AS b")
      val df = baseDf.select(ST_Crosses("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Touches") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((1 1, 1 0, 2 0, 1 1))') AS b")
      val df = baseDf.select(ST_Touches("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Overlaps") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((0.5 1, 1.5 0, 2 0, 0.5 1))') AS b")
      val df = baseDf.select(ST_Overlaps("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Disjoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((2 0, 3 0, 3 1, 2 0))') AS b")
      val df = baseDf.select(ST_Disjoint("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_OrderingEquals") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS a, ST_GeomFromWKT('LINESTRING (1 0, 0 0)') AS b")
      val df = baseDf.select(ST_OrderingEquals("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(!actualResult)
    }

    it("Passed ST_Covers") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(1.0, 0.0) AS b, ST_Point(0.0, 1.0) AS c")
      val df = baseDf.select(ST_Covers("a", "b"), ST_Covers("a", "c"))
      val actualResult = df.take(1)(0)
      assert(actualResult.getBoolean(0))
      assert(!actualResult.getBoolean(1))
    }

    it("Passed ST_CoveredBy") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(1.0, 0.0) AS b, ST_Point(0.0, 1.0) AS c")
      val df = baseDf.select(ST_CoveredBy("b", "a"), ST_CoveredBy("c", "a"))
      val actualResult = df.take(1)(0)
      assert(actualResult.getBoolean(0))
      assert(!actualResult.getBoolean(1))
    }

    // aggregates
    it("Passed ST_Envelope_Aggr") {
      val baseDf = sparkSession.sql("SELECT explode(array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0))) AS geom")
      val df = baseDf.select(ST_Envelope_Aggr("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Union_Aggr") {
      val baseDf = sparkSession.sql("SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))) AS geom")
      val df = baseDf.select(ST_Union_Aggr("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((1 0, 0 0, 0 1, 1 1, 2 1, 2 0, 1 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Intersection_Aggr") {
      val baseDf = sparkSession.sql("SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))'))) AS geom")
      val df = baseDf.select(ST_Intersection_Aggr("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((2 0, 1 0, 1 1, 2 1, 2 0))"
      assert(actualResult == expectedResult)
    }

    it ("Passed ST_LineFromMultiPoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))') AS multipoint")
      val df = baseDf.select(ST_LineFromMultiPoint("multipoint"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (10 40, 40 30, 20 20, 30 10)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_S2CellIDs") {
      val baseDF = sparkSession.sql("SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as geom")
      val df = baseDF.select(ST_S2CellIDs("geom", 6))
      val dfMRB = baseDF.select(ST_S2CellIDs(ST_Envelope(col("geom")), lit(6)))
      val actualResult = df.take(1)(0).getAs[mutable.WrappedArray[Long]](0).toSet
      val mbrResult = dfMRB.take(1)(0).getAs[mutable.WrappedArray[Long]](0).toSet
      assert (actualResult.subsetOf(mbrResult))
    }
  }
}
