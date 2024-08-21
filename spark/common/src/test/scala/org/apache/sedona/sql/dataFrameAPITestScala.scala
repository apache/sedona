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

import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, col, element_at, expr, lit}
import org.apache.spark.sql.sedona_sql.expressions.InferredExpressionException
import org.apache.spark.sql.sedona_sql.expressions.st_aggregates._
import org.apache.spark.sql.sedona_sql.expressions.st_constructors._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.sedona_sql.expressions.st_predicates._
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.locationtech.jts.geom.{Geometry, Point, Polygon}
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.operation.buffer.BufferParameters

import scala.collection.mutable
import scala.collection.mutable.WrappedArray

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
      val df = sparkSession
        .sql("SELECT 0.0 AS x, 1.0 AS y, 2.0 AS z")
        .select(ST_AsText(ST_PointZ("x", "y", "z")))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "POINT Z(0 1 2)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_HasZ") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON Z ((30 10 5, 40 40 10, 20 40 15, 10 20 20, 30 10 5))') as poly")
      val actual = baseDf.select(ST_HasZ("poly")).first().getBoolean(0)
      assert(actual)
    }

    it("Passed ST_HasM") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))') as poly")
      val actual = baseDf.select(ST_HasM("poly")).first().getBoolean(0)
      assert(actual)
    }

    it("Passed ST_M") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT ZM (10 20 30 40)') AS point")
      val actual = baseDf.select(ST_M("point")).first().getDouble(0)
      assert(actual == 40.0)
    }

    it("Passed ST_MMin") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') as line")
      val actual = baseDf.select(ST_MMin("line")).first().getDouble(0)
      assert(actual == -1.0)

      baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line")
      val actualNull = baseDf.select(ST_MMin("line")).first().get(0)
      assert(actualNull == null)
    }

    it("Passed ST_MMax()") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') as line")
      val actual = baseDf.select(ST_MMax("line")).first().getDouble(0)
      assert(actual == 3.0)

      baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line")
      val actualNull = baseDf.select(ST_MMax("line")).first().get(0)
      assert(actualNull == null)
    }

    it("Passed st_pointm") {
      val pointDf =
        sparkSession.sql("SELECT ST_PointM(1,2,100) as point1, ST_PointM(1,2,100,4326) as point2")
      val point1 = pointDf.select(ST_AsEWKT("point1")).take(1)(0).getString(0)
      val point2 = pointDf.select(ST_AsEWKT("point2")).take(1)(0).getString(0)
      assertEquals("POINT ZM(1 2 0 100)", point1)
      assertEquals("SRID=4326;POINT ZM(1 2 0 100)", point2)
    }

    it("passed ST_MakePointM") {
      val df = sparkSession
        .sql("SELECT 0.0 AS x, 1.0 AS y, 2.0 AS m")
        .select(ST_AsText(ST_MakePointM("x", "y", "m")))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      val expectedResult = "POINT M(0 1 2)"
      assert(actualResult == expectedResult)
    }

    it("passed st_makepoint") {
      val df = sparkSession
        .sql("SELECT 0.0 AS x, 1.0 AS y, 2.0 AS z")
        .select(ST_AsText(ST_MakePoint("x", "y", "z")))
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

    it("passed st_pointfromwkb") {
      val wkbSeq = Seq[Array[Byte]](
        Array[Byte](1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 46, 64))
      val df = wkbSeq.toDF("wkb").select(ST_PointFromWKB("wkb"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (10 15)"
      assert(actualResult == expectedResult)

      val wkbStringSeq = Seq("010100000000000000000024400000000000002E40")
      val dfWithString = wkbStringSeq.toDF("wkb").select(ST_PointFromWKB("wkb"))
      val actualStringResult = dfWithString.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualStringResult == expectedResult)
    }

    it("passed st_polygonfromtext") {
      val df = sparkSession
        .sql("SELECT '0.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0' AS c")
        .select(ST_PolygonFromText($"c", lit(',')))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("passed st_linefromtext") {
      val df =
        sparkSession.sql("SELECT 'Linestring(1 2, 3 4)' AS wkt").select(ST_LineFromText("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 2, 3 4)"
      assert(actualResult == expectedResult)
    }

    it("passed st_linestringfromtext") {
      val df = sparkSession
        .sql("SELECT '0.0,0.0,1.0,0.0' AS c")
        .select(ST_LineStringFromText($"c", lit(',')))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0)"
      assert(actualResult == expectedResult)
    }

    it("passed st_linefromwkb") {
      val wkbSeq = Seq[Array[Byte]](
        Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128,
          -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
      val df = wkbSeq.toDF("wkb").select(ST_LineFromWKB("wkb"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)"
      assert(actualResult == expectedResult)

      val wkbStringSeq =
        Seq("0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf")
      val dfWithString = wkbStringSeq.toDF("wkb").select(ST_LineFromWKB("wkb"))
      val actualStringResult = dfWithString.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualStringResult == expectedResult)
    }

    it("passed st_linestringfromwkb") {
      val wkbSeq = Seq[Array[Byte]](
        Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128,
          -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
      val df = wkbSeq.toDF("wkb").select(ST_LinestringFromWKB("wkb"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)"
      assert(actualResult == expectedResult)

      val wkbStringSeq =
        Seq("0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf")
      val dfWithString = wkbStringSeq.toDF("wkb").select(ST_LinestringFromWKB("wkb"))
      val actualStringResult = dfWithString.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualStringResult == expectedResult)
    }

    it("passed st_geomfromwkt") {
      val df = sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromWKT("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromwkt with srid") {
      val df =
        sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromWKT("wkt", 4326))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      assert(actualResult.toText == "POINT (0 1)")
      assert(actualResult.getSRID == 4326)
    }

    it("passed st_geomfromewkt") {
      val df = sparkSession
        .sql("SELECT 'SRID=4269;POINT(0.0 1.0)' AS wkt")
        .select(ST_GeomFromEWKT("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      assert(actualResult.toText == "POINT (0 1)")
      assert(actualResult.getSRID == 4269)
    }

    it("passed ST_GeometryFromText") {
      val df =
        sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeometryFromText("wkt"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed ST_GeometryFromText with srid") {
      val df = sparkSession
        .sql("SELECT 'POINT(0.0 1.0)' AS wkt")
        .select(ST_GeometryFromText("wkt", 4326))
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
      val df =
        sparkSession.sql("SELECT 'POINT(0.0 1.0)' AS wkt").select(ST_GeomFromText("wkt", 4326))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      assert(actualResult.toText == "POINT (0 1)")
      assert(actualResult.getSRID == 4326)
    }

    it("passed st_geomfromwkb") {
      val wkbSeq = Seq[Array[Byte]](
        Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128,
          -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
      val df = wkbSeq.toDF("wkb").select(ST_GeomFromWKB("wkb"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromewkb") {
      val wkbSeq = Seq[Array[Byte]](
        Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128,
          -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
      val df = wkbSeq.toDF("wkb").select(ST_GeomFromEWKB("wkb"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromgeojson") {
      val geojson =
        "{ \"type\": \"Feature\", \"properties\": { \"prop\": \"01\" }, \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 0.0, 1.0 ] }},"
      val df = Seq[String](geojson).toDF("geojson").select(ST_GeomFromGeoJSON("geojson"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 1)"
      assert(actualResult == expectedResult)
    }

    it("passed st_polygonfromenvelope with column values") {
      val df = sparkSession.sql("SELECT 0.0 AS minx, 1.0 AS miny, 2.0 AS maxx, 3.0 AS maxy")
      val actualResult = df
        .select(ST_PolygonFromEnvelope("minx", "miny", "maxx", "maxy"))
        .take(1)(0)
        .get(0)
        .asInstanceOf[Geometry]
        .toText()
      val expectedResult = "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"
      assert(actualResult == expectedResult)
    }

    it("passed st_polygonfromenvelope with literal values") {
      val df =
        sparkSession.sql("SELECT null AS c").select(ST_PolygonFromEnvelope(0.0, 1.0, 2.0, 3.0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MPointFromText()") {
      val df = sparkSession.sql("SELECT 'MULTIPOINT ((10 10), (20 20), (30 30))' as geom")
      var actual =
        df.select(ST_MPointFromText("geom")).first().get(0).asInstanceOf[Geometry].toText
      val expected = "MULTIPOINT ((10 10), (20 20), (30 30))"
      assert(expected == actual)

      val actualGeom =
        df.select(ST_MPointFromText("geom", 4326)).first().get(0).asInstanceOf[Geometry]
      actual = actualGeom.toText
      assert(actual == expected)
      val actualSrid = actualGeom.getSRID
      assert(4326 == actualSrid)
    }

    it("Passed ST_GeomCollFromText") {
      val df = sparkSession.sql(
        "SELECT 'GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))' as geom")
      var actual =
        df.select(ST_GeomCollFromText("geom")).first().get(0).asInstanceOf[Geometry].toText
      val expected =
        "GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))"
      assert(expected == actual)

      val actualGeom =
        df.select(ST_GeomCollFromText("geom", 4326)).first().get(0).asInstanceOf[Geometry]
      actual = actualGeom.toText
      assert(actual == expected)
      val actualSrid = actualGeom.getSRID
      assert(4326 == actualSrid)
    }

    it("passed st_geomfromgeohash") {
      val df = sparkSession
        .sql("SELECT 's00twy01mt' AS geohash")
        .select(ST_GeomFromGeoHash("geohash", 4))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "POLYGON ((0.703125 0.87890625, 0.703125 1.0546875, 1.0546875 1.0546875, 1.0546875 0.87890625, 0.703125 0.87890625))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_PointFromGeoHash") {
      val df = sparkSession
        .sql("SELECT 's00twy01mt' AS geohash")
        .select(ST_PointFromGeoHash("geohash", 4))
      val actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText
      val expected = "POINT (0.87890625 0.966796875)"
      assert(expected.equals(actual))
    }

    it("passed st_geomfromgml") {
      val gmlString =
        "<gml:LineString srsName=\"EPSG:4269\"><gml:coordinates>-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932</gml:coordinates></gml:LineString>"
      val df = sparkSession.sql(s"SELECT '$gmlString' AS gml").select(ST_GeomFromGML("gml"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "LINESTRING (-71.16028 42.258729, -71.160837 42.259112, -71.161143 42.25932)"
      assert(actualResult == expectedResult)
    }

    it("passed st_geomfromkml") {
      val kmlString =
        "<LineString><coordinates>-71.1663,42.2614 -71.1667,42.2616</coordinates></LineString>"
      val df = sparkSession.sql(s"SELECT '$kmlString' as kml").select(ST_GeomFromKML("kml"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (-71.1663 42.2614, -71.1667 42.2616)"
      assert(actualResult == expectedResult)
    }

    // functions
    it("Passed ST_ConcaveHull") {
      val baseDF = sparkSession.sql(
        "SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as mline")
      val df = baseDF.select(ST_ConcaveHull("mline", 1, true))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualResult == "POLYGON ((1 2, 2 2, 3 2, 5 0, 4 0, 1 0, 0 0, 1 2))")
    }

    it("Passed ST_ConvexHull") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_ConvexHull("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualResult == "POLYGON ((0 0, 1 1, 1 0, 0 0))")
    }

    it("Passed ST_Buffer") {
      val polygonDf = sparkSession.sql("SELECT ST_Point(1.0, 1.0) AS geom")
      val df = polygonDf
        .select(ST_Buffer("geom", 1.0).as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      var actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      var expected =
        "POLYGON ((1.98 0.8, 1.92 0.62, 1.83 0.44, 1.71 0.29, 1.56 0.17, 1.38 0.08, 1.2 0.02, 1 0, 0.8 0.02, 0.62 0.08, 0.44 0.17, 0.29 0.29, 0.17 0.44, 0.08 0.62, 0.02 0.8, 0 1, 0.02 1.2, 0.08 1.38, 0.17 1.56, 0.29 1.71, 0.44 1.83, 0.62 1.92, 0.8 1.98, 1 2, 1.2 1.98, 1.38 1.92, 1.56 1.83, 1.71 1.71, 1.83 1.56, 1.92 1.38, 1.98 1.2, 2 1, 1.98 0.8))"
      assertEquals(expected, actual)

      var linestringDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(0 0, 50 70, 100 100)') AS geom, 'side=left' as params")
      var dfLine = linestringDf
        .select(ST_Buffer("geom", 10, false, "params").as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      actual = dfLine.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected =
        "POLYGON ((50 70, 0 0, -8.14 5.81, 41.86 75.81, 43.22 77.35, 44.86 78.57, 94.86 108.57, 100 100, 50 70))"
      assertEquals(expected, actual)

      linestringDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(0 0, 50 70, 70 -3)') AS geom, 'endcap=square' AS params")
      dfLine = linestringDf
        .select(ST_Buffer("geom", 10, false, "params").as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      actual = dfLine.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected =
        "POLYGON ((43.22 77.35, 44.85 78.57, 46.7 79.44, 48.69 79.91, 50.74 79.97, 52.75 79.61, 54.65 78.85, 56.36 77.72, 57.79 76.27, 58.91 74.55, 59.64 72.64, 79.64 -0.36, 82.29 -10, 63 -15.29, 45.91 47.07, 8.14 -5.81, 2.32 -13.95, -13.95 -2.32, 41.86 75.81, 43.22 77.35))"
      assertEquals(expected, actual)

      linestringDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(0 0, 50 70, 70 -3)') AS geom, 'endcap=square' AS params")
      dfLine = linestringDf
        .select(ST_Buffer("geom", 10, true, "params").as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 4)")
      actual = dfLine.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected =
        "POLYGON ((50 70, 50.0001 70, 70.0001 -3, 70.0001 -3.0001, 69.9999 -3.0001, 50 69.9999, 0.0001 0, 0 -0.0001, -0.0001 0, 49.9999 70, 50 70))"
      assertEquals(expected, actual)

      var pointDf = sparkSession.sql("SELECT ST_Point(100, 90) AS geom, 'quad_segs=4' as params")
      var dfPoint = pointDf
        .select(ST_Buffer("geom", 200, false, "params").as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      actual = dfPoint.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected =
        "POLYGON ((284.78 13.46, 241.42 -51.42, 176.54 -94.78, 100 -110, 23.46 -94.78, -41.42 -51.42, -84.78 13.46, -100 90, -84.78 166.54, -41.42 231.42, 23.46 274.78, 100 290, 176.54 274.78, 241.42 231.42, 284.78 166.54, 300 90, 284.78 13.46))"
      assertEquals(expected, actual)

      pointDf = sparkSession.sql("SELECT ST_Point(-180, 60) AS geom, 'quad_segs=4' as params")
      dfPoint = pointDf
        .select(ST_Buffer("geom", 200, true, "params").as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 6)")
      actual = dfPoint.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected =
        "POLYGON ((-179.99663 60.000611, -179.997353 60.001211, -179.998479 60.001626, -179.999837 60.001793, 179.99878 60.001688, 179.997583 60.001326, 179.996754 60.000761, 179.996419 60.000081, 179.99663 59.999389, 179.997353 59.998789, 179.99848 59.998374, 179.999837 59.998207, -179.99878 59.998312, -179.997583 59.998674, -179.996754 59.999238, -179.996419 59.999919, -179.99663 60.000611))"
      assertEquals(expected, actual)
    }

    it("Passed ST_BestSRID") {
      val pointDf = sparkSession.sql("SELECT ST_Point(-177, -60) AS geom")
      val df = pointDf.select(ST_BestSRID("geom").as("geom"))
      var actual = df.take(1)(0).get(0).asInstanceOf[Int]
      var expected = 32701
      assertEquals(expected, actual)

      val linestringDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(-91.185 30.4505, -91.187 30.452, -91.189 30.4535)') AS geom")
      val dfLine = linestringDf.select(ST_BestSRID("geom").as("geom"))
      actual = dfLine.take(1)(0).get(0).asInstanceOf[Int]
      expected = 32615
      assertEquals(expected, actual)

      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((-120 30, -80 30, -80 50, -120 50, -120 30))') AS geom")
      val dfPolygon = polygonDf.select(ST_BestSRID("geom").as("geom"))
      actual = dfPolygon.take(1)(0).get(0).asInstanceOf[Int]
      expected = 3395
      assertEquals(expected, actual)

      val polygonDf2 = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((-73.9980 40.7265, -73.9970 40.7265, -73.9970 40.7255, -73.9980 40.7255, -73.9980 40.7265))') AS geom")
      val dfPolygon2 = polygonDf2.select(ST_BestSRID("geom").as("geom"))
      actual = dfPolygon2.take(1)(0).get(0).asInstanceOf[Int]
      expected = 32618
      assertEquals(expected, actual)
    }

    it("Passed ST_ShiftLongitude") {
      val polygonDf = sparkSession.sql("SELECT ST_Point(1.0, 1.0) AS geom")
      val df = polygonDf.select(ST_ShiftLongitude("geom").as("geom"))
      var actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      var expected = "POINT (1 1)"
      assertEquals(expected, actual)

      var linestringDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(179 75, 180 75, 181 75)') AS geom")
      var dfLine = linestringDf.select(ST_ShiftLongitude("geom").as("geom"))
      actual = dfLine.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected = "LINESTRING (179 75, 180 75, -179 75)"
      assertEquals(expected, actual)

      linestringDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(0 10, -1 10)') AS geom")
      dfLine = linestringDf.select(ST_ShiftLongitude("geom").as("geom"))
      actual = dfLine.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      expected = "LINESTRING (0 10, 359 10)"
      assertEquals(expected, actual)
    }

    it("Passed ST_Envelope") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_Envelope("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Expand") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))') as geom")
      var actual = baseDf.select(ST_AsText(ST_Expand("geom", 10))).first().get(0)
      var expected = "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))"
      assertEquals(expected, actual)

      actual = baseDf.select(ST_AsText(ST_Expand("geom", 5, 6))).first().get(0)
      expected = "POLYGON Z((45 44 1, 45 86 1, 85 86 3, 85 44 3, 45 44 1))"
      assertEquals(expected, actual)

      actual = baseDf.select(ST_AsText(ST_Expand("geom", 6, 5, -3))).first().get(0)
      expected = "POLYGON Z((44 45 4, 44 85 4, 86 85 0, 86 45 0, 44 45 4))"
      assertEquals(expected, actual)
    }

    it("Passed ST_YMax") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_YMax("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_YMin") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_YMin("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 0.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Centroid") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom")
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

    it("Passed ST_Length2D") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = lineDf.select(ST_Length2D("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 1.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Area") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_Area("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 0.5
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Dimension") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = polygonDf.select(ST_Dimension("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Int]
      val expectedResult = 2
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
      val pointDf =
        sparkSession.sql("SELECT ST_PointZ(0.0, 0.0, 0.0) AS a, ST_PointZ(3.0, 0.0, 4.0) as b")
      val df = pointDf.select(ST_3DDistance("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 5.0
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Transform") {
      var pointDf = sparkSession.sql("SELECT ST_Point(1.0, 1.0) AS geom")
      var df = pointDf
        .select(ST_Transform($"geom", lit("EPSG:4326"), lit("EPSG:32649")).as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (-33741810.95 1823994.03)"
      assertEquals(expectedResult, actualResult)

      pointDf = sparkSession
        .sql("SELECT ST_Point(40.0, 100.0) AS geom")
        .select(ST_SetSRID("geom", 4326).as("geom"))
      df = pointDf
        .select(ST_Transform($"geom", lit("EPSG:32649")).as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      val actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expected = "POINT (1560393.11 10364606.84)"
      assertEquals(expected, actual)
    }

    it("Passed ST_Intersection") {
      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS a, ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') AS b")
      val df = polygonDf.select(ST_Intersection("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsValid") {
      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS geom")
      val df = polygonDf.select(ST_IsValid("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Boolean]
      assert(actualResult)

      // Geometry that is invalid under both OGC and ESRI standards
      val selfTouchingWKT = "POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))"
      val specialCaseTable =
        Seq(selfTouchingWKT).toDF("wkt").select(ST_GeomFromWKT($"wkt").as("geom"))

      // Test with OGC flag (OGC_SFS_VALIDITY = 0)
      val ogcValidityTable = specialCaseTable.select(ST_IsValid($"geom", lit(0)))
      val ogcValidity = ogcValidityTable.take(1)(0).getBoolean(0)
      assertEquals(false, ogcValidity)

      // Test with ESRI flag (ESRI_VALIDITY = 1)
      val esriValidityTable = specialCaseTable.select(ST_IsValid($"geom", lit(1)))
      val esriValidity = esriValidityTable.take(1)(0).getBoolean(0)
      assertEquals(false, esriValidity)
    }

    it("Passed ST_ReducePrecision") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.12, 0.23) AS geom")
      val df = pointDf.select(ST_ReducePrecision("geom", 1))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0.1 0.2)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsSimple") {
      val triangleDf =
        sparkSession.sql("SELECT ST_GeomFromWkt('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = triangleDf.select(ST_IsSimple("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Boolean]
      assert(actualResult)
    }

    it("Passed ST_MakeLine") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 1.0) AS b")
      val df = pointDf.select(ST_MakeLine("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 1)"
      assert(actualResult == expectedResult)

      val df2 = sparkSession.sql(
        "SELECT ST_MakeLine(ARRAY(ST_Point(0, 0), ST_Point(1, 1), ST_Point(2, 2)))")
      val actualResult2 = df2.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assert(actualResult2 == "LINESTRING (0 0, 1 1, 2 2)")
    }

    it("Passed ST_MakeValid On Invalid Polygon") {
      val invalidDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS geom")
      val df = invalidDf.select(ST_MakeValid("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Points") {
      val invalidDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 1, 5 1, 5 0, 1 0, 0 0))') AS geom")
      val df = invalidDf.select(ST_Normalize(ST_Points("geom")))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      actualResult.normalize()
      val expectedResult = "MULTIPOINT ((0 0), (0 0), (1 0), (1 1), (5 0), (5 1))"
      assert(actualResult.toText() == expectedResult)
    }

    it("Passed ST_Polygon") {
      val invalidDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = invalidDf.select(ST_Polygon("geom", 4236))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
      assert(actualResult.toText() == expectedResult)
      assert(actualResult.getSRID() == 4236)
    }

    it("Passed ST_Polygonize") {
      val invalidDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION (LINESTRING (2 0, 2 1, 2 2), LINESTRING (2 2, 2 3, 2 4), LINESTRING (0 2, 1 2, 2 2), LINESTRING (2 2, 3 2, 4 2), LINESTRING (0 2, 1 3, 2 4), LINESTRING (2 4, 3 3, 4 2))') AS geom")
      val df = invalidDf.select(ST_Polygonize("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry]
      actualResult.normalize()
      val expectedResult =
        "GEOMETRYCOLLECTION (POLYGON ((0 2, 1 3, 2 4, 2 3, 2 2, 1 2, 0 2)), POLYGON ((2 2, 2 3, 2 4, 3 3, 4 2, 3 2, 2 2)))"
      assert(actualResult.toText() == expectedResult)
    }

    it("Passed `ST_MakePolygon`") {
      val invalidDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = invalidDf.select(ST_MakePolygon("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MakePolygon with holes") {
      val invalidDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom, array(ST_GeomFromWKT('LINESTRING (0.5 0.1, 0.7 0.1, 0.7 0.3, 0.5 0.1)')) AS holes")
      val df = invalidDf.select(ST_MakePolygon("geom", "holes"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 0, 1 1, 0 0), (0.5 0.1, 0.7 0.1, 0.7 0.3, 0.5 0.1))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SimplifyPreserveTopology") {
      val polygonDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom")
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
      var df = pointDf.select(ST_AsGeoJSON("geom"))
      var actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      var expectedResult = "{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}"
      assert(actualResult == expectedResult)

      df = pointDf.select(ST_AsGeoJSON(col("geom"), lit("feature")))
      actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      expectedResult =
        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0]},\"properties\":{}}"
      assert(actualResult == expectedResult)

      df = pointDf.select(ST_AsGeoJSON(col("geom"), lit("featureCollection")))
      actualResult = df.take(1)(0).get(0).asInstanceOf[String]
      expectedResult =
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0]},\"properties\":{}}]}"
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
      val expectedResult =
        "<gml:Point>\n  <gml:coordinates>\n    0.0,0.0 \n  </gml:coordinates>\n</gml:Point>\n"
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

    it("Passed ST_AsHEXEWKB") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT(1 2)') as point")
      var actual = baseDf.select(ST_AsHEXEWKB("point")).first().get(0)
      var expected = "0101000000000000000000F03F0000000000000040"
      assert(expected.equals(actual))

      actual = baseDf.select(ST_AsHEXEWKB(col("point"), lit("xdr"))).first().get(0)
      expected = "00000000013FF00000000000004000000000000000"
      assert(expected.equals(actual))
    }

    it("Passed ST_NPoints") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS geom")
      val df = lineDf.select(ST_NPoints("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 2
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SimplifyVW") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)') AS geom")
      val actual =
        baseDf.select(ST_SimplifyVW("geom", 30)).first().get(0).asInstanceOf[Geometry].toText
      val expected = "LINESTRING (5 2, 7 25, 10 10)"
      assertEquals(expected, actual)
    }

    it("Passed ST_SimplifyPolygonHull") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))') AS geom")
      var actual = baseDf
        .select(ST_SimplifyPolygonHull("geom", 0.3, false))
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      var expected = "POLYGON ((30 10, 40 40, 10 20, 30 10))"
      assertEquals(expected, actual)

      actual = baseDf
        .select(ST_SimplifyPolygonHull("geom", 0.3))
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      expected = "POLYGON ((30 10, 15 15, 10 20, 20 40, 45 45, 30 10))"
      assertEquals(expected, actual)
    }

    it("Passed ST_GeometryType") {
      val pointDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS geom")
      val df = pointDf.select(ST_GeometryType("geom"))
      val actualResult = df.take(1)(0).getString(0)
      val expectedResult = "ST_Point"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Difference") {
      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') AS a,ST_GeomFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))') AS b")
      val df = polygonDf.select(ST_Difference("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_SymDifference") {
      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') AS a, ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') AS b")
      val df = polygonDf.select(ST_SymDifference("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Union") {
      val polygonDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') AS a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') AS b")
      val df = polygonDf.select(ST_Union("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Union array variant") {
      val polygonDf = sparkSession.sql(
        "SELECT array(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') , ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))')) AS polys")
      val df = polygonDf.select(ST_Union("polys"))
      val actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText
      val expected = "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"
      assert(expected.equals(actual))
    }

    it("Passed ST_UnaryUnion") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))') AS geom")
      val actual =
        baseDf.select(ST_UnaryUnion("geom")).first().get(0).asInstanceOf[Geometry].toText
      val expected = "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))"
      assertEquals(expected, actual)
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

    it("Should pass ST_Zmflag") {
      var actual = sparkSession
        .sql("SELECT ST_GeomFromWKT('POINT (1 2)') AS geom")
        .select(ST_Zmflag("geom"))
        .first()
        .get(0)
      assert(actual == 0)

      actual = sparkSession
        .sql("SELECT ST_GeomFromWKT('LINESTRING (1 2 3, 4 5 6)') AS geom")
        .select(ST_Zmflag("geom"))
        .first()
        .get(0)
      assert(actual == 2)

      actual = sparkSession
        .sql("SELECT ST_GeomFromWKT('POLYGON M((1 2 3, 3 4 3, 5 6 3, 3 4 3, 1 2 3))') AS geom")
        .select(ST_Zmflag("geom"))
        .first()
        .get(0)
      assert(actual == 1)

      actual = sparkSession
        .sql("SELECT ST_GeomFromWKT('MULTIPOLYGON ZM (((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1)), ((15 5 3 1, 20 10 6 2, 10 10 7 3, 15 5 3 1)))') AS geom")
        .select(ST_Zmflag("geom"))
        .first()
        .get(0)
      assert(actual == 3)
    }

    it("Passed ST_StartPoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_StartPoint("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Snap") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))') AS poly, ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)') AS line")
      var actual = baseDf
        .select(ST_Snap("poly", "line", 2.525))
        .take(1)(0)
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      var expected = "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))"
      assert(expected.equals(actual))

      actual = baseDf
        .select(ST_Snap("poly", "line", 3.125))
        .take(1)(0)
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      expected = "POLYGON ((0.5 10.7, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 5.4 8.4, 0.5 10.7))"
      assert(expected.equals(actual))
    }

    it("Passed ST_Boundary") {
      val baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
      val df = baseDf.select(ST_Boundary("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 1 1, 0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_CrossesDateLine") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))') AS geom")
      val df = baseDf.select(ST_CrossesDateLine("geom"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_EndPoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_EndPoint("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_ExteriorRing") {
      val baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
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
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
      val df = baseDf.select(ST_InteriorRingN("geom", 0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 1, 2 2, 2 1, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Dump") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
      val df = baseDf.select(ST_Dump("geom"))
      val actualResult =
        df.take(1)(0).get(0).asInstanceOf[WrappedArray[Any]].map(_.asInstanceOf[Geometry].toText)
      val expectedResult = Array("POINT (0 0)", "POINT (1 1)")
      assert(actualResult(0) == expectedResult(0))
      assert(actualResult(1) == expectedResult(1))
    }

    it("Passed ST_DumpPoints") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf.select(ST_DumpPoints("geom"))
      val actualResult =
        df.take(1)(0).get(0).asInstanceOf[WrappedArray[Any]].map(_.asInstanceOf[Geometry].toText)
      val expectedResult = Array("POINT (0 0)", "POINT (1 0)")
      assert(actualResult(0) == expectedResult(0))
      assert(actualResult(1) == expectedResult(1))
    }

    it("Passed ST_IsClosed") {
      val baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = baseDf.select(ST_IsClosed("geom"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_NumInteriorRings") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
      val df = baseDf.select(ST_NumInteriorRings("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 1
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AddMeasure") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (1 1, 2 2, 2 2, 3 3)') as line, ST_GeomFromWKT('MULTILINESTRING M((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))') as mline")
      var actual = baseDf.select(ST_AsText(ST_AddMeasure("line", 1, 70))).first().get(0)
      var expected = "LINESTRING M(1 1 1, 2 2 35.5, 2 2 35.5, 3 3 70)"
      assertEquals(expected, actual)

      actual = baseDf.select(ST_AsText(ST_AddMeasure("mline", 10, 70))).first().get(0)
      expected = "MULTILINESTRING M((1 0 10, 2 0 20, 4 0 40), (1 0 40, 2 0 50, 4 0 70))"
      assertEquals(expected, actual)

      baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTILINESTRING M((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))') as mline, 10.0 as measureRangeStart, 70.0 as measureRangeEnd")
      actual = baseDf
        .select(ST_AsText(ST_AddMeasure("mline", "measureRangeEnd", "measureRangeStart")))
        .first()
        .get(0)
      expected = "MULTILINESTRING M((1 0 70, 2 0 60, 4 0 40), (1 0 40, 2 0 30, 4 0 10))"
      assertEquals(expected, actual)
    }

    it("Passed ST_AddPoint without index") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
      val df = baseDf.select(ST_AddPoint("line", "point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_AddPoint with index") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
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
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
      val df = baseDf.select(ST_SetPoint("line", 1, "point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 1)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsRing") {
      val baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
      val df = baseDf.select(ST_IsRing("geom"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Subdivide") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom")
      val df = baseDf.select(ST_SubDivide("geom", 5))
      val actualResult =
        df.take(1)(0).get(0).asInstanceOf[WrappedArray[Any]].map(_.asInstanceOf[Geometry].toText)
      val expectedResult = Array[String]("LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)")
      assert(actualResult(0) == expectedResult(0))
      assert(actualResult(1) == expectedResult(1))
    }

    it("Passed ST_SubdivideExplode") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom")
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
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 2 0))') AS geom")
      val df = baseDf.select(ST_LineMerge("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (0 0, 1 0, 2 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_LocateAlong") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTILINESTRING M((1 2 3, 3 4 2, 9 4 3),(1 2 3, 5 4 5))') AS geom")
      var actual = baseDf.select(ST_AsText(ST_LocateAlong("geom", 2))).first().get(0)
      var expected = "MULTIPOINT M((3 4 2))"
      assertEquals(expected, actual)

      actual = baseDf.select(ST_AsText(ST_LocateAlong("geom", 2, -3))).first().get(0)
      expected = "MULTIPOINT M((5.121320343559642 1.8786796564403572 2), (3 1 2))"
      assertEquals(expected, actual)
    }

    it("Passed ST_LongestLine") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom")
      val actual =
        baseDf.select(ST_LongestLine("geom", "geom")).first().get(0).asInstanceOf[Geometry].toText
      val expected = "LINESTRING (180 180, 20 50)"
      assert(expected.equals(actual))
    }

    it("Passed ST_MaxDistance()") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom")
      val actual = baseDf.select(ST_MaxDistance("geom", "geom")).first().get(0)
      val expected = 206.15528128088303
      assert(expected == actual)
    }

    it("Passed ST_FlipCoordinates") {
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 1.0) AS geom")
      val df = baseDf.select(ST_FlipCoordinates("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MinimumClearance") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom")
      val actual = baseDf.select(ST_MinimumClearance("geom")).first().get(0)
      val expected = 0.5
      assertEquals(expected, actual)
    }

    it("Passed ST_MinimumClearanceLine") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom")
      val actual = baseDf
        .select(ST_MinimumClearanceLine("geom"))
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      val expected = "LINESTRING (64.5 16, 65 16)"
      assertEquals(expected, actual)
    }

    it("Passed ST_MinimumBoundingCircle with default quadrantSegments") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf
        .select(ST_MinimumBoundingCircle("geom").as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates().length
      val expectedResult = BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6 * 4 + 1;
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MinimumBoundingCircle with specified quadrantSegments") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df = baseDf
        .select(ST_MinimumBoundingCircle("geom", 2).as("geom"))
        .selectExpr("ST_ReducePrecision(geom, 2)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult =
        "POLYGON ((0.85 -0.35, 0.5 -0.5, 0.15 -0.35, 0 0, 0.15 0.35, 0.5 0.5, 0.85 0.35, 1 0, 0.85 -0.35))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_MinimumBoundingRadius") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
      val df =
        baseDf.select(ST_MinimumBoundingRadius("geom").as("c")).select("c.center", "c.radius")
      val rowResult = df.take(1)(0)

      val actualCenter = rowResult.get(0).asInstanceOf[Geometry].toText()
      val actualRadius = rowResult.getDouble(1)

      val expectedCenter = "POINT (0.5 0)"
      val expectedRadius = 0.5

      assert(actualCenter == expectedCenter)
      assert(actualRadius == expectedRadius)
    }

    it("Passed ST_LineSubstring") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line")
      val df = baseDf.select(ST_LineSubstring("line", 0.5, 1.0))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (1 0, 2 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_LineInterpolatePoint") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line")
      val df = baseDf.select(ST_LineInterpolatePoint("line", 0.5))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (1 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_LineLocatePoint") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)') AS line, ST_GeomFromWKT('POINT (0 2)') AS point")
      val df = baseDf.select(ST_LineLocatePoint("line", "point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Double]
      val expectedResult = 0.5
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Multi") {
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS point")
      val df = baseDf.select(ST_Multi("point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_PointOnSurface") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
      val df = baseDf.select(ST_PointOnSurface("line"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Reverse") {
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

    it("Passed ST_ClosestPoint") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (0 1)') as g1, ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') as g2")
      val df = polyDf.select(ST_ClosestPoint("g1", "g2"))
      val expected = "POINT (0 1)"
      val actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      assertEquals(expected, actual)
    }

    it("Passed ST_AsEWKT") {
      val baseDf = sparkSession.sql("SELECT ST_SetSRID(ST_Point(0.0, 0.0), 4326) AS point")
      val df = baseDf.select(ST_AsEWKT("point"))
      val actualResult = df.take(1)(0).getString(0)
      val expectedResult = "SRID=4326;POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Force_2D") {
      val baseDf = sparkSession.sql("SELECT ST_PointZ(0.0, 0.0, 1.0) AS point")
      val df = baseDf.select(ST_Force_2D("point"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POINT (0 0)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsEmpty") {
      val baseDf = sparkSession.sql(
        "SELECT ST_Difference(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0)) AS empty_geom")
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

    it("Passed ST_BuildArea") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 0))') AS multiline")
      val df =
        baseDf.select(ST_BuildArea("multiline").as("geom")).selectExpr("ST_Normalize(geom)")
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Collect with array argument") {
      val baseDf =
        sparkSession.sql("SELECT array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)) as points")
      val df = baseDf.select(ST_Collect("points"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0), (1 1))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Collect with variable arguments") {
      val baseDf = sparkSession.sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 1.0) AS b")
      val df = baseDf.select(ST_Collect("a", "b"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0), (1 1))"
      assert(actualResult == expectedResult)
    }

    it("Passed St_CollectionExtract with default geomType") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
      val df = baseDf.select(ST_CollectionExtract("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTILINESTRING ((0 0, 1 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed St_CollectionExtract with specified geomType") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
      val df = baseDf.select(ST_CollectionExtract("geom", 1))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "MULTIPOINT ((0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Normalize") {
      val baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 0))') AS polygon")
      val df = baseDf.select(ST_Normalize("polygon"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Split") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1.5 1.5, 2 2)') AS input, ST_GeomFromWKT('MULTIPOINT (0.5 0.5, 1 1)') AS blade")
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
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(0.5, 0.25) AS b")
      val df = baseDf.select(ST_Contains("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }
    it("Passed ST_Intersects") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS a, ST_GeomFromWKT('LINESTRING (0 1, 1 0)') AS b")
      val df = baseDf.select(ST_Intersects("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Within") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(0.5, 0.25) AS b")
      val df = baseDf.select(ST_Within("b", "a"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Equals for ST_Point") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS a, ST_GeomFromWKT('LINESTRING (1 0, 0 0)') AS b")
      val df = baseDf.select(ST_Equals("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Crosses") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') AS a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') AS b")
      val df = baseDf.select(ST_Crosses("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Relate") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (1 1, 5 5)') AS g1, ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))') as g2, '1010F0212' as im")
      val actual = baseDf.select(ST_Relate("g1", "g2")).first().get(0)
      assert(actual.equals("1010F0212"))

      val actualBoolean = baseDf.select(ST_Relate("g1", "g2", "im")).first().getBoolean(0)
      assert(actualBoolean)
    }

    it("Passed ST_RelateMatch") {
      val baseDf = sparkSession.sql("SELECT '101202FFF' as matrix1, 'TTTTTTFFF' as matrix2")
      val actual = baseDf.select(ST_RelateMatch("matrix1", "matrix2")).first().getBoolean(0)
      assert(actual)
    }

    it("Passed ST_Touches") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((1 1, 1 0, 2 0, 1 1))') AS b")
      val df = baseDf.select(ST_Touches("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Overlaps") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((0.5 1, 1.5 0, 2 0, 0.5 1))') AS b")
      val df = baseDf.select(ST_Overlaps("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_Disjoint") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((2 0, 3 0, 3 1, 2 0))') AS b")
      val df = baseDf.select(ST_Disjoint("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(actualResult)
    }

    it("Passed ST_OrderingEquals") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS a, ST_GeomFromWKT('LINESTRING (1 0, 0 0)') AS b")
      val df = baseDf.select(ST_OrderingEquals("a", "b"))
      val actualResult = df.take(1)(0).getBoolean(0)
      assert(!actualResult)
    }

    it("Passed ST_Covers") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(1.0, 0.0) AS b, ST_Point(0.0, 1.0) AS c")
      val df = baseDf.select(ST_Covers("a", "b"), ST_Covers("a", "c"))
      val actualResult = df.take(1)(0)
      assert(actualResult.getBoolean(0))
      assert(!actualResult.getBoolean(1))
    }

    it("Passed ST_CoveredBy") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(1.0, 0.0) AS b, ST_Point(0.0, 1.0) AS c")
      val df = baseDf.select(ST_CoveredBy("b", "a"), ST_CoveredBy("c", "a"))
      val actualResult = df.take(1)(0)
      assert(actualResult.getBoolean(0))
      assert(!actualResult.getBoolean(1))
    }

    // aggregates
    it("Passed ST_Envelope_Aggr") {
      val baseDf =
        sparkSession.sql("SELECT explode(array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0))) AS geom")
      val df = baseDf.select(ST_Envelope_Aggr("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Union_Aggr") {
      val baseDf = sparkSession.sql(
        "SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))) AS geom")
      val df = baseDf.select(ST_Union_Aggr("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((0 0, 0 1, 1 1, 2 1, 2 0, 1 0, 0 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Intersection_Aggr") {
      val baseDf = sparkSession.sql(
        "SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))'))) AS geom")
      val df = baseDf.select(ST_Intersection_Aggr("geom"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "POLYGON ((2 0, 1 0, 1 1, 2 1, 2 0))"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_LineFromMultiPoint") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))') AS multipoint")
      val df = baseDf.select(ST_LineFromMultiPoint("multipoint"))
      val actualResult = df.take(1)(0).get(0).asInstanceOf[Geometry].toText()
      val expectedResult = "LINESTRING (10 40, 40 30, 20 20, 30 10)"
      assert(actualResult == expectedResult)
    }

    it("Passed ST_S2CellIDs") {
      val baseDF = sparkSession.sql(
        "SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as geom")
      val df = baseDF.select(ST_S2CellIDs("geom", 6))
      val dfMRB = baseDF.select(ST_S2CellIDs(ST_Envelope(col("geom")), lit(6)))
      val actualResult = df.take(1)(0).getAs[mutable.WrappedArray[Long]](0).toSet
      val mbrResult = dfMRB.take(1)(0).getAs[mutable.WrappedArray[Long]](0).toSet
      assert(actualResult.subsetOf(mbrResult))
    }

    it("Passed ST_S2ToGeom") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0.1 0.1, 0.5 0.1, 1 0.3, 1 1, 0.1 1, 0.1 0.1))') as geom")
      val df = baseDf.select(ST_S2ToGeom(ST_S2CellIDs("geom", 10)).as("polygons"), col("geom"))
      var intersectsDf = df.select(ST_Intersects(col("geom"), element_at(col("polygons"), 1)))
      assert(intersectsDf.first().getBoolean(0))
      intersectsDf = df.select(ST_Intersects(col("geom"), element_at(col("polygons"), 21)))
      assert(intersectsDf.first().getBoolean(0))
      intersectsDf = df.select(ST_Intersects(col("geom"), element_at(col("polygons"), 101)))
      assert(intersectsDf.first().getBoolean(0))
    }

    it("Passed ST_H3CellIDs") {
      val baseDF = sparkSession.sql(
        "SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as geom")
      val df = baseDF.select(ST_MakeValid(ST_Collect(ST_H3ToGeom(ST_H3CellIDs("geom", 6, true)))))
      val actualResult = df.take(1)(0).getAs[Geometry](0)
      val targetShape = baseDF.take(1)(0).getAs[Polygon](0);
      assert(actualResult.contains(targetShape))
    }

    it("Passed ST_H3CellDistance") {
      val distance = sparkSession
        .sql("SELECT ST_Point(1.0, 2.0) lp, ST_Point(1.23, 1.59) rp")
        .withColumn("lc", element_at(ST_H3CellIDs("lp", 8, true), 1))
        .withColumn("rc", element_at(ST_H3CellIDs("rp", 8, true), 1))
        .select(ST_H3CellDistance(col("lc"), col("rc")))
        .collectAsList()
        .get(0)
        .get(0)
      assert(78 == distance)
    }

    it("Passed ST_H3KRing") {
      val neighborsAll = sparkSession
        .sql("SELECT ST_Point(1.0, 2.0) geom")
        .withColumn("cell", element_at(ST_H3CellIDs("geom", 8, true), 1))
        .select(ST_H3KRing(col("cell"), 3, false))
        .collectAsList()
        .get(0)
        .get(0)
        .asInstanceOf[mutable.WrappedArray[Long]]
      val neighborsExactRing = sparkSession
        .sql("SELECT ST_Point(1.0, 2.0) geom")
        .withColumn("cell", element_at(ST_H3CellIDs("geom", 8, true), 1))
        .select(ST_H3KRing(col("cell"), 3, true))
        .collectAsList()
        .get(0)
        .get(0)
        .asInstanceOf[mutable.WrappedArray[Long]]
      assert(neighborsExactRing.toSet.subsetOf(neighborsAll.toSet))
    }

    it("Passed ST_DistanceSphere") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (0 0)') AS geom1, ST_GeomFromWKT('POINT (90 0)') AS geom2")
      var df = baseDf.select(ST_DistanceSphere("geom1", "geom2"))
      var actualResult = df.take(1)(0).getDouble(0)
      var expectedResult = 1.00075559643809e7
      assert(actualResult == expectedResult)

      df = baseDf.select(ST_DistanceSphere("geom1", "geom2", 6378137.0))
      actualResult = df.take(1)(0).getDouble(0)
      expectedResult = 1.0018754171394622e7
      assertEquals(expectedResult, actualResult, 0.1)
    }

    it("Passed ST_DistanceSpheroid") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (0 0)') AS geom1, ST_GeomFromWKT('POINT (90 0)') AS geom2")
      val df = baseDf.select(ST_DistanceSpheroid("geom1", "geom2"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 10018754.171394622
      assertEquals(expectedResult, actualResult, 0.1)
    }

    it("Passed ST_AreaSpheroid") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('Polygon ((34 35, 28 30, 25 34, 34 35))') AS geom")
      val df = baseDf.select(ST_AreaSpheroid("geom"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 201824850811.76245
      assertEquals(expectedResult, actualResult, 0.1)
    }

    it("Passed ST_LengthSpheroid") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('LineString (0 0, 90 0)') AS geom")
      val df = baseDf.select(ST_LengthSpheroid("geom"))
      val actualResult = df.take(1)(0).getDouble(0)
      val expectedResult = 10018754.171394622
      assertEquals(expectedResult, actualResult, 0.1)
    }

    it("Passed ST_NumPoints") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val df = lineDf.select(ST_NumPoints("geom"))
      val actualResult = df.take(1)(0).getInt(0)
      val expectedResult = 3
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Force3D") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val expectedGeom = "LINESTRING Z(0 1 2.3, 1 0 2.3, 2 0 2.3)"
      val expectedGeomDefaultValue = "LINESTRING Z(0 1 0, 1 0 0, 2 0 0)"
      val wktWriter = new WKTWriter(3)
      val forcedGeom =
        lineDf.select(ST_Force3D("geom", 2.3)).take(1)(0).get(0).asInstanceOf[Geometry]
      assertEquals(expectedGeom, wktWriter.write(forcedGeom))
      val lineDfDefaultValue =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val actualGeomDefaultValue =
        lineDfDefaultValue.select(ST_Force3D("geom")).take(1)(0).get(0).asInstanceOf[Geometry]
      assertEquals(expectedGeomDefaultValue, wktWriter.write(actualGeomDefaultValue))
    }

    it("Passed ST_Force3DM") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val expectedGeom = "LINESTRING M(0 1 2.3, 1 0 2.3, 2 0 2.3)"
      val expectedGeomDefaultValue = "LINESTRING M(0 1 0, 1 0 0, 2 0 0)"
      val forcedGeom =
        lineDf.select(ST_AsText(ST_Force3DM("geom", 2.3))).take(1)(0).get(0).asInstanceOf[String]
      assertEquals(expectedGeom, forcedGeom)
      val lineDfDefaultValue =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val actualGeomDefaultValue = lineDfDefaultValue
        .select(ST_AsText(ST_Force3DM("geom")))
        .take(1)(0)
        .get(0)
        .asInstanceOf[String]
      assertEquals(expectedGeomDefaultValue, actualGeomDefaultValue)
    }

    it("Passed ST_Force3DZ") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val expectedGeom = "LINESTRING Z(0 1 2.3, 1 0 2.3, 2 0 2.3)"
      val expectedGeomDefaultValue = "LINESTRING Z(0 1 0, 1 0 0, 2 0 0)"
      val wktWriter = new WKTWriter(3)
      val forcedGeom =
        lineDf.select(ST_Force3DZ("geom", 2.3)).take(1)(0).get(0).asInstanceOf[Geometry]
      assertEquals(expectedGeom, wktWriter.write(forcedGeom))
      val lineDfDefaultValue =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val actualGeomDefaultValue =
        lineDfDefaultValue.select(ST_Force3DZ("geom")).take(1)(0).get(0).asInstanceOf[Geometry]
      assertEquals(expectedGeomDefaultValue, wktWriter.write(actualGeomDefaultValue))
    }

    it("Passed ST_Force4D") {
      val lineDf = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val expectedGeom = "LINESTRING ZM(0 1 4 4, 1 0 4 4, 2 0 4 4)"
      val expectedGeomDefaultValue = "LINESTRING ZM(0 1 0 0, 1 0 0 0, 2 0 0 0)"
      val forcedGeom =
        lineDf.select(ST_AsText(ST_Force4D("geom", 4, 4))).take(1)(0).get(0).asInstanceOf[String]
      assertEquals(expectedGeom, forcedGeom)
      val lineDfDefaultValue =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (0 1, 1 0, 2 0)') AS geom")
      val actualGeomDefaultValue = lineDfDefaultValue
        .select(ST_AsText(ST_Force4D("geom")))
        .take(1)(0)
        .get(0)
        .asInstanceOf[String]
      assertEquals(expectedGeomDefaultValue, actualGeomDefaultValue)
    }

    it("Passed ST_ForceCollection") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)') AS mpoint, ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS poly")
      var actual = baseDf.select(ST_NumGeometries(ST_ForceCollection("mpoint"))).first().get(0)
      assert(actual == 6)

      actual = baseDf.select(ST_NumGeometries(ST_ForceCollection("poly"))).first().get(0)
      assert(actual == 1)
    }

    it("Passed ST_TriangulatePolygon") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))') as poly")
      val actual = baseDf.select(ST_AsText(ST_TriangulatePolygon("poly"))).first().getString(0)
      val expected =
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))"
      assertEquals(expected, actual)
    }

    it("Passed ST_ForceRHR") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') AS poly")
      val actual =
        baseDf.select(ST_AsText(ST_ForceRHR("poly"))).take(1)(0).get(0).asInstanceOf[String]
      val expected =
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))"
      assertEquals(expected, actual)
    }

    it("Passed ST_ForcePolygonCW") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') AS poly")
      val actual =
        baseDf.select(ST_AsText(ST_ForcePolygonCW("poly"))).take(1)(0).get(0).asInstanceOf[String]
      val expected =
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))"
      assertEquals(expected, actual)
    }

    it("Passed ST_IsPolygonCW") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') as poly")
      var actual = baseDf.select(ST_IsPolygonCW("poly")).take(1)(0).get(0).asInstanceOf[Boolean]
      assertFalse(actual)

      baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))') as poly")
      actual = baseDf.select(ST_IsPolygonCW("poly")).take(1)(0).get(0).asInstanceOf[Boolean]
      assertTrue(actual)
    }

    it("Should pass ST_GeneratePoints") {
      var poly = sparkSession
        .sql(
          "SELECT ST_Buffer(ST_GeomFromWKT('LINESTRING(50 50,150 150,150 50)'), 10, false, 'endcap=round join=round') AS geom")
      var actual = poly
        .select(ST_NumGeometries(ST_GeneratePoints("geom", 15)))
        .first()
        .get(0)
        .asInstanceOf[Int]
      assert(actual == 15)

      poly = sparkSession
        .sql(
          "SELECT ST_GeomFromWKT('MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))') AS geom")
      actual = poly
        .select(ST_NumGeometries(ST_GeneratePoints("geom", 30)))
        .first()
        .get(0)
        .asInstanceOf[Int]
      assert(actual == 30)
    }

    it("Passed ST_NRings") {
      val polyDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))') AS geom")
      val expected = 1
      val df = polyDf.select(ST_NRings("geom"))
      val actual = df.take(1)(0).getInt(0)
      assert(expected == actual)
    }

    it("Passed ST_ForcePolygonCCW") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))') AS poly")
      val actual = baseDf
        .select(ST_AsText(ST_ForcePolygonCCW("poly")))
        .take(1)(0)
        .get(0)
        .asInstanceOf[String]
      val expected =
        "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
      assertEquals(expected, actual)
    }

    it("Passed ST_IsPolygonCCW") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') as poly")
      var actual = baseDf.select(ST_IsPolygonCCW("poly")).take(1)(0).get(0).asInstanceOf[Boolean]
      assertTrue(actual)

      baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))') as poly")
      actual = baseDf.select(ST_IsPolygonCCW("poly")).take(1)(0).get(0).asInstanceOf[Boolean]
      assertFalse(actual)
    }

    it("Passed ST_Translate") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((1 0 1, 1 1 1, 2 1 1, 2 0 1, 1 0 1))') AS geom")
      val df = polyDf.select(ST_Translate("geom", 2, 3, 1))
      val wktWriter3D = new WKTWriter(3);
      val actualGeom = df.take(1)(0).get(0).asInstanceOf[Geometry]
      val actual = wktWriter3D.write(actualGeom)
      val expected = "POLYGON Z((3 3 2, 3 4 2, 4 4 2, 4 3 2, 3 3 2))"
      assert(expected == actual)

      val dfDefaultValue = polyDf.select(ST_Translate("geom", 2, 3))
      val actualGeomDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[Geometry]
      val actualDefaultValue = wktWriter3D.write(actualGeomDefaultValue)
      val expectedDefaultValue = "POLYGON Z((3 3 1, 3 4 1, 4 4 1, 4 3 1, 3 3 1))"
      assert(expectedDefaultValue == actualDefaultValue)
    }

    it("Passed ST_VoronoiPolygons") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOINT (0 0, 2 2)') AS geom, ST_Buffer(ST_GeomFromWKT('POINT(1 1)'), 10.0) as buf")
      val df = polyDf.select(ST_VoronoiPolygons("geom"))
      val wktWriter3D = new WKTWriter(3);
      val actualGeom = df.take(1)(0).get(0).asInstanceOf[Geometry]
      val actual = wktWriter3D.write(actualGeom)
      val expected =
        "GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 -2, -2 -2)), POLYGON ((-2 4, 4 4, 4 -2, -2 4)))"
      assert(expected == actual)
    }

    it("Passed ST_FrechetDistance") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (1 2)') as g1, ST_GeomFromWKT('POINT (100 230)') as g2")
      val df = polyDf.select(ST_FrechetDistance("g1", "g2"))
      val expected = 248.5658866377283
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      assertEquals(expected, actual, 1e-9)
    }

    it("Passed ST_Affine") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((2 3 1, 4 5 1, 7 8 2, 2 3 1))') AS geom")
      val df = polyDf.select(ST_Affine("geom", 1, 2, 3, 3, 4, 4, 1, 4, 2, 1, 2, 1));
      val dfDefaultValue = polyDf.select(ST_Affine("geom", 1, 2, 1, 2, 1, 2))
      val wKTWriter3D = new WKTWriter(3);
      val actualGeom = df.take(1)(0).get(0).asInstanceOf[Geometry]
      val actualGeomDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[Geometry]
      val actual = wKTWriter3D.write(actualGeom)
      val expected = "POLYGON Z((12 24 17, 18 38 27, 30 63 44, 12 24 17))"
      val actualDefaultValue = wKTWriter3D.write(actualGeomDefaultValue)
      val expectedDefaultValue = "POLYGON Z((9 10 1, 15 16 1, 24 25 2, 9 10 1))"
      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }

    it("Passed ST_BoundingDiagonal") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((1 0 1, 2 3 2, 5 0 1, 5 2 9, 1 0 1))') AS geom")
      val df = polyDf.select(ST_BoundingDiagonal("geom"))
      val wKTWriter = new WKTWriter(3);
      val expected = "LINESTRING Z(1 0 1, 5 3 9)"
      val actual = wKTWriter.write(df.take(1)(0).get(0).asInstanceOf[Geometry])
      assertEquals(expected, actual)
    }

    it("Passed ST_CoordDim with 3D point") {
      val polyDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT(1 1 2)') AS geom")
      val expected = 3
      val df = polyDf.select(ST_CoordDim("geom"))
      val actual = df.take(1)(0).getInt(0)
      assert(expected == actual)
    }

    it("Passed ST_CoordDim with Z coordinates") {
      val polyDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINTZ(1 1 0.5)') AS geom")
      val expected = 3
      val df = polyDf.select(ST_CoordDim("geom"))
      val actual = df.take(1)(0).getInt(0)
      assert(expected == actual)
    }

    it("Passed ST_CoordDim with XYM point") {
      val polyDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT M(1 2 3)') AS geom")
      val expected = 3
      val df = polyDf.select(ST_CoordDim("geom"))
      val actual = df.take(1)(0).getInt(0)
      assert(expected == actual)
    }

    it("Passed ST_CoordDim with XYZM point") {
      val polyDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT ZM(1 2 3 4)') AS geom")
      val expected = 4
      val df = polyDf.select(ST_CoordDim("geom"))
      val actual = df.take(1)(0).getInt(0)
      assert(expected == actual)
    }

    it("Passed ST_IsCollection with a collection") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(3 2), POINT(6 4))') AS collection")
      val df = baseDf.select(ST_IsCollection("collection"))
      val actualResult = df.take(1)(0).getBoolean(0)
      val expectedResult = true
      assert(actualResult == expectedResult)
    }

    it("Passed ST_IsCollection without a collection") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT(9 9)') AS collection")
      val df = baseDf.select(ST_IsCollection("collection"))
      val actualResult = df.take(1)(0).getBoolean(0)
      val expectedResult = false
      assert(actualResult == expectedResult)
    }

    it("Passed ST_Angle - 4 Points") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (10 10)') AS p1, ST_GeomFromWKT('POINT (0 0)') AS p2," +
          " ST_GeomFromWKT('POINT (90 90)') AS p3, ST_GeomFromWKT('POINT (100 80)') AS p4")
      val df = polyDf.select(ST_Angle("p1", "p2", "p3", "p4"))
      val actualRad = df.take(1)(0).get(0).asInstanceOf[Double]
      val dfDegrees = sparkSession.sql(s"SELECT ST_Degrees($actualRad)")
      val actualDegrees = dfDegrees.take(1)(0).get(0).asInstanceOf[Double]
      val expectedDegrees = 269.9999999999999
      assertEquals(expectedDegrees, actualDegrees, 1e-9)
    }

    it("Passed ST_Angle - 3 Points") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (0 0)') AS p1, ST_GeomFromWKT('POINT (10 10)') AS p2," +
          " ST_GeomFromWKT('POINT (20 0)') AS p3")
      val df = polyDf.select(ST_Angle("p1", "p2", "p3"))
      val actualRad = df.take(1)(0).get(0).asInstanceOf[Double]
      val dfDegrees = sparkSession.sql(s"SELECT ST_Degrees($actualRad)")
      val actualDegrees = dfDegrees.take(1)(0).get(0).asInstanceOf[Double]
      val expectedDegrees = 270
      assertEquals(expectedDegrees, actualDegrees, 1e-9)
    }

    it("Passed ST_Angle - 2 LineStrings") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(0 0, 0.3 0.7, 1 1)') AS line1, ST_GeomFromWKT('LINESTRING(0 0, 0.2 0.5, 1 0)') AS line2")
      val df = polyDf.select(ST_Angle("line1", "line2"))
      val actualRad = df.take(1)(0).get(0).asInstanceOf[Double]
      val dfDegrees = sparkSession.sql(s"SELECT ST_Degrees($actualRad)")
      val actualDegrees = dfDegrees.take(1)(0).get(0).asInstanceOf[Double]
      val expectedDegrees = 45
      assertEquals(expectedDegrees, actualDegrees, 1e-9)
    }

    it("Should pass ST_DelaunayTriangles") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOLYGON (((10 10, 10 20, 20 20, 20 10, 10 10)),((25 10, 25 20, 35 20, 35 10, 25 10)))') AS geom")
      var actual =
        baseDf.select(ST_DelaunayTriangles("geom")).first().get(0).asInstanceOf[Geometry].toText
      var expected =
        "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 20 10, 10 20)), POLYGON ((10 20, 20 10, 20 20, 10 20)), POLYGON ((20 20, 20 10, 25 10, 20 20)), POLYGON ((20 20, 25 10, 25 20, 20 20)), POLYGON ((25 20, 25 10, 35 10, 25 20)), POLYGON ((25 20, 35 10, 35 20, 25 20)))"
      assertEquals(expected, actual)

      actual = baseDf
        .select(ST_DelaunayTriangles("geom", 20))
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      expected = "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 35 10, 10 20)))"
      assertEquals(expected, actual)

      actual = baseDf
        .select(ST_DelaunayTriangles("geom", 20, 1))
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      expected = "MULTILINESTRING ((10 20, 35 10), (10 10, 10 20), (10 10, 35 10))"
      assertEquals(expected, actual)
    }

    it("Passed ST_HausdorffDistance") {
      val polyDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((1 2, 2 1, 2 0, 4 1, 1 2))') AS g1, " +
          "ST_GeomFromWKT('MULTILINESTRING ((1 1, 2 1, 4 4, 5 5), (10 10, 11 11, 12 12, 14 14), (-11 -20, -11 -21, -15 -19))') AS g2")
      val df = polyDf.select(ST_HausdorffDistance("g1", "g2", 0.05))
      val dfDefaultValue = polyDf.select(ST_HausdorffDistance("g1", "g2"))
      val expected = 25.495097567963924
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[Double]
      assert(expected == actual)
      assert(expected == actualDefaultValue)
    }

    it("Passed GeometryType") {
      val polyDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON ((1 2, 2 1, 2 0, 4 1, 1 2))') AS geom")
      val df = polyDf.select(GeometryType("geom"))
      val expected = "POLYGON"
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      assert(expected == actual)
    }

    it("Passed ST_DWithin") {
      val pointDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POINT (0 0)') as origin, ST_GeomFromWKT('POINT (1 0)') as point")
      val df = pointDf.select(ST_DWithin("origin", "point", 2.0))
      val actual = df.head()(0).asInstanceOf[Boolean]
      assertTrue(actual)
    }

    it("Should pass ST_MaximumInscribedCircle") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))') AS geom")
      val actual: Row = baseDf.select(ST_MaximumInscribedCircle("geom")).first().getAs[Row](0)
      val expected = Row(
        sparkSession
          .sql("SELECT ST_GeomFromWKT('POINT (96.953125 76.328125)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry],
        sparkSession
          .sql("SELECT ST_GeomFromWKT('POINT (140 90)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry],
        45.165845650018)
      assertTrue(actual.equals(expected))
    }

    it("Should pass ST_IsValidTrajectory") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromText('LINESTRING M (0 0 1, 0 1 2)') as geom1, ST_GeomFromText('LINESTRING M (0 0 1, 0 1 1)') as geom2")
      var actual =
        baseDf.select(ST_IsValidTrajectory("geom1")).first().get(0).asInstanceOf[Boolean]
      assertTrue("Valid", actual)

      actual = baseDf.select(ST_IsValidTrajectory("geom2")).first().get(0).asInstanceOf[Boolean]
      assertFalse("Not valid", actual)
    }

    it("Passed ST_IsValidDetail") {
      // Valid Geometry
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 1 1, 0 0))') AS geom")
      var actual = baseDf.select(ST_IsValidDetail($"geom")).first().getAs[Row](0)
      var expected = Row(true, null, null)
      assert(expected.equals(actual))

      // Geometry that is invalid under both OGC and ESRI standards, but with different reasons
      baseDf = sparkSession.sql(
        "SELECT ST_GeomFromText('POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))') AS geom")

      // Test with OGC flag (OGC_SFS_VALIDITY = 0)
      actual = baseDf.select(ST_IsValidDetail("geom", 0)).first().getAs[Row](0)
      expected = Row(
        false,
        "Ring Self-intersection at or near point (1.0, 1.0, NaN)",
        sparkSession
          .sql("SELECT ST_GeomFromText('POINT (1 1)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry])
      assert(expected.equals(actual))

      // Test with ESRI flag (ESRI_VALIDITY = 1)
      actual = baseDf.select(ST_IsValidDetail($"geom", lit(1))).first().getAs[Row](0)
      expected = Row(
        false,
        "Interior is disconnected at or near point (1.0, 1.0, NaN)",
        sparkSession
          .sql("SELECT ST_GeomFromText('POINT (1 1)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry])
      assert(expected.equals(actual))
    }

    it("Passed ST_IsValidReason") {
      // Valid Geometry
      val validPolygonWKT = "POLYGON ((0 0, 2 0, 2 2, 0 2, 1 1, 0 0))"
      val validTable = Seq(validPolygonWKT).toDF("wkt").select(ST_GeomFromWKT($"wkt").as("geom"))
      val validityTable = validTable.select(ST_IsValidReason($"geom"))
      val validityReason = validityTable.take(1)(0).getString(0)
      assertEquals("Valid Geometry", validityReason)

      // Geometry that is invalid under both OGC and ESRI standards, but with different reasons
      val selfTouchingWKT = "POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))"
      val specialCaseTable =
        Seq(selfTouchingWKT).toDF("wkt").select(ST_GeomFromWKT($"wkt").as("geom"))

      // Test with OGC flag (OGC_SFS_VALIDITY = 0)
      val ogcValidityTable = specialCaseTable.select(ST_IsValidReason($"geom", lit(0)))
      val ogcValidityReason = ogcValidityTable.take(1)(0).getString(0)
      assertEquals("Ring Self-intersection at or near point (1.0, 1.0, NaN)", ogcValidityReason)

      // Test with ESRI flag (ESRI_VALIDITY = 1)
      val esriValidityTable = specialCaseTable.select(ST_IsValidReason($"geom", lit(1)))
      val esriValidityReason = esriValidityTable.take(1)(0).getString(0)
      assertEquals(
        "Interior is disconnected at or near point (1.0, 1.0, NaN)",
        esriValidityReason)
    }

    it("Passed ST_PointZM") {
      val pointDf = sparkSession.sql(
        "SELECT ST_PointZM(1,2,3,100) as point1, ST_PointZM(1,2,3,100,4326) as point2")
      val point1 = pointDf.select(ST_AsEWKT("point1")).take(1)(0).getString(0)
      val point2 = pointDf.select(ST_AsEWKT("point2")).take(1)(0).getString(0)
      assertEquals("POINT ZM(1 2 3 100)", point1)
      assertEquals("SRID=4326;POINT ZM(1 2 3 100)", point2)
    }

    it("Should pass ST_RotateX") {
      val geomTestCases = Map(
        (
          1,
          "'LINESTRING (50 160, 50 50, 100 50)'",
          Math.PI) -> "'LINESTRING (50 -160, 50 -50, 100 -50)'",
        (
          2,
          "'LINESTRING(1 2 3, 1 1 1)'",
          Math.PI / 2) -> "'LINESTRING Z(1 -3 2, 1 -0.9999999999999999 1)'")

      for (((index, geom, angle), expectedResult) <- geomTestCases) {
        val baseDf = sparkSession.sql(s"SELECT ST_GeomFromEWKT($geom) as geom")
        val df = baseDf.select(ST_AsEWKT(ST_RotateX("geom", angle)))

        val actual = df.take(1)(0).get(0).asInstanceOf[String]
        assert(actual == expectedResult.stripPrefix("'").stripSuffix("'"))
      }
    }

    it("Passed ST_Rotate") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromEWKT('SRID=4326;POLYGON ((0 0, 2 0, 2 2, 0 2, 1 1, 0 0))') AS geom1, ST_GeomFromText('POINT (2 2)') AS geom2")
      var actual = baseDf.select(ST_AsEWKT(ST_Rotate("geom1", Math.PI))).first().get(0)
      var expected =
        "SRID=4326;POLYGON ((0 0, -2 0.0000000000000002, -2.0000000000000004 -1.9999999999999998, -0.0000000000000002 -2, -1.0000000000000002 -0.9999999999999999, 0 0))"
      assert(expected.equals(actual))

      actual = baseDf.select(ST_AsEWKT(ST_Rotate("geom1", 50, "geom2"))).first().get(0)
      expected =
        "SRID=4326;POLYGON ((-0.4546817643920842 0.5948176504236309, 1.4752502925921425 0.0700679430157733, 2 2, 0.0700679430157733 2.5247497074078575, 0.7726591178039579 1.2974088252118154, -0.4546817643920842 0.5948176504236309))"
      assert(expected.equals(actual))

      actual = baseDf.select(ST_AsEWKT(ST_Rotate("geom1", 50, 2, 2))).first().get(0)
      expected =
        "SRID=4326;POLYGON ((-0.4546817643920842 0.5948176504236309, 1.4752502925921425 0.0700679430157733, 2 2, 0.0700679430157733 2.5247497074078575, 0.7726591178039579 1.2974088252118154, -0.4546817643920842 0.5948176504236309))"
      assert(expected.equals(actual))
    }

    it("Passed returning exception with geometry when exception is thrown") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromEWKT('SRID=4326;POLYGON ((0 0, 2 0, 2 2, 0 2, 1 1, 0 0))') AS geom1, ST_GeomFromEWKT('SRID=4326;POLYGON ((0 0, 1 0, 1 1, 0 1, 1 1, 0 0))') AS geom2")

      // Use intercept to assert that an exception is thrown
      val exception = intercept[InferredExpressionException] {
        baseDf.select(ST_Rotate("geom1", 50, "geom2")).take(1)
      }

      // Check the exception message
      assert(exception.getMessage.contains("POLYGON ((0 0, 2 0, 2 2, 0 2, 1 1, 0 0))"))
      assert(exception.getMessage.contains("POLYGON ((0 0, 1 0, 1 1, 0 1, 1 1, 0 0))"))
      assert(exception.getMessage.contains("ST_Rotate"))
      assert(exception.getMessage.contains("The origin must be a non-empty Point geometry."))

      // Non-literal test case: ST_MakeLine will raise an exception
      val exception2 = intercept[Exception] {
        sparkSession
          .range(0, 1)
          .withColumn("geom", expr("ST_PolygonFromEnvelope(id, id, id + 1, id + 1)"))
          .selectExpr("id", "ST_Envelope(ST_MakeLine(geom, geom))")
          .collect()
      }
      assert(exception2.getMessage.contains("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"))
      assert(exception2.getMessage.contains("ST_MakeLine"))
    }
  }
}
