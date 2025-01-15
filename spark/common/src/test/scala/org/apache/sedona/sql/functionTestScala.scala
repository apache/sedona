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
import org.apache.sedona.common.FunctionsGeoTools
import org.apache.sedona.sql.implicits._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.geotools.referencing.CRS
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.locationtech.jts.algorithm.MinimumBoundingCircle
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.linearref.LengthIndexedLine
import org.locationtech.jts.operation.distance3d.Distance3DOp
import org.opengis.referencing.FactoryException
import org.scalatest.{GivenWhenThen, Matchers}
import org.xml.sax.InputSource

import java.io.StringReader
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory

class functionTestScala
    extends TestBaseScala
    with Matchers
    with GeometrySample
    with GivenWhenThen {

  import sparkSession.implicits._

  describe("Sedona-SQL Function Test") {

    it("Passed ST_LabelPoint") {
      var geomDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((-112.637484 33.440546, -112.546852 33.477209, -112.489177 33.550488, -112.41777 33.751684, -111.956371 33.719707, -111.766868 33.616843, -111.775107 33.527595, -111.640533 33.504695, -111.440044 33.463462, -111.415326 33.374055, -111.514197 33.309809, -111.643279 33.222542, -111.893203 33.174278, -111.96461 33.250109, -112.123903 33.261593, -112.252985 33.35341, -112.406784 33.346527, -112.667694 33.316695, -112.637484 33.440546))') AS geom, 2 AS gridResolution, 0.2 AS GoodnessThreshold")
      geomDf.createOrReplaceTempView("geomDf")
      var result =
        sparkSession.sql(
          "SELECT ST_AsEWKT(ST_LabelPoint(geom, gridResolution, goodnessThreshold)) FROM geomDf")
      var expected = "POINT (-112.04278737349767 33.46420809489905)"
      assertEquals(expected, result.take(1)(0).get(0).asInstanceOf[String])

      geomDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POLYGON ((-112.840785 33.435962, -112.840785 33.708284, -112.409597 33.708284, -112.409597 33.435962, -112.840785 33.435962)), POLYGON ((-112.309264 33.398167, -112.309264 33.746007, -111.787444 33.746007, -111.787444 33.398167, -112.309264 33.398167)))') AS geom")
      geomDf.createOrReplaceTempView("geomDf")
      result = sparkSession.sql("SELECT ST_AsEWKT(ST_LabelPoint(geom, 1)) FROM geomDf")
      expected = "POINT (-112.04835399999999 33.57208699999999)"
      assertEquals(expected, result.take(1)(0).get(0).asInstanceOf[String])

      geomDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((-112.654072 33.114485, -112.313516 33.653431, -111.63515 33.314399, -111.497829 33.874913, -111.692825 33.431378, -112.376684 33.788215, -112.654072 33.114485))') AS geom, 0.01 AS goodnessThreshold")
      geomDf.createOrReplaceTempView("geomDf")
      result = sparkSession.sql(
        "SELECT ST_AsEWKT(ST_LabelPoint(geom, 2, goodnessThreshold)) FROM geomDf")
      expected = "POINT (-112.0722602222832 33.53914975012836)"
      assertEquals(expected, result.take(1)(0).get(0).asInstanceOf[String])

      result = sparkSession.sql("SELECT ST_AsEWKT(ST_LabelPoint(geom)) FROM geomDf")
      expected = "POINT (-112.0722602222832 33.53914975012836)"
      assertEquals(expected, result.take(1)(0).get(0).asInstanceOf[String])
    }

    it("Passed ST_ConcaveHull") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf =
        sparkSession.sql("select ST_ConcaveHull(polygondf.countyshape, 1, true) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_ConvexHull") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf =
        sparkSession.sql("select ST_ConvexHull(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_CrossesDateLine") {
      var crossesTesttable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((170 -10, -170 -10, -170 10, 170 10, 170 -10), (175 -5, -175 -5, -175 5, 175 5, 175 -5))') as geom")
      crossesTesttable.createOrReplaceTempView("crossesTesttable")
      var crosses = sparkSession.sql("select(ST_CrossesDateLine(geom)) from crossesTesttable")

      var notCrossesTesttable =
        sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as geom")
      notCrossesTesttable.createOrReplaceTempView("notCrossesTesttable")
      var notCrosses =
        sparkSession.sql("select(ST_CrossesDateLine(geom)) from notCrossesTesttable")

      assert(crosses.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notCrosses.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_Buffer") {
      val polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      val functionDf =
        sparkSession.sql("select ST_Buffer(polygondf.countyshape, 1) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Buffer Spheroid") {
      val polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var functionDf =
        sparkSession.sql("select ST_Buffer(polygondf.countyshape, 1, true) from polygondf")
      assert(functionDf.count() > 0)

      functionDf = sparkSession.sql(
        "select ST_Buffer(polygondf.countyshape, 1, true, 'quad_segs=2') from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_BestSRID") {
      val polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      val functionDf =
        sparkSession.sql("select ST_BestSRID(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_ShiftLongitude") {
      val polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      val functionDf =
        sparkSession.sql("select ST_ShiftLongitude(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Envelope") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf =
        sparkSession.sql("select ST_Envelope(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Expand") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))') as geom")
      var actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 10))").first().get(0)
      var expected = "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))"
      assertEquals(expected, actual)

      actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 5, 6))").first().get(0)
      expected = "POLYGON Z((45 44 1, 45 86 1, 85 86 3, 85 44 3, 45 44 1))"
      assertEquals(expected, actual)

      actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 6, 5, -3))").first().get(0)
      expected = "POLYGON Z((44 45 4, 44 85 4, 86 85 0, 86 45 0, 44 45 4))"
      assertEquals(expected, actual)
    }

    it("Passed ST_YMax") {
      var test = sparkSession.sql(
        "SELECT ST_YMax(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 -2, -3 -1, -3 -3))'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Double] == -1)
    }

    it("Passed ST_YMin") {
      var test = sparkSession.sql(
        "SELECT ST_YMin(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Double] == -3.0)
    }

    it("Passed ST_ZMax") {
      val test = sparkSession.sql(
        "SELECT ST_ZMax(ST_GeomFromWKT('POLYGON((0 0 0,0 5 0,5 0 0,0 0 5),(1 1 0,3 1 0,1 3 0,1 1 0))'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Double] == 5.0)
    }
    it("Passed ST_ZMax with no Z coordinate") {
      val test = sparkSession.sql(
        "SELECT ST_ZMax(ST_GeomFromWKT('POLYGON((0 0,0 5,5 0,0 0),(1 1,3 1,1 3,1 1))'))")
      assert(test.take(1)(0).get(0) == null)
    }

    it("Passed ST_ZMin") {
      val test = sparkSession.sql("SELECT ST_ZMin(ST_GeomFromWKT('LINESTRING(1 3 4, 5 6 7)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Double] == 4.0)
    }

    it("Passed ST_ZMin with no Z coordinate") {
      val test = sparkSession.sql("SELECT ST_ZMin(ST_GeomFromWKT('LINESTRING(1 3, 5 6)'))")
      assert(test.take(1)(0).get(0) == null)
    }

    it("Passed ST_Centroid") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf =
        sparkSession.sql("select ST_Centroid(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Length") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf = sparkSession.sql("select ST_Length(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Length2D") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf =
        sparkSession.sql("select ST_Length2D(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Area") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf = sparkSession.sql("select ST_Area(polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_Dimension with Geometry") {
      val geomTestCases = Map(
        ("'POINT (51.3168 -0.56)'") -> "0",
        ("'LineString (0 0, 0 90)'") -> "1",
        ("'POLYGON((0 0,0 5,5 0,0 0))'") -> "2",
        ("'MULTILINESTRING((0 0, 0 5, 5 0, 0 0))'") -> "1")
      for ((geom, expectedResult) <- geomTestCases) {
        val df = sparkSession.sql(
          s"SELECT ST_Dimension(ST_GeomFromWKT($geom)), " +
            s"$expectedResult")
        val actual = df.take(1)(0).get(0).asInstanceOf[Integer]
        val expected = df.take(1)(0).get(1).asInstanceOf[Integer]
        assertEquals(expected, actual)
      }
    }

    it("Passed DT_Dimension with GeometryCollection") {
      val geomTestCases = Map(
        ("'GEOMETRYCOLLECTION EMPTY'") -> "0",
        ("'GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'") -> "1",
        ("'GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2))), MULTIPOINT(6 6, 7 7, 8 8))'") -> "2")
      for ((geom, expectedResult) <- geomTestCases) {
        val df = sparkSession.sql(
          s"SELECT ST_Dimension(ST_GeomFromWKT($geom)), " +
            s"$expectedResult")
        val actual = df.take(1)(0).get(0).asInstanceOf[Integer]
        val expected = df.take(1)(0).get(1).asInstanceOf[Integer]
        assertEquals(expected, actual)
      }
    }

    it("Passed ST_Distance") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var functionDf = sparkSession.sql(
        "select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf")
      assert(functionDf.count() > 0)
    }

    it("Passed ST_3DDistance") {
      val point1 = wktReader.read("POINT Z (0 0 -5)")
      val point2 = wktReader.read("POINT Z (1 1 -6)")
      val pointDf = Seq(Tuple2(point1, point2)).toDF("p1", "p2")
      pointDf.createOrReplaceTempView("pointdf")
      var functionDf = sparkSession.sql("select ST_3DDistance(p1, p2) from pointdf")
      val expected = Distance3DOp.distance(point1, point2)
      assert(functionDf.take(1)(0).get(0).asInstanceOf[Double].equals(expected))
    }

    it("Passed ST_TriangulatePolygon") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))') as poly")
      val actual =
        baseDf.selectExpr("ST_AsText(ST_TriangulatePolygon(poly))").first().getString(0)
      val expected =
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))"
      assert(expected.equals(actual))
    }

    it("Passed ST_Transform") {
      var point = "POINT (120 60)"
      val transformedResult = sparkSession
        .sql(s"""select ST_Transform(ST_geomFromWKT('$point'),'EPSG:4326', 'EPSG:3857', false)""")
        .rdd
        .map(row => row.getAs[Geometry](0))
        .collect()(0)
      assertEquals(1.3358338895192828e7, transformedResult.getCoordinate.x, FP_TOLERANCE)
      assertEquals(8399737.889818355, transformedResult.getCoordinate.y, FP_TOLERANCE)

      point = "POINT (100 40)"
      val result = sparkSession
        .sql(s"""SELECT
           |ST_AsText(ST_ReducePrecision(ST_Transform(ST_SetSRID(ST_GeomFromWKT('$point'), 4326), 'EPSG:3857'), 2))""".stripMargin)
        .first()
        .get(0)
      assertEquals("POINT (11131949.08 4865942.28)", result)
    }

    it("Passed ST_transform WKT version") {
      val polygon = "POLYGON ((120 60, 121 61, 122 62, 123 63, 124 64, 120 60))"
      val geometryFactory = new GeometryFactory
      val coords = new Array[Coordinate](6)
      coords(0) = new Coordinate(1000961.4042164611, 6685590.893548286)
      coords(1) = new Coordinate(1039394.2790044537, 6804110.988854166)
      coords(2) = new Coordinate(1074157.4382441062, 6923060.447921266)
      coords(3) = new Coordinate(1105199.4259604653, 7042351.1239674715)
      coords(4) = new Coordinate(1132473.1932022288, 7161889.652860963)
      coords(5) = coords(0)
      val polygonExpected = geometryFactory.createPolygon(coords)
      val EPSG_TGT_CRS = CRS.decode("EPSG:32649", true)
      val EPSG_TGT_WKT = EPSG_TGT_CRS.toWKT()
      val EPSG_SRC_CRS = CRS.decode("EPSG:4326", true)
      val EPSG_SRC_WKT = EPSG_SRC_CRS.toWKT()

      sparkSession
        .createDataset(Seq(polygon))
        .withColumn("geom", expr("ST_GeomFromWKT(value)"))
        .createOrReplaceTempView("df")
      val result_TGT_WKT = sparkSession
        .sql(
          s"""select ST_Transform(ST_geomFromWKT('$polygon'),'EPSG:4326', '$EPSG_TGT_WKT', false)""")
        .rdd
        .map(row => row.getAs[Geometry](0))
        .collect()(0)
      assertEquals(0, result_TGT_WKT.compareTo(polygonExpected, COORDINATE_SEQUENCE_COMPARATOR))

      val result_SRC_WKT = sparkSession
        .sql(
          s"""select ST_Transform(ST_geomFromWKT('$polygon'),'$EPSG_SRC_WKT', 'EPSG:32649', false)""")
        .rdd
        .map(row => row.getAs[Geometry](0))
        .collect()(0)
      assertEquals(0, result_SRC_WKT.compareTo(polygonExpected, COORDINATE_SEQUENCE_COMPARATOR))

      val result_SRC_TGT_WKT = sparkSession
        .sql(
          s"""select ST_Transform(ST_geomFromWKT('$polygon'),'$EPSG_SRC_WKT', '$EPSG_TGT_WKT', false)""")
        .rdd
        .map(row => row.getAs[Geometry](0))
        .collect()(0)
      assertEquals(
        0,
        result_SRC_TGT_WKT.compareTo(polygonExpected, COORDINATE_SEQUENCE_COMPARATOR))
    }

    it("Passed Function exception check") {
      val EPSG_TGT_CRS = CRS.decode("EPSG:32649", true)
      val EPSG_TGT_WKT = EPSG_TGT_CRS.toWKT()
      val epsgFactoryErrorString = EPSG_TGT_WKT.substring(0, EPSG_TGT_WKT.length() - 1)
      val epsgString = "EPSG:4326"
      val epsgNoSuchAuthorityString = "EPSG:9377"
      val polygon =
        "POLYGON ((110.54671 55.818002, 110.54671 55.143743, 110.940494 55.143743, 110.940494 55.818002, 110.54671 55.818002))"
      import org.locationtech.jts.io.WKTReader
      val reader = new WKTReader
      val polygeom = reader.read(polygon)

      intercept[FactoryException] {
        val d = FunctionsGeoTools.transform(polygeom, epsgString, epsgFactoryErrorString)
      }

      intercept[FactoryException] {
        val d2 = FunctionsGeoTools.transform(polygeom, epsgString, epsgNoSuchAuthorityString)
      }

    }

    it("Passed ST_Intersection - intersects but not contains") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(
        intersec
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"))
    }

    it("Passed ST_Intersection - intersects but left contains right") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(
        intersec
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((2 2, 2 3, 3 3, 2 2))"))
    }

    it("Passed ST_Intersection - intersects but right contains left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as a,ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(
        intersec
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((2 2, 2 3, 3 3, 2 2))"))
    }

    it("Passed ST_Intersection - not intersects") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON((40 21, 40 22, 40 23, 40 21))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersect = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersect.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON EMPTY"))
    }

    it("Passed ST_IsValid") {

      var testtable = sparkSession.sql("SELECT ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))')) AS a, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')) as b, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'), 1) AS c, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'), 1) as d, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'), 0) AS e, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'), 0) as f")
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(testtable.take(1)(0).get(1).asInstanceOf[Boolean])
      assert(!testtable.take(1)(0).get(2).asInstanceOf[Boolean])
      assert(testtable.take(1)(0).get(3).asInstanceOf[Boolean])
      assert(!testtable.take(1)(0).get(4).asInstanceOf[Boolean])
      assert(testtable.take(1)(0).get(5).asInstanceOf[Boolean])
    }

    it("Fixed nullPointerException in ST_IsValid") {

      var testtable = sparkSession.sql("SELECT ST_IsValid(null)")
      assert(testtable.take(1).head.get(0) == null)
    }

    it("Passed ST_ReducePrecision") {
      var testtable = sparkSession.sql("""
          |SELECT ST_ReducePrecision(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 8)
        """.stripMargin)
      assert(
        testtable.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates()(0).x == 0.12345679)
      testtable = sparkSession.sql("""
          |SELECT ST_ReducePrecision(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 11)
        """.stripMargin)
      assert(
        testtable.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates()(0).x == 0.12345678901)

    }

    it("Passed ST_IsSimple") {

      var testtable = sparkSession.sql(
        "SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')) AS a, " +
          "ST_IsSimple(ST_GeomFromText('POLYGON((1 1,3 1,3 3,2 0,1 1))')) as b")
      assert(testtable.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!testtable.take(1)(0).get(1).asInstanceOf[Boolean])
    }

    it("Passed ST_HasZ") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON Z ((30 10 5, 40 40 10, 20 40 15, 10 20 20, 30 10 5))') as poly")
      val actual = baseDf.selectExpr("ST_HasZ(poly)").first().getBoolean(0)
      assert(actual)
    }

    it("Passed ST_HasM") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))') as poly")
      val actual = baseDf.selectExpr("ST_HasM(poly)").first().getBoolean(0)
      assert(actual)
    }

    it("Passed ST_M") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT ZM (1 2 3 4)') AS point")
      val actual = baseDf.selectExpr("ST_M(point)").first().getDouble(0)
      assert(actual == 4.0)
    }

    it("Passed ST_MMin") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') as line")
      val actual = baseDf.selectExpr("ST_MMin(line)").first().getDouble(0)
      assert(actual == -1.0)

      baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line")
      val actualNull = baseDf.selectExpr("ST_MMin(line)").first().get(0)
      assert(actualNull == null)
    }

    it("Passed ST_MMax") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)') as line")
      var actual = baseDf.selectExpr("ST_MMax(line)").first().getDouble(0)
      assert(actual == 3.0)

      baseDf =
        sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 2 2, 3 3, -1 -1)') AS line")
      val actualNull = baseDf.selectExpr("ST_MMax(line)").first().get(0)
      assert(actualNull == null)

      actual = sparkSession
        .sql("SELECT ST_MMax(ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))'))")
        .first()
        .getDouble(0)
      assert(actual == 4.0)
    }

    it("Passed ST_MakeLine") {
      val testtable = sparkSession.sql("""SELECT
          |ST_MakeLine(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)')),
          |ST_MakeLine(ARRAY(ST_Point(5, 6), ST_Point(7, 8), ST_Point(9, 10)))
          |""".stripMargin)
      val row = testtable.take(1)(0)
      assert(row.get(0).asInstanceOf[Geometry].toText.equals("LINESTRING (1 2, 3 4)"))
      assert(row.get(1).asInstanceOf[Geometry].toText.equals("LINESTRING (5 6, 7 8, 9 10)"))
    }

    it("Passed ST_Perimeter") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))') AS geom")
      var actual = baseDf.selectExpr("ST_Perimeter(geom)").first().get(0)
      var expected = 122.63074400009504
      assertEquals(expected, actual)

      baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))', 4326) AS geom")
      actual = baseDf.selectExpr("ST_Perimeter(geom, true)").first().get(0)
      expected = 443770.91724830196
      assertEquals(expected, actual)

      actual = baseDf.selectExpr("ST_Perimeter(geom, true, false)").first().get(0)
      expected = 443770.91724830196
      assertEquals(expected, actual)
    }

    it("Passed ST_Perimeter2D") {
      var baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))') AS geom")
      var actual = baseDf.selectExpr("ST_Perimeter2D(geom)").first().get(0)
      var expected = 122.63074400009504
      assertEquals(expected, actual)

      baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))', 4326) AS geom")
      actual = baseDf.selectExpr("ST_Perimeter2D(geom, true)").first().get(0)
      expected = 443770.91724830196
      assertEquals(expected, actual)

      actual = baseDf.selectExpr("ST_Perimeter2D(geom, true, false)").first().get(0)
      expected = 443770.91724830196
      assertEquals(expected, actual)
    }

    it("Passed ST_Points") {

      val testtable = sparkSession.sql(
        "SELECT ST_Points(ST_GeomFromText('MULTIPOLYGON (((0 0, 1 1, 1 0, 0 0)), ((2 2, 3 3, 3 2, 2 2)))'))")

      val result = testtable.take(1)(0).get(0).asInstanceOf[Geometry]

      result.normalize()
      assert(
        result.toText() == "MULTIPOINT ((0 0), (0 0), (1 0), (1 1), (2 2), (2 2), (3 2), (3 3))")
    }

    it("Passed ST_Polygon") {

      var testtable = sparkSession.sql(
        "SELECT ST_Polygon(ST_GeomFromText('LINESTRING(75.15 29.53,77 29,77.6 29.5, 75.15 29.53)'), 4326)")
      assert(
        testtable
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText == "POLYGON ((75.15 29.53, 77 29, 77.6 29.5, 75.15 29.53))")
      assert(testtable.take(1)(0).get(0).asInstanceOf[Geometry].getSRID == 4326)
    }

    it("Passed ST_Polygonize") {

      val testtable = sparkSession.sql(
        "SELECT ST_Polygonize(ST_GeomFromText('GEOMETRYCOLLECTION (LINESTRING (180 40, 30 20, 20 90), LINESTRING (180 40, 160 160), LINESTRING (80 60, 120 130, 150 80), LINESTRING (80 60, 150 80), LINESTRING (20 90, 70 70, 80 130), LINESTRING (80 130, 160 160), LINESTRING (20 90, 20 160, 70 190), LINESTRING (70 190, 80 130), LINESTRING (70 190, 160 160))'))")

      val result = testtable.take(1)(0).get(0).asInstanceOf[Geometry]

      result.normalize()
      assert(
        result.toText() == "GEOMETRYCOLLECTION (POLYGON ((20 90, 20 160, 70 190, 80 130, 70 70, 20 90)), POLYGON ((20 90, 70 70, 80 130, 160 160, 180 40, 30 20, 20 90), (80 60, 150 80, 120 130, 80 60)), POLYGON ((70 190, 160 160, 80 130, 70 190)), POLYGON ((80 60, 120 130, 150 80, 80 60)))")
    }

    it("Passed ST_Project") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT(0 0)') as point")
      var actual = baseDf.selectExpr("ST_Project(point, 10, radians(45))").first().get(0).toString
      var expected = "POINT (7.0710678118654755 7.071067811865475)"
      assertEquals(expected, actual)

      actual = sparkSession
        .sql("SELECT ST_Project(ST_MakeEnvelope(0, 1, 2, 0), 10, radians(50), true)")
        .first()
        .get(0)
        .toString
      expected = "POINT EMPTY"
      assertEquals(expected, actual)
    }

    it("Passed ST_MakeValid On Invalid Polygon") {

      val df = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS polygon")

      val result = df.withColumn("polygon", expr("ST_MakeValid(polygon)")).collect()

      assert(result.length == 1)
      assert(
        result
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText() == "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))")
    }

    it("Passed ST_MakeValid On Invalid MultiPolygon") {

      val df = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 0, 3 0, 3 3, 0 3, 0 0)), ((3 0, 6 0, 6 3, 3 3, 3 0)))') AS multipolygon")

      val result = df.withColumn("multipolygon", expr("ST_MakeValid(multipolygon)")).collect()

      assert(result.length == 1)
      assert(
        result
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText() == "MULTIPOLYGON (((0 3, 3 3, 6 3, 6 0, 3 0, 0 0, 0 3)))")
    }

    it("Passed ST_MakeValid On Valid Polygon") {

      val df =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS polygon")

      val result = df.withColumn("polygon", expr("ST_MakeValid(polygon)")).collect()

      assert(result.length == 1)
      assert(
        result
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText() == "POLYGON ((1 1, 1 8, 8 8, 8 1, 1 1))")
    }

    it("Passed ST_MakeValid on Invalid LineString") {

      val df = sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING(1 1, 1 1)') AS geom")

      val result = df.selectExpr("ST_MakeValid(geom)", "ST_MakeValid(geom, true)").collect()

      assert(result.length == 1)
      assert(result.take(1)(0).get(0).asInstanceOf[Geometry].toText() == "LINESTRING EMPTY")
      assert(result.take(1)(0).get(1).asInstanceOf[Geometry].toText() == "POINT (1 1)")
    }

    it("Passed ST_SimplifyPreserveTopology") {

      val testtable = sparkSession.sql(
        "SELECT ST_SimplifyPreserveTopology(ST_GeomFromText('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 10) AS b")
      assert(
        testtable
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals(
            "POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 35 36, 43 19, 24 39, 8 25))"))
    }

    it("Passed ST_AsText") {
      var polygonWktDf = sparkSession.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql(
        "select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var wktDf = sparkSession.sql("select ST_AsText(countyshape) as wkt from polygondf")
      assert(
        polygonDf
          .take(1)(0)
          .getAs[Geometry]("countyshape")
          .toText
          .equals(wktDf.take(1)(0).getAs[String]("wkt")))
      val wkt = sparkSession
        .sql("select ST_AsText(ST_SetSRID(ST_Point(1.0,1.0), 3021))")
        .first()
        .getString(0)
      assert(wkt == "POINT (1 1)", "WKT should not contain SRID")
    }

    it("Passed ST_AsText 3D") {
      val geometryDf = Seq(
        "Point Z(21 52 87)",
        "Polygon Z((0 0 1, 0 1 1, 1 1 1, 1 0 1, 0 0 1))",
        "Linestring Z(0 0 1, 1 1 2, 1 0 3)",
        "MULTIPOINT Z((10 40 66), (40 30 77), (20 20 88), (30 10 99))",
        "MULTIPOLYGON Z(((30 20 11, 45 40 11, 10 40 11, 30 20 11)), ((15 5 11, 40 10 11, 10 20 11, 5 10 11, 15 5 11)))",
        "MULTILINESTRING Z((10 10 11, 20 20 11, 10 40 11), (40 40 11, 30 30 11, 40 20 11, 30 10 11))",
        "MULTIPOLYGON Z(((40 40 11, 20 45 11, 45 30 11, 40 40 11)), ((20 35 11, 10 30 11, 10 10 11, 30 5 11, 45 20 11, 20 35 11), (30 20 11, 20 15 11, 20 25 11, 30 20 11)))",
        "POLYGON Z((0 0 11, 0 5 11, 5 5 11, 5 0 11, 0 0 11), (1 1 11, 2 1 11, 2 2 11, 1 2 11, 1 1 11))")
        .map(wkt => Tuple1(wktReader.read(wkt)))
        .toDF("geom")

      geometryDf.createOrReplaceTempView("geometrytable")
      var wktDf = sparkSession.sql("select ST_AsText(geom) as wkt from geometrytable")
      val wktWriter = new WKTWriter(3)
      val expected = geometryDf.collect().map(row => wktWriter.write(row.getAs[Geometry]("geom")))
      val actual = wktDf.collect().map(row => row.getAs[String]("wkt"))
      actual should contain theSameElementsAs expected
    }

    it("Passed ST_AsGeoJSON") {
      val df =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS polygon")
      df.createOrReplaceTempView("table")

      var geojsonDf = sparkSession.sql("""select ST_AsGeoJSON(polygon) as geojson
          |from table""".stripMargin)

      var expectedGeoJson =
        """{"type":"Polygon","coordinates":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]}"""
      assert(geojsonDf.first().getString(0) === expectedGeoJson)

      geojsonDf = sparkSession.sql("""select ST_AsGeoJSON(polygon, 'Feature') as geojson
                                         |from table""".stripMargin)

      expectedGeoJson =
        """{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]},"properties":{}}"""
      assert(geojsonDf.first().getString(0) === expectedGeoJson)

      geojsonDf = sparkSession.sql("""select ST_AsGeoJSON(polygon, 'FeatureCollection') as geojson
                                     |from table""".stripMargin)

      expectedGeoJson =
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]},"properties":{}}]}"""
      assert(geojsonDf.first().getString(0) === expectedGeoJson)
    }

    it("Passed ST_AsBinary") {
      val df = sparkSession.sql("SELECT ST_AsBinary(ST_GeomFromWKT('POINT (1 1)'))")
      val s = "0101000000000000000000f03f000000000000f03f"
      assert(Hex.encodeHexString(df.first().get(0).asInstanceOf[Array[Byte]]) == s)
    }

    it("Passed ST_AsBinary empty geometry") {
      val df = sparkSession.sql("SELECT ST_AsBinary(ST_GeomFromWKT('POINT EMPTY'))")
      val s = "0101000000000000000000f87f000000000000f87f"
      assert(Hex.encodeHexString(df.first().get(0).asInstanceOf[Array[Byte]]) == s)
    }

    it("Passed ST_AsBinary with srid") {
      // ST_AsBinary should return a WKB.
      // WKB does not contain any srid.
      val df = sparkSession.sql(
        "SELECT ST_AsBinary(ST_SetSRID(ST_Point(1.0,1.0), 3021)), ST_AsBinary(ST_Point(1.0,1.0))")
      val withSrid: Array[Byte] = df.first().getAs(0)
      val withoutSrid: Array[Byte] = df.first().getAs(1)
      assert(withSrid.seq == withoutSrid.seq)
    }

    it("Passed ST_AsGML") {
      val df =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS polygon")
      df.createOrReplaceTempView("table")

      val gmlDf = sparkSession.sql("""select ST_AsGML(polygon) as geojson
          |from table""".stripMargin)

      val gml = DocumentBuilderFactory.newInstance.newDocumentBuilder.parse(
        new InputSource(new StringReader(gmlDf.first().getString(0))))
      val coordinates = XPathFactory.newInstance.newXPath
        .evaluate("/Polygon/outerBoundaryIs/LinearRing/coordinates", gml)
      assert(coordinates.trim === "1.0,1.0 8.0,1.0 8.0,8.0 1.0,8.0 1.0,1.0")
    }

    it("Passed ST_AsKML") {
      val df =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS polygon")
      df.createOrReplaceTempView("table")

      val kmlDf = sparkSession.sql("""select ST_AsKML(polygon) as geojson
          |from table""".stripMargin)

      val kml = DocumentBuilderFactory.newInstance.newDocumentBuilder.parse(
        new InputSource(new StringReader(kmlDf.first().getString(0))))
      val coordinates = XPathFactory.newInstance.newXPath
        .evaluate("/Polygon/outerBoundaryIs/LinearRing/coordinates", kml)
      assert(coordinates.trim === "1.0,1.0 8.0,1.0 8.0,8.0 1.0,8.0 1.0,1.0")
    }

    it("Passed ST_SRID") {
      val df =
        sparkSession.sql("SELECT ST_SRID(ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'))")
      assert(df.first().getInt(0) == 0)
    }

    it("Passed ST_SetSRID") {
      var df =
        sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as polygon")
      df.createOrReplaceTempView("table")
      df = sparkSession.sql("SELECT ST_SetSRID(polygon, 3021) from table")
      assert(df.first().get(0).asInstanceOf[Polygon].getSRID == 3021)
    }

    it("Passed ST_AsEWKB") {
      var df = sparkSession.sql("SELECT ST_SetSrid(ST_GeomFromWKT('POINT (1 1)'), 3021) as point")
      df.createOrReplaceTempView("table")
      df = sparkSession.sql("SELECT ST_AsEWKB(point) from table")
      val s = "0101000020cd0b0000000000000000f03f000000000000f03f"
      assert(Hex.encodeHexString(df.first().get(0).asInstanceOf[Array[Byte]]) == s)
    }

    it("Passed ST_AsHEXEWKB") {
      val baseDf = sparkSession.sql("SELECT ST_GeomFromWKT('POINT(1 2)') as point")
      var actual = baseDf.selectExpr("ST_AsHEXEWKB(point)").first().get(0)
      var expected = "0101000000000000000000F03F0000000000000040"
      assert(expected.equals(actual))

      actual = baseDf.selectExpr("ST_AsHEXEWKB(point, 'XDR')").first().get(0)
      expected = "00000000013FF00000000000004000000000000000"
      assert(expected.equals(actual))
    }

    it("Passed ST_AsEWKB empty geometry") {
      val df =
        sparkSession.sql("SELECT ST_AsEWKB(ST_SetSrid(ST_GeomFromWKT('POINT EMPTY'), 3021))")
      val s = "0101000020cd0b0000000000000000f87f000000000000f87f"
      assert(Hex.encodeHexString(df.first().get(0).asInstanceOf[Array[Byte]]) == s)
    }

    it("Passed ST_Simplify") {
      val baseDf = sparkSession.sql("SELECT ST_Buffer(ST_GeomFromWKT('POINT (0 2)'), 10) AS geom")
      val actualPoints = baseDf.selectExpr("ST_NPoints(ST_Simplify(geom, 1))").first().get(0)
      val expectedPoints = 9
      assertEquals(expectedPoints, actualPoints)
    }

    it("Passed ST_SimplifyVW") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)') AS geom")
      val actual =
        baseDf.selectExpr("ST_SimplifyVW(geom, 30)").first().get(0).asInstanceOf[Geometry].toText
      val expected = "LINESTRING (5 2, 7 25, 10 10)"
      assertEquals(expected, actual)
    }

    it("Passed ST_SimplifyPolygonHull") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))') AS geom")
      var actual = baseDf
        .selectExpr("ST_SimplifyPolygonHull(geom, 0.3, false)")
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      var expected = "POLYGON ((30 10, 40 40, 10 20, 30 10))"
      assertEquals(expected, actual)

      actual = baseDf
        .selectExpr("ST_SimplifyPolygonHull(geom, 0.3)")
        .first()
        .get(0)
        .asInstanceOf[Geometry]
        .toText
      expected = "POLYGON ((30 10, 15 15, 10 20, 20 40, 45 45, 30 10))"
      assertEquals(expected, actual)
    }

    it("Passed ST_NPoints") {
      var test = sparkSession.sql(
        "SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 4)

    }

    it("Passed ST_NDims with 2D point") {
      val test = sparkSession.sql("SELECT ST_NDims(ST_GeomFromWKT('POINT(1 1)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 2)
    }

    it("Passed ST_NDims with 3D point") {
      val test = sparkSession.sql("SELECT ST_NDims(ST_GeomFromWKT('POINT(1 1 2)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 3)
    }

    it("Passed ST_NDims with Z coordinates") {
      val test = sparkSession.sql("SELECT ST_NDims(ST_GeomFromWKT('POINTZ(1 1 0.5)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 3)
    }

    it("Passed ST_NDims with XYM point") {
      val test = sparkSession.sql("SELECT ST_NDims(ST_GeomFromWKT('POINT M(1 2 3)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 3)
    }

    it("Passed ST_NDims with XYZM point") {
      val test = sparkSession.sql("SELECT ST_NDims(ST_GeomFromWKT('POINT ZM(1 2 3 4)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 4)
    }

    it("Passed ST_GeometryType") {
      var test = sparkSession.sql(
        "SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[String].toUpperCase() == "ST_LINESTRING")
    }

    it("Passed ST_Difference - part of right overlaps left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))') as b")
      testtable.createOrReplaceTempView("testtable")
      val diff = sparkSession.sql("select ST_Difference(a,b) from testtable")
      assert(
        diff
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))"))
    }

    it("Passed ST_Difference - right not overlaps left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
      testtable.createOrReplaceTempView("testtable")
      val diff = sparkSession.sql("select ST_Difference(a,b) from testtable")
      assert(
        diff
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3))"))
    }

    it("Passed ST_Difference - left contains right") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as b")
      testtable.createOrReplaceTempView("testtable")
      val diff = sparkSession.sql("select ST_Difference(a,b) from testtable")
      assert(
        diff
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3), (-1 -1, 1 -1, 1 1, -1 1, -1 -1))"))
    }

    it("Passed ST_Difference - right contains left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as a,ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as b")
      testtable.createOrReplaceTempView("testtable")
      val diff = sparkSession.sql("select ST_Difference(a,b) from testtable")
      assert(diff.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON EMPTY"))
    }

    it("Passed ST_Difference - one null") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a")
      testtable.createOrReplaceTempView("testtable")
      val diff = sparkSession.sql("select ST_Difference(a,null) from testtable")
      assert(diff.first().get(0) == null)
    }

    it("Passed ST_SymDifference - part of right overlaps left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as a,ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') as b")
      testtable.createOrReplaceTempView("sym_table")
      val symDiff = sparkSession.sql("select ST_SymDifference(a,b) from sym_table")
      assert(
        symDiff
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))"))
    }

    it("Passed ST_SymDifference - right not overlaps left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
      testtable.createOrReplaceTempView("sym_table")
      val symDiff = sparkSession.sql("select ST_SymDifference(a,b) from sym_table")
      assert(
        symDiff
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals(
            "MULTIPOLYGON (((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)))"))
    }

    it("Passed ST_SymDifference - contains") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') as b")
      testtable.createOrReplaceTempView("sym_table")
      val symDiff = sparkSession.sql("select ST_SymDifference(a,b) from sym_table")
      assert(
        symDiff
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((-3 -3, -3 3, 3 3, 3 -3, -3 -3), (-1 -1, 1 -1, 1 1, -1 1, -1 -1))"))
    }

    it("Passed ST_SymDifference - one null") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a")
      testtable.createOrReplaceTempView("sym_table")
      val symDiff = sparkSession.sql("select ST_SymDifference(a,null) from sym_table")
      assert(symDiff.first().get(0) == null)
    }

    it("Passed ST_UnaryUnion") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))') AS geom")
      val actual =
        baseDf.selectExpr("ST_UnaryUnion(geom)").first().get(0).asInstanceOf[Geometry].toText
      val expected = "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))"
      assertEquals(expected, actual)
    }

    it("Passed ST_Union - part of right overlaps left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') as b")
      testtable.createOrReplaceTempView("union_table")
      val union = sparkSession.sql("select ST_Union(a,b) from union_table")
      assert(
        union
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals("POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"))
    }

    it("Passed ST_Union - right not overlaps left") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a,ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))') as b")
      testtable.createOrReplaceTempView("union_table")
      val union = sparkSession.sql("select ST_Union(a,b) from union_table")
      assert(
        union
          .take(1)(0)
          .get(0)
          .asInstanceOf[Geometry]
          .toText
          .equals(
            "MULTIPOLYGON (((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)))"))
    }

    it("Passed ST_Union - one null") {

      val testtable = sparkSession.sql(
        "select ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') as a")
      testtable.createOrReplaceTempView("union_table")
      val union = sparkSession.sql("select ST_Union(a,null) from union_table")
      assert(union.first().get(0) == null)
    }

    it("Passed ST_Union - array variant") {
      val polyDf = sparkSession.sql(
        "select array(ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'),ST_GeomFromWKT('POLYGON ((5 -3, 7 -3, 7 -1, 5 -1, 5 -3))'), ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))')) as polys")
      val actual =
        polyDf.selectExpr("ST_Union(polys)").take(1)(0).get(0).asInstanceOf[Geometry].toText
      val expected =
        "MULTIPOLYGON (((5 -3, 5 -1, 7 -1, 7 -3, 5 -3)), ((-3 -3, -3 3, 3 3, 3 -3, -3 -3)), ((4 4, 4 6, 6 6, 6 4, 4 4)))"
      assert(expected.equals(actual))
    }

    it("Passed ST_Azimuth") {

      val pointDataFrame = samplePoints
        .map(point => (point, samplePoints.tail.head))
        .toDF("geomA", "geomB")

      pointDataFrame
        .selectExpr("ST_Azimuth(geomA, geomB)")
        .as[Double]
        .map(180 / math.Pi * _)
        .collect() should contain theSameElementsAs List(240.0133139011053, 0.0, 270.0,
        286.8042682202057, 315.0, 314.9543472191815, 315.0058223408927, 245.14762725688198,
        314.84984546897755, 314.8868529256147, 314.9510567053395, 314.95443984912936,
        314.89925480835245, 314.6018799143881, 314.6834083423315, 314.80689827870725,
        314.90290827689506, 314.90336326341765, 314.7510398533675, 314.73608518601935)

      val geometries = Seq(
        ("POINT(25.0 45.0)", "POINT(75.0 100.0)"),
        ("POINT(75.0 100.0)", "POINT(25.0 45.0)"),
        ("POINT(0.0 0.0)", "POINT(25.0 0.0)"),
        ("POINT(25.0 0.0)", "POINT(0.0 0.0)"),
        ("POINT(0.0 25.0)", "POINT(0.0 0.0)"),
        ("POINT(0.0 0.0)", "POINT(0.0 25.0)"))
        .map({ case (wktA, wktB) => (wktReader.read(wktA), wktReader.read(wktB)) })
        .toDF("geomA", "geomB")

      geometries
        .selectExpr("ST_Azimuth(geomA, geomB)")
        .as[Double]
        .map(180 / math.Pi * _)
        .collect()
        .toList should contain theSameElementsAs List(42.27368900609374, 222.27368900609375,
        270.00, 90.0, 180.0, 0.0)
    }

    it("Should pass ST_X") {

      Given("Given polygon, point and linestring dataframe")
      val pointDF = createSamplePointDf(5, "geom")
      val polygonDF = createSamplePolygonDf(5, "geom")
      val lineStringDF = createSampleLineStringsDf(5, "geom")

      When("Running ST_X function on polygon, point and linestring data frames")

      val points = pointDF
        .selectExpr("ST_X(geom)")
        .as[Double]
        .collect()
        .toList

      val polygons = polygonDF
        .selectExpr("ST_X(geom) as x")
        .filter("x IS NOT NULL")
        .as[Double]
        .collect()
        .toList

      val linestrings = lineStringDF
        .selectExpr("ST_X(geom) as x")
        .filter("x IS NOT NULL")
        .as[Double]
        .collect()
        .toList

      Then("Point x coordinates Should match to expected point coordinates")

      points should contain theSameElementsAs List(-71.064544, -88.331492, 88.331492, 1.0453,
        32.324142)

      And("LineString count should be 0")
      linestrings.length shouldBe 0

      And("Polygon count should be 0")
      polygons.length shouldBe 0
    }

    it("Should pass ST_Y") {

      Given("Given polygon, point and linestring dataframe")
      val pointDF = createSamplePointDf(5, "geom")
      val polygonDF = createSamplePolygonDf(5, "geom")
      val lineStringDF = createSampleLineStringsDf(5, "geom")

      When("Running ST_Y function on polygon, point and linestring data frames")

      val points = pointDF
        .selectExpr("ST_Y(geom)")
        .as[Double]
        .collect()
        .toList

      val polygons = polygonDF
        .selectExpr("ST_Y(geom) as y")
        .filter("y IS NOT NULL")
        .as[Double]
        .collect()
        .toList

      val linestrings = lineStringDF
        .selectExpr("ST_Y(geom) as y")
        .filter("y IS NOT NULL")
        .as[Double]
        .collect()
        .toList

      Then("Point y coordinates Should match to expected point coordinates")

      points should contain theSameElementsAs List(42.28787, 32.324142, 32.324142, 5.3324324,
        -88.331492)

      And("LineString count should be 0")
      linestrings.length shouldBe 0

      And("Polygon count should be 0")
      polygons.length shouldBe 0

    }

    it("Should pass ST_Z") {

      Given("Given polygon, point and linestring dataframe")
      val pointDF = Seq("POINT Z (1 2 3)").map(geom => Tuple1(wktReader.read(geom))).toDF("geom")
      val polygonDF = Seq("POLYGON Z ((0 0 2, 0 1 2, 1 1 2, 1 0 2, 0 0 2))")
        .map(geom => Tuple1(wktReader.read(geom)))
        .toDF("geom")
      val lineStringDF =
        Seq("LINESTRING Z (0 0 1, 0 1 2)").map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

      When("Running ST_Z function on polygon, point and linestring data frames")

      val points = pointDF
        .selectExpr("ST_Z(geom) as z")
        .as[Double]
        .collect()
        .toList

      val polygons = polygonDF
        .selectExpr("ST_Z(geom) as z")
        .filter("z IS NOT NULL")
        .as[Double]
        .collect()
        .toList

      val linestrings = lineStringDF
        .selectExpr("ST_Z(geom) as z")
        .filter("z IS NOT NULL")
        .as[Double]
        .collect()
        .toList

      Then("Point z coordinates Should match to expected point coordinates")

      points should contain theSameElementsAs List(3)

      And("LineString count should be 0")
      linestrings.length shouldBe 0

      And("Polygon count should be 0")
      polygons.length shouldBe 0

    }

    it("Passed ST_Zmflag") {
      var actual =
        sparkSession.sql("SELECT ST_Zmflag(ST_GeomFromWKT('POINT (1 2)'))").first().get(0)
      assert(actual == 0)

      actual = sparkSession
        .sql("SELECT ST_Zmflag(ST_GeomFromWKT('LINESTRING (1 2 3, 4 5 6)'))")
        .first()
        .get(0)
      assert(actual == 2)

      actual = sparkSession
        .sql("SELECT ST_Zmflag(ST_GeomFromWKT('POLYGON M((1 2 3, 3 4 3, 5 6 3, 3 4 3, 1 2 3))'))")
        .first()
        .get(0)
      assert(actual == 1)

      actual = sparkSession
        .sql("SELECT ST_Zmflag(ST_GeomFromWKT('MULTIPOLYGON ZM (((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1)), ((15 5 3 1, 20 10 6 2, 10 10 7 3, 15 5 3 1)))'))")
        .first()
        .get(0)
      assert(actual == 3)
    }

    it("Should pass ST_StartPoint function") {
      Given("Polygon Data Frame, Point DataFrame, LineString Data Frame")

      val pointDF = createSamplePointDf(5, "geom")
      val polygonDF = createSamplePolygonDf(5, "geom")
      val lineStringDF = createSampleLineStringsDf(5, "geom")

      When(
        "Running ST_StartPoint on Point Data Frame, LineString DataFrame and Polygon DataFrame")
      val points = pointDF
        .selectExpr("ST_StartPoint(geom) as geom")
        .filter("geom IS NOT NULL")
        .toSeq[Geometry]

      val polygons = polygonDF
        .selectExpr("ST_StartPoint(geom) as geom")
        .filter("geom IS NOT NULL")
        .toSeq[Geometry]

      val linestrings = lineStringDF
        .selectExpr("ST_StartPoint(geom) as geom")
        .filter("geom IS NOT NULL")
        .toSeq[Geometry]

      Then("Linestring should result in list of points")
      linestrings should contain theSameElementsAs expectedStartingPoints.map(_.toGeom)

      And("Point DataFrame should result with empty list")
      points.isEmpty shouldBe true

      And("Polygon DataFrame should result with empty list ")
      polygons.isEmpty shouldBe true
    }

    it("Should pass ST_ForcePolygonCCW") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))') as poly")
      val actual = baseDf.selectExpr("ST_AsText(ST_ForcePolygonCCW(poly))").first().getString(0)
      val expected =
        "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
      assert(expected.equals(actual))
    }

    it("Should pass ST_IsPolygonCCW") {
      var actual = sparkSession
        .sql("SELECT ST_IsPolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))'))")
        .first()
        .getBoolean(0)
      assert(actual == true)

      actual = sparkSession
        .sql("SELECT ST_IsPolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))'))")
        .first()
        .getBoolean(0)
      assert(actual == false)
    }

    it("Should pass ST_Snap") {
      val baseDf = sparkSession.sql(
        "SELECT ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))') AS poly, ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)') AS line")
      var actual = baseDf.selectExpr("ST_AsText(ST_Snap(poly, line, 2.525))").first().getString(0)
      var expected = "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))"
      assert(expected.equals(actual))

      actual = baseDf.selectExpr("ST_AsText(ST_Snap(poly, line, 3.125))").first().getString(0)
      expected = "POLYGON ((0.5 10.7, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 5.4 8.4, 0.5 10.7))"
      assert(expected.equals(actual))
    }
  }

  it("Should pass ST_ForcePolygonCW") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') as poly")
    val actual = baseDf.selectExpr("ST_AsText(ST_ForcePolygonCW(poly))").first().getString(0)
    val expected =
      "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))"
    assert(expected.equals(actual))
  }

  it("Should pass ST_ForceRHR") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))') as poly")
    val actual = baseDf.selectExpr("ST_AsText(ST_ForceRHR(poly))").first().getString(0)
    val expected =
      "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))"
    assert(expected.equals(actual))
  }

  it("Should pass ST_IsPolygonCW") {
    var actual = sparkSession
      .sql("SELECT ST_IsPolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))'))")
      .first()
      .getBoolean(0)
    assert(actual == false)

    actual = sparkSession
      .sql("SELECT ST_IsPolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))'))")
      .first()
      .getBoolean(0)
    assert(actual == true)
  }

  it("Should pass ST_Boundary") {
    Given("Sample geometry data frame")
    val geometryTable = Seq(
      "LINESTRING(1 1,0 0, -1 1)",
      "LINESTRING(100 150,50 60, 70 80, 160 170)",
      "POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))",
      "POLYGON((1 1,0 0, -1 1, 1 1))").map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

    When("Using ST_Boundary function")

    val boundaryTable = geometryTable.selectExpr("ST_Boundary(geom) as geom")

    Then("Result should match List of boundary geometries")

    boundaryTable
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs List(
      "MULTIPOINT ((1 1), (-1 1))",
      "MULTIPOINT ((100 150), (160 170))",
      "MULTILINESTRING ((10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130), (70 40, 100 50, 120 80, 80 110, 50 90, 70 40))",
      "LINESTRING (1 1, 0 0, -1 1, 1 1)")
  }

  it("Should pass ST_EndPoint") {
    Given("Dataframe with linestring and dataframe with other geometry types")
    val lineStringDataFrame = createSampleLineStringsDf(5, "geom")
    val otherGeometryDataFrame = createSamplePolygonDf(5, "geom")
      .union(createSamplePointDf(5, "geom"))

    When("Using ST_EndPoint")
    val pointDataFrame = lineStringDataFrame
      .selectExpr("ST_EndPoint(geom) as geom")
      .filter("geom IS NOT NULL")
    val emptyDataFrame = otherGeometryDataFrame
      .selectExpr("ST_EndPoint(geom) as geom")
      .filter("geom IS NOT NULL")

    Then("Linestring Df should result with Point Df and other geometry DF as empty DF")
    pointDataFrame
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs expectedEndingPoints
    emptyDataFrame.count shouldBe 0
  }

  it("Should pass ST_ExteriorRing") {
    Given("Polygon DataFrame and other geometries DataFrame")
    val polygonDf = createSimplePolygons(5, "geom")
      .union(
        Seq("POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))")
          .map(wkt => Tuple1(wktReader.read(wkt)))
          .toDF("geom"))

    val otherGeometryDf = createSampleLineStringsDf(5, "geom")
      .union(createSamplePointDf(5, "geom"))

    When("Using ST_ExteriorRing on Polygon Frame and Other geometry frame")
    val lineStringDf = polygonDf
      .selectExpr("ST_ExteriorRing(geom) as geom")
      .filter("geom IS NOT NULL")

    val emptyDf = otherGeometryDf
      .selectExpr("ST_ExteriorRing(geom) as geom")
      .filter("geom IS NOT NULL")

    Then("Polygon Dataframe should product LineString Data Frame and others should be null")

    lineStringDf
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect() should contain theSameElementsAs List(
      "LINESTRING (0 0, 0 1, 1 1, 1 0, 0 0)",
      "LINESTRING (0 0, 1 1, 1 2, 1 1, 0 0)")

    emptyDf.count shouldBe 0
  }

  it("Should pass ST_GeometryN") {
    calculateGeometryN("MULTIPOINT((1 2), (3 4), (5 6), (8 9))", 0) shouldBe Some("POINT (1 2)")
    calculateGeometryN("MULTIPOINT((1 2), (3 4), (5 6), (8 9))", 1) shouldBe Some("POINT (3 4)")
    calculateGeometryN("MULTIPOINT((1 2), (3 4), (5 6), (8 9))", 2) shouldBe Some("POINT (5 6)")
    calculateGeometryN("MULTIPOINT((1 2), (3 4), (5 6), (8 9))", 3) shouldBe Some("POINT (8 9)")
    calculateGeometryN("MULTIPOINT((1 2), (3 4), (5 6), (8 9))", 4) shouldBe None
    calculateGeometryN("MULTIPOINT((1 2), (3 4), (5 6), (8 9))", 4) shouldBe None

  }

  it("Should pass ST_InteriorRingN") {
    Given("DataFrame with polygons and DataFrame with other geometries ")
    val polygonDf = Seq(
      "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))")
      .map(wkt => Tuple1(wktReader.read(wkt)))
      .toDF("geom")

    val otherGeometry = createSamplePointDf(5, "geom")
      .union(createSampleLineStringsDf(5, "geom"))

    When("Using ST_InteriorRingN")
    val wholes = (0 until 3).flatMap(index =>
      polygonDf
        .selectExpr(s"ST_InteriorRingN(geom, $index) as geom")
        .selectExpr("ST_AsText(geom)")
        .as[String]
        .collect())

    val emptyDf = otherGeometry
      .selectExpr("ST_InteriorRingN(geom, 1) as geom")
      .filter("geom IS NOT NULL")

    Then(
      "Polygon with wholes should return Nth whole and other geometries should produce null values")

    emptyDf.count shouldBe 0
    wholes should contain theSameElementsAs List(
      "LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)",
      "LINESTRING (1 3, 2 3, 2 4, 1 4, 1 3)",
      "LINESTRING (3 3, 4 3, 4 4, 3 4, 3 3)")

  }

  it("Should pass ST_Dumps") {
    Given("Geometries Df")
    val geometryDf = Seq(
      "Point(21 52)",
      "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))",
      "Linestring(0 0, 1 1, 1 0)",
      "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
      "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
      "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
      "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))",
      "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))")
      .map(wkt => Tuple1(wktReader.read(wkt)))
      .toDF("geom")

    When("Using ST_Dumps")
    val dumpedGeometries = geometryDf.selectExpr("ST_Dump(geom) as geom")
    Then("Should return geometries list")

    dumpedGeometries.select(explode($"geom")).count shouldBe 14
    dumpedGeometries
      .select(explode($"geom").alias("geom"))
      .selectExpr("ST_AsText(geom) as geom")
      .as[String]
      .collect() should contain theSameElementsAs List(
      "POINT (21 52)",
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
      "LINESTRING (0 0, 1 1, 1 0)",
      "POINT (10 40)",
      "POINT (40 30)",
      "POINT (20 20)",
      "POINT (30 10)",
      "POLYGON ((30 20, 45 40, 10 40, 30 20))",
      "POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))",
      "LINESTRING (10 10, 20 20, 10 40)",
      "LINESTRING (40 40, 30 30, 40 20, 30 10)",
      "POLYGON ((40 40, 20 45, 45 30, 40 40))",
      "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))",
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))")

  }

  it("Should pass ST_DumpPoints") {
    Given("DataFrame with geometries")
    val geometryDf = createSampleLineStringsDf(1, "geom")
      .union(createSamplePointDf(1, "geom"))
      .union(createSimplePolygons(1, "geom"))

    When("Using ST_DumpPoints and explode")

    val dumpedPoints = geometryDf
      .selectExpr("ST_DumpPoints(geom) as geom")
      .select(explode($"geom").alias("geom"))

    Then("Number of rows should match and collected to list should match expected point list")
    dumpedPoints.count shouldBe 10
    dumpedPoints
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs List(
      "POINT (-112.506968 45.98186)",
      "POINT (-112.506968 45.983586)",
      "POINT (-112.504872 45.983586)",
      "POINT (-112.504872 45.98186)",
      "POINT (-71.064544 42.28787)",
      "POINT (0 0)",
      "POINT (0 1)",
      "POINT (1 1)",
      "POINT (1 0)",
      "POINT (0 0)")
  }

  it("Should pass ST_IsClosed") {
    Given("Dataframe with geometries")
    val geometryDf = Seq(
      (1, "Point(21 52)"),
      (2, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      (3, "Linestring(0 0, 1 1, 1 0)"),
      (4, "Linestring(0 0, 1 1, 1 0, 0 0)"),
      (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
      (7, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))"),
      (8, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))"),
      (9, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
      (
        10,
        "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"))
      .map({ case (index, wkt) => Tuple2(index, wktReader.read(wkt)) })
      .toDF("id", "geom")

    When("Using ST_IsClosed")

    val isClosed = geometryDf.selectExpr("id", "ST_IsClosed(geom)").as[(Int, Boolean)]

    Then("Result should match to list")

    isClosed.collect().toList should contain theSameElementsAs List(
      (1, true),
      (2, true),
      (3, false),
      (4, true),
      (5, true),
      (6, true),
      (7, true),
      (8, false),
      (9, false),
      (10, false))
  }

  it("Should pass ST_NumInteriorRings") {
    Given("Geometry DataFrame")
    val geometryDf = Seq(
      (1, "Point(21 52)"),
      (2, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      (3, "Linestring(0 0, 1 1, 1 0)"),
      (4, "Linestring(0 0, 1 1, 1 0, 0 0)"),
      (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
      (7, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))"),
      (8, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))"),
      (9, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
      (
        10,
        "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
      (11, "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"))
      .map({ case (index, wkt) => Tuple2(index, wktReader.read(wkt)) })
      .toDF("id", "geom")

    When("Using ST_NumInteriorRings")
    val numberOfInteriorRings = geometryDf.selectExpr("id", "ST_NumInteriorRings(geom) as num")

    Then("Result should match with expected values")

    numberOfInteriorRings
      .filter("num is not null")
      .as[(Int, Int)]
      .collect()
      .toList should contain theSameElementsAs List((2, 0), (11, 1))
  }

  it("Should pass ST_NumInteriorRing") {
    Given("Geometry DataFrame")
    val geometryDf = Seq(
      (1, "Point(21 52)"),
      (2, "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      (3, "Linestring(0 0, 1 1, 1 0)"),
      (4, "Linestring(0 0, 1 1, 1 0, 0 0)"),
      (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
      (7, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))"),
      (8, "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))"),
      (9, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
      (
        10,
        "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
      (11, "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"))
      .map({ case (index, wkt) => Tuple2(index, wktReader.read(wkt)) })
      .toDF("id", "geom")

    When("Using ST_NumInteriorRing")
    val numberOfInteriorRings = geometryDf.selectExpr("id", "ST_NumInteriorRing(geom) as num")

    Then("Result should match with expected values")

    numberOfInteriorRings
      .filter("num is not null")
      .as[(Int, Int)]
      .collect()
      .toList should contain theSameElementsAs List((2, 0), (11, 1))
  }

  it("Should pass ST_AddMeasure") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('LINESTRING (1 1, 2 2, 2 2, 3 3)') as line, ST_GeomFromWKT('MULTILINESTRING M((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))') as mline")
    var actual = baseDf.selectExpr("ST_AsText(ST_AddMeasure(line, 1, 70))").first().get(0)
    var expected = "LINESTRING M(1 1 1, 2 2 35.5, 2 2 35.5, 3 3 70)"
    assertEquals(expected, actual)

    actual = baseDf.selectExpr("ST_AsText(ST_AddMeasure(mline, 10, 70))").first().get(0)
    expected = "MULTILINESTRING M((1 0 10, 2 0 20, 4 0 40), (1 0 40, 2 0 50, 4 0 70))"
    assertEquals(expected, actual)
  }

  it("Should pass ST_AddPoint") {
    Given("Geometry df")
    val geometryDf = Seq(
      ("Point(21 52)", "Point(21 52)"),
      ("Point(21 52)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      ("Linestring(0 0, 1 1, 1 0)", "Point(21 52)"),
      ("Linestring(0 0, 1 1, 1 0, 0 0)", "Linestring(0 0, 1 1, 1 0, 0 0)"),
      ("Point(21 52)", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      (
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
        "Point(21 52)"),
      (
        "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))",
        "Point(21 52)"),
      (
        "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))",
        "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
      (
        "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))",
        "Point(21 52)"),
      ("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", "Point(21 52)"))
      .map({ case (geomA, geomB) => Tuple2(wktReader.read(geomA), wktReader.read(geomB)) })
      .toDF("geomA", "geomB")
    When("Using ST_AddPoint")
    val modifiedGeometries = geometryDf.selectExpr("ST_AddPoint(geomA, geomB) as geom")

    Then("Result should match")

    modifiedGeometries
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect() should contain theSameElementsAs List("LINESTRING (0 0, 1 1, 1 0, 21 52)")
  }

  it("Should pass ST_AddPoint with index") {
    Given("Geometry df")
    val geometryDf = Seq(
      ("Point(21 52)", "Point(21 52)"),
      ("Point(21 52)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      ("Linestring(0 0, 1 1, 1 0)", "Point(21 52)"),
      ("Linestring(0 0, 1 1, 1 0, 0 0)", "Linestring(0 0, 1 1, 1 0, 0 0)"),
      ("Point(21 52)", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      (
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
        "Point(21 52)"),
      (
        "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))",
        "Point(21 52)"),
      (
        "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))",
        "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
      (
        "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))",
        "Point(21 52)"),
      ("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", "Point(21 52)"))
      .map({ case (geomA, geomB) => Tuple2(wktReader.read(geomA), wktReader.read(geomB)) })
      .toDF("geomA", "geomB")
    When("Using ST_AddPoint")

    val modifiedGeometries = geometryDf
      .selectExpr("ST_AddPoint(geomA, geomB, 1) as geom")
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 0) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 2) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 3) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 4) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, -1) as geom"))

    Then("Result should match")

    modifiedGeometries
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect() should contain theSameElementsAs List(
      "LINESTRING (0 0, 21 52, 1 1, 1 0)",
      "LINESTRING (21 52, 0 0, 1 1, 1 0)",
      "LINESTRING (0 0, 1 1, 21 52, 1 0)",
      "LINESTRING (0 0, 1 1, 1 0, 21 52)",
      "LINESTRING (0 0, 1 1, 1 0, 21 52)")
  }

  it("Should correctly remove using ST_RemovePoint") {
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)") shouldBe Some(
      "LINESTRING (0 0, 1 1, 1 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 0) shouldBe Some(
      "LINESTRING (1 1, 1 0, 0 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1)", 0) shouldBe None
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 1) shouldBe Some(
      "LINESTRING (0 0, 1 0, 0 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 2) shouldBe Some(
      "LINESTRING (0 0, 1 1, 0 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 3) shouldBe Some(
      "LINESTRING (0 0, 1 1, 1 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 4) shouldBe None
    calculateStRemovePointOption("POINT(0 1)", 3) shouldBe None
    calculateStRemovePointOption(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))",
      3) shouldBe None
    calculateStRemovePointOption(
      "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40))",
      0) shouldBe None
    calculateStRemovePointOption(
      "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
      3) shouldBe None
    calculateStRemovePointOption(
      "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))",
      3) shouldBe None
  }

  it("Should pass ST_RemoveRepeatedPoints") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION (POINT (10 10),LINESTRING (20 20, 20 20, 30 30, 30 30),POLYGON ((40 40, 50 50, 50 50, 60 60, 60 60, 70 70, 70 70, 40 40)), MULTIPOINT ((80 80), (90 90), (90 90), (100 100)))', 1000) AS geom")
    val actualDf = baseDf.selectExpr("ST_RemoveRepeatedPoints(geom, 1000) as geom")
    var actual = actualDf.selectExpr("ST_AsText(geom)").first().get(0)
    var expected =
      "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (20 20, 30 30), POLYGON ((40 40, 70 70, 70 70, 40 40)), MULTIPOINT ((80 80)))"
    assertEquals(expected, actual)
    val actualSRID = actualDf.selectExpr("ST_SRID(geom)").first().get(0)
    assertEquals(1000, actualSRID)

    actual = sparkSession
      .sql("SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromWKT('MULTIPOINT ((1 1), (2 2), (3 3), (2 2))')))")
      .first()
      .get(0)
    expected = "MULTIPOINT ((1 1), (2 2), (3 3))"
    assertEquals(expected, actual)

    actual = sparkSession
      .sql("SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromWKT('LINESTRING (0 0, 0 0, 1 1, 0 0, 1 1, 2 2)')))")
      .first()
      .get(0)
    expected = "LINESTRING (0 0, 1 1, 0 0, 1 1, 2 2)"
    assertEquals(expected, actual)

    actual = sparkSession
      .sql("SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromWKT('GEOMETRYCOLLECTION (LINESTRING (1 1, 2 2, 2 2, 3 3), POINT (4 4), POINT (4 4), POINT (5 5))')))")
      .first()
      .get(0)
    expected =
      "GEOMETRYCOLLECTION (LINESTRING (1 1, 2 2, 3 3), POINT (4 4), POINT (4 4), POINT (5 5))"
    assertEquals(expected, actual)

    actual = sparkSession
      .sql("SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromWKT('LINESTRING (0 0, 0 0, 1 1, 5 5, 1 1, 2 2)'), 2))")
      .first()
      .get(0)
    expected = "LINESTRING (0 0, 5 5, 2 2)"
    assertEquals(expected, actual)
  }

  it("Should correctly set using ST_SetPoint") {
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 0, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 1, 1 1, 1 0, 0 0)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 1, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 0, 0 1, 1 0, 0 0)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 2, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 0, 1 1, 0 1, 0 0)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 3, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 0, 1 1, 1 0, 0 1)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 4, "Point(0 1)") shouldBe None
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", -1, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 0, 1 1, 1 0, 0 1)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", -2, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 0, 1 1, 0 1, 0 0)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", -3, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 0, 0 1, 1 0, 0 0)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", -4, "Point(0 1)") shouldBe Some(
      "LINESTRING (0 1, 1 1, 1 0, 0 0)")
    calculateStSetPointOption("Linestring(0 0, 1 1, 1 0, 0 0)", -5, "Point(0 1)") shouldBe None
    calculateStSetPointOption("POINT(0 1)", 0, "Point(0 1)") shouldBe None
    calculateStSetPointOption(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))",
      0,
      "Point(0 1)") shouldBe None
    calculateStSetPointOption(
      "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40))",
      0,
      "Point(0 1)") shouldBe None
    calculateStSetPointOption(
      "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
      0,
      "Point(0 1)") shouldBe None
    calculateStSetPointOption(
      "MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))",
      0,
      "Point(0 1)") shouldBe None
  }

  it("Should pass ST_IsRing") {
    calculateStIsRing("LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)") shouldBe Some(false)
    calculateStIsRing("LINESTRING(2 0, 2 2, 3 3)") shouldBe Some(false)
    calculateStIsRing("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)") shouldBe Some(true)
    calculateStIsRing("POINT (21 52)") shouldBe None
    calculateStIsRing(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))") shouldBe None
  }

  it("should handle subdivision on huge amount of data") {
    Given("dataframe with 100 huge polygons")
    val expectedCount = 55880
    val polygonWktDf = sparkSession.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(mixedWktGeometryInputLocation)
      .selectExpr("ST_GeomFromText(_c0) AS geom", "_c3 as id")

    When("running st subdivide")
    val subDivisionExplode =
      polygonWktDf.selectExpr("id", "ST_SubDivideExplode(geom, 10) as divided")
    val subDivision = polygonWktDf.selectExpr("id", "ST_SubDivide(geom, 10) as divided")

    Then("result should be appropriate")
    subDivision.count shouldBe polygonWktDf.count
    subDivision.select(explode(col("divided"))).count shouldBe expectedCount
    subDivisionExplode.count shouldBe expectedCount
  }

  it("should return empty data when the input geometry is null") {
    Given("dataframe with null geometries")
    val geodataframe = Seq((1, null)).toDF("id", "geom")

    When("calculating sub divide")
    val subDivisionExplode = geodataframe.selectExpr("id", "ST_SubDivideExplode(geom, 10)")
    val subDivision = geodataframe.selectExpr("id", "ST_SubDivide(geom, 10)")

    Then("dataframe exploded should be empty")
    subDivisionExplode.count shouldBe 0

    And(s"dataframe based on subdivide should be as ${geodataframe.count}")
    subDivision.count shouldBe 1
  }

  it("it should return appropriate default column names for st_subdivide") {
    Given("Sample geometry dataframe with different geometry types")
    val polygonWktDf = sparkSession.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(mixedWktGeometryInputLocation)
      .selectExpr("ST_GeomFromText(_c0) AS geom", "_c3 as id")

    When("using ST_SubDivide function")
    val subDivisionExplode = polygonWktDf.selectExpr("id", "ST_SubDivideExplode(geom, 10)")
    val subDivision = polygonWktDf.selectExpr("id", "ST_SubDivide(geom, 10)")

    Then("column names should be as expected")
    subDivisionExplode.columns shouldBe Seq("id", "geom")
    subDivision.columns shouldBe Seq("id", "st_subdivide(geom, 10)")

  }

  it("should return null values from st subdivide when input row has null value") {
    Given("Sample dataframe with null values and geometries")
    val nonNullGeometries = Seq(
      (1, "LINESTRING (0 0, 1 1, 2 2)"),
      (2, "POINT (0 0)"),
      (3, "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")).map { case (id, geomWkt) =>
      (id, wktReader.read(geomWkt))
    }
    val nullGeometries: Tuple2[Int, Geometry] = (4, null)

    val geometryTable = (nullGeometries +: nonNullGeometries).toDF("id", "geom")

    When("running ST_SubDivide on dataframe")
    val subDivision = geometryTable.selectExpr("id", "ST_SubDivide(geom, 10) as divided")

    Then("for null values return value should be null also")
    subDivision
      .filter("divided is null")
      .select("id")
      .as[Long]
      .collect()
      .headOption shouldBe Some(nullGeometries._1)

    And("for geometry type value should be appropriate")
    subDivision
      .filter("divided is not null")
      .select("id")
      .as[Long]
      .collect() should contain theSameElementsAs nonNullGeometries.map(_._1)
  }

  it("should allow to use lateral view with st sub divide explode") {
    Given("geometry dataframe")
    val geometryDf = Seq(
      (1, "LINESTRING (0 0, 1 1, 2 2)"),
      (2, "POINT (0 0)"),
      (3, "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
      (4, "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"))
      .map { case (id, geomWkt) =>
        (id, wktReader.read(geomWkt))
      }
      .toDF("id", "geometry")
    geometryDf.createOrReplaceTempView("geometries")

    When("using lateral view on data")
    val lateralViewResult = sparkSession.sql("""
        |select id, geom from geometries
        |LATERAL VIEW ST_SubdivideExplode(geometry, 5) AS geom
        |""".stripMargin)

    Then("result should include exploded geometries")
    lateralViewResult.count() shouldBe 17

  }

  private val expectedStartingPoints = List(
    "POINT (-112.506968 45.98186)",
    "POINT (-112.519856 45.983586)",
    "POINT (-112.504872 45.919281)",
    "POINT (-112.574945 45.987772)",
    "POINT (-112.520691 42.912313)")
  private val expectedEndingPoints = List(
    "POINT (-112.504872 45.98186)",
    "POINT (-112.506968 45.983586)",
    "POINT (-112.41643 45.919281)",
    "POINT (-112.519856 45.987772)",
    "POINT (-112.442664 42.912313)")

  private def calculateGeometryN(wkt: String, index: Int): Option[Row] = {
    Seq(wkt)
      .map(wkt => Tuple1(wktReader.read(wkt)))
      .toDF("geom")
      .selectExpr(s"ST_GeometryN(geom, $index) as geom")
      .filter("geom is not null")
      .selectExpr(s"ST_AsText(geom) as geom_text")
      .toSeqOption
  }

  private def calculateStIsRing(wkt: String): Option[Boolean] =
    wktToDf(wkt)
      .selectExpr("ST_IsRing(geom) as is_ring")
      .filter("is_ring is not null")
      .as[Boolean]
      .collect()
      .headOption

  private def wktToDf(wkt: String): DataFrame =
    Seq(Tuple1(wktReader.read(wkt))).toDF("geom")

  private def calculateStRemovePointOption(wkt: String): Option[String] =
    calculateStRemovePoint(wkt).headOption

  private def calculateStRemovePoint(wkt: String): Array[String] =
    wktToDf(wkt)
      .selectExpr(s"ST_RemovePoint(geom) as geom")
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()

  private def calculateStRemovePointOption(wkt: String, index: Int): Option[String] =
    calculateStRemovePoint(wkt, index).headOption

  private def calculateStRemovePoint(wkt: String, index: Int): Array[String] =
    wktToDf(wkt)
      .selectExpr(s"ST_RemovePoint(geom, $index) as geom")
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()

  private def calculateStSetPointOption(wktA: String, index: Int, wktB: String): Option[String] =
    calculateStSetPoint(wktA, index, wktB).headOption

  private def calculateStSetPoint(wktA: String, index: Int, wktB: String): Array[String] =
    Seq(Tuple3(wktReader.read(wktA), index, wktReader.read(wktB)))
      .toDF("geomA", "index", "geomB")
      .selectExpr(s"ST_SetPoint(geomA, index, geomB) as geom")
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()

  it("Passed ST_NumGeometries") {
    Given("Some different types of geometries in a DF")
    // Test data
    val testData = Seq(
      ("LINESTRING (-29 -27, -30 -29.7, -45 -33)"),
      ("MULTILINESTRING ((-29 -27, -30 -29.7, -36 -31, -45 -33), (-45.2 -33.2, -46 -32))"),
      ("POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 35 36, 43 19, 24 39, 8 25))"),
      ("MULTIPOLYGON(((0 0, 3 0, 3 3, 0 3, 0 0)), ((3 0, 6 0, 6 3, 3 3, 3 0)))"),
      ("GEOMETRYCOLLECTION(MULTIPOINT(-2 3 , -2 2), LINESTRING(5 5 ,10 10), POLYGON((-7 4.2,-7.1 5,-7.1 4.3,-7 4.2)))"))
      .toDF("Geometry")

    When("Using ST_NumGeometries")
    val testDF =
      testData.selectExpr("Geometry", "ST_NumGeometries(ST_GeomFromText(Geometry)) as ngeom")

    Then("Result should match")
    testDF
      .selectExpr("ngeom")
      .as[Int]
      .collect() should contain theSameElementsAs List(1, 2, 1, 2, 3)
  }

  it("Passed ST_LineMerge") {
    Given("Some different types of geometries in a DF")
    val testData = Seq(
      ("MULTILINESTRING ((-29 -27, -30 -29.7, -45 -33), (-45 -33, -46 -32))"),
      ("MULTILINESTRING ((-29 -27, -30 -29.7, -36 -31, -45 -33), (-45.2 -33.2, -46 -32))"),
      ("POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 35 36, 43 19, 24 39, 8 25))")).toDF(
      "Geometry")

    When("Using ST_LineMerge")
    val testDF = testData.selectExpr("ST_LineMerge(ST_GeomFromText(Geometry)) as geom")

    Then("Result should match")
    testDF
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect() should contain theSameElementsAs
      List(
        "LINESTRING (-29 -27, -30 -29.7, -45 -33, -46 -32)",
        "MULTILINESTRING ((-29 -27, -30 -29.7, -36 -31, -45 -33), (-45.2 -33.2, -46 -32))",
        "GEOMETRYCOLLECTION EMPTY")
  }

  it("Should pass ST_LocateAlong") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('MULTILINESTRING M((1 2 3, 3 4 2, 9 4 3),(1 2 3, 5 4 5))') AS geom")
    var actual = baseDf.selectExpr("ST_AsText(ST_LocateAlong(geom, 2))").first().get(0)
    var expected = "MULTIPOINT M((3 4 2))"
    assertEquals(expected, actual)

    actual = baseDf.selectExpr("ST_AsText(ST_LocateAlong(geom, 2, -3))").first().get(0)
    expected = "MULTIPOINT M((5.121320343559642 1.8786796564403572 2), (3 1 2))"
    assertEquals(expected, actual)
  }

  it("Should pass ST_LongestLine") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom")
    val actual =
      baseDf.selectExpr("ST_LongestLine(geom, geom)").first().get(0).asInstanceOf[Geometry].toText
    val expected = "LINESTRING (180 180, 20 50)"
    assert(expected.equals(actual))
  }

  it("Should pass ST_MaxDistance") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))') as geom")
    val actual = baseDf.selectExpr("ST_MaxDistance(geom, geom)").first().get(0)
    val expected = 206.15528128088303
    assert(expected == actual)
  }

  it("Should pass ST_FlipCoordinates") {
    val pointDF = createSamplePointDf(5, "geom")
    val oldX = pointDF.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinate.x
    val oldY = pointDF.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinate.y
    val newDf = pointDF.withColumn("geom", callUDF("ST_FlipCoordinates", col("geom")))
    val newX = newDf.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinate.x
    val newY = newDf.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinate.y
    assert(newX == oldY)
    assert(newY == oldX)
  }

  it("Should pass ST_MinimumClearance") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom")
    val actual = baseDf.selectExpr("ST_MinimumClearance(geom)").first().get(0)
    val expected = 0.5
    assertEquals(expected, actual)
  }

  it("Should pass ST_MinimumClearanceLine") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))') as geom")
    val actual = baseDf
      .selectExpr("ST_MinimumClearanceLine(geom)")
      .first()
      .get(0)
      .asInstanceOf[Geometry]
      .toText
    val expected = "LINESTRING (64.5 16, 65 16)"
    assertEquals(expected, actual)
  }

  it("Should pass ST_MinimumBoundingCircle") {
    Given("Sample geometry data frame")
    val geometryTable = Seq("POINT(0 2)", "LINESTRING(0 0,0 1)")
      .map(geom => Tuple1(wktReader.read(geom)))
      .toDF("geom")

    When("Using ST_MinimumBoundingCircle function")

    val circleTable = geometryTable.selectExpr("ST_MinimumBoundingCircle(geom, 8) as geom")
    val circleTableWithSeg = geometryTable.selectExpr("ST_MinimumBoundingCircle(geom, 1) as geom")

    Then("Result should match List of circles")

    val lineString = geometryTable.collect()(1)(0).asInstanceOf[Geometry]
    val mbcLineString = new MinimumBoundingCircle(lineString)
    val mbcCentre = lineString.getFactory.createPoint(mbcLineString.getCentre)

    circleTable
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs List(
      "POINT (0 2)",
      mbcCentre.buffer(mbcLineString.getRadius, 8).toText)

    circleTableWithSeg
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs List(
      "POINT (0 2)",
      mbcCentre.buffer(mbcLineString.getRadius, 1).toText)
  }

  it("Should pass ST_MinimumBoundingRadius") {
    Given("Sample geometry data frame")
    val geometryTable = Seq(
      "POINT (0 1)",
      "LINESTRING(1 1,0 0, -1 1)",
      "POLYGON((1 1,0 0, -1 1, 1 1))").map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

    When("Using ST_MinimumBoundingRadius function")

    val radiusTable =
      geometryTable.selectExpr("ST_MinimumBoundingRadius(geom) as tmp").select("tmp.*")

    Then("Result should match List of centers and radius")

    radiusTable
      .selectExpr("ST_AsText(center)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs List("POINT (0 1)", "POINT (0 1)", "POINT (0 1)")

    radiusTable
      .selectExpr("radius")
      .as[Double]
      .collect()
      .toList should contain theSameElementsAs List(0, 1, 1)
  }

  it("Should pass ST_LineSubstring") {
    Given("Sample geometry dataframe")
    val geometryTable = Seq("LINESTRING(25 50, 100 125, 150 190)")
      .map(geom => Tuple1(wktReader.read(geom)))
      .toDF("geom")

    When("Using ST_LineSubstring")

    val substringTable =
      geometryTable.selectExpr("ST_LineSubstring(geom, 0.333, 0.666) as subgeom")

    Then("Result should match")

    val lineString = geometryTable.collect()(0)(0).asInstanceOf[Geometry]
    val indexedLineString = new LengthIndexedLine(lineString)
    val substring = indexedLineString.extractLine(
      lineString.getLength() * 0.333,
      lineString.getLength() * 0.666)

    substringTable
      .selectExpr("ST_AsText(subgeom)")
      .as[String]
      .collect() should contain theSameElementsAs
      List(substring.toText)
  }

  it("Should pass ST_LineInterpolatePoint") {
    Given("Sample geometry dataframe")
    val geometryTable = Seq("LINESTRING(25 50, 100 125, 150 190)", "LINESTRING(1 2, 4 5, 6 7)")
      .map(geom => Tuple1(wktReader.read(geom)))
      .toDF("geom")

    When("Using ST_LineInterpolatePoint")

    val interpolatedPointTable =
      geometryTable.selectExpr("ST_LineInterpolatePoint(geom, 0.50) as interpts")

    Then("Result should match")

    val lineString2D = geometryTable.collect()(0)(0).asInstanceOf[Geometry]
    val lineString3D = geometryTable.collect()(1)(0).asInstanceOf[Geometry]

    val interPoint2D =
      new LengthIndexedLine(lineString2D).extractPoint(lineString2D.getLength() * 0.5)
    val interPoint3D =
      new LengthIndexedLine(lineString3D).extractPoint(lineString3D.getLength() * 0.5)

    interpolatedPointTable
      .selectExpr("ST_AsText(interpts)")
      .as[String]
      .collect() should contain theSameElementsAs
      List(
        lineString2D.getFactory.createPoint(interPoint2D).toText,
        lineString2D.getFactory.createPoint(interPoint3D).toText)
  }

  it("Should pass ST_LineLocatePoint") {
    Given("geometry dataframe")
    val geometryDf = Seq(
      ("POINT (-1 1)", "LINESTRING (0 0, 1 1, 2 2)"),
      ("POINT (0 2)", "LINESTRING (0 0, 1 1, 2 2)"),
      ("POINT (0 0)", "LINESTRING (0 2, 1 1, 2 0)"),
      ("POINT (3 1)", "LINESTRING (0 2, 1 1, 2 0)"))
      .map { case (point, linestring) =>
        (wktReader.read(point), wktReader.read(linestring))
      }
      .toDF("Point", "LineString")
    geometryDf.createOrReplaceTempView("geometries")

    When("using ST_LineLocatePoint")
    val resultDf = sparkSession.sql(
      "SELECT ST_LineLocatePoint(LineString, Point) as Locations FROM geometries")

    val result = resultDf.collect()

    Then("Result should match")

    assert(result(0).get(0).asInstanceOf[Double] == 0.0)
    assert(result(1).get(0).asInstanceOf[Double] == 0.5)
    assert(result(2).get(0).asInstanceOf[Double] == 0.5)
    assert(result(3).get(0).asInstanceOf[Double] == 1.0)
  }

  it("Should pass ST_Multi") {
    val df = sparkSession.sql("select ST_Astext(ST_Multi(ST_Point(1.0,1.0)))")
    val result = df.collect()
    assert(result.head.get(0).asInstanceOf[String] == "MULTIPOINT ((1 1))")

  }

  it("Should pass ST_PointOnSurface") {

    val geomTestCases1 = Map(
      "'POINT(0 5)'"
        -> "POINT (0 5)",
      "'LINESTRING(0 5, 0 10)'"
        -> "POINT (0 5)",
      "'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'"
        -> "POINT (2.5 2.5)",
      "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'"
        -> "POINT Z(0 0 1)")

    for ((inputGeom, expectedGeom) <- geomTestCases1) {
      val df =
        sparkSession.sql(s"select ST_AsText(ST_PointOnSurface(ST_GeomFromText($inputGeom)))")
      val result = df.collect()
      assert(result.head.get(0).asInstanceOf[String] == expectedGeom)
    }

    val geomTestCases2 = Map(
      "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'"
        -> "POINT Z(0 0 1)")

    for ((inputGeom, expectedGeom) <- geomTestCases2) {
      val df =
        sparkSession.sql(s"select ST_AsEWKT(ST_PointOnSurface(ST_GeomFromWKT($inputGeom)))")
      val result = df.collect()
      assert(result.head.get(0).asInstanceOf[String] == expectedGeom)
    }
  }

  it("Should pass ST_Reverse") {
    val geomTestCases = Map(
      "'POLYGON((-1 0 0, 1 0 0, 0 0 1, 0 1 0, -1 0 0))'"
        -> "POLYGON Z((-1 0 0, 0 1 0, 0 0 1, 1 0 0, -1 0 0))",
      "'LINESTRING(0 0, 1 2, 2 4, 3 6)'"
        -> "LINESTRING (3 6, 2 4, 1 2, 0 0)",
      "'POINT(1 2)'"
        -> "POINT (1 2)",
      "'MULTIPOINT((10 40 66), (40 30 77), (20 20 88), (30 10 99))'"
        -> "MULTIPOINT Z((10 40 66), (40 30 77), (20 20 88), (30 10 99))",
      """'MULTIPOLYGON(((30 20 11, 45 40 11, 10 40 11, 30 20 11)),
      |((15 5 11, 40 10 11, 10 20 11, 5 10 11, 15 5 11)))'""".stripMargin.replaceAll("\n", " ")
        -> """MULTIPOLYGON Z(((30 20 11, 10 40 11, 45 40 11, 30 20 11)),
          |((15 5 11, 5 10 11, 10 20 11, 40 10 11, 15 5 11)))""".stripMargin
          .replaceAll("\n", " "),
      """'MULTILINESTRING((10 10 11, 20 20 11, 10 40 11),
        |(40 40 11, 30 30 11, 40 20 11, 30 10 11))'""".stripMargin.replaceAll("\n", " ")
        -> """MULTILINESTRING Z((10 40 11, 20 20 11, 10 10 11),
          |(30 10 11, 40 20 11, 30 30 11, 40 40 11))""".stripMargin.replaceAll("\n", " "),
      """'MULTIPOLYGON(((40 40 11, 20 45 11, 45 30 11, 40 40 11)),
      |((20 35 11, 10 30 11, 10 10 11, 30 5 11, 45 20 11, 20 35 11),
      |(30 20 11, 20 15 11, 20 25 11, 30 20 11)))'""".stripMargin.replaceAll("\n", " ")
        -> """MULTIPOLYGON Z(((40 40 11, 45 30 11, 20 45 11, 40 40 11)),
          |((20 35 11, 45 20 11, 30 5 11, 10 10 11, 10 30 11, 20 35 11),
          |(30 20 11, 20 25 11, 20 15 11, 30 20 11)))""".stripMargin.replaceAll("\n", " "),
      """'POLYGON((0 0 11, 0 5 11, 5 5 11, 5 0 11, 0 0 11),
      |(1 1 11, 2 1 11, 2 2 11, 1 2 11, 1 1 11))'""".stripMargin.replaceAll("\n", " ")
        -> """POLYGON Z((0 0 11, 5 0 11, 5 5 11, 0 5 11, 0 0 11),
          |(1 1 11, 1 2 11, 2 2 11, 2 1 11, 1 1 11))""".stripMargin.replaceAll("\n", " "))
    for ((inputGeom, expectedGeom) <- geomTestCases) {
      var df = sparkSession.sql(s"select ST_AsText(ST_Reverse(ST_GeomFromText($inputGeom)))")
      var result = df.collect()
      assert(result.head.get(0).asInstanceOf[String] == expectedGeom)
    }
  }

  it("Should pass ST_PointN") {

    Given("Some different types of geometries in a DF")

    val sampleLineString = "LINESTRING(0 0, 1 2, 2 4, 3 6)"
    val testData = Seq(
      (sampleLineString, 1),
      (sampleLineString, 2),
      (sampleLineString, -1),
      (sampleLineString, -2),
      (sampleLineString, 3),
      (sampleLineString, 4),
      (sampleLineString, 5),
      (sampleLineString, -5),
      ("POLYGON((-1 0 0, 1 0 0, 0 0 1, 0 1 0, -1 0 0))", 2),
      ("POINT(1 2)", 1)).toDF("Geometry", "N")

    When("Using ST_PointN for getting the nth point in linestring type of Geometries")

    val testDF = testData.selectExpr("ST_PointN(ST_GeomFromText(Geometry), N) as geom")

    Then("Result should match the list of nth points")

    testDF
      .selectExpr("ST_AsText(geom)")
      .as[String]
      .collect() should contain theSameElementsAs
      List(
        "POINT (0 0)",
        "POINT (1 2)",
        "POINT (3 6)",
        "POINT (2 4)",
        "POINT (2 4)",
        "POINT (3 6)",
        null,
        null,
        null,
        null)
  }

  it("Should pass ST_AsEWKT") {
    var df = sparkSession.sql(
      "SELECT ST_SetSrid(ST_GeomFromWKT('POLYGON((0 0,0 1,1 1,1 0,0 0))'), 4326) as point")
    df.createOrReplaceTempView("table")
    df = sparkSession.sql("SELECT ST_AsEWKT(point) from table")
    val s = "SRID=4326;POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
    assert(df.first().get(0).asInstanceOf[String] == s)
  }

  it("Should pass ST_Force_2D") {
    val geomTestCases1 = Map(
      "'POINT(0 5)'"
        -> "POINT (0 5)",
      "'POLYGON((0 0 2, 0 5 2, 5 0 2, 0 0 2), (1 1 2, 3 1 2, 1 3 2, 1 1 2))'"
        -> "POLYGON ((0 0, 0 5, 5 0, 0 0), (1 1, 3 1, 1 3, 1 1))",
      "'LINESTRING(0 5 1, 0 0 1, 0 10 2)'"
        -> "LINESTRING (0 5, 0 0, 0 10)")

    for ((inputGeom, expectedGeom) <- geomTestCases1) {
      var df = sparkSession.sql(s"select ST_AsText(ST_Force_2D(ST_GeomFromText($inputGeom)))")
      var result = df.collect()
      assert(result.head.get(0).asInstanceOf[String] == expectedGeom)
    }
  }

  it("Should pass ST_IsEmpty") {
    var df = sparkSession.sql(
      "SELECT ST_SetSrid(ST_GeomFromWKT('POLYGON((0 0,0 1,1 1,1 0,0 0))'), 4326) as point")
    df.createOrReplaceTempView("table")
    df = sparkSession.sql("SELECT ST_IsEmpty(point) from table")
    val s = false
    assert(df.first().get(0).asInstanceOf[Boolean] == s)
  }

  it("Passed ST_XMax") {
    var test = sparkSession.sql(
      "SELECT ST_XMax(ST_GeomFromWKT('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))")
    assert(test.take(1)(0).get(0).asInstanceOf[Double] == 2.0)

  }

  it("Passed ST_XMin") {
    var test = sparkSession.sql(
      "SELECT ST_XMin(ST_GeomFromWKT('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))")
    assert(test.take(1)(0).get(0).asInstanceOf[Double] == -1.0)

  }

  it("Should pass ST_BuildArea") {
    val geomTestCases = Map(
      "'MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 20, 10 20, 10 10))'"
        -> "MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10)))",
      "'MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 0, 10 0, 10 10))'"
        -> "POLYGON ((0 0, 0 10, 10 10, 20 10, 20 0, 10 0, 0 0))",
      "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2))'"
        -> "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2))",
      "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2), (8 8, 8 12, 12 12, 12 8, 8 8))'"
        -> "MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)), ((8 8, 8 12, 12 12, 12 8, 8 8)))",
      """'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),
        |(8 8, 8 9, 8 10, 8 11, 8 12, 9 12, 10 12, 11 12, 12 12, 12 11, 12 10, 12 9, 12 8, 11 8, 10 8, 9 8, 8 8))'""".stripMargin
        .replaceAll("\n", " ")
        -> """MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)),
             |((8 8, 8 9, 8 10, 8 11, 8 12, 9 12, 10 12, 11 12, 12 12, 12 11, 12 10, 12 9, 12 8, 11 8, 10 8, 9 8, 8 8)))""".stripMargin
          .replaceAll("\n", " "),
      "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(8 8, 8 12, 12 12, 12 8, 8 8),(10 8, 10 12))'"
        -> "MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)), ((8 8, 8 12, 12 12, 12 8, 8 8)))",
      "'MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(10 2, 10 18))'"
        -> "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2))",
      """'MULTILINESTRING( (0 0, 70 0, 70 70, 0 70, 0 0), (10 10, 10 60, 40 60, 40 10, 10 10),
        |(20 20, 20 30, 30 30, 30 20, 20 20), (20 30, 30 30, 30 50, 20 50, 20 30), (50 20, 60 20, 60 40, 50 40, 50 20),
        |(50 40, 60 40, 60 60, 50 60, 50 40), (80 0, 110 0, 110 70, 80 70, 80 0), (90 60, 100 60, 100 50, 90 50, 90 60))'""".stripMargin
        .replaceAll("\n", " ")
        -> """MULTIPOLYGON (((0 0, 0 70, 70 70, 70 0, 0 0), (10 10, 40 10, 40 60, 10 60, 10 10), (50 20, 60 20, 60 40, 60 60, 50 60, 50 40, 50 20)),
          |((20 20, 20 30, 20 50, 30 50, 30 30, 30 20, 20 20)),
          |((80 0, 80 70, 110 70, 110 0, 80 0), (90 50, 100 50, 100 60, 90 60, 90 50)))""".stripMargin
          .replaceAll("\n", " "))

    for ((inputGeom, expectedGeom) <- geomTestCases) {
      val df = sparkSession.sql(s"select ST_AsText(ST_BuildArea(ST_GeomFromText($inputGeom)))")
      val result = df.collect()
      assert(result.head.get(0).asInstanceOf[String] == expectedGeom)
    }
  }

  it("handles nulls") {
    var functionDf: DataFrame = null
    functionDf = sparkSession.sql("select ST_Distance(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_3DDistance(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_ConcaveHull(null, 1, true)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_ConcaveHull(null, 1)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_ConvexHull(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_NPoints(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Buffer(null, 0)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Envelope(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Length(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Area(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Centroid(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Transform(null, null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Intersection(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_IsValid(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_IsSimple(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_SimplifyPreserveTopology(null, 1)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_ReducePrecision(null, 1)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_AsText(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_AsGeoJSON(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_AsBinary(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_AsEWKB(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_SRID(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_SetSRID(null, 4326)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_GeometryType(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_LineMerge(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Azimuth(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_X(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Y(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Z(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_StartPoint(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Boundary(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_MinimumBoundingRadius(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_LineSubstring(null, 0, 0)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_LineInterpolatePoint(null, 0)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_EndPoint(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_ExteriorRing(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_GeometryN(null, 0)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_InteriorRingN(null, 0)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Dump(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_DumpPoints(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_IsClosed(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_NumInteriorRings(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_AddPoint(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_RemovePoint(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_IsRing(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_NumGeometries(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_FlipCoordinates(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_SubDivide(null, 0)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_SubDivideExplode(null, 0)")
    assert(functionDf.count() == 0)
    functionDf = sparkSession.sql("select ST_MakePolygon(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_GeoHash(null, 1)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Difference(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_SymDifference(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Union(null, null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_PointOnSurface(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Reverse(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_AsEWKT(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Force_2D(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_BuildArea(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_Normalize(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_LineFromMultiPoint(null)")
    assert(functionDf.first().get(0) == null)
    functionDf = sparkSession.sql("select ST_GeometricMedian(null)")
    assert(functionDf.first().get(0) == null)
  }

  it("Should pass St_CollectionExtract") {
    var df = sparkSession.sql(
      "SELECT ST_GeomFromText('GEOMETRYCOLLECTION(POINT(40 10), LINESTRING(0 5, 0 10), POLYGON((0 0, 0 5, 5 5, 5 0, 0 0)))') as geom")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom))")
        .collect()
        .head
        .get(0) == "MULTIPOLYGON (((0 0, 0 5, 5 5, 5 0, 0 0)))")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom, 3))")
        .collect()
        .head
        .get(0) == "MULTIPOLYGON (((0 0, 0 5, 5 5, 5 0, 0 0)))")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom, 1))")
        .collect()
        .head
        .get(0) == "MULTIPOINT ((40 10))")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom, 2))")
        .collect()
        .head
        .get(0) == "MULTILINESTRING ((0 5, 0 10))")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom, 2))")
        .collect()
        .head
        .get(0) == "MULTILINESTRING ((0 5, 0 10))")
    df = sparkSession.sql(
      "SELECT ST_GeomFromText('GEOMETRYCOLLECTION (POINT (40 10), POINT (40 10))') as geom")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom, 1))")
        .collect()
        .head
        .get(0) == "MULTIPOINT ((40 10), (40 10))")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom, 2))")
        .collect()
        .head
        .get(0) == "MULTILINESTRING EMPTY")
    assert(
      df.selectExpr("ST_AsText(ST_CollectionExtract(geom))")
        .collect()
        .head
        .get(0) == "MULTIPOINT ((40 10), (40 10))")
  }

  it("Should pass ST_Normalize") {
    val df = sparkSession.sql(
      "SELECT ST_AsEWKT(ST_Normalize(ST_GeomFromWKT('POLYGON((0 1, 1 1, 1 0, 0 0, 0 1))')))")
    assert(df.first().get(0).asInstanceOf[String] == "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
  }

  it("Should pass ST_LineFromMultiPoint") {
    val geomTestCases = Map(
      "'POLYGON((-1 0 0, 1 0 0, 0 0 1, 0 1 0, -1 0 0))'"
        -> null,
      "'LINESTRING(0 0, 1 2, 2 4, 3 6)'"
        -> null,
      "'POINT(1 2)'"
        -> null,
      "'MULTIPOINT((10 40), (40 30), (20 20), (30 10))'"
        -> "LINESTRING (10 40, 40 30, 20 20, 30 10)",
      "'MULTIPOINT((10 40 66), (40 30 77), (20 20 88), (30 10 99))'"
        -> "LINESTRING Z(10 40 66, 40 30 77, 20 20 88, 30 10 99)")
    for ((inputGeom, expectedGeom) <- geomTestCases) {
      var df =
        sparkSession.sql(s"select ST_AsText(ST_LineFromMultiPoint(ST_GeomFromText($inputGeom)))")
      var result = df.collect()
      assert(result.head.get(0).asInstanceOf[String] == expectedGeom)
    }
  }

  it("Should pass ST_Split") {
    val geomTestCases = Map(
      ("'LINESTRING (0 0, 1.5 1.5, 2 2)'", "'MULTIPOINT (0.5 0.5, 1 1)'")
        -> "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))",
      ("null", "'MULTIPOINT (0.5 0.5, 1 1)'")
        -> null,
      ("'LINESTRING (0 0, 1.5 1.5, 2 2)'", "null")
        -> null)
    for (((target, blade), expected) <- geomTestCases) {
      var df =
        sparkSession.sql(s"SELECT ST_Split(ST_GeomFromText($target), ST_GeomFromText($blade))")
      var result = df.take(1)(0).get(0).asInstanceOf[Geometry]
      var textResult = if (result == null) null else result.toText
      assert(textResult == expected)
    }
  }

  it("Should pass ST_GeometricMedian") {
    val geomTestCases = Map(
      ("'MULTIPOINT((10 40), (40 30), (20 20), (30 10))'", 1e-15) -> "'POINT(22.5 21.25)'",
      (
        "'MULTIPOINT((0 0), (1 1), (2 2), (200 200))'",
        1e-6) -> "'POINT (1.9761550281255005 1.9761550281255005)'",
      ("'MULTIPOINT ((0 0), (10 1), (5 1), (20 20))'", 1e-15) -> "'POINT (5 1)'",
      ("'MULTIPOINT ((0 -1), (0 0), (0 0), (0 1))'", 1e-6) -> "'POINT (0 0)'",
      ("'POINT (7 6)'", 1e-6) -> "'POINT (7 6)'",
      (
        "'MULTIPOINT ((12 5),(62 7),(100 -1),(100 -5),(10 20),(105 -5))'",
        1e-15) -> "'POINT(84.21672412761632 0.1351485929395439)'")
    for (((targetWkt, tolerance), expectedWkt) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_GeometricMedian(ST_GeomFromWKT($targetWkt), $tolerance), " +
          s"ST_GeomFromWKT($expectedWkt)")
      val actual = df.take(1)(0).get(0).asInstanceOf[Geometry]
      val expected = df.take(1)(0).get(1).asInstanceOf[Geometry]
      assert(expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR) == 0)
    }
  }

  it("Should pass ST_DistanceSphere") {
    val geomTestCases = Map(
      ("'POINT (-0.56 51.3168)'", "'POINT (-3.1883 55.9533)'") -> "543796.9506134904",
      ("'LineString (0 0, 90 0)'", "'LineString (1 0, 0 0)'") -> "4948180.449055")
    for (((geom1, geom2), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_DistanceSphere(ST_GeomFromWKT($geom1), ST_GeomFromWKT($geom2)), " +
          s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.math.BigDecimal].doubleValue()
      assertEquals(expected, actual, 0.1)
    }

    val geomTestCases2 = Map(
      ("'POINT (-0.56 51.3168)'", "'POINT (-3.1883 55.9533)'", "6378137") -> "544405.4459192449",
      ("'LineString (0 0, 90 0)'", "'LineString (1 0, 0 0)'", "6378137.0") -> "4953717.340300673")
    for (((geom1, geom2, radius), expectedResult) <- geomTestCases2) {
      val df = sparkSession.sql(
        s"SELECT ST_DistanceSphere(ST_GeomFromWKT($geom1), ST_GeomFromWKT($geom2), $radius), " +
          s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.math.BigDecimal].doubleValue()
      assertEquals(expected, actual, 0.1)
    }
  }

  it("Should pass ST_DistanceSpheroid") {
    val geomTestCases = Map(
      ("'POINT (-0.56 51.3168)'", "'POINT (-3.1883 55.9533)'") -> "544430.94119962039",
      ("'LineString (0 0, 90 0)'", "'LineString (1 0, 0 0)'") -> "4953717.340300673")
    for (((geom1, geom2), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_DistanceSpheroid(ST_GeomFromWKT($geom1), ST_GeomFromWKT($geom2)), " +
          s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.math.BigDecimal].doubleValue()
      assertEquals(expected, actual, 0.1)
    }
  }

  it("should pass ST_ClosestPoint") {
    val geomTestCases = Map(
      (
        "'POINT (160 40)'",
        "'LINESTRING (10 30, 50 50, 30 110, 70 90, 180 140, 130 190)'") -> "POINT (160 40)",
      ("'LINESTRING (0 0, 100 0)'", "'LINESTRING (0 0, 50 50, 100 0)'") -> "POINT (0 0)")
    for (((geom), expectedResult) <- geomTestCases) {
      val g1 = geom._1
      val g2 = geom._2
      val df =
        sparkSession.sql(s"SELECT ST_ClosestPoint(ST_GeomFromWKT($g1), ST_GeomFromWKT($g2))")
      val actual = df.take(1)(0).get(0).asInstanceOf[Geometry].toText
      val expected = expectedResult
      assertEquals(expected, actual)

    }
  }

  it("Should pass ST_AreaSpheroid") {
    val geomTestCases = Map(
      ("'POINT (-0.56 51.3168)'") -> "0.0",
      ("'LineString (0 0, 90 0)'") -> "0.0",
      ("'Polygon ((34 35, 28 30, 25 34, 34 35))'") -> "201824850811.76245")
    for (((geom1), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AreaSpheroid(ST_GeomFromWKT($geom1)), " +
          s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.math.BigDecimal].doubleValue()
      assertEquals(expected, actual, 0.1)
    }
  }

  it("Should pass ST_LengthSpheroid") {
    val geomTestCases = Map(
      ("'POINT (-0.56 51.3168)'") -> "0.0",
      ("'LineString (0 0, 90 0)'") -> "10018754.17139462",
      ("'Polygon ((0 0, 90 0, 0 0))'") -> "0.0")
    for (((geom1), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_LengthSpheroid(ST_GeomFromWKT($geom1)), " +
          s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.math.BigDecimal].doubleValue()
      assertEquals(expected, actual, 0.1)
    }
  }

  it("Should pass ST_NumPoints") {
    val geomTestCases = Map(("'LINESTRING (0 1, 1 0, 2 0)'") -> "3")
    for (((geom), expectedResult) <- geomTestCases) {
      val df =
        sparkSession.sql(s"SELECT ST_NumPoints(ST_GeomFromWKT($geom)), " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Int]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.lang.Integer].intValue()
      assertEquals(expected, actual)
    }
  }

  it("should pass ST_Force3D") {
    val geomTestCases = Map(
      ("'LINESTRING (0 1, 1 0, 2 0)'") -> ("'LINESTRING Z(0 1 1, 1 0 1, 2 0 1)'", "'LINESTRING Z(0 1 0, 1 0 0, 2 0 0)'"),
      ("'LINESTRING Z(0 1 3, 1 0 3, 2 0 3)'") -> ("'LINESTRING Z(0 1 3, 1 0 3, 2 0 3)'", "'LINESTRING Z(0 1 3, 1 0 3, 2 0 3)'"),
      ("'LINESTRING EMPTY'") -> ("'LINESTRING EMPTY'", "'LINESTRING EMPTY'"))
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force3D(ST_GeomFromWKT($geom), 1)) AS geom, " + s"$expectedResult")
      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force3D(ST_GeomFromWKT($geom))) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected =
        df.take(1)(0).get(1).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[String]
      val expectedDefaultValue = dfDefaultValue
        .take(1)(0)
        .get(1)
        .asInstanceOf[GenericRowWithSchema]
        .get(1)
        .asInstanceOf[String]
      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }
  }

  it("Should pass ST_Force3DZ") {
    val geomTestCases = Map(
      ("'LINESTRING (0 1, 1 0, 2 0)'") -> ("'LINESTRING Z(0 1 1, 1 0 1, 2 0 1)'", "'LINESTRING Z(0 1 0, 1 0 0, 2 0 0)'"),
      ("'LINESTRING Z(0 1 3, 1 0 3, 2 0 3)'") -> ("'LINESTRING Z(0 1 3, 1 0 3, 2 0 3)'", "'LINESTRING Z(0 1 3, 1 0 3, 2 0 3)'"),
      ("'LINESTRING EMPTY'") -> ("'LINESTRING EMPTY'", "'LINESTRING EMPTY'"))
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force3DZ(ST_GeomFromWKT($geom), 1)) AS geom, " + s"$expectedResult")
      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force3DZ(ST_GeomFromWKT($geom))) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected =
        df.take(1)(0).get(1).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[String]
      val expectedDefaultValue = dfDefaultValue
        .take(1)(0)
        .get(1)
        .asInstanceOf[GenericRowWithSchema]
        .get(1)
        .asInstanceOf[String]
      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }
  }

  it("Should pass ST_Force3DM") {
    val geomTestCases = Map(
      ("'LINESTRING (0 1, 1 0, 2 0)'") -> ("'LINESTRING M(0 1 1, 1 0 1, 2 0 1)'", "'LINESTRING M(0 1 0, 1 0 0, 2 0 0)'"),
      ("'LINESTRING M(0 1 3, 1 0 3, 2 0 3)'") -> ("'LINESTRING M(0 1 3, 1 0 3, 2 0 3)'", "'LINESTRING M(0 1 3, 1 0 3, 2 0 3)'"),
      ("'LINESTRING EMPTY'") -> ("'LINESTRING EMPTY'", "'LINESTRING EMPTY'"))
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force3DM(ST_GeomFromWKT($geom), 1)) AS geom, " + s"$expectedResult")
      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force3DM(ST_GeomFromWKT($geom))) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected =
        df.take(1)(0).get(1).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[String]
      val expectedDefaultValue = dfDefaultValue
        .take(1)(0)
        .get(1)
        .asInstanceOf[GenericRowWithSchema]
        .get(1)
        .asInstanceOf[String]
      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }
  }

  it("Should pass ST_Force4D") {
    val geomTestCases = Map(
      ("'LINESTRING (0 1, 1 0, 2 0)'") -> ("'LINESTRING ZM(0 1 1 1, 1 0 1 1, 2 0 1 1)'", "'LINESTRING ZM(0 1 0 0, 1 0 0 0, 2 0 0 0)'"),
      ("'LINESTRING ZM(0 1 3 2, 1 0 3 2, 2 0 3 2)'") -> ("'LINESTRING ZM(0 1 3 2, 1 0 3 2, 2 0 3 2)'", "'LINESTRING ZM(0 1 3 2, 1 0 3 2, 2 0 3 2)'"),
      ("'LINESTRING EMPTY'") -> ("'LINESTRING EMPTY'", "'LINESTRING EMPTY'"))
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force4D(ST_GeomFromWKT($geom), 1, 1)) AS geom, " + s"$expectedResult")
      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_AsText(ST_Force4D(ST_GeomFromWKT($geom))) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected =
        df.take(1)(0).get(1).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[String]
      val expectedDefaultValue = dfDefaultValue
        .take(1)(0)
        .get(1)
        .asInstanceOf[GenericRowWithSchema]
        .get(1)
        .asInstanceOf[String]
      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }
  }

  it("Passed ST_ForceCollection") {
    var actual = sparkSession
      .sql("SELECT ST_NumGeometries(ST_ForceCollection(ST_GeomFromWKT('MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)')))")
      .first()
      .get(0)
    assert(actual == 6)

    actual = sparkSession
      .sql("SELECT ST_NumGeometries(ST_ForceCollection(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')))")
      .first()
      .get(0)
    assert(actual == 1)
  }

  it("Should pass ST_GeneratePoints") {
    var actual = sparkSession
      .sql("SELECT ST_NumGeometries(ST_GeneratePoints(ST_Buffer(ST_GeomFromWKT('LINESTRING(50 50,150 150,150 50)'), 10, false, 'endcap=round join=round'), 15))")
      .first()
      .get(0)
    assert(actual == 15)

    actual = sparkSession
      .sql("SELECT ST_NumGeometries(ST_GeneratePoints(ST_GeomFromWKT('MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))'), 30))")
      .first()
      .get(0)
    assert(actual == 30)

    actual = sparkSession
      .sql("SELECT ST_AsText(ST_ReducePrecision(ST_GeneratePoints(ST_GeomFromWKT('MULTIPOLYGON (((10 0, 10 10, 20 10, 20 0, 10 0)), ((50 0, 50 10, 70 10, 70 0, 50 0)))'), 5, 10), 5))")
      .first()
      .get(0)
    val expected =
      "MULTIPOINT ((53.82582 2.57803), (13.55212 2.44117), (59.12854 3.70611), (61.37698 7.14985), (10.49657 4.40622))"
    assertEquals(expected, actual)
  }

  it("should pass ST_NRings") {
    val geomTestCases = Map(
      ("'POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'") -> 1,
      ("'MULTIPOLYGON (((1 0, 1 6, 6 6, 6 0, 1 0), (2 1, 2 2, 3 2, 3 1, 2 1)), ((10 0, 10 6, 16 6, 16 0, 10 0), (12 1, 12 2, 13 2, 13 1, 12 1)))'") -> 4,
      ("'POLYGON EMPTY'") -> 0)
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(s"SELECT ST_NRings(ST_GeomFromWKT($geom)), " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[Int]
      val expected = df.take(1)(0).get(1).asInstanceOf[java.lang.Integer].intValue()
      assertEquals(expected, actual)
    }
  }

  it("should pass ST_Translate") {
    val geomTestCases = Map(
      ("'POINT (1 1 1)'") -> ("'POINT Z(2 2 2)'", "'POINT Z(2 2 1)'"),
      ("'POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'") -> ("'POLYGON ((2 1, 2 2, 3 2, 3 1, 2 1))'", "'POLYGON ((2 1, 2 2, 3 2, 3 1, 2 1))'"),
      ("'LINESTRING EMPTY'") -> ("'LINESTRING EMPTY'", "'LINESTRING EMPTY'"),
      ("'GEOMETRYCOLLECTION (MULTIPOLYGON (((1 0, 1 1, 2 1, 2 0, 1 0)), ((1 2, 3 4, 3 5, 1 2))))'") -> ("'GEOMETRYCOLLECTION (MULTIPOLYGON (((2 1, 2 2, 3 2, 3 1, 2 1)), ((2 3, 4 5, 4 6, 2 3))))'", "'GEOMETRYCOLLECTION (MULTIPOLYGON (((2 1, 2 2, 3 2, 3 1, 2 1)), ((2 3, 4 5, 4 6, 2 3))))'"))
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_Translate(ST_GeomFromWKT($geom), 1, 1, 1)) AS geom, " + s"$expectedResult")
      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_AsText(ST_Translate(ST_GeomFromWKT($geom), 1, 1)) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[String]
      val expected =
        df.take(1)(0).get(1).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]
      val expectedDefaultValue = dfDefaultValue
        .take(1)(0)
        .get(1)
        .asInstanceOf[GenericRowWithSchema]
        .get(1)
        .asInstanceOf[String]
      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }
  }

  it("should pass ST_VoronoiPolygons") {
    val geomTestCases = Map(
      (
        "'MULTIPOINT ((0 0), (2 2))'",
        0.0) -> ("GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 -2, -2 -2)), POLYGON ((-2 4, 4 4, 4 -2, -2 4)))"),
      (
        "'MULTIPOINT ((0 0), (2 2))'",
        30) -> ("GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 4, 4 -2, -2 -2)))"),
      ("'LINESTRING EMPTY'", 0.0) -> ("GEOMETRYCOLLECTION EMPTY"))
    for (((geom), expectedResult) <- geomTestCases) {
      val g1 = geom._1
      val tolerance = geom._2
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_VoronoiPolygons(ST_GeomFromWKT($g1), ($tolerance)))")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      assertGeometryEquals(expectedResult, actual)
    }
  }

  it("should pass ST_VoronoiPolygons with Extend Geometry") {
    val geomTestCases = Map(
      (
        "'MULTIPOINT ((0 0), (2 2))'",
        0.0,
        "'POINT(1 1)'") -> ("GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 -9, -9 -9)), POLYGON ((-9 11, 11 11, 11 -9, -9 11)))"),
      (
        "'MULTIPOINT ((0 0), (2 2))'",
        30,
        "'POINT(1 1)'") -> ("GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 11, 11 -9, -9 -9)))"),
      ("'LINESTRING EMPTY'", 0.0, "'POINT(1 1)'") -> ("GEOMETRYCOLLECTION EMPTY"))
    for (((geom), expectedResult) <- geomTestCases) {
      val g1 = geom._1
      val tolerance = geom._2
      val extendTo = geom._3
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_VoronoiPolygons(ST_GeomFromWKT($g1), $tolerance, ST_Buffer(ST_GeomFromWKT($extendTo), 10.0)))")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      assertEquals(expectedResult, actual)
    }
  }

  it("should pass ST_FrechetDistance") {
    val geomTestCases = Map(
      ("'POINT (1 2)'", "'POINT (10 10)'") -> 12.041594578792296d,
      ("'LINESTRING (0 0, 100 0)'", "'LINESTRING (0 0, 50 50, 100 0)'") -> 70.7106781186548d)
    for (((geom), expectedResult) <- geomTestCases) {
      val g1 = geom._1
      val g2 = geom._2
      val df =
        sparkSession.sql(s"SELECT ST_FrechetDistance(ST_GeomFromWKT($g1), ST_GeomFromWKT($g2))")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val expected = expectedResult
      assertEquals(expected, actual, 1e-9)
    }
  }

  it("should pass ST_Affine") {
    val geomTestCases = Map(
      ("'POLYGON ((1 0 1, 1 1 1, 2 2 2, 1 0 1))'") -> ("'POLYGON Z((9 11 11, 11 12 13, 18 16 23, 9 11 11))'", "'POLYGON Z((2 3 1, 4 5 1, 7 8 2, 2 3 1))'"),
      ("'LINESTRING EMPTY'") -> ("'LINESTRING EMPTY'", "'LINESTRING EMPTY'"),
      ("'GEOMETRYCOLLECTION (MULTIPOLYGON (((1 0, 1 1, 2 1, 2 0, 1 0), (1 0.5, 1 0.75, 1.5 0.75, 1.5 0.5, 1 0.5)), ((5 0, 5 5, 7 5, 7 0, 5 0))), POINT (10 10))'") ->
        ("'GEOMETRYCOLLECTION (MULTIPOLYGON (((5 9, 7 10, 8 11, 6 10, 5 9), (6 9.5, 6.5 9.75, 7 10.25, 6.5 10, 6 9.5)), ((9 13, 19 18, 21 20, 11 15, 9 13))), POINT (34 28))'",
        "'GEOMETRYCOLLECTION (MULTIPOLYGON (((2 3, 4 5, 5 6, 3 4, 2 3), (3 4, 3.5 4.5, 4 5, 3.5 4.5, 3 4)), ((6 7, 16 17, 18 19, 8 9, 6 7))), POINT (31 32))'"))
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_Affine(ST_GeomFromWKT($geom), 1, 2, 4, 1, 1, 2, 3, 2, 5, 4, 8, 3)) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected =
        df.take(1)(0).get(1).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]

      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_AsText(ST_Affine(ST_GeomFromWKT($geom), 1, 2, 1, 2, 1, 2)) AS geom, " + s"$expectedResult")
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[String]
      val expectedDefaultValue = dfDefaultValue
        .take(1)(0)
        .get(1)
        .asInstanceOf[GenericRowWithSchema]
        .get(1)
        .asInstanceOf[String]

      assertEquals(expected, actual)
      assertEquals(expectedDefaultValue, actualDefaultValue)
    }
  }

  it("should pass ST_BoundingDiagonal") {
    val geomTestCases = Map(
      ("'POINT (10 10)'") -> "'LINESTRING (10 10, 10 10)'",
      ("'POLYGON ((1 1 1, 4 4 4, 0 9 3, 0 9 9, 1 1 1))'") -> "'LINESTRING Z(0 1 1, 4 9 9)'",
      ("'GEOMETRYCOLLECTION (MULTIPOLYGON (((1 1, 1 -1, 2 2, 2 9, 9 1, 1 1)), ((5 5, 4 4, 2 2 , 5 5))), POINT (-1 0))'") -> "'LINESTRING (-1 -1, 9 9)'")
    for (((geom), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT ST_AsText(ST_BoundingDiagonal(ST_GeomFromWKT($geom))) AS geom, " + s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected = df.take(1)(0).get(1).asInstanceOf[String]
      assertEquals(expected, actual)
    }
  }

  it("should pass ST_Angle - 4 points") {
    val geomTestCases = Map(
      (
        "'POINT (0 0)'",
        "'POINT (1 1)'",
        "'POINT (1 0)'",
        "'POINT (6 2)'") -> (0.4048917862850834, 23.198590513648185))
    for (((geom), expectedResult) <- geomTestCases) {
      val p1 = geom._1
      val p2 = geom._2
      val p3 = geom._3
      val p4 = geom._4
      val df = sparkSession.sql(
        s"SELECT ST_Angle(ST_GeomFromWKT($p1), ST_GeomFromWKT($p2), ST_GeomFromWKT($p3), ST_GeomFromWKT($p4)) AS angleInRadian")
      val expectedRadian = expectedResult._1
      val expectedDegrees = expectedResult._2

      val actualRadian = df.take(1)(0).get(0).asInstanceOf[Double]
      val actualDegrees =
        df.selectExpr("ST_Degrees(angleInRadian)").take(1)(0).get(0).asInstanceOf[Double]

      assertEquals(expectedRadian, actualRadian, 1e-9)
      assertEquals(expectedDegrees, actualDegrees, 1e-9)
    }
  }

  it("should pass ST_Angle - 3 points") {
    val geomTestCases = Map(
      (
        "'POINT (1 1)'",
        "'POINT (0 0)'",
        "'POINT (3 2)'") -> (0.19739555984988044, 11.309932474020195))
    for (((geom), expectedResult) <- geomTestCases) {
      val p1 = geom._1
      val p2 = geom._2
      val p3 = geom._3
      val df = sparkSession.sql(
        s"SELECT ST_Angle(ST_GeomFromWKT($p1), ST_GeomFromWKT($p2), ST_GeomFromWKT($p3)) AS angleInRadian")
      val expectedRadian = expectedResult._1
      val expectedDegrees = expectedResult._2

      val actualRadian = df.take(1)(0).get(0).asInstanceOf[Double]
      val actualDegrees =
        df.selectExpr("ST_Degrees(angleInRadian)").take(1)(0).get(0).asInstanceOf[Double]

      assertEquals(expectedRadian, actualRadian, 1e-9)
      assertEquals(expectedDegrees, actualDegrees, 1e-9)
    }
  }

  it("Should pass ST_DelaunayTriangles") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('MULTIPOLYGON (((10 10, 10 20, 20 20, 20 10, 10 10)),((25 10, 25 20, 35 20, 35 10, 25 10)))') AS geom")
    var actual =
      baseDf.selectExpr("ST_DelaunayTriangles(geom)").first().get(0).asInstanceOf[Geometry].toText
    var expected =
      "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 20 10, 10 20)), POLYGON ((10 20, 20 10, 20 20, 10 20)), POLYGON ((20 20, 20 10, 25 10, 20 20)), POLYGON ((20 20, 25 10, 25 20, 20 20)), POLYGON ((25 20, 25 10, 35 10, 25 20)), POLYGON ((25 20, 35 10, 35 20, 25 20)))"
    assertEquals(expected, actual)

    actual = baseDf
      .selectExpr("ST_DelaunayTriangles(geom, 20)")
      .first()
      .get(0)
      .asInstanceOf[Geometry]
      .toText
    expected = "GEOMETRYCOLLECTION (POLYGON ((10 20, 10 10, 35 10, 10 20)))"
    assertEquals(expected, actual)

    actual = baseDf
      .selectExpr("ST_DelaunayTriangles(geom, 20, 1)")
      .first()
      .get(0)
      .asInstanceOf[Geometry]
      .toText
    expected = "MULTILINESTRING ((10 20, 35 10), (10 10, 10 20), (10 10, 35 10))"
    assertEquals(expected, actual)
  }

  it("should pass ST_Angle - 2 lines") {
    val geomTestCases = Map(
      (
        "'LINESTRING (0 0, 1 1)'",
        "'LINESTRING (0 0, 3 2)'") -> (0.19739555984988044, 11.309932474020195))
    for (((geom), expectedResult) <- geomTestCases) {
      val p1 = geom._1
      val p2 = geom._2

      val df = sparkSession.sql(
        s"SELECT ST_Angle(ST_GeomFromWKT($p1), ST_GeomFromWKT($p2)) AS angleInRadian")
      val expectedRadian = expectedResult._1
      val expectedDegrees = expectedResult._2

      val actualRadian = df.take(1)(0).get(0).asInstanceOf[Double]
      val actualDegrees =
        df.selectExpr("ST_Degrees(angleInRadian)").take(1)(0).get(0).asInstanceOf[Double]

      assertEquals(expectedRadian, actualRadian, 1e-9)
      assertEquals(expectedDegrees, actualDegrees, 1e-9)
    }
  }

  it("should pass ST_HausdorffDistance") {
    val geomTestCases = Map(
      (
        "'LINESTRING (1 2, 1 5, 2 6, 1 2)'",
        "'POINT (10 34)'",
        0.34) -> (33.24154027718932, 33.24154027718932),
      ("'LINESTRING (1 0, 1 1, 2 1, 2 0, 1 0)'", "'POINT EMPTY'", 0.23) -> (0.0, 0.0),
      (
        "'POLYGON ((1 2, 2 1, 2 0, 4 1, 1 2))'",
        "'MULTIPOINT ((1 0), (40 10), (-10 -40))'",
        0.0001) -> (41.7612260356422, 41.7612260356422))
    for (((geom), expectedResult) <- geomTestCases) {
      val geom1 = geom._1
      val geom2 = geom._2
      val densityFrac = geom._3
      val df = sparkSession.sql(
        s"SELECT ST_HausdorffDistance(ST_GeomFromWKT($geom1), ST_GeomFromWKT($geom2), $densityFrac) AS dist")
      val dfDefaultValue = sparkSession.sql(
        s"SELECT ST_HausdorffDistance(ST_GeomFromWKT($geom1), ST_GeomFromWKT($geom2)) as dist")
      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      val actualDefaultValue = dfDefaultValue.take(1)(0).get(0).asInstanceOf[Double]
      val expected = expectedResult._1
      val expectedDefaultValue = expectedResult._2
      assert(expected == actual)
      assert(expectedDefaultValue == actualDefaultValue)
    }
  }

  it("Passed ST_CoordDim with 3D point") {
    val test = sparkSession.sql("SELECT ST_CoordDim(ST_GeomFromWKT('POINT(1 1 2)'))")
    assert(test.take(1)(0).get(0).asInstanceOf[Int] == 3)
  }

  it("Passed ST_CoordDim with Z coordinates") {
    val test = sparkSession.sql("SELECT ST_CoordDim(ST_GeomFromWKT('POINTZ(1 1 0.5)'))")
    assert(test.take(1)(0).get(0).asInstanceOf[Int] == 3)
  }

  it("Passed ST_CoordDim with XYM point") {
    val test = sparkSession.sql("SELECT ST_CoordDim(ST_GeomFromWKT('POINT M(1 2 3)'))")
    assert(test.take(1)(0).get(0).asInstanceOf[Int] == 3)
  }

  it("Passed ST_CoordDim with XYZM point") {
    val test = sparkSession.sql("SELECT ST_CoordDim(ST_GeomFromWKT('POINT ZM(1 2 3 4)'))")
    assert(test.take(1)(0).get(0).asInstanceOf[Int] == 4)
  }

  it("Should pass ST_IsCollection") {
    val test = sparkSession.sql(
      "SELECT ST_IsCollection(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(2 3), POINT(4 6), LINESTRING(15 15, 20 20))'))")
    val expected = true
    assert(test.take(1)(0).get(0).asInstanceOf[Boolean] == expected)
  }

  it("should pass GeometryType") {
    val geomTestCases = Map(
      ("'POINT (51.3168 -0.56)'") -> "'POINT'",
      ("'POINT (0 0 1)'") -> "'POINT'",
      ("'LINESTRING (0 0, 0 90)'") -> "'LINESTRING'",
      ("'POLYGON ((0 0,0 5,5 0,0 0))'") -> "'POLYGON'",
      ("'POINTM (1 2 3)'") -> "'POINTM'",
      ("'LINESTRINGM (0 0 1, 0 90 1)'") -> "'LINESTRINGM'",
      ("'POLYGONM ((0 0 1, 0 5 1, 5 0 1, 0 0 1))'") -> "'POLYGONM'")
    for ((geom, expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(
        s"SELECT GeometryType(ST_GeomFromText($geom)), " +
          s"$expectedResult")
      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      val expected = df.take(1)(0).get(1).asInstanceOf[String]
      assertEquals(expected, actual)
    }

  }

  it("Should pass ST_MaximumInscribedCircle") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))') AS geom")
    val actual: Row = baseDf.selectExpr("ST_MaximumInscribedCircle(geom)").first().getAs[Row](0)
    assertGeometryEquals("POINT (96.9287109375 76.3232421875)", actual.getAs[Geometry](0))
    assertGeometryEquals(
      "POINT (61.64205411585366 104.55256764481707)",
      actual.getAs[Geometry](1))
    assertEquals(45.18896951053177, actual.getDouble(2), 1e-6)
  }

  it("Should pass ST_IsValidTrajectory") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromText('LINESTRING M (0 0 1, 0 1 2)') as geom1, ST_GeomFromText('LINESTRING M (0 0 1, 0 1 1)') as geom2")
    var actual =
      baseDf.selectExpr("ST_IsValidTrajectory(geom1)").first().get(0).asInstanceOf[Boolean]
    assertTrue("Valid", actual)

    actual = baseDf.selectExpr("ST_IsValidTrajectory(geom2)").first().get(0).asInstanceOf[Boolean]
    assertFalse("Not valid", actual)
  }

  it("Should pass ST_IsValidDetail") {
    val testData = Seq(
      (5330, "POLYGON ((0 0, 3 3, 0 3, 3 0, 0 0))"),
      (5340, "POLYGON ((100 100, 300 300, 100 300, 300 100, 100 100))"),
      (5350, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (20 20, 20 30, 30 30, 30 20, 20 20))"),
      (5360, "LINESTRING (220227 150406, 2220227 150407, 222020 150410)"))

    var df = sparkSession
      .createDataFrame(testData)
      .toDF("gid", "wkt")
      .select($"gid", expr("ST_GeomFromWKT(wkt) as geom"))

    val expectedResults = Map(
      5330 -> Row(
        false,
        "Self-intersection at or near point (1.5, 1.5, NaN)",
        sparkSession
          .sql("SELECT ST_GeomFromWKT('POINT (1.5 1.5)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry]),
      5340 -> Row(
        false,
        "Self-intersection at or near point (200.0, 200.0, NaN)",
        sparkSession
          .sql("SELECT ST_GeomFromWKT('POINT (200 200)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry]),
      5350 -> Row(
        false,
        "Hole lies outside shell at or near point (20.0, 20.0)",
        sparkSession
          .sql("SELECT ST_GeomFromWKT('POINT (20 20)')")
          .first()
          .get(0)
          .asInstanceOf[Geometry]),
      5360 -> Row(true, null, null))

    df = df.selectExpr("gid", "ST_IsValidDetail(geom) as validDetail")

    df.collect().foreach { row =>
      val gid = row.getAs[Int]("gid")
      val validDetailRow = row.getAs[Row]("validDetail")
      assert(expectedResults(gid).equals(validDetailRow))
    }
  }

  it("ST_IsValidReason should provide reasons for invalid geometries") {
    val testData = Seq(
      (5330, "POLYGON ((0 0, 3 3, 0 3, 3 0, 0 0))"),
      (5340, "POLYGON ((100 100, 300 300, 100 300, 300 100, 100 100))"),
      (5350, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (20 20, 20 30, 30 30, 30 20, 20 20))"))

    val df = sparkSession
      .createDataFrame(testData)
      .toDF("gid", "wkt")
      .select($"gid", expr("ST_GeomFromWKT(wkt) as geom"))
    df.createOrReplaceTempView("geometry_table")

    val result = sparkSession.sql("""
        |SELECT gid, ST_IsValidReason(geom) as validity_info
        |FROM geometry_table
        |WHERE ST_IsValid(geom) = false
        |ORDER BY gid
        """.stripMargin)

    val expectedResults = Map(
      5330 -> "Self-intersection at or near point (1.5, 1.5, NaN)",
      5340 -> "Self-intersection at or near point (200.0, 200.0, NaN)",
      5350 -> "Hole lies outside shell at or near point (20.0, 20.0)")

    result.collect().foreach { row =>
      val gid = row.getAs[Int]("gid")
      val validityInfo = row.getAs[String]("validity_info")
      assert(validityInfo == expectedResults(gid))
    }
  }

  it("Should pass ST_Scale") {
    val baseDf =
      sparkSession.sql("SELECT ST_GeomFromWKT('LINESTRING (50 160, 50 50, 100 50)') AS geom")
    val actual = baseDf.selectExpr("ST_AsText(ST_Scale(geom, -10, 5))").first().get(0)
    val expected = "LINESTRING (-500 800, -500 250, -1000 250)"
    assertEquals(expected, actual)
  }

  it("Should pass ST_ScaleGeom") {
    val baseDf = sparkSession.sql(
      "SELECT ST_GeomFromWKT('POLYGON ((0 0, 0 1.5, 1.5 1.5, 1.5 0, 0 0))') AS geometry, ST_GeomFromWKT('POINT (1.8 2.1)') AS factor, ST_GeomFromWKT('POINT (0.32959 0.796483)') AS origin")
    var actual = baseDf.selectExpr("ST_AsText(ST_ScaleGeom(geometry, factor))").first().get(0)
    var expected = "POLYGON ((0 0, 0 3.1500000000000004, 2.7 3.1500000000000004, 2.7 0, 0 0))"
    assertEquals(expected, actual)

    actual = baseDf.selectExpr("ST_AsText(ST_ScaleGeom(geometry, factor, origin))").first().get(0)
    expected =
      "POLYGON ((-0.263672 -0.8761313000000002, -0.263672 2.2738687000000004, 2.436328 2.2738687000000004, 2.436328 -0.8761313000000002, -0.263672 -0.8761313000000002))"
    assertEquals(expected, actual)
  }

  it("Should pass ST_RotateX") {
    val geomTestCases = Map(
      (
        1,
        "'LINESTRING (50 160, 50 50, 100 50)'",
        "PI()") -> "'LINESTRING (50 -160, 50 -50, 100 -50)'",
      (
        2,
        "'LINESTRING(1 2 3, 1 1 1)'",
        "PI()/2") -> "'LINESTRING Z(1 -3 2, 1 -0.9999999999999999 1)'")

    for (((index, geom, angle), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(s"""
                                   |SELECT
                                   |  ST_AsEWKT(
                                   |    ST_RotateX(
                                   |      ST_GeomFromEWKT($geom),
                                   |      $angle
                                   |    )
                                   |  ) AS geom
         """.stripMargin)

      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      assert(actual == expectedResult.stripPrefix("'").stripSuffix("'"))
    }
  }

  it("Should pass ST_RotateY") {
    val geomTestCases = Map(
      (
        1,
        "'LINESTRING (50 160, 50 50, 100 50)'",
        "PI()") -> "'LINESTRING (-50 160, -50 50, -100 50)'",
      (
        2,
        "'LINESTRING(1 2 3, 1 1 1)'",
        "PI()/2") -> "LINESTRING Z(3 2 -0.9999999999999998, 1 1 -0.9999999999999999)")

    for (((index, geom, angle), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(s"""
                                   |SELECT
                                   |  ST_AsEWKT(
                                   |    ST_RotateY(
                                   |      ST_GeomFromEWKT($geom),
                                   |      $angle
                                   |    )
                                   |  ) AS geom
         """.stripMargin)

      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      assert(actual == expectedResult.stripPrefix("'").stripSuffix("'"))
    }
  }

  it("Should pass ST_Rotate") {
    val geomTestCases = Map(
      (
        1,
        "'LINESTRING (50 160, 50 50, 100 50)'",
        "PI()",
        "null",
        "null") -> "'LINESTRING (-50.00000000000002 -160, -50.00000000000001 -49.99999999999999, -100 -49.999999999999986)'",
      (
        2,
        "'LINESTRING (50 160, 50 50, 100 50)'",
        "PI()/6",
        "50.0",
        "160.0") -> "'LINESTRING (50 160, 104.99999999999999 64.73720558371174, 148.30127018922192 89.73720558371173)'",
      (
        3,
        "'SRID=4326;LINESTRING (50 160, 50 50, 100 50)'",
        "PI()/6",
        "null",
        "null") -> "'SRID=4326;LINESTRING (-36.69872981077805 163.5640646055102, 18.301270189221942 68.30127018922194, 61.60254037844388 93.30127018922192)'",
      (4, "'POINT EMPTY'", "PI()/6", "null", "null") -> "'POINT EMPTY'",
      (
        5,
        "'LINESTRING (50 160 10, 50 50 10, 100 50 10)'",
        "PI()",
        "null",
        "null") -> "'LINESTRING Z(-50.00000000000002 -160 10, -50.00000000000001 -49.99999999999999 10, -100 -49.999999999999986 10)'",
      (
        6,
        "'SRID=4326;GEOMETRYCOLLECTION(POINT(10 10), LINESTRING (50 160, 50 50, 100 50))'",
        "PI()",
        "null",
        "null") -> "'SRID=4326;GEOMETRYCOLLECTION (POINT (-10.000000000000002 -9.999999999999998), LINESTRING (-50.00000000000002 -160, -50.00000000000001 -49.99999999999999, -100 -49.999999999999986))'",
      (
        7,
        "'POINT (10 10)'",
        "PI()/4",
        "null",
        "null") -> "'POINT (0.0000000000000009 14.142135623730951)'",
      (
        8,
        "'SRID=0;POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'",
        "PI()/2",
        "5.0",
        "5.0") -> "'POLYGON ((10 -0.0000000000000003, 10 10, 0 10, 0 0.0000000000000003, 10 -0.0000000000000003))'",
      (
        9,
        "'MULTIPOINT ((1 1), (2 2), (3 3))'",
        "PI()/2",
        "null",
        "null") -> "'MULTIPOINT ((-0.9999999999999999 1), (-1.9999999999999998 2), (-3 3))'",
      (
        10,
        "'MULTILINESTRING ((0 0, 10 0), (0 0, 0 10))'",
        "PI()/4",
        "null",
        "null") -> "'MULTILINESTRING ((0 0, 7.0710678118654755 7.071067811865475), (0 0, -7.071067811865475 7.0710678118654755))'",
      (
        11,
        "'MULTIPOLYGON (((0 0, 5 0, 5 5, 0 5, 0 0)), ((10 10, 15 10, 15 15, 10 15, 10 10)))'",
        "PI()/2",
        "null",
        "null") -> "'MULTIPOLYGON (((0 0, 0.0000000000000003 5, -5 5, -5 0.0000000000000003, 0 0)), ((-10 10, -9.999999999999998 15, -14.999999999999998 15.000000000000002, -15 10.000000000000002, -10 10)))'")

    for (((index, geom, angle, x, y), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(s"""
           |SELECT
           |  ST_AsEWKT(
           |    ST_Rotate(
           |      ST_GeomFromEWKT($geom),
           |      $angle${if (x != "null" && y != "null") s", $x, $y" else ""}
           |    )
           |  ) AS geom
         """.stripMargin)

      val actual = df.take(1)(0).get(0).asInstanceOf[String]
      assert(actual == expectedResult.stripPrefix("'").stripSuffix("'"))
    }

    // Test invalid origin type
    val invalidOriginDf = sparkSession.sql("""
        |SELECT ST_Rotate(ST_GeomFromText('LINESTRING (50 160, 50 50, 100 50)'), PI()/6, ST_GeomFromText('LINESTRING (0 0, 1 1)')) as result
    """.stripMargin)

    val exception = intercept[Exception] {
      invalidOriginDf.collect()
    }
    exception.getMessage should include("The origin must be a non-empty Point geometry.")
  }

  it("Should pass ST_InterpolatePoint") {
    val geomTestCases = Map(
      ("'LINESTRING M (0 0 0, 2 0 2, 4 0 4)'", "'POINT(1 1)'") -> 1.0,
      ("'LINESTRING M (0 0 0, -2 2 2, -4 4 4)'", "'POINT(-1 1)'") -> 1.0,
      ("'LINESTRING M (0 0 0, 2 2 2, 4 0 4)'", "'POINT(2 0)'") -> 1.0,
      ("'LINESTRING M (0 0 0, 2 2 2, 4 0 4)'", "'POINT(2.5 1)'") -> 2.75)

    for (((geom1, geom2), expectedResult) <- geomTestCases) {
      val df = sparkSession.sql(s"""
                                   |SELECT
                                   |   ST_InterpolatePoint(
                                   |     ST_GeomFromEWKT($geom1),
                                   |     ST_GeomFromEWKT($geom2)
                                   |   ) AS mValue
         """.stripMargin)

      val actual = df.take(1)(0).get(0).asInstanceOf[Double]
      assert(actual == expectedResult)
    }

    // Test invalid arguments
    var invalidOriginDf = sparkSession.sql("""
                                             |SELECT ST_InterpolatePoint(ST_GeomFromText('LINESTRING (50 160, 50 50, 100 50)'), ST_GeomFromText('POINT (1 1)')) as result
    """.stripMargin)

    var exception = intercept[Exception] {
      invalidOriginDf.collect()
    }
    exception.getMessage should include("The given linestring does not have a measure value.")

    invalidOriginDf = sparkSession.sql("""
                                         |SELECT ST_InterpolatePoint(ST_GeomFromText('POINT (0 1)'), ST_GeomFromText('POINT (1 1)')) as result
    """.stripMargin)

    exception = intercept[Exception] {
      invalidOriginDf.collect()
    }
    exception.getMessage should include(
      "First argument is of type Point, should be a LineString.")

    invalidOriginDf = sparkSession.sql("""
                                         |SELECT ST_InterpolatePoint(ST_GeomFromText('LINESTRING M (0 0 0, 2 0 2, 4 0 4)'), ST_GeomFromText('LINESTRING (50 160, 50 50, 100 50)')) as result
    """.stripMargin)

    exception = intercept[Exception] {
      invalidOriginDf.collect()
    }
    exception.getMessage should include(
      "Second argument is of type LineString, should be a Point.")

    invalidOriginDf = sparkSession.sql("""
                                         |SELECT ST_InterpolatePoint(ST_SetSRID(ST_GeomFromText('LINESTRING M (0 0 0, 2 0 2, 4 0 4)'), 4326), ST_SetSRID(ST_GeomFromText('POINT (1 1)'), 3857)) as result
    """.stripMargin)

    exception = intercept[Exception] {
      invalidOriginDf.collect()
    }
    exception.getMessage should include(
      "The Line has SRID 4326 and Point has SRID 3857. The Line and Point should be in the same SRID.")
  }

}
