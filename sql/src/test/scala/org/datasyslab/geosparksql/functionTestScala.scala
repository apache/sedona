/*
 * FILE: functionTestScala.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.{Geometry}
import org.geotools.geometry.jts.WKTReader2
import org.scalatest.{GivenWhenThen, Matchers}
import implicits._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

class functionTestScala extends TestBaseScala with Matchers with GeometrySample with GivenWhenThen{

  import sparkSession.implicits._

  describe("GeoSpark-SQL Function Test") {

    it("Passed ST_ConvexHull") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_ConvexHull(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Buffer") {
      val polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      val polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      val functionDf = sparkSession.sql("select ST_Buffer(polygondf.countyshape, 1) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Envelope") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Envelope(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Centroid") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Centroid(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Length") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Length(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Area") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Area(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Distance") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Transform") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false) from polygondf")
      functionDf.show()

      val polygon = "POLYGON ((110.54671 55.818002, 110.54671 55.143743, 110.940494 55.143743, 110.940494 55.818002, 110.54671 55.818002))"
      val forceXYExpect = "POLYGON ((471596.69167460164 6185916.951191288, 471107.5623640998 6110880.974228167, 496207.109151055 6110788.804712435, 496271.31937046186 6185825.60569904, 471596.69167460164 6185916.951191288))"

      sparkSession.createDataset(Seq(polygon))
        .withColumn("geom", expr("ST_GeomFromWKT(value)"))
        .createOrReplaceTempView("df")

      sparkSession.sql("select ST_Transform(geom, 'EPSG:4326', 'EPSG:32649', false, false)  from df")
        .show(false)

      sparkSession.sql("select ST_Transform(geom, 'EPSG:4326', 'EPSG:32649', true, false)  from df")
        .show(false)

      val forceXYResult = sparkSession.sql(s"""select ST_Transform(ST_geomFromWKT('$polygon'),'EPSG:4326', 'EPSG:32649', true, false)""").rdd.map(row => row.getAs[Geometry](0).toString).collect()(0)
      assert(forceXYResult == forceXYExpect)

    }

    it("Passed ST_Intersection - intersects but not contains") {

      val testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"))
    }

    it("Passed ST_Intersection - intersects but left contains right") {

      val testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 2, 2 3, 3 3, 2 2))"))
    }

    it("Passed ST_Intersection - intersects but right contains left") {

      val testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as a,ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 2, 2 3, 3 3, 2 2))"))
    }

    it("Passed ST_Intersection - not intersects") {

      val testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((40 21, 40 22, 40 23, 40 21))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersect = sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersect.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON EMPTY"))
    }

    it("Passed ST_IsValid") {

      var testtable = sparkSession.sql(
        "SELECT ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))')) AS a, " +
          "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')) as b"
      )
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(testtable.take(1)(0).get(1).asInstanceOf[Boolean])
    }

    it("Fixed nullPointerException in ST_IsValid") {

      var testtable = sparkSession.sql(
        "SELECT ST_IsValid(null)"
      )
      assert(testtable.take(1).head.get(0) == null)
    }

    it("Passed ST_PrecisionReduce") {
      var testtable = sparkSession.sql(
        """
          |SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 8)
        """.stripMargin)
      testtable.show(false)
      assert(testtable.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates()(0).x == 0.12345679)
      testtable = sparkSession.sql(
        """
          |SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 11)
        """.stripMargin)
      testtable.show(false)
      assert(testtable.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates()(0).x == 0.12345678901)

    }

    it("Passed ST_IsSimple") {

      var testtable = sparkSession.sql(
        "SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')) AS a, " +
          "ST_IsSimple(ST_GeomFromText('POLYGON((1 1,3 1,3 3,2 0,1 1))')) as b"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!testtable.take(1)(0).get(1).asInstanceOf[Boolean])
    }

    it("Passed ST_MakeValid On Invalid Polygon") {

      val df = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS polygon")
      df.createOrReplaceTempView("table")

      val result = sparkSession.sql(
        """
          |SELECT geometryValid.polygon
          |FROM table
          |LATERAL VIEW ST_MakeValid(polygon, false) geometryValid AS polygon
          |""".stripMargin
      ).collect()

      val wktReader = new WKTReader2()
      val firstValidGeometry = wktReader.read("POLYGON ((1 5, 3 3, 1 1, 1 5))")
      val secondValidGeometry = wktReader.read("POLYGON ((5 3, 7 5, 7 1, 5 3))")

      assert(result.exists(row => row.getAs[Geometry](0).equals(firstValidGeometry)))
      assert(result.exists(row => row.getAs[Geometry](0).equals(secondValidGeometry)))
    }

    it("Passed ST_MakeValid On Invalid MultiPolygon") {

      val df = sparkSession.sql("SELECT ST_GeomFromWKT('MULTIPOLYGON(((0 0, 3 0, 3 3, 0 3, 0 0)), ((3 0, 6 0, 6 3, 3 3, 3 0)))') AS multipolygon")
      df.createOrReplaceTempView("table")

      val result = sparkSession.sql(
        """
          |SELECT geometryValid.multipolygon
          |FROM table
          |LATERAL VIEW ST_MakeValid(multipolygon, false) geometryValid AS multipolygon
          |""".stripMargin
      ).collect()

      val wktReader = new WKTReader2()
      val firstValidGeometry = wktReader.read("POLYGON ((0 3, 3 3, 3 0, 0 0, 0 3))")
      val secondValidGeometry = wktReader.read("POLYGON ((3 3, 6 3, 6 0, 3 0, 3 3))")

      assert(result.exists(row => row.getAs[Geometry](0).equals(firstValidGeometry)))
      assert(result.exists(row => row.getAs[Geometry](0).equals(secondValidGeometry)))
    }

    it("Passed ST_MakeValid On Valid Polygon") {

      val df = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS polygon")
      df.createOrReplaceTempView("table")

      val result = sparkSession.sql(
        """
          |SELECT geometryValid.polygon
          |FROM table
          |LATERAL VIEW ST_MakeValid(polygon, false) geometryValid AS polygon
          |""".stripMargin
      ).collect()

      val wktReader = new WKTReader2()
      val validGeometry = wktReader.read("POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))")

      assert(result.exists(row => row.getAs[Geometry](0).equals(validGeometry)))
    }

    it("Passed ST_SimplifyPreserveTopology") {

      val testtable = sparkSession.sql(
        "SELECT ST_SimplifyPreserveTopology(ST_GeomFromText('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 10) AS b"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 35 36, 43 19, 24 39, 8 25))"))
    }

    it("Passed ST_AsText") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      var wktDf = sparkSession.sql("select ST_AsText(countyshape) as wkt from polygondf")
      assert(polygonDf.take(1)(0).getAs[Geometry]("countyshape").toText.equals(wktDf.take(1)(0).getAs[String]("wkt")))
    }

    it("Passed ST_AsGeoJSON") {
      val df = sparkSession.sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS polygon")
      df.createOrReplaceTempView("table")

      val geojsonDf = sparkSession.sql(
        """select ST_AsGeoJSON(polygon) as geojson
          |from table""".stripMargin)

      val expectedGeoJson = """{"type":"Polygon","coordinates":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]}"""
      assert(geojsonDf.first().getString(0) === expectedGeoJson)
    }

    it("Passed ST_NPoints") {
      var test = sparkSession.sql("SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[Int] == 4)

    }

    it("Passed ST_GeometryType") {
      var test = sparkSession.sql("SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
      assert(test.take(1)(0).get(0).asInstanceOf[String].toUpperCase() == "ST_LINESTRING")
    }


    it("Passed ST_Azimuth") {

      val pointDataFrame = samplePoints
        .map(point => (point, samplePoints.tail.head))
        .toDF("geomA", "geomB")

      pointDataFrame.selectExpr("ST_Azimuth(geomA, geomB)")
        .as[Double]
        .map(180 / math.Pi * _)
        .collect() should contain theSameElementsAs List(
        240.0133139011053, 0.0, 270.0, 286.8042682202057, 315.0, 314.9543472191815, 315.0058223408927,
        245.14762725688198, 314.84984546897755, 314.8868529256147, 314.9510567053395, 314.95443984912936,
        314.89925480835245, 314.6018799143881, 314.6834083423315, 314.80689827870725, 314.90290827689506,
        314.90336326341765, 314.7510398533675, 314.73608518601935

      )

      val geometries = Seq(
        ("POINT(25.0 45.0)", "POINT(75.0 100.0)"),
        ("POINT(75.0 100.0)", "POINT(25.0 45.0)"),
        ("POINT(0.0 0.0)", "POINT(25.0 0.0)"),
        ("POINT(25.0 0.0)", "POINT(0.0 0.0)"),
        ("POINT(0.0 25.0)", "POINT(0.0 0.0)"),
        ("POINT(0.0 0.0)", "POINT(0.0 25.0)")
      ).map({ case (wktA, wktB) => (wktReader.read(wktA), wktReader.read(wktB)) })
        .toDF("geomA", "geomB")

      geometries
        .selectExpr("ST_Azimuth(geomA, geomB)")
        .show
      geometries
        .selectExpr("ST_Azimuth(geomA, geomB)")
        .as[Double]
        .map(180 / math.Pi * _)
        .collect()
        .toList should contain theSameElementsAs List(
        42.27368900609374, 222.27368900609375,
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

      points should contain theSameElementsAs List(-71.064544, -88.331492, 88.331492, 1.0453, 32.324142)

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

      points should contain theSameElementsAs List(42.28787, 32.324142, 32.324142, 5.3324324, -88.331492)

      And("LineString count should be 0")
      linestrings.length shouldBe 0

      And("Polygon count should be 0")
      polygons.length shouldBe 0

    }
    it("Should pass ST_StartPoint function") {
      Given("Polygon Data Frame, Point DataFrame, LineString Data Frame")

      val pointDF = createSamplePointDf(5, "geom")
      val polygonDF = createSamplePolygonDf(5, "geom")
      val lineStringDF = createSampleLineStringsDf(5, "geom")

      When("Running ST_StartPoint on Point Data Frame, LineString DataFrame and Polygon DataFrame")
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
  }

  it("Should pass ST_Boundary"){
    Given("Sample geometry data frame")
    val geometryTable = Seq(
      "LINESTRING(1 1,0 0, -1 1)",
      "LINESTRING(100 150,50 60, 70 80, 160 170)",
      "POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))",
      "POLYGON((1 1,0 0, -1 1, 1 1))"
    ).map(geom => Tuple1(wktReader.read(geom))).toDF("geom")

    When("Using ST_Boundary function")

    val boundaryTable = geometryTable.selectExpr("ST_Boundary(geom) as geom")

    Then("Result should match List of boundary geometries")

    boundaryTable.selectExpr("ST_AsText(geom)")
      .as[String].collect().toList should contain theSameElementsAs List(
      "MULTIPOINT ((1 1), (-1 1))",
      "MULTIPOINT ((100 150), (160 170))",
      "MULTILINESTRING ((10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130), (70 40, 100 50, 120 80, 80 110, 50 90, 70 40))",
      "LINESTRING (1 1, 0 0, -1 1, 1 1)"
    )
  }

  it("Should pass ST_EndPoint"){
    Given("Dataframe with linestring and dataframe with other geometry types")
    val lineStringDataFrame = createSampleLineStringsDf(5, "geom")
    val otherGeometryDataFrame = createSamplePolygonDf(5, "geom")
      .union(createSamplePointDf(5, "geom"))

    When("Using ST_EndPoint")
    val pointDataFrame = lineStringDataFrame.selectExpr("ST_EndPoint(geom) as geom")
      .filter("geom IS NOT NULL")
    val emptyDataFrame = otherGeometryDataFrame.selectExpr("ST_EndPoint(geom) as geom")
      .filter("geom IS NOT NULL")

    Then("Linestring Df should result with Point Df and other geometry DF as empty DF")
    pointDataFrame.selectExpr("ST_AsText(geom)")
      .as[String]
      .collect()
      .toList should contain theSameElementsAs expectedEndingPoints
    emptyDataFrame.count shouldBe 0
  }

  it("Should pass ST_ExteriorRing"){
    Given("Polygon DataFrame and other geometries DataFrame")
    val polygonDf = createSimplePolygons(5, "geom")
      .union(Seq("POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))")
        .map(wkt => Tuple1(wktReader.read(wkt))).toDF("geom")
      )

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

    lineStringDf.selectExpr("ST_AsText(geom)")
      .as[String]
      .collect() should contain theSameElementsAs List(
      "LINESTRING (0 0, 0 1, 1 1, 1 0, 0 0)", "LINESTRING (0 0, 1 1, 1 2, 1 1, 0 0)"
    )

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

  it("Should pass ST_InteriorRingN"){
    Given("DataFrame with polygons and DataFrame with other geometries ")
    val polygonDf = Seq(
      "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))")
      .map(wkt => Tuple1(wktReader.read(wkt))).toDF("geom")

    val otherGeometry = createSamplePointDf(5, "geom")
      .union(createSampleLineStringsDf(5, "geom"))

    When("Using ST_InteriorRingN")
    val wholes = (0 until 3).flatMap(
      index => polygonDf.selectExpr(s"ST_InteriorRingN(geom, $index) as geom")
        .selectExpr("ST_AsText(geom)").as[String].collect()
    )

    val emptyDf = otherGeometry.selectExpr("ST_InteriorRingN(geom, 1) as geom")
      .filter("geom IS NOT NULL")

    Then("Polygon with wholes should return Nth whole and other geometries should produce null values")

    emptyDf.count shouldBe 0
    wholes should contain theSameElementsAs List(
      "LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)", "LINESTRING (1 3, 2 3, 2 4, 1 4, 1 3)",
      "LINESTRING (3 3, 4 3, 4 4, 3 4, 3 3)"
    )

  }

  it("Should pass ST_Dumps"){
    Given("Geometries Df")
    val geometryDf = Seq(
      "Point(21 52)",
      "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))",
      "Linestring(0 0, 1 1, 1 0)",
      "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
      "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
      "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
      "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))",
      "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"
    ).map(wkt => Tuple1(wktReader.read(wkt))).toDF("geom")

    When("Using ST_Dumps")
    val dumpedGeometries = geometryDf.selectExpr("ST_Dump(geom) as geom")
    dumpedGeometries.show(10, false)
    Then("Should return geometries list")

    dumpedGeometries.select(explode($"geom")).count shouldBe 14
    dumpedGeometries
      .select(explode($"geom").alias("geom"))
      .selectExpr("ST_AsText(geom) as geom")
      .as[String]
      .collect() should contain theSameElementsAs List(
      "POINT (21 52)", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", "LINESTRING (0 0, 1 1, 1 0)",
      "POINT (10 40)", "POINT (40 30)", "POINT (20 20)", "POINT (30 10)", "POLYGON ((30 20, 45 40, 10 40, 30 20))",
      "POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))", "LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)",
      "POLYGON ((40 40, 20 45, 45 30, 40 40))", "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))",
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"
    )

  }

  it("Should pass ST_DumpPoints"){
    Given("DataFrame with geometries")
    val geometryDf = createSampleLineStringsDf(1, "geom")
      .union(createSamplePointDf(1, "geom"))
      .union(createSimplePolygons(1, "geom"))

    When("Using ST_DumpPoints and explode")

    val dumpedPoints = geometryDf.selectExpr("ST_DumpPoints(geom) as geom")
      .select(explode($"geom").alias("geom"))

    Then("Number of rows should match and collected to list should match expected point list")
    dumpedPoints.count shouldBe 10
    dumpedPoints.selectExpr("ST_AsText(geom)")
      .as[String].collect().toList should contain theSameElementsAs List(
      "POINT (-112.506968 45.98186)",
      "POINT (-112.506968 45.983586)",
      "POINT (-112.504872 45.983586)",
      "POINT (-112.504872 45.98186)",
      "POINT (-71.064544 42.28787)",
      "POINT (0 0)", "POINT (0 1)",
      "POINT (1 1)", "POINT (1 0)",
      "POINT (0 0)"
    )
  }

  it("Should pass ST_IsClosed"){
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
      (10, "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))")
    ).map({case (index, wkt) => Tuple2(index, wktReader.read(wkt))}).toDF("id", "geom")

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
      (10, false)
    )
  }


  it("Should pass ST_NumInteriorRings"){
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
      (10, "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))"),
      (11, "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))")
    ).map({case (index, wkt) => Tuple2(index, wktReader.read(wkt))}).toDF("id", "geom")

    When("Using ST_NumInteriorRings")
    val numberOfInteriorRings = geometryDf.selectExpr(
      "id", "ST_NumInteriorRings(geom) as num"
    )

    Then("Result should match with expected values")

    numberOfInteriorRings
      .filter("num is not null")
      .as[(Int, Int)]
      .collect().toList should contain theSameElementsAs List((2, 0), (11, 1))
  }

  it("Should pass ST_AddPoint"){
    Given("Geometry df")
    val geometryDf = Seq(
      ("Point(21 52)", "Point(21 52)"),
      ("Point(21 52)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      ("Linestring(0 0, 1 1, 1 0)", "Point(21 52)"),
      ("Linestring(0 0, 1 1, 1 0, 0 0)", "Linestring(0 0, 1 1, 1 0, 0 0)"),
      ("Point(21 52)", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      ("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))", "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
      ("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))", "Point(21 52)"),
      ("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", "Point(21 52)")
    ).map({case (geomA, geomB) => Tuple2(wktReader.read(geomA), wktReader.read(geomB))}).toDF("geomA", "geomB")
    When("Using ST_AddPoint")
    val modifiedGeometries = geometryDf.selectExpr("ST_AddPoint(geomA, geomB) as geom")

    Then("Result should match")

    modifiedGeometries
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String].collect() should contain theSameElementsAs List(
      "LINESTRING (0 0, 1 1, 1 0, 21 52)"
    )
  }

  it("Should pass ST_AddPoint with index"){
    Given("Geometry df")
    val geometryDf = Seq(
      ("Point(21 52)", "Point(21 52)"),
      ("Point(21 52)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"),
      ("Linestring(0 0, 1 1, 1 0)", "Point(21 52)"),
      ("Linestring(0 0, 1 1, 1 0, 0 0)", "Linestring(0 0, 1 1, 1 0, 0 0)"),
      ("Point(21 52)", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
      ("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))", "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
      ("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))", "Point(21 52)"),
      ("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40), POLYGON ((40 40, 20 45, 45 30, 40 40)))", "Point(21 52)"),
      ("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", "Point(21 52)")
    ).map({case (geomA, geomB) => Tuple2(wktReader.read(geomA), wktReader.read(geomB))}).toDF("geomA", "geomB")
    When("Using ST_AddPoint")

    val modifiedGeometries = geometryDf.
      selectExpr("ST_AddPoint(geomA, geomB, 1) as geom")
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 0) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 2) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 3) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, 4) as geom"))
      .union(geometryDf.selectExpr("ST_AddPoint(geomA, geomB, -1) as geom"))

    Then("Result should match")

    modifiedGeometries
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)")
      .as[String].collect() should contain theSameElementsAs List(
      "LINESTRING (0 0, 21 52, 1 1, 1 0)",
      "LINESTRING (21 52, 0 0, 1 1, 1 0)",
      "LINESTRING (0 0, 1 1, 21 52, 1 0)",
      "LINESTRING (0 0, 1 1, 1 0, 21 52)",
      "LINESTRING (0 0, 1 1, 1 0, 21 52)"
    )
  }

  it("Should correctly remove using ST_RemovePoint"){
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 0) shouldBe Some("LINESTRING (1 1, 1 0, 0 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1)", 0) shouldBe None
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 1) shouldBe Some("LINESTRING (0 0, 1 0, 0 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 2) shouldBe Some("LINESTRING (0 0, 1 1, 0 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 3) shouldBe Some("LINESTRING (0 0, 1 1, 1 0)")
    calculateStRemovePointOption("Linestring(0 0, 1 1, 1 0, 0 0)", 4) shouldBe None
    calculateStRemovePointOption("POINT(0 1)", 3) shouldBe None
    calculateStRemovePointOption("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))", 3) shouldBe None
    calculateStRemovePointOption("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40))", 0) shouldBe None
    calculateStRemovePointOption("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 3) shouldBe None
    calculateStRemovePointOption("MULTILINESTRING ((10 10, 20 20, 10 40, 10 10), (40 40, 30 30, 40 20, 30 10, 40 40))", 3) shouldBe None
  }

  it("Should pass ST_IsRing"){
    calculateStIsRing("LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)") shouldBe Some(false)
    calculateStIsRing("LINESTRING(2 0, 2 2, 3 3)") shouldBe Some(false)
    calculateStIsRing("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)") shouldBe Some(true)
    calculateStIsRing("POINT (21 52)") shouldBe None
    calculateStIsRing("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))") shouldBe None
  }

  private def calculateGeometryN(wkt: String, index: Int): Option[Row] = {
    Seq(wkt).map(wkt => Tuple1(wktReader.read(wkt))).toDF("geom")
      .selectExpr(s"ST_GeometryN(geom, $index) as geom")
      .filter("geom is not null")
      .selectExpr(s"ST_AsText(geom) as geom_text")
      .toSeqOption
  }

  private def calculateStIsRing(wkt: String): Option[Boolean] =
    wktToDf(wkt).selectExpr("ST_IsRing(geom) as is_ring")
      .filter("is_ring is not null").as[Boolean].collect().headOption

  private def calculateStRemovePointOption(wkt: String, index: Int): Option[String] =
    calculateStRemovePoint(wkt, index).headOption

  private def calculateStRemovePoint(wkt: String, index: Int): Array[String] =
    wktToDf(wkt).selectExpr(s"ST_RemovePoint(geom, $index) as geom")
      .filter("geom is not null")
      .selectExpr("ST_AsText(geom)").as[String].collect()

  private def wktToDf(wkt: String): DataFrame =
    Seq(Tuple1(wktReader.read(wkt))).toDF("geom")

  private val expectedStartingPoints = List(
    "POINT (-112.506968 45.98186)",
    "POINT (-112.519856 45.983586)",
    "POINT (-112.504872 45.919281)",
    "POINT (-112.574945 45.987772)",
    "POINT (-112.520691 42.912313)"
  )

  private val expectedEndingPoints = List(
    "POINT (-112.504872 45.98186)",
    "POINT (-112.506968 45.983586)",
    "POINT (-112.41643 45.919281)",
    "POINT (-112.519856 45.987772)",
    "POINT (-112.442664 42.912313)"
  )
  
   it("Passed ST_NumGeometries"){
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
    val testDF = testData.selectExpr("Geometry", "ST_NumGeometries(ST_GeomFromText(Geometry)) as ngeom")

    Then("Result should match")
    testDF.selectExpr("ngeom")
      .as[Int].collect() should contain theSameElementsAs List(1, 2, 1, 2, 3)
   }

  it("Passed ST_LineMerge"){
    Given("Some different types of geometries in a DF")
    val testData = Seq(
      ("MULTILINESTRING ((-29 -27, -30 -29.7, -45 -33), (-45 -33, -46 -32))"),
      ("MULTILINESTRING ((-29 -27, -30 -29.7, -36 -31, -45 -33), (-45.2 -33.2, -46 -32))"),
      ("POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 35 36, 43 19, 24 39, 8 25))")
    ).toDF("Geometry")

    When("Using ST_LineMerge")
    val testDF = testData.selectExpr("ST_LineMerge(ST_GeomFromText(Geometry)) as geom")

    Then("Result should match")
    testDF.selectExpr("ST_AsText(geom)")
      .as[String].collect() should contain theSameElementsAs
      List("LINESTRING (-29 -27, -30 -29.7, -45 -33, -46 -32)",
        "MULTILINESTRING ((-29 -27, -30 -29.7, -36 -31, -45 -33), (-45.2 -33.2, -46 -32))",
        "GEOMETRYCOLLECTION EMPTY")
  }

}
