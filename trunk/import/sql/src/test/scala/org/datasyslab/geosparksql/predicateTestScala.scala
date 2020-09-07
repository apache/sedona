/*
 * FILE: predicateTestScala.scala
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

class predicateTestScala extends TestBaseScala {

  describe("GeoSpark-SQL Predicate Test") {

    it("Passed ST_Contains") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql("select * from pointdf where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
      resultDf.show()
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Intersects") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql("select * from pointdf where ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
      resultDf.show()
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Within") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql("select * from pointdf where ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))")
      resultDf.show()
      assert(resultDf.count() == 999)
    }

    it("Passed ST_Equals for ST_Point") {
      // Select a point from the table and check if any point in the table is equal to the selected point.

      // Read csv to get the points table
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPoint1InputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")

      // Convert the pointtable to pointdf using ST_Point
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as point from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var equaldf = sparkSession.sql("select * from pointdf where ST_Equals(pointdf.point, ST_Point(100.1, 200.1)) ")
      equaldf.show()

      assert(equaldf.count() == 5, s"Expected 5 value but got ${equaldf.count()}")

    }

    it("Passed ST_Equals for ST_Polygon") {

      // Select a polygon from the table and check if any polygon in the table is equal to the selected polygon.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      // Selected polygon is Polygon (100.01,200.01,100.5,200.5)
      var equaldf1 = sparkSession.sql("select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromEnvelope(100.01,200.01,100.5,200.5)) ")
      equaldf1.show()

      assert(equaldf1.count() == 5, s"Expected 5 value but got ${equaldf1.count()}")

      // Change the order of the polygon points (100.5,200.5,100.01,200.01)
      var equaldf2 = sparkSession.sql("select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromEnvelope(100.5,200.5,100.01,200.01)) ")
      equaldf2.show()

      assert(equaldf2.count() == 5, s"Expected 5 value but got ${equaldf2.count()}")

    }

    it("Passed ST_Equals for ST_Point and ST_Polygon") {

      // Test a Point against any polygon in the table for equality.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope and cast
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      // Selected point is Point (91.01,191.01)
      var equaldf = sparkSession.sql("select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_Point(91.01,191.01)) ")
      equaldf.show()

      assert(equaldf.count() == 0, s"Expected 0 value but got ${equaldf.count()}")

    }

    it("Passed ST_Equals for ST_LineString and ST_Polygon") {

      // Test a LineString against any polygon in the table for equality.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope and cast
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      /* Selected LineString is ST_LineStringFromText - (100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01)
       * It forms the boundary of the polygon Polygon(100.01,200.01,100.5,200.5)
       * x1 = 100.01, y1 = 200.01, x2 = 100.5, y2 = 200.5
       * LineString(P1, P2, P3, P4) -
       * P1->100.01,200.01
       * P2->100.5,200.01
       * P3->100.5,200.5
       * P4->100.01,200.5
       * P5->100.01,200.01
       */
      val string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

      var equaldf = sparkSession.sql(s"select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_LineStringFromText(\'$string\', \',\')) ")
      equaldf.show()

      assert(equaldf.count() == 0, s"Expected 0 value but got ${equaldf.count()}")

    }
    it("Passed ST_Equals for ST_PolygonFromEnvelope and ST_PolygonFromText") {

      // Test a Polygon formed using ST_PolygonFromText against any polygon in the table formed using ST_PolygonFromEnvelope for equality.

      // Read csv to get the polygon table
      var polygonCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1InputLocation)
      polygonCsvDF.createOrReplaceTempView("polygontable")

      // Convert the polygontable to polygons using ST_PolygonFromEnvelope and cast
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      // Selected Polygon is ST_PolygonFromText - Polygon(100.01,200.01,100.5,200.5) formed using ST_PolygonFromText.
      val string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

      var equaldf = sparkSession.sql(s"select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromText(\'$string\', \',\')) ")
      equaldf.show()

      assert(equaldf.count() == 5, s"Expected 5 value but got ${equaldf.count()}")
    }

    it("Passed ST_Crosses") {
      var crossesTesttable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') as b")
      crossesTesttable.createOrReplaceTempView("crossesTesttable")
      var crosses = sparkSession.sql("select(ST_Crosses(a, b)) from crossesTesttable")

      var notCrossesTesttable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))') as b")
      notCrossesTesttable.createOrReplaceTempView("notCrossesTesttable")
      var notCrosses = sparkSession.sql("select(ST_Crosses(a, b)) from notCrossesTesttable")

      assert(crosses.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notCrosses.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_Touches") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var resultDf = sparkSession.sql("select * from pointdf where ST_Touches(pointdf.arealandmark, ST_PolygonFromEnvelope(0.0,99.0,1.1,101.1))")
      resultDf.show()
      assert(resultDf.count() == 1)
    }
    it("Passed ST_Overlaps") {
      var testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((2.5 2.5, 2.5 4.5, 4.5 4.5, 4.5 2.5, 2.5 2.5))') as a,ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))') as b, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as c, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as d")
      testtable.createOrReplaceTempView("testtable")
      var overlaps = sparkSession.sql("select ST_Overlaps(a,b) from testtable")
      var notoverlaps = sparkSession.sql("select ST_Overlaps(c,d) from testtable")
      assert(overlaps.take(1)(0).get(0) == true)
      assert(notoverlaps.take(1)(0).get(0) == false)
    }
  }
}
