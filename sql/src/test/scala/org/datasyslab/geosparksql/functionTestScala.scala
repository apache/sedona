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

import com.vividsolutions.jts.geom.Geometry

class functionTestScala extends TestBaseScala {

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
    }

    it("Passed ST_Intersection") {

      var testtable=sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      var intersec=sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"))
    }

    it("Passed ST_IsValid") {

      var testtable=sparkSession.sql(
        "SELECT ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))')) AS a, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')) as b"
      )
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(testtable.take(1)(0).get(1).asInstanceOf[Boolean])
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
  }
}
