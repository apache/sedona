/*
 * FILE: constructorTestScala.scala
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

import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter

class constructorTestScala extends TestBaseScala {

  describe("GeoSpark-SQL Constructor Test") {

    it("Passed ST_Point") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      assert(pointDf.count() == 1000)
    }

    it("Passed ST_PointFromText") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show(false)

      var pointDf = sparkSession.sql("select ST_PointFromText(concat(_c0,',',_c1),',') as arealandmark from pointtable")
      assert(pointDf.count() == 121960)
    }

    it("Passed ST_GeomFromWKT") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWkt(polygontable._c0) as countyshape from polygontable")
      polygonDf.show(10)
      assert(polygonDf.count() == 100)
    }

    it("Passed ST_GeomFromText") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0) as countyshape from polygontable")
      polygonDf.show(10)
      assert(polygonDf.count() == 100)
    }

    it("Passed ST_GeomFromWKB") {
      var polygonWkbDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation)
      polygonWkbDf.createOrReplaceTempView("polygontable")
      polygonWkbDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKB(polygontable._c0) as countyshape from polygontable")
      polygonDf.show(10)
      assert(polygonDf.count() == 100)
    }

    it("Passed GeoJsonReader to DataFrame") {
      var spatialRDD = GeoJsonReader.readToGeometryRDD(sparkSession.sparkContext, geojsonInputLocation)
      var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
      spatialDf.show()
    }

    it("Read shapefile -> DataFrame > RDD -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      df.show
      assert (df.columns(1) == "STATEFP")
      import org.apache.spark.sql.functions.{callUDF, col}
      df = df.withColumn("geometry", callUDF("ST_GeomFromWKT", col("geometry")))
      df.show()
      var spatialRDD2 = Adapter.toSpatialRdd(df, "geometry")
      println(spatialRDD2.rawSpatialRDD.take(1).get(0).getUserData)
      Adapter.toDf(spatialRDD2, sparkSession).show()
    }
  }
}
