/*
 * FILE: predicateJoinTestScala.scala
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.geosparksql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.types._
import org.datasyslab.geospark.utils.GeoSparkConf

class predicateJoinTestScala extends TestBaseScala {

  describe("GeoSpark-SQL Predicate Join Test") {

    it("Passed ST_Contains in a join") {
      val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
      println(geosparkConf)

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Intersects in a join") {
      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.polygonshape,pointdf.pointshape) ")
      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Touches in a join") {
      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Touches(polygondf.polygonshape,pointdf.pointshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Within in a join") {
      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Within(pointdf.pointshape, polygondf.polygonshape) ")

      assert(rangeJoinDf.count() == 1000)
    }
    
    it("Passed ST_Overlaps in a join") {
      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var polygonCsvOverlapDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(overlapPolygonInputLocation)
      polygonCsvOverlapDf.createOrReplaceTempView("polygonoverlaptable")
      var polygonOverlapDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygonoverlaptable._c0 as Decimal(24,20)),cast(polygonoverlaptable._c1 as Decimal(24,20)), cast(polygonoverlaptable._c2 as Decimal(24,20)), cast(polygonoverlaptable._c3 as Decimal(24,20))) as polygonshape from polygonoverlaptable")
      polygonOverlapDf.createOrReplaceTempView("polygonodf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, polygonodf where ST_Overlaps(polygondf.polygonshape, polygonodf.polygonshape)")

      assert(rangeJoinDf.count() == 57)
    }

    it("Passed ST_Crosses in a join") {
      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Crosses(pointdf.pointshape, polygondf.polygonshape) ")

      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= radius in a join") {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var pointCsvDF1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF1.createOrReplaceTempView("pointtable")
      var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
      pointDf1.createOrReplaceTempView("pointdf1")

      var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable")
      var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
      pointDf2.createOrReplaceTempView("pointdf2")

      var distanceJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) <= 2")

      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < radius in a join") {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var pointCsvDF1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF1.createOrReplaceTempView("pointtable")
      var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
      pointDf1.createOrReplaceTempView("pointdf1")

      var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable")
      var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
      pointDf2.createOrReplaceTempView("pointdf2")

      var distanceJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")

      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Contains in a range and join") {
      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
        "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

      assert(rangeJoinDf.count() == 500)
    }

    it("Passed super small data join") {
      val rawPointDf = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(
          Seq(Row(1, "40.0", "-120.0"), Row(2, "30.0", "-110.0"), Row(3, "20.0", "-100.0"))),
        StructType(
          List(StructField("id", IntegerType, true), StructField("lat", StringType, true), StructField("lon", StringType, true))
        ))
      rawPointDf.createOrReplaceTempView("rawPointDf")

      val pointDF = sparkSession.sql("select id, ST_Point(cast(lat as Decimal(24,20)), cast(lon as Decimal(24,20))) AS latlon_point FROM rawPointDf")
      pointDF.createOrReplaceTempView("pointDf")

      val rawPolygonDf = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(
          Seq(Row("A", 25.0, -115.0, 35.0, -105.0), Row("B", 25.0, -135.0, 35.0, -125.0))),
        StructType(
          List(StructField("id", StringType, true), StructField("latmin", DoubleType, true),
            StructField("lonmin", DoubleType, true), StructField("latmax", DoubleType, true),
            StructField("lonmax", DoubleType, true))
        ))
      rawPolygonDf.createOrReplaceTempView("rawPolygonDf")

      val polygonEnvelopeDF = sparkSession.sql("select id, ST_PolygonFromEnvelope(" +
        "cast(latmin as Decimal(24,20)), cast(lonmin as Decimal(24,20)), " +
        "cast(latmax as Decimal(24,20)), cast(lonmax as Decimal(24,20))) AS polygon FROM rawPolygonDf")
      polygonEnvelopeDF.createOrReplaceTempView("polygonDf")

      val withinEnvelopeDF = sparkSession.sql("select * FROM pointDf, polygonDf WHERE ST_Within(pointDf.latlon_point, polygonDf.polygon)")
      assert(withinEnvelopeDF.count() == 1)
    }
    it("Passed ST_Equals in a join for ST_Point") {
      var pointCsvDf1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPoint1InputLocation)
      pointCsvDf1.createOrReplaceTempView("pointtable1")
      var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable1._c0 as Decimal(24,20)),cast(pointtable1._c1 as Decimal(24,20)) ) as pointshape1 from pointtable1")
      pointDf1.createOrReplaceTempView("pointdf1")

      var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPoint2InputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable2")
      var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable2._c0 as Decimal(24,20)),cast(pointtable2._c1 as Decimal(24,20))) as pointshape2 from pointtable2")
      pointDf2.createOrReplaceTempView("pointdf2")

      var equalJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Equals(pointdf1.pointshape1,pointdf2.pointshape2) ")

      assert(equalJoinDf.count() == 100, s"Expected 100 but got ${equalJoinDf.count()}")
    }
    it("Passed ST_Equals in a join for ST_Polygon") {
      var polygonCsvDf1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1InputLocation)
      polygonCsvDf1.createOrReplaceTempView("polygontable1")
      var polygonDf1 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable1._c0 as Decimal(24,20)),cast(polygontable1._c1 as Decimal(24,20)), cast(polygontable1._c2 as Decimal(24,20)), cast(polygontable1._c3 as Decimal(24,20))) as polygonshape1 from polygontable1")
      polygonDf1.createOrReplaceTempView("polygondf1")


      var polygonCsvDf2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon2InputLocation)
      polygonCsvDf2.createOrReplaceTempView("polygontable2")
      var polygonDf2 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable2._c0 as Decimal(24,20)),cast(polygontable2._c1 as Decimal(24,20)), cast(polygontable2._c2 as Decimal(24,20)), cast(polygontable2._c3 as Decimal(24,20))) as polygonshape2 from polygontable2")
      polygonDf2.createOrReplaceTempView("polygondf2")

      var equalJoinDf = sparkSession.sql("select * from polygondf1, polygondf2 where ST_Equals(polygondf1.polygonshape1,polygondf2.polygonshape2) ")

      assert(equalJoinDf.count() == 100, s"Expected 100 but got ${equalJoinDf.count()}")
    }
    it("Passed ST_Equals in a join for ST_Polygon Random Shuffle") {
      var polygonCsvDf1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1RandomInputLocation)
      polygonCsvDf1.createOrReplaceTempView("polygontable1")
      var polygonDf1 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable1._c0 as Decimal(24,20)),cast(polygontable1._c1 as Decimal(24,20)), cast(polygontable1._c2 as Decimal(24,20)), cast(polygontable1._c3 as Decimal(24,20))) as polygonshape1 from polygontable1")
      polygonDf1.createOrReplaceTempView("polygondf1")


      var polygonCsvDf2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon2RandomInputLocation)
      polygonCsvDf2.createOrReplaceTempView("polygontable2")
      var polygonDf2 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable2._c0 as Decimal(24,20)),cast(polygontable2._c1 as Decimal(24,20)), cast(polygontable2._c2 as Decimal(24,20)), cast(polygontable2._c3 as Decimal(24,20))) as polygonshape2 from polygontable2")
      polygonDf2.createOrReplaceTempView("polygondf2")

      var equalJoinDf = sparkSession.sql("select * from polygondf1, polygondf2 where ST_Equals(polygondf1.polygonshape1,polygondf2.polygonshape2) ")

      assert(equalJoinDf.count() == 100, s"Expected 100 but got ${equalJoinDf.count()}")
    }
    it("Passed ST_Equals in a join for ST_Point and ST_Polygon") {
      var pointCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPoint1InputLocation)
      pointCsvDf.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)) ) as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygon1InputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")

      var equalJoinDf = sparkSession.sql("select * from pointdf, polygondf where ST_Equals(pointdf.pointshape,polygondf.polygonshape) ")

      assert(equalJoinDf.count() == 0, s"Expected 0 but got ${equalJoinDf.count()}")
    }
  }
}
