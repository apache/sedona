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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.geosparksql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.types._
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class predicateJoinTestScala extends FunSpec with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  override def afterAll(): Unit = {
    //GeoSparkSQLRegistrator.dropAll(sparkSession)
    //sparkSession.stop
  }

  describe("GeoSpark-SQL Predicate Join Test") {
    sparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

    val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
    val csvPointInputLocation = resourceFolder + "testpoint.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/polygon"

    it("Passed ST_Contains in a join") {
      val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
      println(geosparkConf)

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      polygonCsvDf.show()
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20)), \"mypolygonid\") as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) ")

      rangeJoinDf.explain()
      rangeJoinDf.show(3)
      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Intersects in a join") {
      val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
      println(geosparkConf)

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      polygonCsvDf.show()
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20)), \"mypolygonid\") as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.polygonshape,pointdf.pointshape) ")

      rangeJoinDf.explain()
      rangeJoinDf.show(3)
      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Within in a join") {
      val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
      println(geosparkConf)

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      polygonCsvDf.show()
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20)), \"mypolygonid\") as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Within(pointdf.pointshape, polygondf.polygonshape) ")

      rangeJoinDf.explain()
      rangeJoinDf.show(3)
      assert(rangeJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= radius in a join") {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var pointCsvDF1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF1.createOrReplaceTempView("pointtable")
      pointCsvDF1.show()
      var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape1 from pointtable")
      pointDf1.createOrReplaceTempView("pointdf1")
      pointDf1.show()

      var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable")
      pointCsvDF2.show()
      var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape2 from pointtable")
      pointDf2.createOrReplaceTempView("pointdf2")
      pointDf2.show()

      var distanceJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) <= 2")
      distanceJoinDf.explain()
      distanceJoinDf.show(10)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < radius in a join") {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var pointCsvDF1 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF1.createOrReplaceTempView("pointtable")
      pointCsvDF1.show()
      var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape1 from pointtable")
      pointDf1.createOrReplaceTempView("pointdf1")
      pointDf1.show()

      var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF2.createOrReplaceTempView("pointtable")
      pointCsvDF2.show()
      var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape2 from pointtable")
      pointDf2.createOrReplaceTempView("pointdf2")
      pointDf2.show()

      var distanceJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
      distanceJoinDf.explain()
      distanceJoinDf.show(10)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Contains in a range and join") {
      val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
      println(geosparkConf)

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      polygonCsvDf.show()
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20)), \"mypolygonid\") as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
        "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

      rangeJoinDf.explain()
      rangeJoinDf.show(3)
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
      pointDF.show(false)

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
  }
}
