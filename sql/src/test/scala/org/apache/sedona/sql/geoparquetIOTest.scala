package org.apache.sedona.sql

import org.apache.spark.sql.functions.{col, hex}
import org.scalatest.BeforeAndAfter

class geoparquetIOTest extends TestBaseScala  with BeforeAndAfter{

  var geoparquetdatalocation1: String = resourceFolder + "geoparquet/example1.parquet"
  var geoparquetdatalocation2: String = resourceFolder + "geoparquet/example2.parquet"
  var geoparquetdatalocation3: String = resourceFolder + "geoparquet/example3.parquet"

  describe("GeoParquet IO tests"){

    it("Test example1 i.e. naturalearth_lowers "){
      val df = sparkSession.read.format("geoparquet").option("dropInvalid",true).load(geoparquetdatalocation1)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("pop_est") == 920938)
      assert(rows.getAs[String]("continent") == "Oceania")
      assert(rows.getAs[String]("name") == "Fiji")
      assert(rows.getAs[String]("iso_a3") == "FJI")
      assert(rows.getAs[Double]("gdp_md_est") == 8374.0)
    }
    it("Test example1 i.e. naturalearth_lowers with geometry column "){
      var df = sparkSession.read.format("geoparquet").option("dropInvalid",true).load(geoparquetdatalocation1)
      df = df.withColumn("geometry",hex(col("geometry")))
      df.createOrReplaceTempView("GeoParquetTable")
      val geodf = sparkSession.sql("SELECT ST_AsText(ST_GeomFromWKB(geometry)) as geometry FROM GeoParquetTable")
      assert(geodf.first().getAs[String]("geometry") == "MULTIPOLYGON (((180 -16.067132663642447, 180 -16.555216566639196, 179.36414266196414 -16.801354076946883, 178.72505936299711 -17.01204167436804, 178.59683859511713 -16.639150000000004, 179.0966093629971 -16.433984277547403, 179.4135093629971 -16.379054277547404, 180 -16.067132663642447)), ((178.12557 -17.50481, 178.3736 -17.33992, 178.71806 -17.62846, 178.55271 -18.15059, 177.93266000000003 -18.28799, 177.38146 -18.16432, 177.28504 -17.72465, 177.67087 -17.381140000000002, 178.12557 -17.50481)), ((-179.79332010904864 -16.020882256741224, -179.9173693847653 -16.501783135649397, -180 -16.555216566639196, -180 -16.067132663642447, -179.79332010904864 -16.020882256741224)))")
    }

    it("Test example2 i.e. naturalearth_cities"){
      val df = sparkSession.read.format("geoparquet").option("dropInvalid",true).load(geoparquetdatalocation2)
      val rows = df.collect()(0)
      assert(rows.getAs[String]("name") == "Vatican City")
    }
    it("Test example2 i.e. naturalearth_citie with geometry column"){
      var df = sparkSession.read.format("geoparquet").option("dropInvalid",true).load(geoparquetdatalocation2)
      df = df.withColumn("geometry",hex(col("geometry")))
      df.createOrReplaceTempView("GeoParquetTable")
      val geodf = sparkSession.sql("SELECT ST_AsText(ST_GeomFromWKB(geometry)) as geometry FROM GeoParquetTable")
      assert(geodf.first().getAs[String]("geometry") == "POINT (12.453386544971766 41.903282179960115)")
    }

    it("Test example3 i.e. nybb"){
      val df=sparkSession.read.format("geoparquet").option("dropInvalid", true).load(geoparquetdatalocation3)
      val rows = df.collect()(0)
      assert(rows.getAs[Long]("BoroCode") == 5)
      assert(rows.getAs[String]("BoroName") == "Staten Island")
      assert(rows.getAs[Double]("Shape_Leng") == 330470.010332)
      assert(rows.getAs[Double]("Shape_Area") == 1.62381982381E9)
    }
    it("Test example3 nybb with geometry column"){
      var df = sparkSession.read.format("geoparquet").option("dropInvalid",true).load(geoparquetdatalocation3)
      df = df.withColumn("geometry",hex(col("geometry")))
      df.createOrReplaceTempView("GeoParquetTablenybb")
      val geodf = sparkSession.sql("SELECT ST_AsText(ST_GeomFromWKB(geometry)) as  geometry FROM GeoParquetTablenybb")
      assert(geodf.first().getAs[String]("geometry").startsWith("MULTIPOLYGON (((970217.022"))
    }
  }
}
