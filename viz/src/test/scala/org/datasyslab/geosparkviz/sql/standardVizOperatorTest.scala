package org.datasyslab.geosparkviz.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparkviz.core.{ImageGenerator, ImageSerializableWrapper}
import org.datasyslab.geosparkviz.utils.{GeoSparkVizRegistrator, ImageType}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class standardVizOperatorTest extends FunSpec with BeforeAndAfterAll {

  var spark: SparkSession = _


  override def afterAll(): Unit = {
  }

  describe("GeoSparkViz SQL function Test") {
    spark = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    GeoSparkSQLRegistrator.registerAll(spark)
    GeoSparkVizRegistrator.registerAll(spark)

    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

    val polygonInputLocationWkt = resourceFolder + "county_small.tsv"
    val polygonInputLocation = resourceFolder + "primaryroads-polygon.csv"
    val csvPointInputLocation = resourceFolder + "arealm.csv"

    it("Passed the pipeline on points") {
      var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation) //.createOrReplaceTempView("polygontable")
      pointDf.createOrReplaceTempView("pointtable")
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pointtable AS
          |SELECT ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as shape
          |FROM pointtable
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pointtable AS
          |SELECT *
          |FROM pointtable
          |WHERE ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)
        """.stripMargin)
      println(spark.table("pointtable").count())
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW ST_Pixelize(shape, 1000, 800, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)
      println(spark.table("pixels").count())
      val pixels = spark.table("pixels")
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      println(spark.table("pixelaggregates").count())
      val pixelaggregates = spark.table("pixelaggregates")
      pixelaggregates.show()
    }

    it("Passed the pipeline on polygons") {
      var polygonDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(polygonInputLocationWkt)
      polygonDf.createOrReplaceTempView("polygontable")
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW polygontable AS
          |SELECT ST_GeomFromWKT(polygontable._c0) as shape, _c1 as rate, _c2, _c3
          |FROM polygontable
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW usdata AS
          |SELECT *
          |FROM polygontable
          |WHERE ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, rate, shape FROM usdata
          |LATERAL VIEW ST_Pixelize(shape, 1000, 800, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      println(spark.table("pixelaggregates").count())
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, weight, (SELECT max(weight) FROM pixelaggregates)) AS image
          |FROM pixelaggregates
        """.stripMargin)
      var imageDf = spark.sql(
        """
          |SELECT image
          |FROM images
        """.stripMargin)
      var image = imageDf.take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
      var imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/polygons", ImageType.PNG)
    }
    it("Passed cache pixel aggregate") {
      var polygonDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(polygonInputLocationWkt)
      polygonDf.createOrReplaceTempView("polygontable")
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW polygontable AS
          |SELECT ST_GeomFromWKT(polygontable._c0) as shape, _c1 as rate, _c2, _c3
          |FROM polygontable
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW usdata AS
          |SELECT *
          |FROM polygontable
          |WHERE ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, rate, shape FROM usdata
          |LATERAL VIEW ST_Pixelize(shape, 1000, 800, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin).cache()
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, weight, (SELECT max(weight) FROM pixelaggregates)) AS image
          |FROM pixelaggregates
        """.stripMargin)
      var imageDf = spark.sql(
        """
          |SELECT image
          |FROM images
        """.stripMargin)
      imageDf.show()

      imageDf = spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, weight, (SELECT max(weight) FROM pixelaggregates)) AS image
          |FROM pixelaggregates
        """.stripMargin)
      imageDf.explain()
    }

    it("Passed ST_UniPartitioner") {
      var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation) //.createOrReplaceTempView("polygontable")
      pointDf.sample(0.01).createOrReplaceTempView("pointtable")
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pointtable AS
          |SELECT ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as shape
          |FROM pointtable
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pointtable AS
          |SELECT *
          |FROM pointtable
          |WHERE ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, weight, ST_UniPartition(pixel, 10, 10) AS pid
          |FROM pixelaggregates
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, weight, (SELECT max(weight) FROM pixelaggregates)) AS image
          |FROM pixelaggregates
          |GROUP BY pid
        """.stripMargin).explain()
      var imageDf = spark.table("images")
      imageDf.show()
    }

  }
}
