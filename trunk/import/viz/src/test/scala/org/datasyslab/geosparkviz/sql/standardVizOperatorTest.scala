package org.datasyslab.geosparkviz.sql

import org.datasyslab.geosparkviz.core.{ImageGenerator, ImageSerializableWrapper}
import org.datasyslab.geosparkviz.utils.ImageType

class standardVizOperatorTest extends TestBaseScala {

  describe("GeoSparkViz SQL function Test") {

    it("Generate a single image") {
      var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation) //.createOrReplaceTempView("polygontable")
      pointDf.sample(false, 1).createOrReplaceTempView("pointtable")
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
          |LATERAL VIEW ST_Pixelize(shape, 256, 256, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
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
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates), 'red')) AS image
          |FROM pixelaggregates
        """.stripMargin)
      var image = spark.table("images").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
      var imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/points", ImageType.PNG)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW imagestring AS
          |SELECT ST_EncodeImage(image)
          |FROM images
        """.stripMargin)
      spark.table("imagestring").show()
    }

    it("Generate a single image using a fat query") {
      var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation) //.createOrReplaceTempView("polygontable")
      pointDf.sample(false, 1).createOrReplaceTempView("pointtable")
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
          |CREATE OR REPLACE TEMP VIEW boundtable AS
          |SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable)) AS pixel
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
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_EncodeImage(ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates)))) AS image, (SELECT ST_AsText(bound) FROM boundtable) AS boundary
          |FROM pixelaggregates
        """.stripMargin)
      spark.table("images").show()
    }

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
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW ST_Pixelize(shape, 1000, 800, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
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
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates))) AS image
          |FROM pixelaggregates
        """.stripMargin)
      var imageDf = spark.sql(
        """
          |SELECT image
          |FROM images
        """.stripMargin)
      var image = imageDf.take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
      var imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/polygons", ImageType.PNG)
    }

    it("Passed ST_TileName") {
      var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      var zoomLevel = 2
      pointDf.sample(false, 0.01).createOrReplaceTempView("pointtable")
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
        s"""
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, weight, ST_TileName(pixel, $zoomLevel) AS pid
          |FROM pixelaggregates
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates))) AS image
          |FROM pixelaggregates
          |GROUP BY pid
        """.stripMargin).explain()
      spark.table("images").show()
    }
  }
}
