package org.datasyslab.geosparkviz.sql

import org.locationtech.jts.geom.Envelope
import org.apache.spark.sql.functions._
import org.datasyslab.geosparkviz.sql.operator.{AggregateWithinPartitons, VizPartitioner}
import org.datasyslab.geosparkviz.sql.utils.{Conf, LineageDecoder}

class optVizOperatorTest extends TestBaseScala {

  describe("GeoSparkViz SQL function Test") {

    it("Passed full pipeline using optimized operator") {
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
          |LATERAL VIEW ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)

      // Test visualization partitioner
      val zoomLevel = 2
      val newDf = VizPartitioner(spark.table("pixels"), zoomLevel, "pixel", new Envelope(0, 1000, 0, 1000))
      newDf.createOrReplaceTempView("pixels")
      assert(newDf.select(Conf.PrimaryPID).distinct().count() <= Math.pow(4, zoomLevel))
      val secondaryPID = newDf.select(Conf.SecondaryPID).distinct().count()
      assert(newDf.rdd.getNumPartitions == secondaryPID)

      // Test aggregation within partitions
      val result = AggregateWithinPartitons(newDf.withColumn("weight", lit(100.0)), "pixel", "weight", "avg")
      assert(result.rdd.getNumPartitions == secondaryPID)

      // Test the colorize operator
      result.createOrReplaceTempView("pixelaggregates")
      spark.sql(
        s"""
          |CREATE OR REPLACE TEMP VIEW colors AS
          |SELECT pixel, ${Conf.PrimaryPID}, ${Conf.SecondaryPID}, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates))
          |FROM pixelaggregates
        """.stripMargin)
      spark.table("colors").show()
    }

    it("Passed full pipeline - aggregate:avg - color:uniform") {
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
          |LATERAL VIEW ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)

      // Test visualization partitioner
      val zoomLevel = 2
      val newDf = VizPartitioner(spark.table("pixels"), zoomLevel, "pixel", new Envelope(0, 1000, 0, 1000))
      newDf.createOrReplaceTempView("pixels")
      assert(newDf.select(Conf.PrimaryPID).distinct().count() <= Math.pow(4, zoomLevel))
      val secondaryPID = newDf.select(Conf.SecondaryPID).distinct().count()
      assert(newDf.rdd.getNumPartitions == secondaryPID)

      // Test aggregation within partitions
      val result = AggregateWithinPartitons(newDf, "pixel", "weight", "count")
      assert(result.rdd.getNumPartitions == secondaryPID)

      // Test the colorize operator
      result.createOrReplaceTempView("pixelaggregates")
      spark.sql(
        s"""
           |CREATE OR REPLACE TEMP VIEW colors AS
           |SELECT pixel, ${Conf.PrimaryPID}, ${Conf.SecondaryPID}, ST_Colorize(weight, 0, 'red')
           |FROM pixelaggregates
        """.stripMargin)
      spark.table("colors").show()
    }

    it("Passed lineage decoder"){
      assert(LineageDecoder("01") == "2-1-0")
      assert(LineageDecoder("12") == "2-2-1")
      assert(LineageDecoder("333") == "3-7-7")
      assert(LineageDecoder("012") == "3-2-1")
    }
  }
}
