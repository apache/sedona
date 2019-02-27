package org.datasyslab.geosparkviz.sql

import com.vividsolutions.jts.geom.Envelope
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparkviz.sql.operator.{AggregateWithinPartitons, VizPartitioner}
import org.datasyslab.geosparkviz.sql.utils.{Conf, GeoSparkVizRegistrator}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class optVizOperatorTest extends FunSpec with BeforeAndAfterAll{

  var spark: SparkSession = _


  override def afterAll(): Unit = {
    //BabylonRegistrator.dropAll(sparkSession)
    //sparkSession.stop
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

    it("Passed ST_VizPartitioner") {
      var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation) //.createOrReplaceTempView("polygontable")
      pointDf.show()
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
          |LATERAL VIEW ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
        """.stripMargin)
      println(spark.table("pixels").count())
      val zoomLevel = 2
      val newDf = VizPartitioner(spark.table("pixels"), zoomLevel, "pixel", new Envelope(0, 1000, 0, 1000))
      newDf.explain(true)
      newDf.show()
      newDf.createOrReplaceTempView("pixels")
      assert(newDf.select(Conf.PrimaryPID).distinct().count() <= Math.pow(4, zoomLevel))
      val secondaryPID = newDf.select(Conf.SecondaryPID).distinct().count()
//      println("numpartition " + newDf.rdd.getNumPartitions)

//      newDf.foreachPartition(f=>{
//        var lastPID = "--"
//        if (f.hasNext) {
//          lastPID = f.next().getAs[String]("secondarypid")
//        }
//        while (f.hasNext) {
//          val pid = f.next().getAs[String]("secondarypid")
//          if (!pid.equalsIgnoreCase(lastPID)) println(s"PID is not same! lastPid $lastPID pid = $pid")
//          lastPID = pid
//        }
//      })

      assert(newDf.rdd.getNumPartitions == secondaryPID)

      val result = AggregateWithinPartitons(newDf, "pixel", "weight", "count")
      result.show()
      println(result.rdd.getNumPartitions)
    }
  }
}
