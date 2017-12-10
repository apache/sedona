package org.datasyslab.geosparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class functionTestScala extends FunSpec with BeforeAndAfterAll with GeoSparkSqlTestBase {

	implicit lazy val sparkSession = {
    var tempSparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
		val sc = tempSparkSession.sparkContext
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)
    tempSparkSession
	}


	override def afterAll(): Unit = {
    GeoSparkSQLRegistrator.dropAll()
    sparkSession.stop
	}

	describe("GeoSpark-SQL Predicate Test") {

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

		val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

    val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
    val csvPointInputLocation = resourceFolder + "testpoint.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/polygon"

    it("Passed ST_Point")
    {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      assert(pointDf.count()==1000)
    }

    it("Passed ST_PointFromText")
    {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      assert(pointDf.count()==1000)
    }
	}
}
