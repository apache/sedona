package org.datasyslab.geosparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql.strategy.join.JoinQueryDetector
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class predicateJoinTestScala extends FunSpec with BeforeAndAfterAll {

	implicit lazy val sparkSession = {
    var tempSparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
		val sc = tempSparkSession.sparkContext
		Logger.getLogger("org").setLevel(Level.INFO)
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
    val csvPointInputLocation = resourceFolder + "arealm.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/polygon"

    it("Passed ST_Contains in a join")
    {
      sparkSession.sqlContext.experimental.extraStrategies = JoinQueryDetector :: Nil

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(pointtable._c0,pointtable._c1, \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()
      pointDf.printSchema()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.usacounty,pointdf.arealandmark) ")
      rangeJoinDf.explain()
      rangeJoinDf.show(3)

      var distanceJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Distance(polygondf.usacounty,pointdf.arealandmark) <= 10")
      distanceJoinDf.explain()
      distanceJoinDf.show(3)
    }

    it("Passed ST_Intersects in a join")
    {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0,\"wkt\", \"mypolygonid\") as usacounty, polygontable._c6 from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      polygonDf.printSchema()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(pointtable._c0,pointtable._c1, \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()
      pointDf.printSchema()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.usacounty,pointdf.arealandmark) ")
      rangeJoinDf.explain()
      rangeJoinDf.show(3)

      var distanceJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Distance(polygondf.usacounty,pointdf.arealandmark) <= 10")
      distanceJoinDf.explain()
      distanceJoinDf.show(3)
    }

    it("Passed ST_Within in a join")
    {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0,\"wkt\", \"mypolygonid\") as usacounty, polygontable._c6 from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      polygonDf.printSchema()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(pointtable._c0,pointtable._c1, \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()
      pointDf.printSchema()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.usacounty,pointdf.arealandmark) ")
      rangeJoinDf.explain()
      rangeJoinDf.show(3)

      var distanceJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Distance(polygondf.usacounty,pointdf.arealandmark) <= 10")
      distanceJoinDf.explain()
      distanceJoinDf.show(3)
    }

    it("Passed ST_Distance <= radius in a join")
    {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0,\"wkt\", \"mypolygonid\") as usacounty, polygontable._c6 from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      polygonDf.printSchema()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(pointtable._c0,pointtable._c1, \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()
      pointDf.printSchema()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.usacounty,pointdf.arealandmark) ")
      rangeJoinDf.explain()
      rangeJoinDf.show(3)

      var distanceJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Distance(polygondf.usacounty,pointdf.arealandmark) <= 10")
      distanceJoinDf.explain()
      distanceJoinDf.show(3)
    }

    it("Passed ST_Distance < radius in a join")
    {
      sparkSession.experimental.extraStrategies = JoinQueryDetector :: Nil

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0,\"wkt\", \"mypolygonid\") as usacounty, polygontable._c6 from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      polygonDf.printSchema()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      pointCsvDF.show()
      var pointDf = sparkSession.sql("select ST_Point(pointtable._c0,pointtable._c1, \"myPointId\") as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      pointDf.show()
      pointDf.printSchema()

      var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Intersects(polygondf.usacounty,pointdf.arealandmark) ")
      rangeJoinDf.explain()
      rangeJoinDf.show(3)

      var distanceJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Distance(polygondf.usacounty,pointdf.arealandmark) <= 10")
      distanceJoinDf.explain()
      distanceJoinDf.show(3)
    }
	}
}
