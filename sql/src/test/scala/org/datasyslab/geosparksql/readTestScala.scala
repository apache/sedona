package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.monitoring.GeoSparkListener
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.UDF.UdfRegistrator
import org.datasyslab.geosparksql.utils.Adapter
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class readTestScala extends FunSpec with BeforeAndAfterAll {

	implicit lazy val sparkSession = {
    var tempSparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).master("local[*]").appName("readTestScala").getOrCreate()
		val sc = tempSparkSession.sparkContext
		sc.addSparkListener(new GeoSparkListener)
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)
    tempSparkSession
	}

	override def afterAll(): Unit = {
		sparkSession.stop
	}

	describe("GeoSpark-SQL Scala ReadFromDataFrame Test") {

		val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

    val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
    val csvPointInputLocation = resourceFolder + "arealm.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/polygon"

    it("Read CSV point into a SpatialRDD")
    {
      val udfRegister = new UdfRegistrator
      udfRegister.registerAll(sparkSession)
      var df = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(csvPointInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromTextToType(inputtable._c0,\",\",\"point\") as arealandmark from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD,sparkSession).show()
    }

    it("Read CSV point into a SpatialRDD by passing coordinates")
    {
      val udfRegister = new UdfRegistrator
      udfRegister.registerAll(sparkSession)
      var df = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_Point(inputtable._c0,inputtable._c1) as arealandmark from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD,sparkSession).show()
    }

    it("Read CSV point into a SpatialRDD with unique Id by passing coordinates")
    {
      val udfRegister = new UdfRegistrator
      udfRegister.registerAll(sparkSession)
      var df = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      // Use Column _c0 as the unique Id but the id can be anything in the same row
      var spatialDf = sparkSession.sql("select ST_PointWithId(inputtable._c0,inputtable._c1,inputtable._c0) as arealandmark from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD,sparkSession).show()
    }

    it("Read mixed WKT geometries into a SpatialRDD")
    {
      val udfRegistrator = new UdfRegistrator
      udfRegistrator.registerAll(sparkSession)
      var df = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromText(inputtable._c0,\"wkt\") as usacounty from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD,sparkSession).show()
    }

    it("Read mixed WKT geometries into a SpatialRDD with uniqueId")
    {
      val udfRegistrator = new UdfRegistrator
      udfRegistrator.registerAll(sparkSession)
      var df = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromTextWithId(inputtable._c0,\"wkt\", concat(inputtable._c3,'\t',inputtable._c5)) as usacounty from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD,sparkSession).show()
    }

    it("Read shapefile to DataFrame")
    {
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD,sparkSession).show()
    }

	}
}
