package org.datasyslab.geosparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class functionTestScala extends FunSpec with BeforeAndAfterAll {

	var sparkSession: SparkSession = _

	override def afterAll(): Unit = {
    //GeoSparkSQLRegistrator.dropAll(sparkSession)
    //sparkSession.stop
	}

	describe("GeoSpark-SQL Function Test") {
    sparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

		val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

    val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
    val plainPointInputLocation = resourceFolder + "testpoint.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/polygon"
    val csvPointInputLocation = resourceFolder + "arealm.csv"
    val geoJsonGeomInputLocation = resourceFolder + "testPolygon.json"

    it("Passed ST_ConvexHull")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_ConvexHull(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Envelope")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Envelope(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Centroid")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Centroid(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Length")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Length(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Area")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Area(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Distance")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Transform")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Intersection")
    {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Intersection(polygondf.countyshape, polygondf.countyshape) from polygondf")
      functionDf.show()
    }
	}
}
