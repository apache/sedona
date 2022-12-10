/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}


object ScalaExample extends App{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

	var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
		config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
		master("local[*]").appName("SedonaSQL-demo").getOrCreate()

	SedonaSQLRegistrator.registerAll(sparkSession)
  SedonaVizRegistrator.registerAll(sparkSession)

	val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
  val rasterdatalocation = resourceFolder + "raster/"

  testPredicatePushdownAndRangeJonQuery()
  testDistanceJoinQuery()
  testAggregateFunction()
  testShapefileConstructor()
  testRasterIOAndMapAlgebra()

  System.out.println("All SedonaSQL DEMOs passed!")

  def testPredicatePushdownAndRangeJonQuery():Unit =
  {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    println(sedonaConf)

    var polygonCsvDf = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPolygonInputLocation)
    polygonCsvDf.createOrReplaceTempView("polygontable")
    polygonCsvDf.show()
    var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    polygonDf.createOrReplaceTempView("polygondf")
    polygonDf.show()

    var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    pointCsvDF.show()
    var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    pointDf.show()

    var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
      "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

    rangeJoinDf.explain()
    rangeJoinDf.show(3)
    assert (rangeJoinDf.count()==500)
  }

  def testDistanceJoinQuery(): Unit =
  {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    println(sedonaConf)

    var pointCsvDF1 = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF1.createOrReplaceTempView("pointtable")
    pointCsvDF1.show()
    var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
    pointDf1.createOrReplaceTempView("pointdf1")
    pointDf1.show()

    var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF2.createOrReplaceTempView("pointtable")
    pointCsvDF2.show()
    var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
    pointDf2.createOrReplaceTempView("pointdf2")
    pointDf2.show()

    var distanceJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
    distanceJoinDf.explain()
    distanceJoinDf.show(10)
    assert (distanceJoinDf.count()==2998)
  }

  def testAggregateFunction(): Unit =
  {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    println(sedonaConf)

    var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    var boundary = sparkSession.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")
    val coordinates:Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(1.1,101.1)
    coordinates(1) = new Coordinate(1.1,1100.1)
    coordinates(2) = new Coordinate(1000.1,1100.1)
    coordinates(3) = new Coordinate(1000.1,101.1)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)
    assert(boundary.take(1)(0).get(0)==geometryFactory.createPolygon(coordinates))
  }

  def testShapefileConstructor(): Unit =
  {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
    var rawSpatialDf = Adapter.toDf(spatialRDD,sparkSession)
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    var spatialDf = sparkSession.sql("""
                                       | SELECT geometry, STATEFP, COUNTYFP
                                       | FROM rawSpatialDf
                                     """.stripMargin)
    spatialDf.show()
    spatialDf.printSchema()
  }

  def testRasterIOAndMapAlgebra(): Unit = {
    var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
    df.printSchema()
    df.selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as numBands").show()
    df = df.selectExpr(" image.data as data", "image.nBands as numBands")
    df = df.selectExpr("RS_GetBand(data, 1, numBands) as targetBand")
    df.selectExpr("RS_MultiplyFactor(targetBand, 3) as multiply").show()
  }
}