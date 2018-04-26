/*
 * FILE: adapterTestScala.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class adapterTestScala extends FunSpec with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  override def afterAll(): Unit = {
    //UdfRegistrator.dropAll(sparkSession)
    //sparkSession.stop
  }

  describe("GeoSpark-SQL Scala Adapter Test") {
    sparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

    val mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
    val csvPointInputLocation = resourceFolder + "arealm.csv"
    val shapefileInputLocation = resourceFolder + "shapefiles/polygon"

    it("Read CSV point into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(csvPointInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\",\"mypoint\") as arealandmark from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD, sparkSession).show()
    }

    it("Read CSV point into a SpatialRDD by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==1)
      Adapter.toDf(spatialRDD, sparkSession).show()
    }

    it("Read CSV point into a SpatialRDD with unique Id by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      // Use Column _c0 as the unique Id but the id can be anything in the same row
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20)), 'myPointId') as arealandmark from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==2)
      Adapter.toDf(spatialRDD, sparkSession).show()
    }

    it("Read mixed WKT geometries into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD, sparkSession).show()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==1)
      Adapter.toDf(spatialRDD, sparkSession).show()
    }

    it("Read mixed WKT geometries into a SpatialRDD with uniqueId") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.show()
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0, inputtable._c3, inputtable._c5) as usacounty from inputtable")
      spatialDf.show()
      spatialDf.printSchema()
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==3)
      Adapter.toDf(spatialRDD, sparkSession).show()
    }

    it("Read shapefile to DataFrame") {
      var spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD, sparkSession).show()
    }

    it("Convert spatial join result to DataFrame") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0, polygontable._c3, polygontable._c5) as usacounty from polygontable")
      var polygonRDD = new SpatialRDD[Geometry]
      polygonRDD.rawSpatialRDD = Adapter.toRdd(polygonDf)
      polygonRDD.analyze()

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      var pointRDD = new SpatialRDD[Geometry]
      pointRDD.rawSpatialRDD = Adapter.toRdd(pointDf)
      pointRDD.analyze()

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      var joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true)

      var joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      joinResultDf.show()
    }

    it("Convert distance join result to DataFrame") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      var pointRDD = new SpatialRDD[Geometry]
      pointRDD.rawSpatialRDD = Adapter.toRdd(pointDf)
      pointRDD.analyze()

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0, polygontable._c3, polygontable._c5) as usacounty from polygontable")
      var polygonRDD = new SpatialRDD[Geometry]
      polygonRDD.rawSpatialRDD = Adapter.toRdd(polygonDf)
      polygonRDD.analyze()
      var circleRDD = new CircleRDD(polygonRDD, 0.2)

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      circleRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      var joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true)

      var joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      joinResultDf.show()
    }
  }
}
