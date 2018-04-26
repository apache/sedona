/*
 * FILE: scalaTest.scala
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

package org.datasyslab.geospark

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType, JoinBuildSide}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.monitoring.GeoSparkListener
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class scalaTest extends FunSpec with BeforeAndAfterAll {

  implicit lazy val sc = {
    val conf = new SparkConf().setAppName("scalaTest").setMaster("local[2]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

    val sc = new SparkContext(conf)
    sc.addSparkListener(new GeoSparkListener)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc
  }

  override def afterAll(): Unit = {
    sc.stop
  }

  describe("GeoSpark in Scala") {

    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

    val PointRDDInputLocation = resourceFolder + "arealm-small.csv"
    val PointRDDSplitter = FileDataSplitter.CSV
    val PointRDDIndexType = IndexType.RTREE
    val PointRDDNumPartitions = 5
    val PointRDDOffset = 1

    val PolygonRDDInputLocation = resourceFolder + "primaryroads-polygon.csv"
    val PolygonRDDSplitter = FileDataSplitter.CSV
    val PolygonRDDIndexType = IndexType.RTREE
    val PolygonRDDNumPartitions = 5
    val PolygonRDDStartOffset = 0
    val PolygonRDDEndOffset = 9

    val geometryFactory = new GeometryFactory()
    val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
    val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
    val joinQueryPartitioningType = GridType.QUADTREE
    val eachQueryLoopTimes = 1

    it("should pass the empty constructor test") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      val objectRDDcopy = new PointRDD()
      objectRDDcopy.rawSpatialRDD = objectRDD.rawSpatialRDD
      objectRDDcopy.analyze()
    }

    it("should pass spatial range query") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
      }
    }

    it("should pass spatial range query using index") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.buildIndex(PointRDDIndexType, false)
      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
      }
    }

    it("should pass spatial knn query") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      for (i <- 1 to eachQueryLoopTimes) {
        val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false)
      }
    }

    it("should pass spatial knn query using index") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.buildIndex(PointRDDIndexType, false)
      for (i <- 1 to eachQueryLoopTimes) {
        val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true)
      }
    }

    it("should pass spatial join query") {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, true).count
      }
    }

    it("should pass spatial join query using index on points") {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      objectRDD.buildIndex(PointRDDIndexType, true)

      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count()
      }
    }

    it("should pass spatial join query using index on polygons") {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      queryWindowRDD.buildIndex(PolygonRDDIndexType, true)

      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count()
      }
    }

    it("should pass spatial join query and build index on points the fly") {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count()
      }
    }

    it("should pass spatial join query and build index on polygons on the fly") {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      for (i <- 1 to eachQueryLoopTimes) {
        val joinParams = new JoinParams(false, PolygonRDDIndexType, JoinBuildSide.LEFT)
        val resultSize = JoinQuery.spatialJoin(queryWindowRDD, objectRDD, joinParams).count()
      }
    }

    it("should pass distance join query") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      val queryWindowRDD = new CircleRDD(objectRDD, 0.1)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(GridType.QUADTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true).count()
      }
    }

    it("should pass distance join query using index") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
      val queryWindowRDD = new CircleRDD(objectRDD, 0.1)
      objectRDD.analyze()
      objectRDD.spatialPartitioning(GridType.QUADTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

      objectRDD.buildIndex(IndexType.RTREE, true)

      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, true, true).count
      }
    }

    it("should pass earthdata format mapper test") {
      val InputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv"
      val splitter = FileDataSplitter.CSV
      val indexType = IndexType.RTREE
      val queryEnvelope = new Envelope(-90.01, -80.01, 30.01, 40.01)
      val numPartitions = 5
      val loopTimes = 1
      val HDFIncrement = 5
      val HDFOffset = 2
      val HDFRootGroupName = "MOD_Swath_LST"
      val HDFDataVariableName = "LST"
      val urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/"
      val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")

      val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
      val spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint)
      var i = 0
      while (i < loopTimes) {
        var resultSize = 0L
        resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count
        i = i + 1
      }
    }

    it("should pass CRS transformed spatial range query") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false, StorageLevel.NONE, "epsg:4326", "epsg:3005")
      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
      }
    }

    it("should pass CRS transformed spatial range query using index") {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false, StorageLevel.NONE, "epsg:4326", "epsg:3005")
      objectRDD.buildIndex(PointRDDIndexType, false)
      for (i <- 1 to eachQueryLoopTimes) {
        val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
      }
    }
  }
}
