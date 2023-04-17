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

package org.apache.sedona.core.showcase

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileRDD
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.apache.sedona.core.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory}


/**
  * The Class ScalaExample.
  */
object ScalaExample extends App {

  val conf = new SparkConf().setAppName("SedonaRunnableExample").setMaster("local[2]")
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)

  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

  val PointRDDInputLocation = resourceFolder + "arealm-small.csv"
  val PointRDDSplitter = FileDataSplitter.CSV
  val PointRDDIndexType = IndexType.RTREE
  val PointRDDNumPartitions = 5
  val PointRDDOffset = 1

  val PolygonRDDInputLocation = resourceFolder + "primaryroads-polygon.csv"
  val PolygonRDDSplitter = FileDataSplitter.CSV
  val PolygonRDDNumPartitions = 5
  val PolygonRDDStartOffset = 0
  val PolygonRDDEndOffset = 9

  val geometryFactory = new GeometryFactory()
  val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
  val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
  val joinQueryPartitioningType = GridType.QUADTREE
  val eachQueryLoopTimes = 5

  var ShapeFileInputLocation = resourceFolder + "shapefiles/polygon"

  testSpatialRangeQuery()
  testSpatialRangeQueryUsingIndex()
  testSpatialKnnQuery()
  testSpatialKnnQueryUsingIndex()
  testSpatialJoinQuery()
  testSpatialJoinQueryUsingIndex()
  testDistanceJoinQuery()
  testDistanceJoinQueryUsingIndex()
  testCRSTransformationSpatialRangeQuery()
  testCRSTransformationSpatialRangeQueryUsingIndex()
  sc.stop()
  System.out.println("All DEMOs passed!")


  /**
    * Test spatial range query.
    *
    * @throws Exception the exception
    */
  def testSpatialRangeQuery() {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, SpatialPredicate.COVERED_BY, false).count
    }
  }


  /**
    * Test spatial range query using index.
    *
    * @throws Exception the exception
    */
  def testSpatialRangeQueryUsingIndex() {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
    objectRDD.buildIndex(PointRDDIndexType, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, SpatialPredicate.COVERED_BY, true).count
    }

  }

  /**
    * Test spatial knn query.
    *
    * @throws Exception the exception
    */
  def testSpatialKnnQuery() {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
    for (i <- 1 to eachQueryLoopTimes) {
      val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false)
    }
  }

  /**
    * Test spatial knn query using index.
    *
    * @throws Exception the exception
    */
  def testSpatialKnnQueryUsingIndex() {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
    objectRDD.buildIndex(PointRDDIndexType, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
    for (i <- 1 to eachQueryLoopTimes) {
      val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true)
    }
  }

  /**
    * Test spatial join query.
    *
    * @throws Exception the exception
    */
  def testSpatialJoinQuery() {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, SpatialPredicate.INTERSECTS).count
    }
  }

  /**
    * Test spatial join query using index.
    *
    * @throws Exception the exception
    */
  def testSpatialJoinQueryUsingIndex() {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    objectRDD.buildIndex(PointRDDIndexType, true)

    objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, SpatialPredicate.COVERED_BY).count()
    }
  }

  /**
    * Test spatial join query.
    *
    * @throws Exception the exception
    */
  def testDistanceJoinQuery() {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
    val queryWindowRDD = new CircleRDD(objectRDD, 0.1)

    objectRDD.spatialPartitioning(GridType.QUADTREE)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, SpatialPredicate.INTERSECTS).count()
    }
  }

  /**
    * Test spatial join query using index.
    *
    * @throws Exception the exception
    */
  def testDistanceJoinQueryUsingIndex() {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
    val queryWindowRDD = new CircleRDD(objectRDD, 0.1)

    objectRDD.spatialPartitioning(GridType.QUADTREE)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    objectRDD.buildIndex(IndexType.RTREE, true)

    objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, true, SpatialPredicate.INTERSECTS).count
    }
  }

  @throws[Exception]
  def testCRSTransformationSpatialRangeQuery(): Unit = {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005")
    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
    var i = 0
    while ( {
      i < eachQueryLoopTimes
    }) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, SpatialPredicate.COVERED_BY, false).count
      assert(resultSize > -1)

      {
        i += 1;
        i - 1
      }
    }
  }


  @throws[Exception]
  def testCRSTransformationSpatialRangeQueryUsingIndex(): Unit = {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:3005")
    objectRDD.buildIndex(PointRDDIndexType, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
    var i = 0
    while ( {
      i < eachQueryLoopTimes
    }) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, SpatialPredicate.COVERED_BY, true).count
      assert(resultSize > -1)

      {
        i += 1;
        i - 1
      }
    }
  }

  @throws[Exception]
  def testLoadShapefileIntoPolygonRDD(): Unit = {
    val shapefileRDD = new ShapefileRDD(sc, ShapeFileInputLocation)
    val spatialRDD = new PolygonRDD(shapefileRDD.getPolygonRDD)
    try
      RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), SpatialPredicate.COVERED_BY, false).count
    catch {
      case e: Exception =>
        // TODO Auto-generated catch block
        e.printStackTrace()
    }
  }

}
