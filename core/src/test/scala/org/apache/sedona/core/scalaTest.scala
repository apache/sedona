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

package org.apache.sedona.core

import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType, JoinBuildSide}
import org.apache.sedona.core.formatMapper.EarthdataHDFPointMapper
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.apache.sedona.core.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory}

class scalaTest extends SparkUtil {

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

  test("should pass the empty constructor test") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    val objectRDDcopy = new PointRDD()
    objectRDDcopy.rawSpatialRDD = objectRDD.rawSpatialRDD
    objectRDDcopy.analyze()
  }

  test("should pass spatial range query with spatial predicate") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, SpatialPredicate.COVERED_BY, false).count
    }
  }

  test("should pass spatial range query") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
    }
  }

  test("should pass spatial range query using index") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.buildIndex(PointRDDIndexType, false)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
    }
  }

  test("should pass spatial knn query") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    for (i <- 1 to eachQueryLoopTimes) {
      val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false)
    }
  }

  test("should pass spatial knn query using index") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.buildIndex(PointRDDIndexType, false)
    for (i <- 1 to eachQueryLoopTimes) {
      val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true)
    }
  }

  test("should pass spatial join query with spatial predicate") {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, SpatialPredicate.INTERSECTS).count
    }
  }

  test("should pass spatial join query") {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, true).count
    }
  }

  test("should pass spatial join query using index on points") {
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

  test("should pass spatial join query using index on polygons") {
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

  test("should pass spatial join query and build index on points the fly") {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count()
    }
  }

  test("should pass spatial join query and build index on polygons on the fly") {
    val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    for (i <- 1 to eachQueryLoopTimes) {
      val joinParams = new JoinParams(true, false, PolygonRDDIndexType, JoinBuildSide.LEFT)
      val resultSize = JoinQuery.spatialJoin(queryWindowRDD, objectRDD, joinParams).count()
    }
  }

  test("should pass distance join query with spatial predicate") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    val queryWindowRDD = new CircleRDD(objectRDD, 0.1)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(GridType.QUADTREE)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, SpatialPredicate.INTERSECTS).count()
    }
  }

  test("should pass distance join query") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
    val queryWindowRDD = new CircleRDD(objectRDD, 0.1)
    objectRDD.analyze()
    objectRDD.spatialPartitioning(GridType.QUADTREE)
    queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true).count()
    }
  }

  test("should pass distance join query using index") {
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

  // Tests here have been ignored. A new feature that reads HDF will be added.
  ignore("should pass earthdata format mapper test") {
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

  test("should pass CRS transformed spatial range query") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false, StorageLevel.NONE, "epsg:4326", "epsg:3005")
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
    }
  }

  test("should pass CRS transformed spatial range query using index") {
    val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false, StorageLevel.NONE, "epsg:4326", "epsg:3005")
    objectRDD.buildIndex(PointRDDIndexType, false)
    for (i <- 1 to eachQueryLoopTimes) {
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
    }
  }
}
