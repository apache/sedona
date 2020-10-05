/*
 * FILE: ScalaEarthdataMapperRunnableExample.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.showcase

import org.locationtech.jts.geom.Envelope
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{FileDataSplitter, IndexType}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

object ScalaEarthdataMapperRunnableExample extends App {
  val conf = new SparkConf().setAppName("EarthdataMapperRunnableExample").setMaster("local[2]")
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
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
  testSpatialRangeQuery()
  testSpatialRangeQueryUsingIndex()
  sc.stop()
  System.out.println("All GeoSpark Earthdata DEMOs passed!")

  /**
    * Test spatial range query.
    */
  def testSpatialRangeQuery() {
    val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
    val spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
    var i = 0
    while (i < loopTimes) {
      var resultSize = 0L
      resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count
      i = i + 1
    }
  }

  /**
    * Test spatial range query using index.
    */
  def testSpatialRangeQueryUsingIndex() {
    val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
    val spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
    spatialRDD.buildIndex(IndexType.RTREE, false)
    var i = 0
    while (i < loopTimes) {
      var resultSize = 0L
      resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count
      i = i + 1
    }
  }
}
