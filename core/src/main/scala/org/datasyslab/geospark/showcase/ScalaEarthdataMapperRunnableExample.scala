/**
  * FILE: ScalaEarthdataMapperRunnableExample.java
  * PATH: org.datasyslab.geospark.showcase.ScalaEarthdataMapperRunnableExample.java
  * Copyright (c) 2017 Arizona State University Data Systems Lab
  * All rights reserved.
  */
package org.datasyslab.geospark.showcase

import com.vividsolutions.jts.geom.Envelope
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
