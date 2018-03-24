/**
	* FILE: ScalaExample.java
	* PATH: org.datasyslab.geospark.showcase.ScalaExample.scala
	* Copyright (c) 2017 Arizona State University Data Systems Lab
	* All rights reserved.
	*/
package org.datasyslab.geospark.showcase

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.CircleRDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator


/**
	* The Class ScalaExample.
	*/
object ScalaExample extends App{

	val conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[2]")
	conf.set("spark.serializer", classOf[KryoSerializer].getName)
	conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

	val sc = new SparkContext(conf)
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

	val PointRDDInputLocation = resourceFolder+"arealm-small.csv"
	val PointRDDSplitter = FileDataSplitter.CSV
	val PointRDDIndexType = IndexType.RTREE
	val PointRDDNumPartitions = 5
	val PointRDDOffset = 0

	val PolygonRDDInputLocation = resourceFolder + "primaryroads-polygon.csv"
	val PolygonRDDSplitter = FileDataSplitter.CSV
	val PolygonRDDNumPartitions = 5
	val PolygonRDDStartOffset = 0
	val PolygonRDDEndOffset = 8

	val geometryFactory=new GeometryFactory()
	val kNNQueryPoint=geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val rangeQueryWindow=new Envelope (-90.01,-80.01,30.01,40.01)
	val joinQueryPartitioningType = GridType.QUADTREE
	val eachQueryLoopTimes=5

	var ShapeFileInputLocation = resourceFolder+"shapefiles/polygon"

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
	System.out.println("All GeoSpark DEMOs passed!")


	/**
		* Test spatial range query.
		*
		* @throws Exception the exception
		*/
	def testSpatialRangeQuery() {
	val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
	objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
	for(i <- 1 to eachQueryLoopTimes)
	{
		val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count
	}
	}



	/**
		* Test spatial range query using index.
		*
		* @throws Exception the exception
		*/
	def testSpatialRangeQueryUsingIndex() {
	val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
	objectRDD.buildIndex(PointRDDIndexType,false)
	objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
	for(i <- 1 to eachQueryLoopTimes)
	{
		val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
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
	for(i <- 1 to eachQueryLoopTimes)
	{
		val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false)
	}
	}

	/**
		* Test spatial knn query using index.
		*
		* @throws Exception the exception
		*/
	def testSpatialKnnQueryUsingIndex() {
	val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
	objectRDD.buildIndex(PointRDDIndexType,false)
	objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
	for(i <- 1 to eachQueryLoopTimes)
	{
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
	for(i <- 1 to eachQueryLoopTimes)
	{
		val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count
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

	objectRDD.buildIndex(PointRDDIndexType,true)

	objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
	queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

	for(i <- 1 to eachQueryLoopTimes)
	{
		val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count()
	}
	}

	/**
		* Test spatial join query.
		*
		* @throws Exception the exception
		*/
	def testDistanceJoinQuery() {
	val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
	val queryWindowRDD = new CircleRDD(objectRDD,0.1)

	objectRDD.spatialPartitioning(GridType.QUADTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
	queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

	for(i <- 1 to eachQueryLoopTimes)
	{
		val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count()
	}
	}

	/**
		* Test spatial join query using index.
		*
		* @throws Exception the exception
		*/
	def testDistanceJoinQueryUsingIndex() {
	val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
	val queryWindowRDD = new CircleRDD(objectRDD,0.1)

	objectRDD.spatialPartitioning(GridType.QUADTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	objectRDD.buildIndex(IndexType.RTREE,true)

	objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
	queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

	for(i <- 1 to eachQueryLoopTimes)
	{
		val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count
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
			val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
			assert(resultSize > -1)

			{
				i += 1; i - 1
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
			val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
			assert(resultSize > -1)

			{
				i += 1; i - 1
			}
		}
	}

	@throws[Exception]
	def testLoadShapefileIntoPolygonRDD(): Unit = {
		val shapefileRDD = new ShapefileRDD(sc, ShapeFileInputLocation)
		val spatialRDD = new PolygonRDD(shapefileRDD.getPolygonRDD)
		try
			RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count
		catch {
			case e: Exception =>
				// TODO Auto-generated catch block
				e.printStackTrace()
		}
	}

}