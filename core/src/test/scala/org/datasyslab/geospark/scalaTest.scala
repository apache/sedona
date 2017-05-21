package org.datasyslab.geospark

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.scalatest.FunSpec

class scalaTest extends FunSpec {

	describe("GeoSpark in Scala") {

		val conf = new SparkConf().setAppName("scalaTest").setMaster("local[2]")
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
		val joinQueryPartitioningType = GridType.RTREE
		val eachQueryLoopTimes=1

		it("should pass spatial range query") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count
			}
		}

		it("should pass spatial range query using index") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
			objectRDD.buildIndex(PointRDDIndexType,false)
			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
			}
		}

		it("should pass spatial knn query") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
			for(i <- 1 to eachQueryLoopTimes)
			{
				val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false)
			}
		}

		it("should pass spatial knn query using index") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
			objectRDD.buildIndex(PointRDDIndexType,false)
			for(i <- 1 to eachQueryLoopTimes)
			{
				val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true)
			}
		}

		it("should pass spatial join query") {
			val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

			objectRDD.spatialPartitioning(joinQueryPartitioningType)
			queryWindowRDD.spatialPartitioning(objectRDD.grids)

			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count
			}
		}

		it("should pass spatial join query using index") {
			val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

			objectRDD.spatialPartitioning(joinQueryPartitioningType)
			queryWindowRDD.spatialPartitioning(objectRDD.grids)

			objectRDD.buildIndex(PointRDDIndexType,true)

			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count()
			}
		}

		it("should pass distance join query") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
			val queryWindowRDD = new CircleRDD(objectRDD,0.1)

			objectRDD.spatialPartitioning(GridType.RTREE)
			queryWindowRDD.spatialPartitioning(objectRDD.grids)

			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count()
			}
		}

		it("should pass distance join query using index") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
			val queryWindowRDD = new CircleRDD(objectRDD,0.1)

			objectRDD.spatialPartitioning(GridType.RTREE)
			queryWindowRDD.spatialPartitioning(objectRDD.grids)

			objectRDD.buildIndex(IndexType.RTREE,true)

			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count
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
			val spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
			var i = 0
			while (i < loopTimes) {
				var resultSize = 0L
				resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count
				i=i+1
			}
		}

		it("should pass CRS transformed spatial range query") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count
			}
		}

		it("should pass CRS transformed spatial range query using index") {
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
			objectRDD.buildIndex(PointRDDIndexType,false)
			for(i <- 1 to eachQueryLoopTimes)
			{
				val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
			}
		}
	}
}
