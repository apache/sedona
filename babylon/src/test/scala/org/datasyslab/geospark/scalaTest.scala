package org.datasyslab.geospark

import java.awt.Color
import java.io.FileInputStream
import java.util.Properties

import com.vividsolutions.jts.geom.Envelope
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.babylon.core.RasterOverlayOperator
import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator
import org.datasyslab.babylon.extension.visualizationEffect.{ChoroplethMap, HeatMap, ScatterPlot}
import org.datasyslab.babylon.utils.{ColorizeOption, EarthdataHDFPointMapper, ImageType}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, RectangleRDD}
import org.scalatest.FunSpec

class scalaTest extends FunSpec {

	describe("Babylon in Scala") {

		val sparkConf = new SparkConf().setAppName("scalaTest").setMaster("local[4]")
		val sparkContext = new SparkContext(sparkConf)
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)
		val prop = new Properties()
		val resourcePath = "src/test/resources/"
		val demoOutputPath = "target/scala/demo"
		var ConfFile = new FileInputStream(resourcePath + "babylon.point.properties")
		prop.load(ConfFile)
		val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
		val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
		val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
		val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap"
		val earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot"
		val PointInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
		val PointOffset = prop.getProperty("offset").toInt
		val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
		val PointNumPartitions = prop.getProperty("numPartitions").toInt
		ConfFile = new FileInputStream(resourcePath + "babylon.rectangle.properties")
		prop.load(ConfFile)
		val RectangleInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
		val RectangleOffset = prop.getProperty("offset").toInt
		val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
		val RectangleNumPartitions = prop.getProperty("numPartitions").toInt
		ConfFile = new FileInputStream(resourcePath + "babylon.polygon.properties")
		prop.load(ConfFile)
		val PolygonInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
		val PolygonOffset = prop.getProperty("offset").toInt
		val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
		val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
		ConfFile = new FileInputStream(resourcePath + "babylon.linestring.properties")
		prop.load(ConfFile)
		val LineStringInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
		val LineStringOffset = prop.getProperty("offset").toInt
		val LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
		val LineStringNumPartitions = prop.getProperty("numPartitions").toInt
		val USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000)
		val earthdataInputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv"
		val earthdataNumPartitions = 5
		val HDFIncrement = 5
		val HDFOffset = 2
		val HDFRootGroupName = "MOD_Swath_LST"
		val HDFDataVariableName = "LST"
		val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
		val HDFswitchXY = true
		val urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/"

		it("should pass scatter plot") {
			val spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
			var visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false)
			visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			var imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, scatterPlotOutputPath, ImageType.PNG)
			visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, false, true)
			visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, scatterPlotOutputPath, ImageType.SVG)
			visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, true, true)
			visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveVectorImageAsSparkFile(visualizationOperator.distributedVectorImage, "file://" + scatterPlotOutputPath + "-distributed", ImageType.SVG)
			true
		}

		it("should pass heat map") {
			val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
			val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			val imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, heatMapOutputPath, ImageType.PNG)
			true
		}

		it("should pass choropleth map") {
			val spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions)
			val queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
			spatialRDD.spatialPartitioning(GridType.RTREE)
			queryRDD.spatialPartitioning(spatialRDD.grids)
			spatialRDD.buildIndex(IndexType.RTREE, true)
			val joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, false)
			val visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false)
			visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
			visualizationOperator.Visualize(sparkContext, joinResult)
			val frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false)
			frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
			frontImage.Visualize(sparkContext, queryRDD)
			val overlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage)
			overlayOperator.JoinImage(frontImage.rasterImage)
			val imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, choroplethMapOutputPath, ImageType.PNG)
			true
		}

		it("should pass parallel filtering and rendering without stitching image tiles") {
			val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
			val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			val imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, parallelFilterRenderStitchOutputPath, ImageType.PNG)
			true
		}

		it("should pass parallel filtering and rendering with stitching image tiles") {
			val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
			val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			visualizationOperator.stitchImagePartitions
			val imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, parallelFilterRenderStitchOutputPath+"-stitched", ImageType.PNG)
			true
		}

		it("should pass earth data hdf scatter plot") {
			val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
				HDFDataVariableList, HDFDataVariableName, HDFswitchXY, urlPrefix)
			val spatialRDD = new PointRDD(sparkContext, earthdataInputLocation, earthdataNumPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
			val visualizationOperator = new ScatterPlot(1000, 600, spatialRDD.boundaryEnvelope, ColorizeOption.EARTHOBSERVATION, false, false)
			visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.BLUE, true)
			visualizationOperator.Visualize(sparkContext, spatialRDD)
			val imageGenerator = new BabylonImageGenerator
			imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, earthdataScatterPlotOutputPath, ImageType.PNG)
			true
		}
	}
}