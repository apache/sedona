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
package org.apache.sedona.viz.rdd

import java.awt.Color // scalastyle:ignore illegal.imports
import java.io.FileInputStream
import java.util.Properties

import org.locationtech.jts.geom.Envelope
import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
import org.apache.sedona.core.formatMapper.EarthdataHDFPointMapper
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{PointRDD, PolygonRDD, RectangleRDD}
import org.apache.sedona.viz.`extension`.visualizationEffect.{ChoroplethMap, HeatMap, ScatterPlot}
import org.apache.sedona.viz.core.{ImageGenerator, RasterOverlayOperator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.utils.{ColorizeOption, ImageType}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class scalaTest extends FunSpec with BeforeAndAfterAll{
  val sparkConf = new SparkConf().setAppName("scalaTest").setMaster("local[*]")
  sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
  sparkConf.set("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
  var sparkContext:SparkContext = _
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val prop = new Properties()
  val resourceFolder = System.getProperty("user.dir") + "/../core/src/test/resources/"
  val demoOutputPath = "target/scala/demo"
  var ConfFile = new FileInputStream(resourceFolder + "babylon.point.properties")
  prop.load(ConfFile)
  val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
  val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
  val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
  val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap"
  val earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot"
  val PointInputLocation = resourceFolder + prop.getProperty("inputLocation")
  val PointOffset = prop.getProperty("offset").toInt
  val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PointNumPartitions = prop.getProperty("numPartitions").toInt
  ConfFile = new FileInputStream(resourceFolder + "babylon.rectangle.properties")
  prop.load(ConfFile)
  val RectangleInputLocation = resourceFolder + prop.getProperty("inputLocation")
  val RectangleOffset = prop.getProperty("offset").toInt
  val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val RectangleNumPartitions = prop.getProperty("numPartitions").toInt
  ConfFile = new FileInputStream(resourceFolder + "babylon.polygon.properties")
  prop.load(ConfFile)
  val PolygonInputLocation = resourceFolder + prop.getProperty("inputLocation")
  val PolygonOffset = prop.getProperty("offset").toInt
  val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
  ConfFile = new FileInputStream(resourceFolder + "babylon.linestring.properties")
  prop.load(ConfFile)
  val LineStringInputLocation = resourceFolder + prop.getProperty("inputLocation")
  val LineStringOffset = prop.getProperty("offset").toInt
  val LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val LineStringNumPartitions = prop.getProperty("numPartitions").toInt
  val USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000)
  val earthdataInputLocation = resourceFolder + "modis/modis.csv"
  val earthdataNumPartitions = 5
  val HDFIncrement = 5
  val HDFOffset = 2
  val HDFRootGroupName = "MOD_Swath_LST"
  val HDFDataVariableName = "LST"
  val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
  val HDFswitchXY = true
  val urlPrefix = resourceFolder + "modis/"

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(sparkConf)
  }
  override def afterAll(): Unit = {
    sparkContext.stop()
  }
  describe("SedonaViz in Scala") {

    it("should pass scatter plot") {
      val spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY)
      var visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false)
      visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      var imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, scatterPlotOutputPath, ImageType.PNG)
      /*
      visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, false, true)
      visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      imageGenerator = new ImageGenerator
      imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, scatterPlotOutputPath, ImageType.SVG)
      visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, true, true)
      visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      imageGenerator = new ImageGenerator
      imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.distributedVectorImage, scatterPlotOutputPath + "-distributed", ImageType.SVG)
      */
      true
    }

    it("should pass heat map") {
      val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
      val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      val imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, heatMapOutputPath, ImageType.PNG)
      true
    }

    it("should pass choropleth map") {
      val spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY)
      val queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY)
      spatialRDD.spatialPartitioning(GridType.KDBTREE)
      queryRDD.spatialPartitioning(spatialRDD.getPartitioner)
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
      val imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, choroplethMapOutputPath, ImageType.PNG)
      true
    }

    it("should pass parallel filtering and rendering without stitching image tiles") {
      val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
      val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      val imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, parallelFilterRenderStitchOutputPath, ImageType.PNG, 0, 4, 4)
      true
    }

    it("should pass parallel filtering and rendering with stitching image tiles") {
      val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
      val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      val imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, parallelFilterRenderStitchOutputPath, ImageType.PNG)
      true
    }

    // Tests here have been ignored. A new feature that reads HDF will be added.
    ignore("should pass earth data hdf scatter plot") {
      val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
        HDFDataVariableList, HDFDataVariableName, HDFswitchXY, urlPrefix)
      val spatialRDD = new PointRDD(sparkContext, earthdataInputLocation, earthdataNumPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
      val visualizationOperator = new ScatterPlot(1000, 600, spatialRDD.boundaryEnvelope, ColorizeOption.EARTHOBSERVATION, false, false)
      visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.BLUE, true)
      visualizationOperator.Visualize(sparkContext, spatialRDD)
      val imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, earthdataScatterPlotOutputPath, ImageType.PNG)
      true
    }
  }
}
