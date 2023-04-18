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
package org.apache.sedona.viz.showcase

import java.awt.Color // scalastyle:ignore illegal.imports
import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.EarthdataHDFPointMapper
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{PointRDD, PolygonRDD, RectangleRDD}
import org.apache.sedona.viz.`extension`.visualizationEffect.{ChoroplethMap, HeatMap, ScatterPlot}
import org.apache.sedona.viz.core.{ImageGenerator, RasterOverlayOperator}
import org.apache.sedona.viz.utils.{ColorizeOption, ImageType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Envelope


/**
  * The Class ScalaExample.
  */
object ScalaExample extends App {
  val sparkConf = new SparkConf().setAppName("SedonaVizDemo").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val prop = new Properties()
  val resourcePath = "src/test/resources/"
  val demoOutputPath = "target/demo"
  val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
  prop.load(ConfFile)
  val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
  val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
  val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap"
  val earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot"
  val PointInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  val PointOffset = prop.getProperty("offset").toInt
  val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PointNumPartitions = prop.getProperty("numPartitions").toInt
  val RectangleInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  ConfFile = new FileInputStream(resourcePath + "babylon.rectangle.properties")
  prop.load(ConfFile)
  val RectangleOffset = prop.getProperty("offset").toInt
  val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val RectangleNumPartitions = prop.getProperty("numPartitions").toInt
  val PolygonInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  ConfFile = new FileInputStream(resourcePath + "babylon.polygon.properties")
  prop.load(ConfFile)
  val PolygonOffset = prop.getProperty("offset").toInt
  val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
  val LineStringInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  ConfFile = new FileInputStream(resourcePath + "babylon.linestring.properties")
  prop.load(ConfFile)
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
  var ConfFile = new FileInputStream(resourcePath + "babylon.point.properties")

  if (buildScatterPlot(scatterPlotOutputPath) && buildHeatMap(heatMapOutputPath)
    && buildChoroplethMap(choroplethMapOutputPath) && parallelFilterRenderStitch(parallelFilterRenderStitchOutputPath + "-stitched")
    && parallelFilterRenderNoStitch(parallelFilterRenderStitchOutputPath) && earthdataVisualization(earthdataScatterPlotOutputPath))
    System.out.println("All 5 Demos have passed.")
  else System.out.println("Demos failed.")

  /**
    * Builds the scatter plot.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def buildScatterPlot(outputPath: String): Boolean = {
    val spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
    var visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    var imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, false, true)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    imageGenerator = new ImageGenerator
    imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, outputPath, ImageType.SVG)
    visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, true, true)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    imageGenerator = new ImageGenerator
    imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.distributedVectorImage, "file://" + outputPath + "-distributed", ImageType.SVG)
    true
  }

  /**
    * Builds the heat map.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def buildHeatMap(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    true
  }

  /**
    * Builds the choropleth map.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def buildChoroplethMap(outputPath: String): Boolean = {
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
    imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, outputPath, ImageType.PNG)
    true
  }

  /**
    * Parallel filter render stitch.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def parallelFilterRenderNoStitch(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG, 0, 4, 4)
    true
  }

  /**
    * Parallel filter render stitch.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def parallelFilterRenderStitch(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    true
  }

  def earthdataVisualization(outputPath: String): Boolean = {
    val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
      HDFDataVariableList, HDFDataVariableName, HDFswitchXY, urlPrefix)
    val spatialRDD = new PointRDD(sparkContext, earthdataInputLocation, earthdataNumPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
    val visualizationOperator = new ScatterPlot(1000, 600, spatialRDD.boundaryEnvelope, ColorizeOption.EARTHOBSERVATION, false, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.BLUE, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    true
  }


}
