/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Main.resourceFolder
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{PointRDD, PolygonRDD, RectangleRDD}
import org.apache.sedona.viz.core.{ImageGenerator, ImageSerializableWrapper, RasterOverlayOperator}
import org.apache.sedona.viz.extension.visualizationEffect.{ChoroplethMap, HeatMap, ScatterPlot}
import org.apache.sedona.viz.utils.ImageType
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Envelope

import java.awt.Color


object VizExample {

  val demoOutputPath = "target/demo"

  val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
  val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
  val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
  val parallelFilterRenderOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrender-heatmap"
  val sqlApiOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/sql-heatmap"

  val PointInputLocation = resourceFolder + "arealm.csv"
  val PointOffset = 0
  val PointSplitter = FileDataSplitter.CSV
  val PointNumPartitions = 5

  val RectangleInputLocation = resourceFolder + "zcta510.csv"
  val RectangleSplitter = FileDataSplitter.CSV
  val RectangleNumPartitions = 5

  val PolygonInputLocation = resourceFolder + "primaryroads-polygon.csv"
  val PolygonSplitter = FileDataSplitter.CSV
  val PolygonNumPartitions = 5
  val USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000)

  def buildScatterPlot(sedona: SparkSession): Boolean = {
    val spatialRDD = new PolygonRDD(sedona.sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
    var visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sedona.sparkContext, spatialRDD)
    var imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, scatterPlotOutputPath, ImageType.PNG)
    true
  }

  def buildHeatMap(sedona: SparkSession): Boolean = {
    val spatialRDD = new RectangleRDD(sedona.sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
    visualizationOperator.Visualize(sedona.sparkContext, spatialRDD)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, heatMapOutputPath, ImageType.PNG)
    true
  }


  def buildChoroplethMap(sedona: SparkSession): Boolean = {
    val spatialRDD = new PointRDD(sedona.sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions)
    val queryRDD = new PolygonRDD(sedona.sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
    spatialRDD.spatialPartitioning(GridType.KDBTREE)
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner)
    spatialRDD.buildIndex(IndexType.QUADTREE, true)
    val joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, false)
    val visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
    visualizationOperator.Visualize(sedona.sparkContext, joinResult)
    val frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false)
    frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
    frontImage.Visualize(sedona.sparkContext, queryRDD)
    val overlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage)
    overlayOperator.JoinImage(frontImage.rasterImage)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, choroplethMapOutputPath, ImageType.PNG)
    true
  }

  def parallelFilterRenderNoStitch(sedona: SparkSession): Boolean = {
    val spatialRDD = new RectangleRDD(sedona.sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
    visualizationOperator.Visualize(sedona.sparkContext, spatialRDD)
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, parallelFilterRenderOutputPath, ImageType.PNG)
    true
  }

  def sqlApiVisualization(sedona: SparkSession): Boolean = {
    var pointDf = sedona.read.format("csv").option("delimiter", ",").option("header", "false").load(PointInputLocation)
    pointDf.selectExpr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as shape")
      .filter("ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)").createOrReplaceTempView("pointtable")
    sedona.sql(
      """
				|CREATE OR REPLACE TEMP VIEW pixels AS
				|SELECT pixel, shape FROM pointtable
				|LATERAL VIEW Explode(ST_Pixelize(shape, 256, 256, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000))) AS pixel
			""".stripMargin)
    sedona.sql(
      """
				|CREATE OR REPLACE TEMP VIEW pixelaggregates AS
				|SELECT pixel, count(*) as weight
				|FROM pixels
				|GROUP BY pixel
			""".stripMargin)
    sedona.sql(
      """
				|CREATE OR REPLACE TEMP VIEW images AS
				|SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates), 'red')) AS image
				|FROM pixelaggregates
			""".stripMargin)
    var image = sedona.table("images").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
    var imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(image, sqlApiOutputPath, ImageType.PNG)
    sedona.sql(
      """
				|CREATE OR REPLACE TEMP VIEW imagestring AS
				|SELECT ST_EncodeImage(image)
				|FROM images
			""".stripMargin)
    sedona.table("imagestring").show()
    true
  }

}
