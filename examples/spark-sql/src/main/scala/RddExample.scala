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

import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{CircleRDD, SpatialRDD}
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.viz.core.{ImageGenerator, RasterOverlayOperator}
import org.apache.sedona.viz.extension.visualizationEffect.{HeatMap, ScatterPlot}
import org.apache.sedona.viz.utils.ImageType
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import java.awt.Color


object RddExample {

  val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  // Data link (in shapefile): https://geo.nyu.edu/catalog/nyu_2451_34514
  val nycArealandmarkShapefileLocation = resourceFolder+"nyc-area-landmark-shapefile"

  // Data link (in CSV): http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
  val nyctripCSVLocation = resourceFolder+"yellow_tripdata_2009-01-subset.csv"

  val colocationMapLocation = System.getProperty("user.dir")+"/colocationMap"

  /**
   * Visualizes spatial co-location between NYC landmarks and taxi pickup points.
   * Creates an overlay visualization with landmarks (scatter plot) and taxi trips (heat map).
   *
   * Note: This function uses RDD API to demonstrate low-level spatial operations.
   * For DataFrame-based approach, see SqlExample.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def visualizeSpatialColocation(sedona: SparkSession): Unit =
  {
    // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
    val arealmRDD = ShapefileReader.readToPolygonRDD(sedona.sparkContext, nycArealandmarkShapefileLocation)

    // Prepare NYC taxi trips. Only use the taxi trips' pickup points
    val tripDf = sedona.read.format("csv").option("delimiter",",").option("header","false").load(nyctripCSVLocation)
    // Convert from DataFrame to RDD. This can also be done directly through Sedona RDD API.
    tripDf.createOrReplaceTempView("tripdf")
    val tripRDD = Adapter.toSpatialRdd(sedona.sql("select ST_Point(cast(tripdf._c0 as Decimal(24, 14)), cast(tripdf._c1 as Decimal(24, 14))) as point from tripdf")
      , "point")

    // Convert the Coordinate Reference System from degree-based to meter-based. This returns the accurate distance calculate.
    arealmRDD.CRSTransform("epsg:4326","epsg:3857")
    tripRDD.CRSTransform("epsg:4326","epsg:3857")

    // !!!NOTE!!!: Analyze RDD step can be avoided if you know the rectangle boundary of your dataset and approximate total count.
    arealmRDD.analyze()
    tripRDD.analyze()

    val imageResolutionX = 1000
    val imageResolutionY = 1000

    val frontImage = new ScatterPlot(imageResolutionX, imageResolutionY, arealmRDD.boundaryEnvelope, true)
    frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
    frontImage.Visualize(sedona.sparkContext, arealmRDD)

    val backImage = new HeatMap(imageResolutionX, imageResolutionY, arealmRDD.boundaryEnvelope, true, 1)
    backImage.Visualize(sedona.sparkContext, tripRDD)

    val overlayOperator = new RasterOverlayOperator(backImage.rasterImage)
    overlayOperator.JoinImage(frontImage.rasterImage)

    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, colocationMapLocation, ImageType.PNG)
  }

  /**
   * Calculates spatial co-location using Ripley's K function.
   * Analyzes whether taxi trips are clustered around NYC landmarks at various distance thresholds.
   * Uses distance join queries to compute co-location statistics.
   *
   * The Ripley's K function tests for spatial clustering/dispersion by comparing
   * observed vs expected point patterns at increasing distance bands.
   *
   * @param sedona SparkSession with Sedona extensions enabled
   */
  def calculateSpatialColocation(sedona: SparkSession): Unit =
  {

    // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
    var arealmRDD = new SpatialRDD[Geometry]()
    arealmRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, nycArealandmarkShapefileLocation)
    // Use the center point of area landmarks to check co-location. This is required by Ripley's K function.
    arealmRDD.rawSpatialRDD = arealmRDD.rawSpatialRDD.rdd.map[Geometry](f=>
    {
      val geom = f.getCentroid
      // Copy non-spatial attributes
      geom.setUserData(f.getUserData)
      geom
    })

    // The following two lines are optional. The purpose is to show the structure of the shapefile.
    val arealmDf = Adapter.toDf(arealmRDD, sedona)
    arealmDf.show()

    // Prepare NYC taxi trips. Only use the taxi trips' pickup points
    val tripDf = sedona.read.format("csv").option("delimiter",",").option("header","false").load(nyctripCSVLocation)
    tripDf.show() // Optional
    // Convert from DataFrame to RDD. This can also be done directly through Sedona RDD API.
    tripDf.createOrReplaceTempView("tripdf")
    val tripRDD = Adapter.toSpatialRdd(sedona.sql("select ST_Point(cast(tripdf._c0 as Decimal(24, 14)), cast(tripdf._c1 as Decimal(24, 14))) as point, 'def' as trip_attr from tripdf")
      , "point")

    // Convert the Coordinate Reference System from degree-based to meter-based. This returns the accurate distance calculate.
    arealmRDD.CRSTransform("epsg:4326","epsg:3857")
    tripRDD.CRSTransform("epsg:4326","epsg:3857")

    // !!!NOTE!!!: Analyze RDD step can be avoided if you know the rectangle boundary of your dataset and approximate total count.
    arealmRDD.analyze()
    tripRDD.analyze()

    // Cache indexed NYC taxi trip rdd to improve iterative performance
    tripRDD.spatialPartitioning(GridType.KDBTREE)
    tripRDD.buildIndex(IndexType.QUADTREE, true)
    tripRDD.indexedRDD = tripRDD.indexedRDD.cache()

    // Parameter settings. Check the definition of Ripley's K function.
    val area = tripRDD.boundaryEnvelope.getArea
    val maxDistance = 0.01*Math.max(tripRDD.boundaryEnvelope.getHeight,tripRDD.boundaryEnvelope.getWidth)
    val iterationTimes = 10
    val distanceIncrement = maxDistance/iterationTimes
    val beginDistance = 0.0
    var currentDistance = 0.0

    // Start the iteration - test multiple distance bands
    println("distance(meter),observedL,difference,coLocationStatus")
    for (i <- 1 to iterationTimes)
    {
      currentDistance = beginDistance + i*distanceIncrement

      val bufferedArealmRDD = new CircleRDD(arealmRDD,currentDistance)
      bufferedArealmRDD.spatialPartitioning(tripRDD.getPartitioner)
      //    Run Sedona Distance Join Query
      val adjacentMatrix = JoinQuery.DistanceJoinQueryFlat(tripRDD, bufferedArealmRDD,true,true)

      //      Uncomment the following two lines if you want to see what the join result looks like in SparkSQL
      //      import scala.collection.JavaConversions._
      //      val adjacentMatrixDf = Adapter.toDf(adjacentMatrix, arealmRDD.fieldNames, tripRDD.fieldNames, sparkSession)
      //      adjacentMatrixDf.show()

      val observedK = adjacentMatrix.count()*area*1.0/(arealmRDD.approximateTotalCount*tripRDD.approximateTotalCount)
      val observedL = Math.sqrt(observedK/Math.PI)
      val expectedL = currentDistance
      val colocationDifference = observedL  - expectedL
      val colocationStatus = {if (colocationDifference>0) "Co-located" else "Dispersed"}

      println(s"""$currentDistance,$observedL,$colocationDifference,$colocationStatus""")
    }
  }
}
