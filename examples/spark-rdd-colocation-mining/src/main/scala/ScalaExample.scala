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

import java.awt.Color
import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{CircleRDD, SpatialRDD}
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.{ImageGenerator, RasterOverlayOperator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.extension.visualizationEffect.{HeatMap, ScatterPlot}
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.sedona.viz.utils.ImageType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry


object ScalaExample extends App{

	val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  // Data link (in shapefile): https://geo.nyu.edu/catalog/nyu_2451_34514
  val nycArealandmarkShapefileLocation = resourceFolder+"nyc-area-landmark-shapefile"

  // Data link (in CSV): http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
  val nyctripCSVLocation = resourceFolder+"yellow_tripdata_2009-01-subset.csv"

  val colocationMapLocation = System.getProperty("user.dir")+"/colocationMap"

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  visualizeSpatialColocation()
  calculateSpatialColocation()

  System.out.println("Finished Sedona Spatial Analysis Example")


  def visualizeSpatialColocation(): Unit =
  {
    val sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
        .master("local[*]").appName("Sedona-Analysis").getOrCreate()

    SedonaSQLRegistrator.registerAll(sparkSession)
    SedonaVizRegistrator.registerAll(sparkSession)

    // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
    var arealmRDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)

    // Prepare NYC taxi trips. Only use the taxi trips' pickup points
    var tripDf = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(nyctripCSVLocation)
    // Convert from DataFrame to RDD. This can also be done directly through Sedona RDD API.
    tripDf.createOrReplaceTempView("tripdf")
    var tripRDD = Adapter.toSpatialRdd(sparkSession.sql("select ST_Point(cast(tripdf._c0 as Decimal(24, 14)), cast(tripdf._c1 as Decimal(24, 14))) as point from tripdf")
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
    frontImage.Visualize(sparkSession.sparkContext, arealmRDD)

    val backImage = new HeatMap(imageResolutionX, imageResolutionY, arealmRDD.boundaryEnvelope, true, 1)
    backImage.Visualize(sparkSession.sparkContext, tripRDD)

    val overlayOperator = new RasterOverlayOperator(backImage.rasterImage)
    overlayOperator.JoinImage(frontImage.rasterImage)

    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, colocationMapLocation, ImageType.PNG)

    sparkSession.stop()
  }

  def calculateSpatialColocation(): Unit =
  {
    val sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
      master("local[*]").appName("Sedona-Analysis").getOrCreate()

    SedonaSQLRegistrator.registerAll(sparkSession)
    SedonaVizRegistrator.registerAll(sparkSession)

    // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
    var arealmRDD = new SpatialRDD[Geometry]()
    arealmRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, nycArealandmarkShapefileLocation)
    // Use the center point of area landmarks to check co-location. This is required by Ripley's K function.
    arealmRDD.rawSpatialRDD = arealmRDD.rawSpatialRDD.rdd.map[Geometry](f=>
    {
      var geom = f.getCentroid
      // Copy non-spatial attributes
      geom.setUserData(f.getUserData)
      geom
    })

    // The following two lines are optional. The purpose is to show the structure of the shapefile.
    var arealmDf = Adapter.toDf(arealmRDD, sparkSession)
    arealmDf.show()

    // Prepare NYC taxi trips. Only use the taxi trips' pickup points
    var tripDf = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(nyctripCSVLocation)
    tripDf.show() // Optional
    // Convert from DataFrame to RDD. This can also be done directly through Sedona RDD API.
    tripDf.createOrReplaceTempView("tripdf")
    var tripRDD = Adapter.toSpatialRdd(sparkSession.sql("select ST_Point(cast(tripdf._c0 as Decimal(24, 14)), cast(tripdf._c1 as Decimal(24, 14))) as point, 'def' as trip_attr from tripdf")
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

    // Start the iteration
    println("distance(meter),observedL,difference,coLocationStatus")
    for (i <- 1 to iterationTimes)
    {
      currentDistance = beginDistance + i*distanceIncrement

      var bufferedArealmRDD = new CircleRDD(arealmRDD,currentDistance)
      bufferedArealmRDD.spatialPartitioning(tripRDD.getPartitioner)
//    Run Sedona Distance Join Query
      var adjacentMatrix = JoinQuery.DistanceJoinQueryFlat(tripRDD, bufferedArealmRDD,true,true)

//      Uncomment the following two lines if you want to see what the join result looks like in SparkSQL
//      import scala.collection.JavaConversions._
//      var adjacentMatrixDf = Adapter.toDf(adjacentMatrix, arealmRDD.fieldNames, tripRDD.fieldNames, sparkSession)
//      adjacentMatrixDf.show()

      var observedK = adjacentMatrix.count()*area*1.0/(arealmRDD.approximateTotalCount*tripRDD.approximateTotalCount)
      var observedL = Math.sqrt(observedK/Math.PI)
      var expectedL = currentDistance
      var colocationDifference = observedL  - expectedL
      var colocationStatus = {if (colocationDifference>0) "Co-located" else "Dispersed"}

      println(s"""$currentDistance,$observedL,$colocationDifference,$colocationStatus""")
    }
    sparkSession.stop()
  }

}