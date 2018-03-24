/*
 * FILE: SpatialJoinShp.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geospark.showcase

import com.vividsolutions.jts.geom.Polygon
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.spatialRDD.PolygonRDD

object SpatialJoinShp extends App {

  def loadShapefile(path: String, numPartitions: Int = 20): PolygonRDD = {
    val shp = new ShapefileRDD(sc, path)
    val polygon = new PolygonRDD(shp.getPolygonRDD, StorageLevel.MEMORY_ONLY)
    //polygon.rawSpatialRDD = polygon.rawSpatialRDD.repartition(numPartitions)
    //polygon.analyze()
    polygon
  }


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val shp1 = new ShapefileRDD(sc, "/Users/jiayu/Downloads/spark4geo_subset/wdpa")
  val wdpa = new PolygonRDD(shp1.getPolygonRDD, StorageLevel.MEMORY_ONLY)

  val shp2 = new ShapefileRDD(sc, "/Users/jiayu/Downloads/spark4geo_subset/amphib")
  val species = new PolygonRDD(shp2.getPolygonRDD, StorageLevel.MEMORY_ONLY)

  //wdpa.spatialPartitioning(GridType.QUADTREE)
  //species.spatialPartitioning(wdpa.partitionTree)


  val result = shp2.getShapeRDD.collect();

  for (a <- 1 until result.size()) {
    println("print..." + result.get(a).getUserData + " END");
  }

  //val query = JoinQuery.SpatialJoinQuery(wdpa, species, false, false)

  println("polygon is " + shp2.getPolygonRDD.take(100).get(55))
  println("userdata is " + wdpa.rawSpatialRDD.take(100).get(55).asInstanceOf[Polygon].getUserData)
  println(species.rawSpatialRDD.count())


  //val user_data_sample = JoinQuery.SpatialJoinQuery(wdpa, species, false, false).first()._1.getUserData
  //if (user_data_sample.toString.isEmpty) println("UserData is empty") else println(user_data_sample)

  //  val join_result = query.rdd.map((tuple: (Polygon, util.HashSet[Polygon])) => (tuple._1, tuple._2.asScala.map(tuple._1.intersection(_).getArea)) )
  //  val intersections = join_result.collect()
}
