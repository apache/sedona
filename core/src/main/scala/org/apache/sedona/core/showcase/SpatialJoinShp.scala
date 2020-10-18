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

package org.apache.sedona.core.showcase

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileRDD
import org.apache.sedona.core.spatialRDD.PolygonRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Polygon

object SpatialJoinShp extends App {

  val conf = new SparkConf().setAppName("SpatialJoinSpeciesPA").setMaster("local[4]")


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val sc = new SparkContext(conf)
  val shp1 = new ShapefileRDD(sc, "/Users/jiayu/Downloads/spark4geo_subset/wdpa")
  val wdpa = new PolygonRDD(shp1.getPolygonRDD, StorageLevel.MEMORY_ONLY)
  val shp2 = new ShapefileRDD(sc, "/Users/jiayu/Downloads/spark4geo_subset/amphib")
  val species = new PolygonRDD(shp2.getPolygonRDD, StorageLevel.MEMORY_ONLY)
  val result = shp2.getShapeRDD.collect();

  //wdpa.spatialPartitioning(GridType.QUADTREE)
  //species.spatialPartitioning(wdpa.partitionTree)

  def loadShapefile(path: String, numPartitions: Int = 20): PolygonRDD = {
    val shp = new ShapefileRDD(sc, path)
    val polygon = new PolygonRDD(shp.getPolygonRDD, StorageLevel.MEMORY_ONLY)
    //polygon.rawSpatialRDD = polygon.rawSpatialRDD.repartition(numPartitions)
    //polygon.analyze()
    polygon
  }

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
