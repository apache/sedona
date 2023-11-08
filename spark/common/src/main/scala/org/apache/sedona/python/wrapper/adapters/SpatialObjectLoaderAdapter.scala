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

package org.apache.sedona.python.wrapper.adapters

import org.apache.sedona.core.spatialRDD.{LineStringRDD, PointRDD, PolygonRDD, SpatialRDD}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}
import org.locationtech.jts.index.SpatialIndex


object SpatialObjectLoaderAdapter {
  def loadPointSpatialRDD(sc: JavaSparkContext, path: String): PointRDD = {
    new PointRDD(sc.objectFile[Point](path))
  }

  def loadPolygonSpatialRDD(sc: JavaSparkContext, path: String): PolygonRDD = {
    new PolygonRDD(sc.objectFile[Polygon](path))
  }

  def loadSpatialRDD(sc: JavaSparkContext, path: String): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = sc.objectFile[Geometry](path)
    spatialRDD
  }

  def loadLineStringSpatialRDD(sc: JavaSparkContext, path: String): LineStringRDD = {
    new LineStringRDD(sc.objectFile[LineString](path))
  }

  def loadIndexRDD(sc: JavaSparkContext, path: String): JavaRDD[SpatialIndex] = {
    sc.objectFile[SpatialIndex](path)
  }
}
