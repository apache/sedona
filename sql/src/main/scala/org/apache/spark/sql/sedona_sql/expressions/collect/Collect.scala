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

package org.apache.spark.sql.sedona_sql.expressions.collect

import org.apache.sedona.core.geometryObjects.Circle
import org.locationtech.jts.geom.{Geometry, GeometryCollection, GeometryFactory, LineString, Point, Polygon}
import scala.collection.JavaConverters._


object Collect {
  private val geomFactory = new GeometryFactory()

  def createMultiGeometry(geometries : Seq[Geometry]): Geometry = {
    if (geometries.length>1){
      geomFactory.buildGeometry(geometries.asJava)
    }
    else if(geometries.length==1){
      createMultiGeometryFromOneElement(geometries.head)
    }
    else{
      geomFactory.createGeometryCollection()
    }
  }

  private def createMultiGeometryFromOneElement(geom: Geometry): Geometry = {
    geom match {
      case circle: Circle                 => geomFactory.createGeometryCollection(Array(circle))
      case collection: GeometryCollection => collection
      case string: LineString =>
        geomFactory.createMultiLineString(Array(string))
      case point: Point     => geomFactory.createMultiPoint(Array(point))
      case polygon: Polygon => geomFactory.createMultiPolygon(Array(polygon))
      case _                => geomFactory.createGeometryCollection()
    }
  }
}
