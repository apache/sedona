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

package org.apache.spark.sql.sedona_sql.expressions.simplify

import org.apache.sedona.core.geometryObjects.Circle
import org.locationtech.jts.geom.{Geometry, GeometryCollection, GeometryFactory, LineString, Point, Polygon}

object GeometryCollectionSimplifier {
  private val geometryFactory = new GeometryFactory()

  def simplify(geom: GeometryCollection, preserveCollapsed: Boolean, epsilon: Double): Geometry = {

    val numberOfGeometries = geom.getNumGeometries
    val geometries = (0 until numberOfGeometries).map(
      geom.getGeometryN
    ).map {
      case circle: Circle => CircleSimplifier.simplify(circle, preserveCollapsed, epsilon)
      case string: LineString => LinestringSimplifier.simplify(string, epsilon, preserveCollapsed)
      case point: Point => point
      case polygon: Polygon => PolygonSimplifier.simplify(polygon, preserveCollapsed, epsilon)
      case collection: GeometryCollection => simplify(collection, preserveCollapsed, epsilon)
      case _ => null      
    }.filter(_!=null).toArray

    val distinctGeometries = geometries.map(_.getGeometryType).distinct
    if (distinctGeometries.length ==1){
      distinctGeometries.head match {
        case "LineString" => geometryFactory.createMultiLineString(geometries.map(x => x.asInstanceOf[LineString]))
        case "Polygon" => geometryFactory.createMultiPolygon(geometries.map(x => x.asInstanceOf[Polygon]))
        case "Point" => geometryFactory.createMultiPoint(geometries.map(x => x.asInstanceOf[Point]))
      }
    }
    else geometryFactory.createGeometryCollection(geometries)
  }
}
