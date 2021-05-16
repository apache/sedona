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
import org.locationtech.jts.geom._

object GeometrySimplifier {

  def simplify(geom: Geometry, preserveCollapsed: Boolean, epsilon: Double): Geometry = geom match {
    case circle: Circle => simplifyCircle(circle, preserveCollapsed, epsilon)
    case collection: GeometryCollection => simplifyGeometryCollection(collection, preserveCollapsed, epsilon)
    case string: LineString => simplifyLineString(string, epsilon, preserveCollapsed)
    case point: Point => point
    case polygon: Polygon => simplifyPolygon(polygon, preserveCollapsed, epsilon)
    case geom: Geometry => geom
  }

  private def simplifyLineString(geom: LineString, epsilon: Double, preserveCollapsed: Boolean): Geometry =
    LinestringSimplifier.simplify(geom, epsilon, preserveCollapsed)

  private def simplifyPolygon(geom: Polygon, preserveCollapsed: Boolean, epsilon: Double): Geometry =
    PolygonSimplifier.simplify(geom, preserveCollapsed, epsilon)

  private def simplifyCircle(geom: Circle, preserveCollapsed: Boolean, epsilon: Double): Geometry =
    CircleSimplifier.simplify(geom, preserveCollapsed, epsilon)

  private def simplifyGeometryCollection(geom: GeometryCollection,
                                         preserveCollapsed: Boolean,
                                         epsilon: Double): Geometry =
    GeometryCollectionSimplifier.simplify(geom, preserveCollapsed, epsilon)

}
