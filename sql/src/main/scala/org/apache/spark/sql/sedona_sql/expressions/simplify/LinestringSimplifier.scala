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

import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString}

object LinestringSimplifier {
  private val geometryFactory = new GeometryFactory()

  def simplify(geom: LineString, epsilon: Double, preserveCollapsed: Boolean): Geometry = {
    val simplified = CoordinatesSimplifier.simplifyInPlace(geom.getCoordinates, epsilon, 2)

    if (simplified.length == 1){
      if (preserveCollapsed) geometryFactory.createLineString(simplified ++ simplified)
      else geometryFactory.createLineString(simplified)
    }
    else if (simplified.length == 2 && !preserveCollapsed){
      if (simplified(0) == simplified(1)) geometryFactory.createLineString(simplified)
      else geometryFactory.createLineString(simplified)
    }
    else {
      geometryFactory.createLineString(simplified)
    }
  }
}
