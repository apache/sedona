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

import org.apache.spark.sql.sedona_sql.expressions.simplify.CoordinatesSimplifier.simplifyInPlace
import org.locationtech.jts.geom.{Geometry, GeometryFactory, Polygon}

object PolygonSimplifier {
  private val geometryFactory = new GeometryFactory()

  def simplify(geom: Polygon, preserveCollapsed: Boolean, epsilon: Double): Geometry = {
    val exteriorRing = geom.getExteriorRing
    val minPointsExternal = if (preserveCollapsed) 4 else 0

    val simplifiedExterior =
      geometryFactory.createLinearRing(simplifyInPlace(exteriorRing.getCoordinates, epsilon, minPointsExternal))

    if (simplifiedExterior.getNumPoints < 4) {
      simplifiedExterior
    }
    else {
      val exteriorRings = (0 until geom.getNumInteriorRing).map(index => geom.getInteriorRingN(index))
        .map(geom => simplifyInPlace(geom.getCoordinates, epsilon, minPointsExternal))
        .filter(_.length >= 4)
        .map(geometryFactory.createLinearRing)
        .filter(_.getNumPoints >= 4)


      geometryFactory.createPolygon(simplifiedExterior, exteriorRings.toArray)

    }
  }
}
