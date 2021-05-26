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

package org.apache.spark.sql.sedona_sql.expressions.subdivide

import org.apache.spark.sql.sedona_sql.expressions.subdivide.GeometrySubDivider.geometryFactory
import org.apache.spark.sql.sedona_sql.expressions.subdivide.GeometrySubDividerConfiguration.DBL_MAX
import org.locationtech.jts.geom.{Geometry, Polygon}

object PivotFinder {
  def findPivot(geom: Geometry, split_ordinate: Int, center: Double, numberOfVertices: Int): Double = {
    var pivot = DBL_MAX;
    if (geom.isInstanceOf[Polygon]) {
      var pivot_eps = DBL_MAX
      var ptEps = DBL_MAX
      val lwPoly = geom.copy().asInstanceOf[Polygon]
      val ringToTrim = if (numberOfVertices >= 2 * lwPoly.getExteriorRing.getNumPoints)
        (geometryFactory.createPolygon(lwPoly.getExteriorRing).getArea +: (0 until lwPoly.getNumInteriorRing)
        .map(lwPoly.getInteriorRingN)
          .map(x => geometryFactory.createPolygon(x).getArea)).zipWithIndex.maxBy(_._1)._2 else 0

      val pointArray = if (ringToTrim == 0) lwPoly.getExteriorRing else lwPoly.getInteriorRingN(ringToTrim - 1)
      for (i <- 0 until pointArray.getNumPoints) {
        val pt = if (split_ordinate == 0) pointArray.getPointN(i).getX else pointArray.getPointN(i).getY
        ptEps = math.abs(pt - center)
        if (pivot_eps > ptEps) {
          pivot = pt
          pivot_eps = ptEps
        }
      }
    }
    if (pivot == DBL_MAX) center else pivot
  }
}
