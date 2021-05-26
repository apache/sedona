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

import org.locationtech.jts.geom.Coordinate

object CoordinatesSplitter extends SimplifyingParameters{
  def splitInPlace(geom: Array[Coordinate],
                           itFirst: Int,
                           itLast: Int,
                           maxDistanceSquared: Double): SplitInPlace = {
    var split = itFirst
    var maxDistance = maxDistanceSquared
    if ((itFirst - itLast) < 2) SplitInPlace(geom, itFirst)
    val pointA = geom(itFirst)
    val pointB = geom(itLast)
    if (calcualteSquaredDistance(pointA, pointB) < DBL_EPSILON){
      for (itk <- Range(itFirst + 1, itLast)){
        val pk = geom(itk)
        val squaredDistance = calcualteSquaredDistance(pk, pointA)
        if (squaredDistance > maxDistance){
          split = itk
          maxDistance = squaredDistance
        }
      }
    }

    val ba_x = pointB.x - pointA.x
    val ba_y = pointB.y - pointA.y
    val ab_length_sqr = (ba_x * ba_x + ba_y * ba_y)
    maxDistance *= ab_length_sqr
    for (itk <- Range(itFirst + 1, itLast)){
      val c = geom(itk)
      var distance_sqr = 0.0
      val ca_x = c.x - pointA.x
      val ca_y = c.y - pointA.y
      val dot_ac_ab = (ca_x * ba_x + ca_y * ba_y)
      if (dot_ac_ab <= 0.0) {
        distance_sqr = calcualteSquaredDistance(c, pointA) * ab_length_sqr;
      }
      else if (dot_ac_ab >= ab_length_sqr) {
        distance_sqr = calcualteSquaredDistance(c, pointB) * ab_length_sqr;
      }
      else {
        val s_numerator = ca_x * ba_y - ca_y * ba_x
        distance_sqr = s_numerator * s_numerator
      }

      if (distance_sqr > maxDistance)
      {
        split = itk
        maxDistance = distance_sqr
      }
    }

    SplitInPlace(geom, split)
  }

  private def calcualteSquaredDistance(geomA: Coordinate, geomB: Coordinate): Double = {
    val distance = geomA.distance(geomB)
    distance * distance
  }

  case class SplitInPlace(geom: Array[Coordinate], split: Int)
}
