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

import org.apache.spark.sql.sedona_sql.expressions.subdivide.{Stack, ZeroToleranceGeometrySimplifier}
import org.locationtech.jts.geom.Coordinate

import scala.collection.mutable.ArrayBuffer

object CoordinatesSimplifier {
  def simplifyInPlace(geom: Array[Coordinate], tolerance: Double, minPointsExternal: Int): Array[Coordinate] = {
    if (geom.length < 3 || geom.length <= minPointsExternal) {
      geom
    }
    else if (tolerance == 0 && minPointsExternal <= 2) {
      ZeroToleranceGeometrySimplifier.simplifyInPlaceTolerance0(geom)
    }
    else {
      val kept_points = ArrayBuffer.fill(geom.length)(false)
      kept_points(0) = true
      var itLast = geom.length - 1
      kept_points(itLast) = true
      var keptn = 2
      val iteratorStack = new Stack[Int]()
      iteratorStack.push(0)
      var itFirst = 0

      val toleranceSquared = tolerance * tolerance
      var itTool = if (keptn >= minPointsExternal) toleranceSquared else -1.0

      while (!iteratorStack.isEmpty){
        val splitInPlaceRes = CoordinatesSplitter.splitInPlace(geom, itFirst = itFirst, itLast, itTool)
        if (splitInPlaceRes.split == itFirst){
          itFirst = itLast
          itLast = iteratorStack.pull()
        }
        else {
          kept_points(splitInPlaceRes.split) = true
          keptn = keptn + 1

          iteratorStack.push(itLast)
          itLast = splitInPlaceRes.split
          itTool = if (keptn >= minPointsExternal) toleranceSquared else -1.0
        }
      }

      geom.zipWithIndex.filter{
        case (element, index) => kept_points(index)
      }.map(_._1)
    }
  }
}
