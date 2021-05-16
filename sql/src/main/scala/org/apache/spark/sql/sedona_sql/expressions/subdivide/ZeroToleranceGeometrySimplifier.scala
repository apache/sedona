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

import org.locationtech.jts.geom.Coordinate

import scala.annotation.tailrec

object ZeroToleranceGeometrySimplifier {

  def simplifyInPlaceTolerance0(geom: Array[Coordinate]): Array[Coordinate] =
    simplifyInPlaceHelper(geom, Seq(), 1, geom.length - 1, geom(0), 0).toArray

  @tailrec
  private def simplifyInPlaceHelper(geom: Array[Coordinate],
                                    resultArray: Seq[Coordinate],
                                    currentIndex: Int,
                                    lastIndex: Int,
                                    keptPoint: Coordinate,
                                    keptIt: Int
                                   ): Seq[Coordinate] = {
    if (currentIndex == lastIndex) if (keptIt != lastIndex) resultArray :+ keptPoint :+ geom(lastIndex)
    else resultArray :+ geom(lastIndex)
    else {
      val currPt = geom(currentIndex)
      val nextPt = geom(currentIndex + 1)
      val ba_x = nextPt.x - keptPoint.x
      val ba_y = nextPt.y - keptPoint.y
      val ab_length_sqr = ba_x * ba_x + ba_y * ba_y

      val ca_x = currPt.x - keptPoint.x
      val ca_y = currPt.y - keptPoint.y
      val dot_ac_ab = ca_x * ba_x + ca_y * ba_y;
      val s_numerator = ca_x * ba_y - ca_y * ba_x

      val isEligible = dot_ac_ab < 0.0 || dot_ac_ab > ab_length_sqr || s_numerator != 0

      simplifyInPlaceHelper(
        geom = geom,
        resultArray = if (keptIt != currentIndex && isEligible) resultArray :+ keptPoint else resultArray,
        currentIndex + 1,
        lastIndex = lastIndex,
        keptPoint = if (isEligible) currPt else keptPoint,
        keptIt = if (isEligible) keptIt + 1 else keptIt
      )
    }
  }
}
