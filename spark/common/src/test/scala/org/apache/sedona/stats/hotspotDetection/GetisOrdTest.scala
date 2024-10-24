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
package org.apache.sedona.stats.hotspotDetection

import org.apache.sedona.sql.TestBaseScala
import org.apache.sedona.stats.Weighting
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_MakePoint
import org.apache.spark.sql.{DataFrame, functions => f}

class GetisOrdTest extends TestBaseScala {
  case class Point(id: Int, x: Double, y: Double, v: Double)

  def get_data(): DataFrame = {
    sparkSession
      .createDataFrame(
        Seq(
          Point(0, 2.0, 2.0, 0.9),
          Point(1, 2.0, 3.0, 1.2),
          Point(2, 3.0, 3.0, 1.2),
          Point(3, 3.0, 2.0, 1.2),
          Point(4, 3.0, 1.0, 1.2),
          Point(5, 2.0, 1.0, 2.2),
          Point(6, 1.0, 1.0, 1.2),
          Point(7, 1.0, 2.0, 0.2),
          Point(8, 1.0, 3.0, 1.2),
          Point(9, 0.0, 2.0, 1.0),
          Point(10, 4.0, 2.0, 1.2)))
      .withColumn("geometry", ST_MakePoint("x", "y"))
      .drop("x", "y")
  }

  describe("glocal") {
    it("returns one row per input row with expected columns, binary") {
      val distanceBandedDf = Weighting.addDistanceBandColumn(
        get_data(),
        1.0,
        includeZeroDistanceNeighbors = true,
        includeSelf = true,
        geometry = "geometry")

      val actualResults =
        GetisOrd.gLocal(distanceBandedDf, "v", "weights", star = true)

      val expectedColumnNames =
        Array("id", "v", "geometry", "weights", "G", "EG", "VG", "Z", "P").sorted

      assert(actualResults.count() == 11)
      assert(actualResults.columns.sorted === expectedColumnNames)

      for (columnName <- expectedColumnNames) {
        assert(actualResults.filter(f.col(columnName).isNull).count() == 0)
      }
    }

    it("returns one row per input row with expected columns, idw") {

      val distanceBandedDf =
        Weighting.addDistanceBandColumn(get_data(), 1.0, binary = false, alpha = -.5)

      val expectedResults = GetisOrd.gLocal(distanceBandedDf, "v", "weights")

      assert(expectedResults.count() == 11)
      assert(
        expectedResults.columns.sorted === Array(
          "id",
          "v",
          "geometry",
          "weights",
          "G",
          "EG",
          "VG",
          "Z",
          "P").sorted)
    }
  }
}
