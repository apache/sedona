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
package org.apache.sedona.stats.outlierDetection

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_MakePoint
import org.apache.spark.sql.{DataFrame, functions => f}

class LocalOutlierFactorTest extends TestBaseScala {

  case class Point(id: Int, x: Double, y: Double, expected_lof: Double)

  def get_data(): DataFrame = {
    // expected pulled from sklearn.neighbors.LocalOutlierFactor
    sparkSession
      .createDataFrame(
        Seq(
          Point(0, 2.0, 2.0, 0.8747092756023607),
          Point(1, 2.0, 3.0, 0.9460678118688717),
          Point(2, 3.0, 3.0, 1.0797104443580348),
          Point(3, 3.0, 2.0, 1.0517766952923475),
          Point(4, 3.0, 1.0, 1.0797104443580348),
          Point(5, 2.0, 1.0, 0.9460678118688719),
          Point(6, 1.0, 1.0, 1.0797104443580348),
          Point(7, 1.0, 2.0, 1.0517766952923475),
          Point(8, 1.0, 3.0, 1.0797104443580348),
          Point(9, 0.0, 2.0, 1.0517766952923475),
          Point(10, 4.0, 2.0, 1.0517766952923475)))
      .withColumn("geometry", ST_MakePoint("x", "y"))
      .drop("x", "y")
  }

  describe("LocalOutlierFactor") {
    it("returns correct results") {
      val resultDf = LocalOutlierFactor.localOutlierFactor(get_data(), 4)
      assert(resultDf.count() == 11)
      assert(
        resultDf
          .filter(f.abs(f.col("expected_lof") - f.col("lof")) < .00000001)
          .count() == 11)
    }
  }
}
