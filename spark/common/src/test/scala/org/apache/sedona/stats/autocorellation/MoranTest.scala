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
package org.apache.sedona.stats.autocorellation

import org.apache.sedona.sql.TestBaseScala
import org.apache.sedona.stats.Weighting
import org.apache.sedona.stats.autocorelation.Moran
import org.apache.spark.sql.functions.expr

class MoranTest extends TestBaseScala with AutoCorrelationFixtures {
  describe("Moran's I") {
    it("correlation exists") {
      val weights = Weighting
        .addDistanceBandColumn(
          positiveCorrelationFrame,
          1.0,
          savedAttributes = Seq("id", "value"))
        .withColumn(
          "weights",
          expr("transform(weights, w -> struct(w.neighbor, w.value/size(weights) AS value))"))

      weights.cache().count()

      val moranResult = Moran.getGlobal(weights, idColumn = "id")

      assert(moranResult.getPNorm < 0.0001)
      assert(moranResult.getI > 0.99)
    }

    it("different id and value column names") {
      val weights = Weighting
        .addDistanceBandColumn(
          positiveCorrelationFrame
            .selectExpr("id AS index", "value as feature_value", "geometry"),
          1.0,
          savedAttributes = Seq("index", "feature_value"))
        .withColumn(
          "weights",
          expr("transform(weights, w -> struct(w.neighbor, w.value/size(weights) AS value))"))

      weights.cache().count()

      val moranResult =
        Moran.getGlobal(weights, idColumn = "index", valueColumnName = "feature_value")

      assert(moranResult.getPNorm < 0.0001)
      assert(moranResult.getI > 0.99)
    }

    it("correlation is negative") {
      val weights = Weighting
        .addDistanceBandColumn(
          negativeCorrelationFrame,
          1.0,
          savedAttributes = Seq("id", "value"))
        .withColumn(
          "weights",
          expr("transform(weights, w -> struct(w.neighbor, w.value/size(weights) AS value))"))

      weights.cache().count()

      val moranResult = Moran.getGlobal(weights)

      assert(moranResult.getPNorm < 0.0001)
      assert(moranResult.getI < -0.99)
      assert(moranResult.getI > -1)
    }

    it("zero correlation exists") {
      val weights = Weighting
        .addDistanceBandColumn(zeroCorrelationFrame, 2.0, savedAttributes = Seq("id", "value"))
        .withColumn(
          "weights",
          expr("transform(weights, w -> struct(w.neighbor, w.value/size(weights) AS value))"))

      weights.cache().count()

      val moranResult = Moran.getGlobal(weights)

      assert(moranResult.getPNorm < 0.44 && moranResult.getPNorm > 0.43)
      assert(moranResult.getI < 0.16 && moranResult.getI > 0.15)
    }
  }
}
