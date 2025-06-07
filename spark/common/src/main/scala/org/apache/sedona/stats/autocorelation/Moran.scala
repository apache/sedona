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
package org.apache.sedona.stats.autocorelation

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.sedona.stats.autocorrelation.MoranResult
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, explode, pow}

object Moran {
  private val ID_COLUMN = "id"
  private val VALUE_COLUMN = "value"

  private def normSf(x: Double, mean: Double = 0.0, stdDev: Double = 1.0): Double = {
    val normalDist = new NormalDistribution(mean, stdDev)
    1.0 - normalDist.cumulativeProbability(x)
  }

  private def normCdf(x: Double, mean: Double = 0.0, stdDev: Double = 1.0): Double = {
    val normalDist = new NormalDistribution(mean, stdDev)
    normalDist.cumulativeProbability(x)
  }

  def getGlobal(
      dataframe: DataFrame,
      twoTailed: Boolean = true,
      idColumn: String = ID_COLUMN,
      valueColumnName: String = VALUE_COLUMN): MoranResult = {
    val spark = dataframe.sparkSession
    import spark.implicits._

    val data = dataframe
      .selectExpr(s"avg($valueColumnName)", "count(*)")
      .as[(Double, Long)]
      .head()

    val yMean = data._1

    val n = data._2

    val explodedWeights = dataframe
      .select(col(idColumn), explode(col("weights")).alias("col"))
      .select(
        $"$idColumn".alias("id"),
        $"col.neighbor.$idColumn".alias("n_id"),
        $"col.value".alias("weight_value"))

    val s1Data = explodedWeights
      .alias("left")
      .join(
        explodedWeights.alias("right"),
        $"left.n_id" === $"right.id" && $"right.n_id" === $"left.id")
      .select(
        $"left.id",
        $"right.weight_value".alias("b_weight_value"),
        pow(($"right.weight_value" + $"left.weight_value"), 2).alias("s1_comp"),
        $"left.weight_value".alias("a_weight"))

    val sStats = s1Data
      .selectExpr(
        "CAST(sum(s1_comp)/2 AS DOUBLE) AS s1_comp_sum",
        "CAST(sum(a_weight) AS DOUBLE) AS a_weight_sum",
        "CAST(sum(b_weight_value) AS DOUBLE) AS b_weight_value_sum")
      .as[(Double, Double, Double)]
      .head()

    val s1 = sStats._1

    val inumData = dataframe
      .selectExpr(
        s"$idColumn AS id",
        s"$valueColumnName AS value",
        f"$valueColumnName - ${yMean} AS z",
        f"transform(weights, w -> struct(w.neighbor.$idColumn AS id, w.value AS w, w.neighbor.$valueColumnName, w.neighbor.$valueColumnName - ${yMean} AS z)) AS weight")
      .selectExpr(
        "z",
        "AGGREGATE(transform(weight, x-> x.z*x.w), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS ZL",
        "AGGREGATE(transform(weight, x-> x.w), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS w_sum",
        "AGGREGATE(transform(weight, x-> x.w), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS w_sq_sum",
        "z * z AS z2ss_comp")
      .selectExpr("*", "(z * zl) AS inum_comp")
      .selectExpr("sum(inum_comp)", "sum(w_sum)", "sum(z2ss_comp)")

    val s2Data = s1Data
      .groupBy("id")
      .agg(
        functions.sum("b_weight_value").alias("s_b_weight_value"),
        functions.sum("a_weight").alias("s_a_weight"))
      .selectExpr("pow(s_b_weight_value + s_a_weight, 2) AS summed")

    val s2 = s2Data.selectExpr("sum(summed)").as[Double].head()
    val inumResult = inumData.as[(Double, Double, Double)].head()
    val inum = inumResult._1

    val s0 = inumResult._2

    val z2ss = inumResult._3

    val i = n / s0 * inum / z2ss
    val ei = -1.0 / (n - 1)
    val n2 = n * n
    val s02 = s0 * s0
    val vNum = n2 * s1 - n * s2 + 3 * s02
    val vDen = (n - 1) * (n + 1) * s02
    val viNorm = (vNum / vDen) - math.pow(1.0 / (n - 1), 2)
    val seINorm = math.pow(viNorm, 0.5)
    val zNorm = (i - ei) / seINorm

    val pNorm = if (zNorm > 0) normSf(zNorm) else normCdf(zNorm)
    val pNormFinal = if (twoTailed) pNorm * 2.0 else pNorm

    new MoranResult(i, pNormFinal, zNorm)
  }
}
