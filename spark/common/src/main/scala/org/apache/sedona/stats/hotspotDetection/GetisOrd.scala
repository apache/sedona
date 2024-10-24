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

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, DataFrame, functions => f}

object GetisOrd {

  def arraySum(arr: Column): Column = f.aggregate(arr, f.lit(0.0), (acc, x) => acc + x)

  private val cdfUDF = udf((z: Double) => {
    new NormalDistribution().cumulativeProbability(z)
  })

  /**
   * Performs the Gi or Gi* statistic on the x column of the dataframe.
   *
   * Weights should be the neighbors of this row. The members of the weights should be comprised
   * of structs containing a value column and a neighbor column. The neighbor column should be the
   * contents of the neighbors with the same types as the parent row (minus neighbors). You can
   * use `wherobots.weighing.add_distance_band_column` to achieve this. To calculate the Gi*
   * statistic, ensure the focal observation is in the neighbors array (i.e. the row is in the
   * weights column) and `star=true`. Significance is calculated with a z score. Permutation tests
   * are not yet implemented and thus island weight does nothing. The following columns will be
   * added: G, E[G], V[G], Z, P.
   *
   * @param dataframe
   *   the dataframe to perform the G statistic on
   * @param x
   *   The column name we want to perform hotspot analysis on
   * @param weights
   *   The column name containing the neighbors array. The neighbor column should be the contents
   *   of the neighbors with the same types as the parent row (minus neighbors). You can use
   *   `wherobots.weighing.add_distance_band_column` to achieve this.
   * @param permutations
   *   Not used. Permutation tests are not supported yet. The number of permutations to use for
   *   the significance test.
   * @param star
   *   Whether the focal observation is in the neighbors array. If true this calculates Gi*,
   *   otherwise Gi
   * @param islandWeight
   *   Not used. The weight for the simulated neighbor used for records without a neighbor in perm
   *   tests
   *
   * @return
   *   A dataframe with the original columns plus the columns G, E[G], V[G], Z, P.
   */
  def gLocal(
      dataframe: DataFrame,
      x: String,
      weights: String = "weights",
      permutations: Int = 0,
      star: Boolean = false,
      islandWeight: Double = 0.0): DataFrame = {

    val removeSelf = f.lit(if (star) 0.0 else 1.0)

    val setStats = dataframe.agg(f.sum(f.col(x)), f.sum(f.pow(f.col(x), 2)), f.count("*")).first()
    val sumOfAllY = f.lit(setStats.get(0))
    val sumOfSquaresofAllY = f.lit(setStats.get(1))
    val countOfAllY = f.lit(setStats.get(2))

    dataframe
      .withColumn(
        "G",
        arraySum(
          f.transform(
            f.col(weights),
            weight =>
              weight("value") * weight("neighbor")(x))) / (sumOfAllY - removeSelf * f.col(x)))
      .withColumn("W", arraySum(f.transform(f.col(weights), weight => weight.getField("value"))))
      .withColumn("EG", f.col("W") / (countOfAllY - removeSelf))
      .withColumn("Y1", (sumOfAllY - removeSelf * f.col(x)) / (countOfAllY - removeSelf))
      .withColumn(
        "Y2",
        ((sumOfSquaresofAllY - removeSelf * f.pow(f.col(x), 2)) / (countOfAllY - removeSelf)) - f
          .pow("Y1", 2.0))
      .withColumn(
        "VG",
        (f.col("W") * (countOfAllY - removeSelf - f.col("W")) * f.col("Y2")) / (f.pow(
          countOfAllY - removeSelf,
          2.0) * (countOfAllY - 1 - removeSelf) * f.pow(f.col("Y1"), 2.0)))
      .withColumn("Z", (f.col("G") - f.col("EG")) / f.sqrt(f.col("VG")))
      .withColumn("P", f.lit(1.0) - cdfUDF(f.abs(f.col("Z"))))
      .drop("W", "Y1", "Y2")
  }
}
