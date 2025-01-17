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

import org.apache.sedona.util.DfUtils.getGeometryColumnName
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Distance, ST_DistanceSphere}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => f}

object LocalOutlierFactor {

  private val ID_COLUMN_NAME = "__id"
  private val CONTENTS_COLUMN_NAME = "__contents"

  /**
   * Annotates a dataframe with a column containing the local outlier factor for each data record.
   * The dataframe should contain at least one GeometryType column. Rows must be unique. If one
   * geometry column is present it will be used automatically. If two are present, the one named
   * 'geometry' will be used. If more than one are present and neither is named 'geometry', the
   * column name must be provided.
   *
   * @param dataframe
   *   dataframe containing the point geometries
   * @param k
   *   number of nearest neighbors that will be considered for the LOF calculation
   * @param geometry
   *   name of the geometry column
   * @param handleTies
   *   whether to handle ties in the k-distance calculation. Default is false
   * @param useSphere
   *   whether to use a cartesian or spheroidal distance calculation. Default is false
   * @param resultColumnName
   *   the name of the column containing the lof for each row. Default is "lof"
   *
   * @return
   *   A DataFrame containing the lof for each row
   */
  def localOutlierFactor(
      dataframe: DataFrame,
      k: Int = 20,
      geometry: String = null,
      handleTies: Boolean = false,
      useSphere: Boolean = false,
      resultColumnName: String = "lof"): DataFrame = {

    if (k < 1)
      throw new IllegalArgumentException("k must be a positive integer")

    val prior: String = if (handleTies) {
      val prior =
        SparkSession.getActiveSession.get.conf
          .get("spark.sedona.join.knn.includeTieBreakers", "false")
      SparkSession.getActiveSession.get.conf.set("spark.sedona.join.knn.includeTieBreakers", true)
      prior
    } else "false" // else case to make compiler happy

    val distanceFunction: (Column, Column) => Column =
      if (useSphere) ST_DistanceSphere else ST_Distance
    val useSpheroidString = if (useSphere) "True" else "False" // for the SQL expression

    val geometryColumn = if (geometry == null) getGeometryColumnName(dataframe) else geometry

    val KNNFunction = "ST_KNN"

    // Store original contents, prep necessary columns
    val formattedDataframe = dataframe
      .withColumn(CONTENTS_COLUMN_NAME, f.struct("*"))
      .withColumn(ID_COLUMN_NAME, f.sha2(f.to_json(f.col(CONTENTS_COLUMN_NAME)), 256))
      .withColumnRenamed(geometryColumn, "geometry")

    val kDistanceDf = formattedDataframe
      .alias("l")
      .join(
        formattedDataframe.alias("r"),
        // k + 1 because we are not counting the row matching to itself
        f.expr(f"$KNNFunction(l.geometry, r.geometry, $k + 1, $useSpheroidString)") && f.col(
          f"l.$ID_COLUMN_NAME") =!= f.col(f"r.$ID_COLUMN_NAME"))
      .groupBy(f"l.$ID_COLUMN_NAME")
      .agg(
        f.first("l.geometry").alias("geometry"),
        f.first(f"l.$CONTENTS_COLUMN_NAME").alias(CONTENTS_COLUMN_NAME),
        f.max(distanceFunction(f.col("l.geometry"), f.col("r.geometry"))).alias("k_distance"),
        f.collect_list(f"r.$ID_COLUMN_NAME").alias("neighbors"))
      .checkpoint()

    val lrdDf = kDistanceDf
      .alias("A")
      .select(
        f.col(ID_COLUMN_NAME).alias("a_id"),
        f.col(CONTENTS_COLUMN_NAME),
        f.col("geometry").alias("a_geometry"),
        f.explode(f.col("neighbors")).alias("n_id"))
      .join(
        kDistanceDf.select(
          f.col(ID_COLUMN_NAME).alias("b_id"),
          f.col("geometry").alias("b_geometry"),
          f.col("k_distance").alias("b_k_distance")),
        f.expr("n_id = b_id"))
      .select(
        f.col("a_id"),
        f.col("b_id"),
        f.col(CONTENTS_COLUMN_NAME),
        f.array_max(
          f.array(
            f.col("b_k_distance"),
            distanceFunction(f.col("a_geometry"), f.col("b_geometry"))))
          .alias("rd"))
      .groupBy("a_id")
      .agg(
        // + 1e-10 to avoid division by zero, matches sklearn impl
        (f.lit(1.0) / (f.mean("rd") + 1e-10)).alias("lrd"),
        f.collect_list(f.col("b_id")).alias("neighbors"),
        f.first(CONTENTS_COLUMN_NAME).alias(CONTENTS_COLUMN_NAME))

    val ret = lrdDf
      .select(
        f.col("a_id"),
        f.col("lrd").alias("a_lrd"),
        f.col(CONTENTS_COLUMN_NAME),
        f.explode(f.col("neighbors")).alias("n_id"))
      .join(
        lrdDf.select(f.col("a_id").alias("b_id"), f.col("lrd").alias("b_lrd")),
        f.expr("n_id = b_id"))
      .groupBy("a_id")
      .agg(
        f.first(CONTENTS_COLUMN_NAME).alias(CONTENTS_COLUMN_NAME),
        (f.sum("b_lrd") / (f.count("b_lrd") * f.first("a_lrd"))).alias(resultColumnName))
      .select(f.col(f"$CONTENTS_COLUMN_NAME.*"), f.col(resultColumnName))

    if (handleTies)
      SparkSession.getActiveSession.get.conf
        .set("spark.sedona.join.knn.includeTieBreakers", prior)
    ret
  }

}
