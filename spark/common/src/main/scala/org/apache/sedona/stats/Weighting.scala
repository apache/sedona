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
package org.apache.sedona.stats

import org.apache.sedona.stats.Util.getGeometryColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Distance, ST_DistanceSpheroid}
import org.apache.spark.sql.{Column, DataFrame}

object Weighting {

  private val ID_COLUMN = "__id"

  /**
   * Annotates a dataframe with a weights column for each data record containing the other members
   * within the threshold and their weight. The dataframe should contain at least one GeometryType
   * column. Rows must be unique. If one geometry column is present it will be used automatically.
   * If two are present, the one named 'geometry' will be used. If more than one are present and
   * neither is named 'geometry', the column name must be provided. The new column will be named
   * 'cluster'.
   *
   * @param dataframe
   *   DataFrame with geometry column
   * @param threshold
   *   Distance threshold for considering neighbors
   * @param binary
   *   whether to use binary weights or inverse distance weights for neighbors (dist^alpha)
   * @param alpha
   *   alpha to use for inverse distance weights ignored when binary is true
   * @param includeZeroDistanceNeighbors
   *   whether to include neighbors that are 0 distance. If 0 distance neighbors are included and
   *   binary is false, values are infinity as per the floating point spec (divide by 0)
   * @param includeSelf
   *   whether to include self in the list of neighbors
   * @param selfWeight
   *   the value to use for the self weight
   * @param geometry
   *   name of the geometry column
   * @param useSpheroid
   *   whether to use a cartesian or spheroidal distance calculation. Default is false
   * @return
   *   The input DataFrame with a weight column added containing neighbors and their weights added
   *   to each row.
   */
  def addDistanceBandColumn(
      dataframe: DataFrame,
      threshold: Double,
      binary: Boolean = true,
      alpha: Double = -1.0,
      includeZeroDistanceNeighbors: Boolean = false,
      includeSelf: Boolean = false,
      selfWeight: Double = 1.0,
      geometry: String = null,
      useSpheroid: Boolean = false): DataFrame = {

    require(threshold >= 0, "Threshold must be greater than or equal to 0")
    require(alpha < 0, "Alpha must be less than 0")

    val geometryColumn = geometry match {
      case null => getGeometryColumnName(dataframe)
      case _ =>
        require(
          dataframe.schema.fields.exists(_.name == geometry),
          s"Geometry column $geometry not found in dataframe")
        geometry
    }

    val distanceFunction: (Column, Column) => Column =
      if (useSpheroid) ST_DistanceSpheroid else ST_Distance

    val joinCondition = if (includeZeroDistanceNeighbors) {
      distanceFunction(col(s"l.$geometryColumn"), col(s"r.$geometryColumn")) <= threshold
    } else {
      distanceFunction(
        col(s"l.$geometryColumn"),
        col(s"r.$geometryColumn")) <= threshold && distanceFunction(
        col(s"l.$geometryColumn"),
        col(s"r.$geometryColumn")) > 0
    }

    val formattedDataFrame = dataframe.withColumn(ID_COLUMN, sha2(to_json(struct("*")), 256))

    // Since spark 3.0 doesn't support dropFields, we need a work around
    val withoutId = (prefix: String, colFunc: String => Column) => {
      formattedDataFrame.schema.fields
        .map(_.name)
        .filter(name => name != ID_COLUMN)
        .map(x => colFunc(prefix + "." + x).alias(x))
    }

    formattedDataFrame
      .alias("l")
      .join(
        formattedDataFrame.alias("r"),
        joinCondition && col(s"l.$ID_COLUMN") =!= col(
          s"r.$ID_COLUMN"
        ), // we will add self back later if self.includeSelf
        "left")
      .select(
        col(s"l.$ID_COLUMN"),
        struct("l.*").alias("left_contents"),
        struct(
          struct(withoutId("r", col): _*).alias("neighbor"),
          if (!binary)
            pow(distanceFunction(col(s"l.$geometryColumn"), col(s"r.$geometryColumn")), alpha)
              .alias("value")
          else lit(1.0).alias("value")).alias("weight"))
      .groupBy(s"l.$ID_COLUMN")
      .agg(
        first("left_contents").alias("left_contents"),
        concat(
          collect_list(col("weight")),
          if (includeSelf)
            array(
              struct(
                struct(withoutId("left_contents", first): _*).alias("neighbor"),
                lit(selfWeight).alias("value")))
          else array()).alias("weights"))
      .select("left_contents.*", "weights")
      .drop(ID_COLUMN)
      .withColumn("weights", filter(col("weights"), _(f"neighbor")(geometryColumn).isNotNull))
  }

  /**
   * Annotates a dataframe with a weights column for each data record containing the other members
   * within the threshold and their weight. Weights will always be 1.0. The dataframe should
   * contain at least one GeometryType column. Rows must be unique. If one geometry column is
   * present it will be used automatically. If two are present, the one named 'geometry' will be
   * used. If more than one are present and neither is named 'geometry', the column name must be
   * provided. The new column will be named 'cluster'.
   *
   * @param dataframe
   *   DataFrame with geometry column
   * @param threshold
   *   Distance threshold for considering neighbors
   * @param includeZeroDistanceNeighbors
   *   whether to include neighbors that are 0 distance. If 0 distance neighbors are included and
   *   binary is false, values are infinity as per the floating point spec (divide by 0)
   * @param includeSelf
   *   whether to include self in the list of neighbors
   * @param geometry
   *   name of the geometry column
   * @param useSpheroid
   *   whether to use a cartesian or spheroidal distance calculation. Default is false
   * @return
   *   The input DataFrame with a weight column added containing neighbors and their weights
   *   (always 1) added to each row.
   */
  def addBinaryDistanceBandColumn(
      dataframe: DataFrame,
      threshold: Double,
      includeZeroDistanceNeighbors: Boolean = true,
      includeSelf: Boolean = false,
      geometry: String = null,
      useSpheroid: Boolean = false): DataFrame = addDistanceBandColumn(
    dataframe,
    threshold,
    binary = true,
    includeZeroDistanceNeighbors = includeZeroDistanceNeighbors,
    includeSelf = includeSelf,
    geometry = geometry,
    useSpheroid = useSpheroid)

}
