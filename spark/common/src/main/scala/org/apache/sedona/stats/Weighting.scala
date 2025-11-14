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

import org.apache.sedona.util.DfUtils.getGeometryColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Distance, ST_DistanceSpheroid}
import org.apache.spark.sql.{Column, DataFrame}
import scala.collection.JavaConverters._

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
   * @param savedAttributes
   *   the attributes to save in the neighbor column. Default is all columns.
   * @param resultName
   *   the name of the resulting column. Default is 'weights'.
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
      useSpheroid: Boolean = false,
      savedAttributes: Seq[String] = null,
      resultName: String = "weights"): DataFrame = {

    require(threshold >= 0, "Threshold must be greater than or equal to 0")
    require(alpha < 0, "Alpha must be less than 0")

    val geometryColumn = geometry match {
      case null => getGeometryColumnName(dataframe.schema)
      case _ =>
        require(
          dataframe.schema.fields.exists(_.name == geometry),
          s"Geometry column $geometry not found in dataframe")
        geometry
    }

    // Always include the geometry column in the saved attributes
    val savedAttributesWithGeom =
      if (savedAttributes == null) null
      else if (!savedAttributes.contains(geometryColumn)) savedAttributes :+ geometryColumn
      else savedAttributes

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

    val spatiallyJoined = formattedDataFrame
      .alias("l")
      .join(
        formattedDataFrame.alias("r"),
        joinCondition && col(s"l.$ID_COLUMN") =!= col(s"r.$ID_COLUMN"),
        "inner")
      .select(struct("l.*").alias("left"), struct("r.*").alias("right"))

    val mapped = formattedDataFrame
      .alias("f")
      .join(
        spatiallyJoined.alias("s"),
        col(s"s.left.$ID_COLUMN") === col(s"f.$ID_COLUMN"),
        "left")
      .select(
        col(ID_COLUMN),
        struct("f.*").alias("left_contents"),
        struct(
          (
            savedAttributesWithGeom match {
              case null => struct(col("s.right.*")).dropFields(ID_COLUMN)
              case _ =>
                struct(savedAttributesWithGeom.map(c => col(s"s.right.$c")): _*)
            }
          ).alias("neighbor"),
          if (!binary)
            pow(
              distanceFunction(col(s"s.left.$geometryColumn"), col(s"s.right.$geometryColumn")),
              alpha)
              .alias("value")
          else lit(1.0).alias("value")).alias("weight"))

    mapped
      .groupBy(ID_COLUMN)
      .agg(
        first("left_contents").alias("left_contents"),
        concat(
          collect_list(col("weight")),
          if (includeSelf)
            array(struct(
              (savedAttributesWithGeom match {
                case null => first("left_contents").dropFields(ID_COLUMN)
                case _ =>
                  struct(
                    savedAttributesWithGeom.map(c => first(s"left_contents.$c").alias(c)): _*)
              }).alias("neighbor"),
              lit(selfWeight).alias("value")))
          else array()).alias(resultName))
      .select("left_contents.*", resultName)
      .drop(ID_COLUMN)
      .withColumn(resultName, filter(col(resultName), _(f"neighbor")(geometryColumn).isNotNull))
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
   * @param savedAttributes
   *   the attributes to save in the neighbor column. Default is all columns.
   * @param resultName
   *   the name of the resulting column. Default is 'weights'.
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
      useSpheroid: Boolean = false,
      savedAttributes: Seq[String] = null,
      resultName: String = "weights"): DataFrame = addDistanceBandColumn(
    dataframe,
    threshold,
    binary = true,
    includeZeroDistanceNeighbors = includeZeroDistanceNeighbors,
    includeSelf = includeSelf,
    geometry = geometry,
    useSpheroid = useSpheroid,
    savedAttributes = savedAttributes,
    resultName = resultName)

  /**
   * Annotates a dataframe with a weights column for each data record containing the other members
   * within the threshold and their weight. Weights will be dist^alpha. The dataframe should
   * contain at least one GeometryType column. Rows must be unique. If one geometry column is
   * present it will be used automatically. If two are present, the one named 'geometry' will be
   * used. If more than one are present and neither is named 'geometry', the column name must be
   * provided. The new column will be named 'cluster'.
   *
   * @param dataframe
   *   DataFrame with geometry column
   * @param threshold
   *   Distance threshold for considering neighbors
   * @param alpha
   *   alpha to use for inverse distance weights. Computation is dist^alpha. Default is -1.0
   * @param includeZeroDistanceNeighbors
   *   whether to include neighbors that are 0 distance. If 0 distance neighbors are included and
   *   binary is false, values are infinity as per the floating point spec (divide by 0)
   * @param includeSelf
   *   whether to include self in the list of neighbors
   * @param selfWeight
   *   the weight to provide for the self as its own neighbor. Default is 1.0
   * @param geometry
   *   name of the geometry column
   * @param useSpheroid
   *   whether to use a cartesian or spheroidal distance calculation. Default is false
   * @param savedAttributes
   *   the attributes to save in the neighbor column. Default is all columns.
   * @param resultName
   *   the name of the resulting column. Default is 'weights'.
   * @return
   *   The input DataFrame with a weight column added containing neighbors and their weights
   *   (dist^alpha) added to each row.
   */
  def addWeightedDistanceBandColumn(
      dataframe: DataFrame,
      threshold: Double,
      alpha: Double = -1.0,
      includeZeroDistanceNeighbors: Boolean = false,
      includeSelf: Boolean = false,
      selfWeight: Double = 1.0,
      geometry: String = null,
      useSpheroid: Boolean = false,
      savedAttributes: Seq[String] = null,
      resultName: String = "weights"): DataFrame = addDistanceBandColumn(
    dataframe,
    threshold,
    alpha = alpha,
    binary = false,
    includeZeroDistanceNeighbors = includeZeroDistanceNeighbors,
    includeSelf = includeSelf,
    selfWeight = selfWeight,
    geometry = geometry,
    useSpheroid = useSpheroid,
    savedAttributes = savedAttributes,
    resultName = resultName)

  def addDistanceBandColumnPython(
      dataframe: DataFrame,
      threshold: Double,
      binary: Boolean = true,
      alpha: Double = -1.0,
      includeZeroDistanceNeighbors: Boolean = false,
      includeSelf: Boolean = false,
      selfWeight: Double = 1.0,
      geometry: String = null,
      useSpheroid: Boolean = false,
      savedAttributes: java.util.ArrayList[String] = null,
      resultName: String = "weights"): DataFrame = {

    val savedAttributesScala =
      if (savedAttributes != null) savedAttributes.asScala.toSeq
      else null

    addDistanceBandColumn(
      dataframe,
      threshold,
      binary,
      alpha,
      includeZeroDistanceNeighbors,
      includeSelf,
      selfWeight,
      geometry,
      useSpheroid,
      savedAttributesScala,
      resultName)
  }
}
