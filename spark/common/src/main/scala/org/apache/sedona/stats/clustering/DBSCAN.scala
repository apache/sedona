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
package org.apache.sedona.stats.clustering

import org.apache.sedona.stats.Util.getGeometryColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Distance, ST_DistanceSpheroid}
import org.apache.spark.sql.{Column, DataFrame}
import org.graphframes.GraphFrame

object DBSCAN {

  private val ID_COLUMN = "__id"

  /**
   * Annotates a dataframe with a cluster label for each data record using the DBSCAN algorithm.
   * The dataframe should contain at least one GeometryType column. Rows must be unique. If one
   * geometry column is present it will be used automatically. If two are present, the one named
   * 'geometry' will be used. If more than one are present and neither is named 'geometry', the
   * column name must be provided. The new column will be named 'cluster'.
   *
   * @param dataframe
   *   dataframe to cluster. Must contain at least one GeometryType column
   * @param epsilon
   *   minimum distance parameter of DBSCAN algorithm
   * @param minPts
   *   minimum number of points parameter of DBSCAN algorithm
   * @param geometry
   *   name of the geometry column
   * @param includeOutliers
   *   whether to include outliers in the output. Default is false
   * @param useSpheroid
   *   whether to use a cartesian or spheroidal distance calculation. Default is false
   * @return
   *   The input DataFrame with the cluster label added to each row. Outlier will have a cluster
   *   value of -1 if included.
   */
  def dbscan(
      dataframe: DataFrame,
      epsilon: Double,
      minPts: Int,
      geometry: String = null,
      includeOutliers: Boolean = true,
      useSpheroid: Boolean = false): DataFrame = {

    val geometryCol = geometry match {
      case null => getGeometryColumnName(dataframe)
      case _ => geometry
    }

    validateInputs(dataframe, epsilon, minPts, geometryCol)

    val distanceFunction: (Column, Column) => Column =
      if (useSpheroid) ST_DistanceSpheroid else ST_Distance

    val hasIdColumn = dataframe.columns.contains("id")
    val idDataframe = if (hasIdColumn) {
      dataframe
        .withColumnRenamed("id", ID_COLUMN)
        .withColumn("id", sha2(to_json(struct("*")), 256))
    } else {
      dataframe.withColumn("id", sha2(to_json(struct("*")), 256))
    }

    val isCorePointsDF = idDataframe
      .alias("left")
      .join(
        idDataframe.alias("right"),
        distanceFunction(col(s"left.$geometryCol"), col(s"right.$geometryCol")) <= epsilon)
      .groupBy(col(s"left.id"))
      .agg(
        first(struct("left.*")).alias("leftContents"),
        count(col(s"right.id")).alias("neighbors_count"),
        collect_list(col(s"right.id")).alias("neighbors"))
      .withColumn("isCore", col("neighbors_count") >= lit(minPts))
      .select("leftContents.*", "neighbors", "isCore")
      .checkpoint()

    val corePointsDF = isCorePointsDF.filter(col("isCore"))
    val borderPointsDF = isCorePointsDF.filter(!col("isCore"))

    val coreEdgesDf = corePointsDF
      .select(col("id").alias("src"), explode(col("neighbors")).alias("dst"))
      .alias("left")
      .join(corePointsDF.alias("right"), col("left.dst") === col(s"right.id"))
      .select(col("left.src"), col(s"right.id").alias("dst"))

    val connectedComponentsDF = GraphFrame(corePointsDF, coreEdgesDf).connectedComponents.run

    val borderComponentsDF = borderPointsDF
      .select(struct("*").alias("leftContent"), explode(col("neighbors")).alias("neighbor"))
      .join(connectedComponentsDF.alias("right"), col("neighbor") === col(s"right.id"))
      .groupBy(col("leftContent.id"))
      .agg(
        first(col("leftContent")).alias("leftContent"),
        min(col(s"right.component")).alias("component"))
      .select("leftContent.*", "component")

    val clusteredPointsDf = borderComponentsDF.union(connectedComponentsDF)

    val outliersDf = idDataframe
      .join(clusteredPointsDf, Seq("id"), "left_anti")
      .withColumn("isCore", lit(false))
      .withColumn("component", lit(-1))
      .withColumn("neighbors", array().cast("array<string>"))

    val completedDf = (
      if (includeOutliers) clusteredPointsDf.unionByName(outliersDf)
      else clusteredPointsDf
    ).withColumnRenamed("component", "cluster")

    val returnDf = if (hasIdColumn) {
      completedDf.drop("neighbors", "id").withColumnRenamed(ID_COLUMN, "id")
    } else {
      completedDf.drop("neighbors", "id")
    }

    returnDf

  }

  private def validateInputs(
      geo_df: DataFrame,
      epsilon: Double,
      minPts: Int,
      geometry: String): Unit = {
    require(epsilon > 0, "epsilon must be greater than 0")
    require(minPts > 0, "minPts must be greater than 0")
    require(geo_df.columns.contains(geometry), "geometry column not found in dataframe")
    require(
      geo_df.schema.fields(geo_df.schema.fieldIndex(geometry)).dataType == GeometryUDT,
      "geometry column must be of type GeometryType")
  }
}
