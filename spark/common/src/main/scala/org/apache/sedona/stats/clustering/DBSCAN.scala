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

import org.apache.sedona.util.DfUtils.getGeometryColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_AsBinary, ST_Distance, ST_DistanceSpheroid, ST_SRID}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
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
   * @param isCoreColumnName
   *   what the name of the column indicating if this is a core point should be. Default is
   *   "isCore"
   * @param clusterColumnName
   *   what the name of the column indicating the cluster id should be. Default is "cluster"
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
      useSpheroid: Boolean = false,
      isCoreColumnName: String = "isCore",
      clusterColumnName: String = "cluster"): DataFrame = {

    val geometryCol = geometry match {
      case null => getGeometryColumnName(dataframe.schema)
      case _ => geometry
    }

    validateInputs(dataframe, epsilon, minPts, geometryCol)

    val distanceFunction: (Column, Column) => Column =
      if (useSpheroid) ST_DistanceSpheroid else ST_Distance

    val hasIdColumn = dataframe.columns.contains("id")
    val originalDataframe =
      if (hasIdColumn) dataframe.withColumnRenamed("id", ID_COLUMN) else dataframe
    val hashInput = if (SpatialTypeSupport.usesNativeTypes) {
      // Spark's JSON encoder cannot serialize native spatial values, including nested ones.
      // Include both WKB and SRID so distinct reference systems retain distinct row IDs.
      struct(originalDataframe.schema.fields.map { field =>
        val value = col(s"`${field.name.replace("`", "``")}`")
        hashableColumn(value, field.dataType).as(field.name)
      }: _*)
    } else struct("*")
    val idDataframe = originalDataframe.withColumn("id", sha2(to_json(hashInput), 256))

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
      .withColumn(isCoreColumnName, col("neighbors_count") >= lit(minPts))
      .select("leftContents.*", "neighbors", isCoreColumnName)
      .checkpoint()

    val corePointsDF = isCorePointsDF.filter(col(isCoreColumnName))
    val borderPointsDF = isCorePointsDF.filter(!col(isCoreColumnName))

    val coreEdgesDf = corePointsDF
      .select(col("id").alias("src"), explode(col("neighbors")).alias("dst"))
      .alias("left")
      .join(corePointsDF.alias("right"), col("left.dst") === col(s"right.id"))
      .select(col("left.src"), col(s"right.id").alias("dst"))

    // GraphFrames caches its vertex rows, while Spark's columnar cache cannot store native
    // spatial types. Component computation only needs vertex IDs; restore the other columns
    // after it finishes, preserving the original column order for the union below.
    val vertices =
      if (SpatialTypeSupport.usesNativeTypes) corePointsDF.select("id") else corePointsDF
    val components = GraphFrame(vertices, coreEdgesDf).connectedComponents
      .setUseLabelsAsComponents(false)
      .run
    val connectedComponentsDF = if (SpatialTypeSupport.usesNativeTypes) {
      corePointsDF
        .join(components, Seq("id"))
        .select(
          (corePointsDF.columns.map(name => col(s"`${name.replace("`", "``")}`")) :+
            col("component")): _*)
    } else components

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
      .withColumn(isCoreColumnName, lit(false))
      .withColumn("component", lit(-1))
      .withColumn("neighbors", array().cast("array<string>"))

    val completedDf = (
      if (includeOutliers) clusteredPointsDf.unionByName(outliersDf)
      else clusteredPointsDf
    ).withColumnRenamed("component", clusterColumnName)

    val returnDf = if (hasIdColumn) {
      completedDf.drop("neighbors", "id").withColumnRenamed(ID_COLUMN, "id")
    } else {
      completedDf.drop("neighbors", "id")
    }

    returnDf

  }

  private def hashableColumn(value: Column, dataType: DataType): Column = {
    if (SpatialTypeSupport.isNativeSpatial(dataType)) {
      when(value.isNull, lit(null))
        .otherwise(struct(ST_AsBinary(value).as("wkb"), ST_SRID(value).as("srid")))
    } else
      dataType match {
        case StructType(fields) =>
          when(value.isNull, lit(null)).otherwise(struct(fields.map { field =>
            hashableColumn(value.getField(field.name), field.dataType).as(field.name)
          }: _*))
        case ArrayType(elementType, _) =>
          transform(value, element => hashableColumn(element, elementType))
        case MapType(keyType, valueType, _) =>
          // An entry array lets JSON encode spatial map keys as well as values without
          // coercing binary keys to strings.
          transform(
            map_entries(value),
            entry =>
              struct(
                hashableColumn(entry.getField("key"), keyType).as("key"),
                hashableColumn(entry.getField("value"), valueType).as("value")))
        case _ => value
      }
  }

  private def validateInputs(
      geo_df: DataFrame,
      epsilon: Double,
      minPts: Int,
      geometry: String): Unit = {
    require(epsilon >= 0, "epsilon must not be negative")
    require(minPts > 0, "minPts must be greater than 0")
    require(geo_df.columns.contains(geometry), "geometry column not found in dataframe")
    require(
      SpatialTypeSupport.isGeometry(
        geo_df.schema.fields(geo_df.schema.fieldIndex(geometry)).dataType),
      "geometry column must be of type GeometryType")
  }
}
