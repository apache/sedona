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
package org.apache.spark.sql.sedona_sql.adapters

import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialPartitioning.GenericUniquePartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.sedona.util.DfUtils
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.sedona_sql.DataFrameShims
import org.locationtech.jts.geom.Geometry
import org.slf4j.{Logger, LoggerFactory}

/**
 * Adapter for converting between DataFrame and SpatialRDD. It provides methods to convert
 * DataFrame to SpatialRDD and vice versa without losing schema. It is different from
 * [[org.apache.sedona.sql.utils.Adapter]] which loses the schema information during conversion.
 * This should be used if your data starts as a DataFrame and you want to convert it to SpatialRDD
 */
object StructuredAdapter {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Convert RDD[Row] to SpatialRDD. It puts Row as user data of Geometry.
   * @param rdd
   * @param geometryFieldName
   * @return
   */
  def toSpatialRdd(rdd: RDD[Row], geometryFieldName: String): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]
    if (rdd.isEmpty()) {
      spatialRDD.schema = StructType(Seq())
    } else spatialRDD.schema = rdd.first().schema
    spatialRDD.rawSpatialRDD = rdd
      .map(row => {
        val geom = row.getAs[Geometry](geometryFieldName)
        geom.setUserData(row.copy())
        geom
      })
    spatialRDD
  }

  /**
   * Convert RDD[Row] to SpatialRDD. It puts Row as user data of Geometry. It auto-detects
   * geometry column if geometryFieldName is not provided. It uses the first geometry column in
   * RDD.
   * @param rdd
   * @return
   */
  def toSpatialRdd(rdd: RDD[Row]): SpatialRDD[Geometry] = {
    require(rdd.count() > 0, "Input RDD cannot be empty.")
    toSpatialRdd(rdd, DfUtils.getGeometryColumnName(rdd.first().schema))
  }

  /**
   * Convert SpatialRDD to RDD[Row]. It extracts Row from user data of Geometry.
   * @param spatialRDD
   * @return
   */
  def toRowRdd(spatialRDD: SpatialRDD[Geometry]): RDD[Row] = {
    spatialRDD.rawSpatialRDD.map(geometry => {
      val row = geometry.getUserData.asInstanceOf[Row]
      row
    })
  }

  /**
   * Convert DataFrame to SpatialRDD. It puts InternalRow as user data of Geometry.
   *
   * @param dataFrame
   * @param geometryFieldName
   */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName: String): SpatialRDD[Geometry] =
    toSpatialRdd(dataFrame.queryExecution.toRdd, dataFrame.schema, geometryFieldName)

  /**
   * Convert RDD[InternalRow] to SpatialRDD. It puts InternalRow as user data of Geometry.
   *
   * @param rdd
   * @param schema
   * @param geometryFieldName
   * @return
   */
  def toSpatialRdd(
      rdd: RDD[InternalRow],
      schema: StructType,
      geometryFieldName: String): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.schema = schema
    val ordinal = spatialRDD.schema.fieldIndex(geometryFieldName)
    spatialRDD.rawSpatialRDD = rdd
      .map(row => {
        val geom = GeometrySerializer.deserialize(row.getBinary(ordinal))
        geom.setUserData(row.copy())
        geom
      })
    spatialRDD
  }

  /**
   * Convert DataFrame to SpatialRDD. It puts InternalRow as user data of Geometry. It
   * auto-detects geometry column if geometryFieldName is not provided. It uses the first geometry
   * column in DataFrame.
   * @param dataFrame
   * @return
   */
  def toSpatialRdd(dataFrame: DataFrame): SpatialRDD[Geometry] = {
    toSpatialRdd(dataFrame, DfUtils.getGeometryColumnName(dataFrame.schema))
  }

  /**
   * Convert SpatialRDD.rawSpatialRdd to DataFrame
   * @param spatialRDD
   *   The SpatialRDD to convert. It must have rawSpatialRDD set.
   * @param sparkSession
   * @return
   */
  def toDf(spatialRDD: SpatialRDD[Geometry], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.map(geometry => {
      val row = geometry.getUserData.asInstanceOf[InternalRow]
      row
    })
    DataFrameShims.createDataFrame(sparkSession, rowRdd, spatialRDD.schema)
  }

  /**
   * Convert SpatialRDD.spatialPartitionedRDD to DataFrame This is useful when you want to convert
   * SpatialRDD after spatial partitioning.
   * @param spatialRDD
   *   The SpatialRDD to convert. It must have spatialPartitionedRDD set. You must call
   *   spatialPartitioning method before calling this method.
   * @param sparkSession
   * @return
   */
  def toSpatialPartitionedDf(
      spatialRDD: SpatialRDD[Geometry],
      sparkSession: SparkSession): DataFrame = {
    if (spatialRDD.spatialPartitionedRDD == null)
      throw new RuntimeException(
        "SpatialRDD is not spatially partitioned. Please call spatialPartitioning method before calling this method.")

    if (!spatialRDD.getPartitioner().isInstanceOf[GenericUniquePartitioner]) {
      logger.warn(
        "SpatialPartitionedRDD might have duplicate geometries. Please make sure you are aware of it.")
    }
    val rowRdd = spatialRDD.spatialPartitionedRDD.map(geometry => {
      val row = geometry.getUserData.asInstanceOf[InternalRow]
      row
    })
    DataFrameShims.createDataFrame(sparkSession, rowRdd, spatialRDD.schema)
  }

  /**
   * Convert JavaPairRDD[Geometry, Geometry] to DataFrame This method is useful when you want to
   * convert the result of spatial join to DataFrame.
   * @param spatialPairRDD
   *   The JavaPairRDD to convert.
   * @param leftSchemaJson
   *   Schema of the left side. In a json format.
   * @param rightSchemaJson
   *   Schema of the right side. In a json format.
   * @param sparkSession
   * @return
   */
  def toDf(
      spatialPairRDD: JavaPairRDD[Geometry, Geometry],
      leftSchemaJson: String,
      rightSchemaJson: String,
      sparkSession: SparkSession): DataFrame = {
    val leftSchema = DataType.fromJson(leftSchemaJson).asInstanceOf[StructType]
    val rightSchema = DataType.fromJson(rightSchemaJson).asInstanceOf[StructType]
    toDf(spatialPairRDD, leftSchema, rightSchema, sparkSession)
  }

  /**
   * Convert JavaPairRDD[Geometry, Geometry] to DataFrame This method is useful when you want to
   * convert the result of spatial join to DataFrame.
   * @param spatialPairRDD
   *   The JavaPairRDD to convert.
   * @param leftSchema
   *   The schema of the left side.
   * @param rightSchema
   *   The schema of the right side.
   * @param sparkSession
   * @return
   */
  def toDf(
      spatialPairRDD: JavaPairRDD[Geometry, Geometry],
      leftSchema: StructType,
      rightSchema: StructType,
      sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map(pair => {
      val leftRow = pair._1.getUserData.asInstanceOf[InternalRow].toSeq(leftSchema)
      val rightRow = pair._2.getUserData.asInstanceOf[InternalRow].toSeq(rightSchema)
      InternalRow.fromSeq(leftRow ++ rightRow)
    })
    DataFrameShims.createDataFrame(
      sparkSession,
      rowRdd,
      StructType(leftSchema.fields ++ rightSchema.fields))
  }

  /**
   * Convert JavaPairRDD[Geometry, Geometry] to DataFrame This method is useful when you want to
   * convert the result of spatial join to DataFrame.
   * @param spatialPairRDD
   *   The JavaPairRDD to convert.
   * @param originalLeftSpatialRdd
   *   The original left SpatialRDD involved in the join. It is used to get the schema of the left
   *   side.
   * @param originalRightSpatialRdd
   *   The original right SpatialRDD involved in the join. It is used to get the schema of the
   *   right side.
   * @param sparkSession
   * @return
   */
  def toDf(
      spatialPairRDD: JavaPairRDD[Geometry, Geometry],
      originalLeftSpatialRdd: SpatialRDD[Geometry],
      originalRightSpatialRdd: SpatialRDD[Geometry],
      sparkSession: SparkSession): DataFrame = {
    toDf(
      spatialPairRDD,
      originalLeftSpatialRdd.schema,
      originalRightSpatialRdd.schema,
      sparkSession)
  }

  /**
   * Repartition a DataFrame using a spatial partitioning scheme (e.g., KDB-Tree). This is a
   * convenience method that wraps the multi-step process of converting a DataFrame to a
   * SpatialRDD, applying spatial partitioning without duplicates, and converting back to a
   * DataFrame.
   *
   * Example usage:
   * {{{
   * val partitionedDf = StructuredAdapter.repartitionBySpatialKey(df, "geometry", GridType.KDBTREE, 16)
   * partitionedDf.write.format("geoparquet").save("/path/to/output")
   * }}}
   *
   * @param dataFrame
   *   The input DataFrame containing a geometry column.
   * @param geometryFieldName
   *   The name of the geometry column.
   * @param gridType
   *   The spatial partitioning grid type (e.g., GridType.KDBTREE).
   * @param numPartitions
   *   The target number of partitions. If 0, defaults to the current number of partitions.
   * @return
   *   A spatially partitioned DataFrame.
   */
  def repartitionBySpatialKey(
      dataFrame: DataFrame,
      geometryFieldName: String,
      gridType: GridType,
      numPartitions: Int = 0): DataFrame = {
    val spatialRDD = toSpatialRdd(dataFrame, geometryFieldName)
    spatialRDD.analyze()
    val partCount =
      if (numPartitions > 0) numPartitions
      else dataFrame.rdd.getNumPartitions
    spatialRDD.spatialPartitioningWithoutDuplicates(gridType, partCount)
    toSpatialPartitionedDf(spatialRDD, dataFrame.sparkSession)
  }

  /**
   * Repartition a DataFrame using a spatial partitioning scheme (e.g., KDB-Tree). Auto-detects
   * the geometry column.
   *
   * @param dataFrame
   *   The input DataFrame containing a geometry column.
   * @param gridType
   *   The spatial partitioning grid type (e.g., GridType.KDBTREE).
   * @param numPartitions
   *   The target number of partitions. If 0, defaults to the current number of partitions.
   * @return
   *   A spatially partitioned DataFrame.
   */
  def repartitionBySpatialKey(
      dataFrame: DataFrame,
      gridType: GridType,
      numPartitions: Int): DataFrame = {
    repartitionBySpatialKey(
      dataFrame,
      DfUtils.getGeometryColumnName(dataFrame.schema),
      gridType,
      numPartitions)
  }

  /**
   * Repartition a DataFrame using a spatial partitioning scheme (e.g., KDB-Tree). Auto-detects
   * the geometry column and uses the current number of partitions.
   *
   * @param dataFrame
   *   The input DataFrame containing a geometry column.
   * @param gridType
   *   The spatial partitioning grid type (e.g., GridType.KDBTREE).
   * @return
   *   A spatially partitioned DataFrame.
   */
  def repartitionBySpatialKey(dataFrame: DataFrame, gridType: GridType): DataFrame = {
    repartitionBySpatialKey(dataFrame, gridType, 0)
  }
}
