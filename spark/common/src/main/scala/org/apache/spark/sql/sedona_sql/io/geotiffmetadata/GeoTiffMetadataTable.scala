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
package org.apache.spark.sql.sedona_sql.io.geotiffmetadata

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Set => JSet}

case class GeoTiffMetadataTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
    with SupportsRead {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(userSpecifiedSchema.getOrElse(GeoTiffMetadataTable.SCHEMA))

  override def formatName: String = "GeoTiffMetadata"

  override def capabilities(): JSet[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    GeoTiffMetadataScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new UnsupportedOperationException("GeoTiffMetadata is a read-only data source")
}

object GeoTiffMetadataTable {

  val GEO_TRANSFORM_TYPE: StructType = StructType(
    Seq(
      StructField("upperLeftX", DoubleType, nullable = false),
      StructField("upperLeftY", DoubleType, nullable = false),
      StructField("scaleX", DoubleType, nullable = false),
      StructField("scaleY", DoubleType, nullable = false),
      StructField("skewX", DoubleType, nullable = false),
      StructField("skewY", DoubleType, nullable = false)))

  val CORNER_COORDINATES_TYPE: StructType = StructType(
    Seq(
      StructField("minX", DoubleType, nullable = false),
      StructField("minY", DoubleType, nullable = false),
      StructField("maxX", DoubleType, nullable = false),
      StructField("maxY", DoubleType, nullable = false)))

  val BAND_TYPE: StructType = StructType(
    Seq(
      StructField("band", IntegerType, nullable = false),
      StructField("dataType", StringType, nullable = true),
      StructField("colorInterpretation", StringType, nullable = true),
      StructField("noDataValue", DoubleType, nullable = true),
      StructField("blockWidth", IntegerType, nullable = false),
      StructField("blockHeight", IntegerType, nullable = false),
      StructField("description", StringType, nullable = true),
      StructField("unit", StringType, nullable = true)))

  val OVERVIEW_TYPE: StructType = StructType(
    Seq(
      StructField("level", IntegerType, nullable = false),
      StructField("width", IntegerType, nullable = false),
      StructField("height", IntegerType, nullable = false)))

  val SCHEMA: StructType = StructType(
    Seq(
      StructField("path", StringType, nullable = false),
      StructField("driver", StringType, nullable = false),
      StructField("fileSize", LongType, nullable = true),
      StructField("width", IntegerType, nullable = false),
      StructField("height", IntegerType, nullable = false),
      StructField("numBands", IntegerType, nullable = false),
      StructField("srid", IntegerType, nullable = false),
      StructField("crs", StringType, nullable = true),
      StructField("geoTransform", GEO_TRANSFORM_TYPE, nullable = false),
      StructField("cornerCoordinates", CORNER_COORDINATES_TYPE, nullable = false),
      StructField("bands", ArrayType(BAND_TYPE), nullable = true),
      StructField("overviews", ArrayType(OVERVIEW_TYPE), nullable = true),
      StructField("metadata", MapType(StringType, StringType), nullable = true),
      StructField("isTiled", BooleanType, nullable = false),
      StructField("compression", StringType, nullable = true)))
}
