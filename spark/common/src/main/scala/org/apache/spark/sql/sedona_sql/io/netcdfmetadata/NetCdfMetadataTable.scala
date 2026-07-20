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
package org.apache.spark.sql.sedona_sql.io.netcdfmetadata

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

case class NetCdfMetadataTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
    with SupportsRead {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(userSpecifiedSchema.getOrElse(NetCdfMetadataTable.SCHEMA))

  override def formatName: String = "NetCdfMetadata"

  override def capabilities(): JSet[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    NetCdfMetadataScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new UnsupportedOperationException("NetCdfMetadata is a read-only data source")
}

object NetCdfMetadataTable {

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

  val DIMENSION_TYPE: StructType = StructType(
    Seq(
      StructField("name", StringType, nullable = false),
      StructField("length", IntegerType, nullable = false),
      StructField("isUnlimited", BooleanType, nullable = false)))

  val VARIABLE_TYPE: StructType = StructType(
    Seq(
      StructField("name", StringType, nullable = false),
      StructField("dataType", StringType, nullable = true),
      StructField("dimensions", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("shape", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("units", StringType, nullable = true),
      StructField("longName", StringType, nullable = true),
      StructField("standardName", StringType, nullable = true),
      StructField("noDataValue", DoubleType, nullable = true),
      StructField("isCoordinate", BooleanType, nullable = false),
      StructField("attributes", MapType(StringType, StringType), nullable = true)))

  val SCHEMA: StructType = StructType(
    Seq(
      StructField("path", StringType, nullable = false),
      StructField("driver", StringType, nullable = false),
      StructField("fileSize", LongType, nullable = false),
      StructField("format", StringType, nullable = true),
      StructField("width", IntegerType, nullable = true),
      StructField("height", IntegerType, nullable = true),
      StructField("srid", IntegerType, nullable = true),
      StructField("crs", StringType, nullable = true),
      StructField("geoTransform", GEO_TRANSFORM_TYPE, nullable = true),
      StructField("cornerCoordinates", CORNER_COORDINATES_TYPE, nullable = true),
      StructField("dimensions", ArrayType(DIMENSION_TYPE), nullable = true),
      StructField("variables", ArrayType(VARIABLE_TYPE), nullable = true),
      StructField("globalAttributes", MapType(StringType, StringType), nullable = true)))
}
