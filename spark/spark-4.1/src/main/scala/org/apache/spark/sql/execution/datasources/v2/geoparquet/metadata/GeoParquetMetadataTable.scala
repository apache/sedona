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
package org.apache.spark.sql.execution.datasources.v2.geoparquet.metadata

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class GeoParquetMetadataTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def formatName: String = "GeoParquet Metadata"

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(GeoParquetMetadataTable.schema)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new GeoParquetMetadataScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = null

  override def capabilities: java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)
}

object GeoParquetMetadataTable {
  private val columnMetadataType = StructType(
    Seq(
      StructField("encoding", StringType, nullable = true),
      StructField("geometry_types", ArrayType(StringType), nullable = true),
      StructField("bbox", ArrayType(DoubleType), nullable = true),
      StructField("crs", StringType, nullable = true),
      StructField("covering", StringType, nullable = true)))

  private val columnsType = MapType(StringType, columnMetadataType, valueContainsNull = false)

  val schema: StructType = StructType(
    Seq(
      StructField("path", StringType, nullable = false),
      StructField("version", StringType, nullable = true),
      StructField("primary_column", StringType, nullable = true),
      StructField("columns", columnsType, nullable = true)))
}
