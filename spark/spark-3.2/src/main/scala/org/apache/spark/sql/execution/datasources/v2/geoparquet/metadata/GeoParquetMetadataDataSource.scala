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

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Data source for reading GeoParquet metadata. This could be accessed using the `spark.read`
 * interface:
 * {{{
 *  val df = spark.read.format("geoparquet.metadata").load("path/to/geoparquet")
 * }}}
 */
class GeoParquetMetadataDataSource extends FileDataSourceV2 with DataSourceRegister {
  override val shortName: String = "geoparquet.metadata"

  override def fallbackFileFormat: Class[_ <: FileFormat] = null

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GeoParquetMetadataTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      None,
      fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GeoParquetMetadataTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      Some(schema),
      fallbackFileFormat)
  }
}
