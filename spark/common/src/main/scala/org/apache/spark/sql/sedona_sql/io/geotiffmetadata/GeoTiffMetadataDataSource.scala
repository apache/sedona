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

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sedona_sql.io.raster.RasterFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * A read-only Spark SQL data source that extracts GeoTIFF file metadata (dimensions, CRS, bands,
 * overviews, compression, etc.) without loading raster pixel data into memory.
 */
class GeoTiffMetadataDataSource
    extends FileDataSourceV2
    with TableProvider
    with DataSourceRegister {

  override def shortName(): String = "geotiff.metadata"

  private val loadTifPattern = "(.*)/([^/]*\\*[^/]*\\.(?i:tif|tiff))$".r

  private def createTable(
      options: CaseInsensitiveStringMap,
      userSchema: Option[StructType] = None): Table = {
    var paths = getPaths(options)
    var optionsWithoutPaths = getOptionsWithoutPaths(options)
    val tableName = getTableName(options, paths)

    if (paths.size == 1) {
      if (paths.head.endsWith("/")) {
        // Trailing-slash directories: recurse and filter to GeoTIFF files
        val newOptions =
          new java.util.HashMap[String, String](optionsWithoutPaths.asCaseSensitiveMap())
        newOptions.put("recursiveFileLookup", "true")
        if (!newOptions.containsKey("pathGlobFilter")) {
          newOptions.put("pathGlobFilter", "*.{tif,tiff,TIF,TIFF}")
        }
        optionsWithoutPaths = new CaseInsensitiveStringMap(newOptions)
      } else {
        // Rewrite glob patterns like /path/to/some*glob*.tif into /path/to with
        // pathGlobFilter="some*glob*.tif" to avoid listing .tif files as directories
        paths.head match {
          case loadTifPattern(prefix, glob) =>
            paths = Seq(prefix)
            val newOptions =
              new java.util.HashMap[String, String](optionsWithoutPaths.asCaseSensitiveMap())
            newOptions.put("pathGlobFilter", glob)
            optionsWithoutPaths = new CaseInsensitiveStringMap(newOptions)
          case _ =>
        }
      }
    }

    new GeoTiffMetadataTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      userSchema,
      fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    createTable(options)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    createTable(options, Some(schema))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    GeoTiffMetadataTable.SCHEMA

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[RasterFileFormat]
}
