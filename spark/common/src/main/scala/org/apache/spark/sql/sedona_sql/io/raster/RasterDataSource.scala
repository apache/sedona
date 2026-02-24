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
package org.apache.spark.sql.sedona_sql.io.raster

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * A Spark SQL data source for reading and writing raster images. This data source supports
 * reading various raster formats as in-db rasters. Write support is implemented by falling back
 * to the V1 data source [[RasterFileFormat]].
 */
class RasterDataSource extends FileDataSourceV2 with TableProvider with DataSourceRegister {

  override def shortName(): String = "raster"

  private def createRasterTable(
      options: CaseInsensitiveStringMap,
      userSchema: Option[StructType] = None): Table = {
    var paths = getPaths(options)
    var optionsWithoutPaths = getOptionsWithoutPaths(options)
    val tableName = getTableName(options, paths)
    val rasterOptions = new RasterOptions(optionsWithoutPaths.asScala.toMap)

    if (paths.size == 1) {
      if (paths.head.endsWith("/")) {
        // Paths ends with / will be recursively loaded
        val newOptions =
          new java.util.HashMap[String, String](optionsWithoutPaths.asCaseSensitiveMap())
        newOptions.put("recursiveFileLookup", "true")
        if (!newOptions.containsKey("pathGlobFilter")) {
          newOptions.put("pathGlobFilter", "*.{tif,tiff,TIF,TIFF}")
        }
        optionsWithoutPaths = new CaseInsensitiveStringMap(newOptions)
      } else {
        // Rewrite paths such as /path/to/some*glob*.tif into /path/to with
        // pathGlobFilter="some*glob*.tif". This is for avoiding listing .tif
        // files as directories when discovering files to load. Globs ends with
        // .tif or .tiff should be files in the context of raster data loading.
        val loadTifPattern = "(.*)/([^/]*\\*[^/]*\\.(?:tif|tiff))$".r
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

    new RasterTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      userSchema,
      rasterOptions,
      fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    createRasterTable(options)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    createRasterTable(options, Some(schema))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val paths = getPaths(options)
    if (paths.isEmpty) {
      throw new IllegalArgumentException("No paths specified for raster data source")
    }

    val rasterOptions = new RasterOptions(options.asScala.toMap)
    RasterTable.inferSchema(rasterOptions)
  }

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[RasterFileFormat]
}
