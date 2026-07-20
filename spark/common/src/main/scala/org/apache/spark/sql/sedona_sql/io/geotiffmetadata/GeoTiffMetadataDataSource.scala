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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.util.Try

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

    // Explicit user options always win over the defaults below; presence checks go through the
    // CaseInsensitiveStringMap because Spark data source options are case-insensitive, so e.g.
    // an explicit `RecursiveFileLookup` must suppress the lowercase default.
    val newOptions =
      new java.util.HashMap[String, String](optionsWithoutPaths.asCaseSensitiveMap())
    val hasUserGlobFilter = optionsWithoutPaths.containsKey("pathGlobFilter")
    val hasUserRecursive = optionsWithoutPaths.containsKey("recursiveFileLookup")

    if (paths.size == 1 && !isDirectory(paths.head, optionsWithoutPaths)) {
      // Rewrite glob patterns like /path/to/some*glob*.tif into /path/to with
      // pathGlobFilter="some*glob*.tif" to avoid listing .tif files as directories. When the
      // user supplied their own pathGlobFilter, keep the glob path untouched so Spark applies
      // both constraints natively (the rewrite could only keep one of them).
      paths.head match {
        case loadTifPattern(prefix, glob) if !hasUserGlobFilter =>
          paths = Seq(prefix)
          newOptions.put("pathGlobFilter", glob)
        case _ =>
      }
    } else if (paths.exists(p => isDirectory(p, optionsWithoutPaths))) {
      // Directory roots (single or multiple, with or without trailing slash): default to a
      // recursive scan filtered to GeoTIFF extensions. These are defaults only — an explicit
      // recursiveFileLookup=false keeps Hive-style partition discovery available.
      if (!hasUserRecursive) {
        newOptions.put("recursiveFileLookup", "true")
      }
      if (!hasUserGlobFilter) {
        newOptions.put("pathGlobFilter", "*.{tif,tiff,TIF,TIFF}")
      }
    }
    optionsWithoutPaths = new CaseInsensitiveStringMap(newOptions)

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

  // Spark maps a FileDataSourceV2 back to this V1 FileFormat for catalog paths such as
  // CREATE TABLE ... USING; a null class would NPE there, so return a stub that raises a
  // descriptive error instead.
  override def fallbackFileFormat: Class[_ <: FileFormat] =
    classOf[GeoTiffMetadataUnsupportedFileFormat]

  /**
   * Check if a path points to a directory. A trailing `/` is treated as a directory without any
   * FS call; otherwise, Hadoop `FileSystem.getFileStatus(path).isDirectory` is consulted. Returns
   * `false` if the FS call fails (e.g., path does not exist or is a glob pattern). Uses the same
   * per-read options (e.g., `fs.s3a.*`) as the scan so directory detection works on the
   * configured filesystem.
   */
  private def isDirectory(pathStr: String, options: CaseInsensitiveStringMap): Boolean = {
    if (pathStr.endsWith("/")) return true
    Try {
      val hadoopConf =
        sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap)
      val path = new Path(pathStr)
      val fs = path.getFileSystem(hadoopConf)
      fs.getFileStatus(path).isDirectory
    }.getOrElse(false)
  }
}

/**
 * V1 fallback used by Spark for catalog paths such as `CREATE TABLE ... USING geotiff.metadata`.
 * The data source is read-only and path-based, so those operations are unsupported — this stub
 * exists to surface a clear error instead of the NullPointerException a null fallback class would
 * produce.
 *
 * The constructor itself throws: Spark instantiates the fallback format whenever a catalog
 * operation resolves the relation — including `CREATE TABLE` with an explicit schema, which skips
 * `inferSchema` — so failing on instantiation rejects every V1 usage up front. Reads are
 * unaffected because the V2 path only ever references the class, never constructs it.
 */
class GeoTiffMetadataUnsupportedFileFormat extends FileFormat {

  throw GeoTiffMetadataUnsupportedFileFormat.unsupported()

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] =
    throw GeoTiffMetadataUnsupportedFileFormat.unsupported()

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    throw GeoTiffMetadataUnsupportedFileFormat.unsupported()
}

object GeoTiffMetadataUnsupportedFileFormat {
  private def unsupported(): UnsupportedOperationException =
    new UnsupportedOperationException(
      "geotiff.metadata does not support catalog table operations; " +
        "read it with spark.read.format(\"geotiff.metadata\").load(path)")
}
