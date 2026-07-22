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
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.HadoopFSUtils

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

  private def createTable(
      options: CaseInsensitiveStringMap,
      userSchema: Option[StructType] = None): Table = {
    val paths = getPaths(options)
    var optionsWithoutPaths = getOptionsWithoutPaths(options)
    val tableName = getTableName(options, paths)

    // Explicit user options always win over the defaults below; presence checks go through the
    // CaseInsensitiveStringMap because Spark data source options are case-insensitive, so e.g.
    // an explicit `RecursiveFileLookup` must suppress the lowercase default.
    val newOptions =
      new java.util.HashMap[String, String](optionsWithoutPaths.asCaseSensitiveMap())
    val hasUserGlobFilter = optionsWithoutPaths.containsKey("pathGlobFilter")
    val hasUserRecursive = optionsWithoutPaths.containsKey("recursiveFileLookup")

    // File paths and file globs are passed to Spark untouched, keeping native listing
    // semantics: a glob such as /dir/a*.tif matches direct children of /dir only, with or
    // without recursiveFileLookup. Directory roots (single or multiple; plain, with a
    // trailing slash, or produced by a glob such as /data/region*) default to a recursive
    // scan filtered to GeoTIFF extensions. These are defaults only — an explicit
    // recursiveFileLookup=false keeps Hive-style partition discovery available. They apply
    // only when EVERY root stands for directories: Spark applies pathGlobFilter to all
    // roots, so a mixed load(dir, explicitFile) must not silently drop an explicitly named
    // file whose name does not match the extension filter.
    if (paths.nonEmpty && paths.forall(p => isDirectoryRoot(p, optionsWithoutPaths))) {
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
   * Whether a load path stands for directories. A trailing `/` is treated as a directory
   * assertion without any FS call. A glob pattern (unless `__globPaths__` disables expansion,
   * exactly as `FileTable` honors it) is expanded with Hadoop `FileSystem.globStatus` and must
   * match at least one status, with every match visible to Spark's scan being a directory.
   * Hidden-named file matches (`_SUCCESS`, dotfiles, `*._COPYING_`) are invisible — Spark's
   * listing discards them — so they neither sway the classification nor, when a glob matches
   * nothing else, veto the defaults earned by the remaining roots; hidden-named DIRECTORY matches
   * count normally, because Spark traverses a directory root regardless of its name. Anything
   * else is probed literally with `getFileStatus`; the hidden-name rule never applies here,
   * because Spark exempts explicitly given roots from it. Returns `false` when the probe fails or
   * a glob matches nothing (Spark reports the missing path itself during listing). Uses the same
   * per-read options (e.g., `fs.s3a.*`) as the scan so detection works on the configured
   * filesystem.
   */
  private def isDirectoryRoot(pathStr: String, options: CaseInsensitiveStringMap): Boolean = {
    if (pathStr.endsWith("/")) return true
    Try {
      val hadoopConf =
        sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap)
      val path = new Path(pathStr)
      val fs = path.getFileSystem(hadoopConf)
      if (globbingEnabled(options) && isGlobPath(path)) {
        val matches = Option(fs.globStatus(path)).getOrElse(Array.empty[FileStatus])
        // An unmatched glob is not a directory root (Spark itself reports the missing
        // path). Otherwise classify by what Spark's listing will actually read: it drops
        // hidden-named FILE matches (their own name comes back from listStatus and is
        // name-checked) but traverses a directory root regardless of its name — only the
        // children are name-checked. Hidden files are therefore invisible to the scan,
        // and a glob whose visible contribution is empty (e.g. matching only a _SUCCESS
        // marker) must not veto the defaults earned by the other roots.
        matches.nonEmpty && matches
          .filterNot(m =>
            !m.isDirectory && HadoopFSUtils.shouldFilterOutPathName(m.getPath.getName))
          .forall(_.isDirectory)
      } else {
        fs.getFileStatus(path).isDirectory
      }
    }.getOrElse(false)
  }

  /** Mirrors `FileTable.globPaths`: expansion is on unless `__globPaths__` is set to non-true. */
  private def globbingEnabled(options: CaseInsensitiveStringMap): Boolean =
    Option(options.get(DataSource.GLOB_PATHS_KEY)).forall(_ == "true")

  /** The wildcard set of `SparkHadoopUtil.isGlobPath`, which governs Spark's own expansion. */
  private def isGlobPath(path: Path): Boolean =
    path.toString.exists("{}[]*?\\".contains(_))
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
