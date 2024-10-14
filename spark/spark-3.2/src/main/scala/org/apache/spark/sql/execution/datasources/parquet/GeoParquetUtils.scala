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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.language.existentials

object GeoParquetUtils {
  def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parquetOptions = new ParquetOptions(parameters, sparkSession.sessionState.conf)
    val shouldMergeSchemas = parquetOptions.mergeSchema
    val mergeRespectSummaries = sparkSession.sessionState.conf.isParquetSchemaRespectSummaries
    val filesByType = splitFiles(files)
    val filesToTouch =
      if (shouldMergeSchemas) {
        val needMerged: Seq[FileStatus] =
          if (mergeRespectSummaries) {
            Seq.empty
          } else {
            filesByType.data
          }
        needMerged ++ filesByType.metadata ++ filesByType.commonMetadata
      } else {
        // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
        // don't have this.
        filesByType.commonMetadata.headOption
          // Falls back to "_metadata"
          .orElse(filesByType.metadata.headOption)
          // Summary file(s) not found, the Parquet file is either corrupted, or different part-
          // files contain conflicting user defined metadata (two or more values are associated
          // with a same key in different files).  In either case, we fall back to any of the
          // first part-file, and just assume all schemas are consistent.
          .orElse(filesByType.data.headOption)
          .toSeq
      }
    GeoParquetFileFormat.mergeSchemasInParallel(parameters, filesToTouch, sparkSession)
  }

  case class FileTypes(
      data: Seq[FileStatus],
      metadata: Seq[FileStatus],
      commonMetadata: Seq[FileStatus])

  private def splitFiles(allFiles: Seq[FileStatus]): FileTypes = {
    val leaves = allFiles.toArray.sortBy(_.getPath.toString)

    FileTypes(
      data = leaves.filterNot(f => isSummaryFile(f.getPath)),
      metadata = leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE),
      commonMetadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE))
  }

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
    file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  /**
   * Legacy mode option is for reading Parquet files written by old versions of Apache Sedona (<=
   * 1.3.1-incubating). Such files are actually not GeoParquet files and do not have GeoParquet
   * file metadata. Geometry fields were encoded as list of bytes and stored as group type in
   * Parquet files. The Definition of GeometryUDT before 1.4.0 was:
   * {{{
   *  case class GeometryUDT extends UserDefinedType[Geometry] {
   *  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)
   *  // ...
   * }}}
   * Since 1.4.0, the sqlType of GeometryUDT is changed to BinaryType. This is a breaking change
   * for reading old Parquet files. To read old Parquet files, users need to use "geoparquet"
   * format and set legacyMode to true.
   * @param parameters
   *   user provided parameters for reading GeoParquet files using `.option()` method, e.g.
   *   `spark.read.format("geoparquet").option("legacyMode", "true").load("path")`
   * @return
   *   true if legacyMode is set to true, false otherwise
   */
  def isLegacyMode(parameters: Map[String, String]): Boolean =
    parameters.getOrElse("legacyMode", "false").toBoolean

  /**
   * Parse GeoParquet file metadata from Parquet file metadata. Legacy parquet files do not
   * contain GeoParquet file metadata, so we'll simply return an empty GeoParquetMetaData object
   * when legacy mode is enabled.
   * @param keyValueMetaData
   *   Parquet file metadata
   * @param parameters
   *   user provided parameters for reading GeoParquet files
   * @return
   *   GeoParquetMetaData object
   */
  def parseGeoParquetMetaData(
      keyValueMetaData: java.util.Map[String, String],
      parameters: Map[String, String]): GeoParquetMetaData = {
    val isLegacyMode = GeoParquetUtils.isLegacyMode(parameters)
    GeoParquetMetaData.parseKeyValueMetaData(keyValueMetaData).getOrElse {
      if (isLegacyMode) {
        GeoParquetMetaData(None, "", Map.empty)
      } else {
        throw new IllegalArgumentException("GeoParquet file does not contain valid geo metadata")
      }
    }
  }
}
