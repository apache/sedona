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
package org.apache.sedona.sql.datasources.shapefile

import org.apache.hadoop.fs.FileStatus
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.sedona.sql.datasources.shapefile.ShapefileUtils.{baseSchema, fieldDescriptorsToSchema, mergeSchemas}
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.JavaConverters._

/**
 * A Spark DataSource V2 table implementation for reading Shapefiles.
 *
 * Extends [[FileTable]] to leverage Spark's file-based scan infrastructure and implements
 * [[SupportsMetadataColumns]] to expose hidden metadata columns (e.g., `_metadata`) that provide
 * file-level information such as path, name, size, and modification time. These metadata columns
 * are not part of the user-visible schema but can be explicitly selected in queries.
 */
case class ShapefileTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
    with SupportsMetadataColumns {

  override def formatName: String = "Shapefile"

  override def capabilities: java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) None
    else {
      def isDbfFile(file: FileStatus): Boolean = {
        val name = file.getPath.getName.toLowerCase(Locale.ROOT)
        name.endsWith(".dbf")
      }

      def isShpFile(file: FileStatus): Boolean = {
        val name = file.getPath.getName.toLowerCase(Locale.ROOT)
        name.endsWith(".shp")
      }

      if (!files.exists(isShpFile)) None
      else {
        val readOptions = ShapefileReadOptions.parse(options)
        val resolver = sparkSession.sessionState.conf.resolver
        val dbfFiles = files.filter(isDbfFile)
        if (dbfFiles.isEmpty) {
          Some(baseSchema(readOptions, Some(resolver)))
        } else {
          val serializableConf = new SerializableConfiguration(
            sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap))
          val partiallyMergedSchemas = sparkSession.sparkContext
            .parallelize(dbfFiles)
            .mapPartitions { iter =>
              val schemas = iter.map { stat =>
                val fs = stat.getPath.getFileSystem(serializableConf.value)
                val stream = fs.open(stat.getPath)
                try {
                  val dbfParser = new DbfParseUtil()
                  dbfParser.parseFileHead(stream)
                  val fieldDescriptors = dbfParser.getFieldDescriptors
                  fieldDescriptorsToSchema(fieldDescriptors.asScala.toSeq, readOptions, resolver)
                } finally {
                  stream.close()
                }
              }.toSeq
              mergeSchemas(schemas).iterator
            }
            .collect()
          mergeSchemas(partiallyMergedSchemas)
        }
      }
    }
  }

  /** Returns the metadata columns that this table exposes as hidden columns. */
  override def metadataColumns(): Array[MetadataColumn] = ShapefileTable.fileMetadataColumns

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    ShapefileScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = null
}

object ShapefileTable {

  /**
   * Schema of the `_metadata` struct column exposed by [[SupportsMetadataColumns]]. Each field
   * provides file-level information about the source shapefile:
   *
   *   - `file_path`: The fully qualified path of the `.shp` file (e.g.,
   *     `hdfs://host/data/file.shp`).
   *   - `file_name`: The name of the `.shp` file without directory components (e.g., `file.shp`).
   *   - `file_size`: The total size of the `.shp` file in bytes.
   *   - `file_block_start`: The byte offset within the file where this partition's data begins.
   *     For non-splittable formats this is typically 0.
   *   - `file_block_length`: The number of bytes in this partition's data block. For
   *     non-splittable formats this equals the file size.
   *   - `file_modification_time`: The last modification timestamp of the `.shp` file.
   */
  private val FILE_METADATA_STRUCT_TYPE: StructType = StructType(
    Seq(
      StructField("file_path", StringType, nullable = false),
      StructField("file_name", StringType, nullable = false),
      StructField("file_size", LongType, nullable = false),
      StructField("file_block_start", LongType, nullable = false),
      StructField("file_block_length", LongType, nullable = false),
      StructField("file_modification_time", TimestampType, nullable = false)))

  /**
   * The single metadata column `_metadata` exposed to Spark's catalog. This hidden column can be
   * selected in queries (e.g., `SELECT _metadata.file_name FROM shapefile.`...``) but does not
   * appear in `SELECT *`.
   */
  private[shapefile] val fileMetadataColumns: Array[MetadataColumn] = Array(new MetadataColumn {
    override def name: String = "_metadata"
    override def dataType: DataType = FILE_METADATA_STRUCT_TYPE
    override def isNullable: Boolean = false
  })
}
