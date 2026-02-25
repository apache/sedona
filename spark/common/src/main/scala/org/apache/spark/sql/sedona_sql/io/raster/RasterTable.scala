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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Set => JSet}

case class RasterTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    rasterOptions: RasterOptions,
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
    with SupportsRead
    with SupportsWrite {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(userSpecifiedSchema.getOrElse(RasterTable.inferSchema(rasterOptions)))

  override def formatName: String = "Raster"

  override def capabilities(): JSet[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    RasterScanBuilder(sparkSession, fileIndex, schema, dataSchema, options, rasterOptions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    // Note: this code path will never be taken, since Spark will always fall back to V1
    // data source when writing to File source v2. See SPARK-28396: File source v2 write
    // path is currently broken.
    null
}

object RasterTable {
  // Names of the fields in the read schema
  val RASTER = "rast"
  val TILE_X = "x"
  val TILE_Y = "y"
  val RASTER_NAME = "name"

  val MAX_AUTO_TILE_SIZE = 4096

  def inferSchema(options: RasterOptions): StructType = {
    val baseFields = if (options.retile) {
      Seq(
        StructField(RASTER, RasterUDT(), nullable = false),
        StructField(TILE_X, IntegerType, nullable = false),
        StructField(TILE_Y, IntegerType, nullable = false))
    } else {
      Seq(StructField(RASTER, RasterUDT(), nullable = false))
    }

    val nameField = Seq(
      StructField(RASTER_NAME, org.apache.spark.sql.types.StringType, nullable = true))

    StructType(baseFields ++ nameField)
  }
}
