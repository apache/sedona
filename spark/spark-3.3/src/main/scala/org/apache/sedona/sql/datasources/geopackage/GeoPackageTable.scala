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
package org.apache.sedona.sql.datasources.geopackage

import org.apache.hadoop.fs.FileStatus
import org.apache.sedona.sql.datasources.geopackage.connection.{FileSystemUtils, GeoPackageConnectionManager}
import org.apache.sedona.sql.datasources.geopackage.model.{GeoPackageOptions, MetadataSchema, TableType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.jdk.CollectionConverters._

case class GeoPackageTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    loadOptions: GeoPackageOptions)
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (loadOptions.showMetadata) {
      return MetadataSchema.schema
    }

    val serializableConf = new SerializableConfiguration(
      sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap))

    val (tempFile, copied) =
      FileSystemUtils.copyToLocal(serializableConf.value, files.head.getPath)

    if (copied) {
      tempFile.deleteOnExit()
    }

    val tableType = if (loadOptions.showMetadata) {
      TableType.METADATA
    } else {
      GeoPackageConnectionManager.findFeatureMetadata(tempFile.getPath, loadOptions.tableName)
    }

    Some(
      StructType(
        GeoPackageConnectionManager
          .getSchema(tempFile.getPath, loadOptions.tableName)
          .map(field => field.toStructField(tableType))))
  }

  override def formatName: String = {
    "GeoPackage"
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new GeoPackageScanBuilder(
      sparkSession,
      fileIndex,
      schema,
      options,
      loadOptions,
      userSpecifiedSchema)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    null
  }
}
