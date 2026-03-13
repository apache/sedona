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

import org.apache.hadoop.fs.Path
import org.apache.sedona.sql.datasources.geopackage.connection.{FileSystemUtils, GeoPackageConnectionManager}
import org.apache.sedona.sql.datasources.geopackage.model.TableType.TILES
import org.apache.sedona.sql.datasources.geopackage.model.{GeoPackageOptions, GeoPackageReadOptions, PartitionOptions, TableType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class GeoPackagePartitionReaderFactory(
    sparkSession: SparkSession,
    broadcastedConf: Broadcast[SerializableConfiguration],
    loadOptions: GeoPackageOptions,
    dataSchema: StructType)
    extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val partitionFiles = partition match {
      case filePartition: FilePartition => filePartition.files
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }

    val (tempFile, copied) = FileSystemUtils.copyToLocal(
      options = broadcastedConf.value.value,
      file = new Path(partitionFiles.head.filePath.toString()))

    val tableType = if (loadOptions.showMetadata) {
      TableType.METADATA
    } else {
      GeoPackageConnectionManager.findFeatureMetadata(tempFile.getPath, loadOptions.tableName)
    }

    val rs =
      GeoPackageConnectionManager.getTableCursor(tempFile.getAbsolutePath, loadOptions.tableName)

    val schema = GeoPackageConnectionManager.getSchema(tempFile.getPath, loadOptions.tableName)

    if (StructType(schema.map(_.toStructField(tableType))) != dataSchema) {
      throw new IllegalArgumentException(
        s"Schema mismatch: expected $dataSchema, got ${StructType(schema.map(_.toStructField(tableType)))}")
    }

    val tileMetadata = tableType match {
      case TILES =>
        Some(
          GeoPackageConnectionManager.findTilesMetadata(tempFile.getPath, loadOptions.tableName))
      case _ => None
    }

    GeoPackagePartitionReader(
      rs = rs,
      options = GeoPackageReadOptions(
        tableName = loadOptions.tableName,
        tempFile = tempFile,
        partitionOptions =
          PartitionOptions(tableType = tableType, columns = schema, tile = tileMetadata),
        partitionedFiles = scala.collection.mutable.HashSet(partitionFiles: _*),
        currentFile = partitionFiles.head),
      broadcastedConf = broadcastedConf,
      currentTempFile = tempFile,
      copying = copied)
  }
}
