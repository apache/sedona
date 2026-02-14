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
import org.apache.sedona.sql.datasources.geopackage.model.TableType.{FEATURES, METADATA, TILES, UNKNOWN}
import org.apache.sedona.sql.datasources.geopackage.model.{GeoPackageReadOptions, PartitionOptions, TileRowMetadata}
import org.apache.sedona.sql.datasources.geopackage.transform.ValuesMapper
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.util.SerializableConfiguration

import java.io.File
import java.sql.ResultSet

case class GeoPackagePartitionReader(
    var rs: ResultSet,
    options: GeoPackageReadOptions,
    broadcastedConf: Broadcast[SerializableConfiguration],
    var currentTempFile: File,
    copying: Boolean = false)
    extends PartitionReader[InternalRow] {

  private var values: Seq[Any] = Seq.empty
  private var currentFile = options.currentFile
  private val partitionedFiles = options.partitionedFiles

  override def next(): Boolean = {
    if (rs.next()) {
      values = ValuesMapper.mapValues(adjustPartitionOptions, rs)
      return true
    }

    partitionedFiles.remove(currentFile)

    if (partitionedFiles.isEmpty) {
      return false
    }

    rs.close()

    currentFile = partitionedFiles.head
    val (tempFile, _) = FileSystemUtils.copyToLocal(
      options = broadcastedConf.value.value,
      file = new Path(currentFile.filePath.toString()))

    if (copying) {
      currentTempFile.deleteOnExit()
    }

    currentTempFile = tempFile

    rs = GeoPackageConnectionManager.getTableCursor(currentTempFile.getPath, options.tableName)

    if (!rs.next()) {
      return false
    }

    values = ValuesMapper.mapValues(adjustPartitionOptions, rs)

    true
  }

  private def adjustPartitionOptions: PartitionOptions = {
    options.partitionOptions.tableType match {
      case FEATURES | METADATA => options.partitionOptions
      case TILES =>
        val tileRowMetadata = TileRowMetadata(
          zoomLevel = rs.getInt("zoom_level"),
          tileColumn = rs.getInt("tile_column"),
          tileRow = rs.getInt("tile_row"))

        options.partitionOptions.withTileRowMetadata(tileRowMetadata)
      case UNKNOWN => options.partitionOptions
    }

  }

  override def get(): InternalRow = {
    InternalRow.fromSeq(values)
  }

  override def close(): Unit = {
    rs.close()
    if (copying) {
      options.tempFile.delete()
    }
  }
}
