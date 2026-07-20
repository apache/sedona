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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderWithPartitionValues
import org.apache.spark.sql.sedona_sql.io.raster.RasterInputPartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class GeoTiffMetadataPartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType)
    extends PartitionReaderFactory {

  private def buildReader(partition: RasterInputPartition): PartitionReader[InternalRow] = {
    // Each file in a bin-packed partition may belong to a different Hive partition and thus
    // carry its own partition values, so wrap a per-file reader with that file's values rather
    // than applying the first file's values to every row.
    def readerForFile(file: PartitionedFile): PartitionReader[InternalRow] = {
      val fileReader =
        new GeoTiffMetadataPartitionReader(
          broadcastedConf.value.value,
          Array(file),
          readDataSchema)
      new PartitionReaderWithPartitionValues(
        fileReader,
        readDataSchema,
        partitionSchema,
        file.partitionValues)
    }

    new GeoTiffMetadataConcatPartitionReader(partition.files, readerForFile)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case rasterPartition: RasterInputPartition => buildReader(rasterPartition)
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }
  }
}

/**
 * Reads a bin-packed partition file-by-file, delegating each file to a reader built by
 * `buildReader` (which attaches that file's partition values). Sub-readers are opened lazily and
 * closed as the scan advances, so at most one file is open at a time.
 */
private class GeoTiffMetadataConcatPartitionReader(
    files: Array[PartitionedFile],
    buildReader: PartitionedFile => PartitionReader[InternalRow])
    extends PartitionReader[InternalRow] {

  private var fileIndex = 0
  private var current: PartitionReader[InternalRow] = _

  override def next(): Boolean = {
    while (true) {
      if (current == null) {
        if (fileIndex >= files.length) return false
        current = buildReader(files(fileIndex))
        fileIndex += 1
      }
      if (current.next()) return true
      current.close()
      current = null
    }
    false // unreachable; satisfies the compiler
  }

  override def get(): InternalRow = current.get()

  override def close(): Unit = if (current != null) current.close()
}
