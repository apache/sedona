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
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class GeoPackagePartitionReaderFactory(
    sparkSession: SparkSession,
    broadcastedConf: Broadcast[SerializableConfiguration],
    loadOptions: GeoPackageOptions,
    dataSchema: StructType,
    metadataSchema: StructType)
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

    val baseReader = GeoPackagePartitionReader(
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

    if (metadataSchema.nonEmpty) {
      val gpkgFile = partitionFiles.head
      val filePath = gpkgFile.filePath.toString
      val fileName = new Path(filePath).getName

      val allMetadataValues: Map[String, Any] = Map(
        "file_path" -> UTF8String.fromString(filePath),
        "file_name" -> UTF8String.fromString(fileName),
        "file_size" -> gpkgFile.fileSize,
        "file_block_start" -> gpkgFile.start,
        "file_block_length" -> gpkgFile.length,
        "file_modification_time" -> (gpkgFile.modificationTime * 1000L))

      val innerStructType = metadataSchema.fields.head.dataType.asInstanceOf[StructType]
      val prunedValues = innerStructType.fields.map(f => allMetadataValues(f.name))
      val metadataStruct = InternalRow.fromSeq(prunedValues.toSeq)
      val metadataRow = InternalRow.fromSeq(Seq(metadataStruct))

      new PartitionReaderWithMetadata(baseReader, dataSchema, metadataSchema, metadataRow)
    } else {
      baseReader
    }
  }
}

private[geopackage] class PartitionReaderWithMetadata(
    reader: PartitionReader[InternalRow],
    baseSchema: StructType,
    metadataSchema: StructType,
    metadataValues: InternalRow)
    extends PartitionReader[InternalRow] {

  private val joinedRow = new JoinedRow()
  private val unsafeProjection =
    GenerateUnsafeProjection.generate(baseSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, f.nullable)
    } ++ metadataSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(baseSchema.length + i, f.dataType, f.nullable)
    })

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = {
    unsafeProjection(joinedRow(reader.get(), metadataValues))
  }

  override def close(): Unit = reader.close()
}
