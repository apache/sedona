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

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderWithPartitionValues
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale

case class ShapefilePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    /** The metadata fields requested by the query (e.g., fields from `_metadata`). */
    metadataSchema: StructType,
    options: ShapefileReadOptions,
    filters: Seq[Filter])
    extends PartitionReaderFactory {

  private def buildReader(
      partitionedFiles: Array[PartitionedFile]): PartitionReader[InternalRow] = {
    val fileReader =
      new ShapefilePartitionReader(
        broadcastedConf.value.value,
        partitionedFiles,
        readDataSchema,
        options)
    val withPartitionValues = new PartitionReaderWithPartitionValues(
      fileReader,
      readDataSchema,
      partitionSchema,
      partitionedFiles.head.partitionValues)

    if (metadataSchema.nonEmpty) {
      // Build metadata values from the .shp file's partition information.
      // We use the .shp file because it is the primary shapefile component and its path
      // is what users would expect to see in _metadata.file_path / _metadata.file_name.
      val shpFile = partitionedFiles
        .find(_.filePath.toPath.getName.toLowerCase(Locale.ROOT).endsWith(".shp"))
        .getOrElse(partitionedFiles.head)
      val filePath = shpFile.filePath.toString
      val fileName = new Path(filePath).getName

      // Complete map of all metadata field values keyed by field name.
      // The modificationTime from PartitionedFile is in milliseconds but Spark's
      // TimestampType uses microseconds, so we multiply by 1000.
      val allMetadataValues: Map[String, Any] = Map(
        "file_path" -> UTF8String.fromString(filePath),
        "file_name" -> UTF8String.fromString(fileName),
        "file_size" -> shpFile.fileSize,
        "file_block_start" -> shpFile.start,
        "file_block_length" -> shpFile.length,
        "file_modification_time" -> (shpFile.modificationTime * 1000L))

      // The metadataSchema may be pruned by Spark's column pruning (e.g., when the query
      // only selects `_metadata.file_name`). We must construct the inner struct to match
      // the pruned schema exactly, otherwise field ordinals will be misaligned.
      val innerStructType = metadataSchema.fields.head.dataType.asInstanceOf[StructType]
      val prunedValues = innerStructType.fields.map(f => allMetadataValues(f.name))
      val metadataStruct = InternalRow.fromSeq(prunedValues.toSeq)

      // Wrap the struct in an outer row since _metadata is a single StructType column
      val metadataRow = InternalRow.fromSeq(Seq(metadataStruct))
      val baseSchema = StructType(readDataSchema.fields ++ partitionSchema.fields)
      new PartitionReaderWithMetadata(
        withPartitionValues,
        baseSchema,
        metadataSchema,
        metadataRow)
    } else {
      withPartitionValues
    }
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case filePartition: ShapefilePartition => buildReader(filePartition.files)
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }
  }
}

/**
 * Wraps a partition reader to append metadata column values to each row. This follows the same
 * pattern as [[PartitionReaderWithPartitionValues]] but for metadata columns: it uses a
 * [[JoinedRow]] to concatenate the base row (data + partition values) with the metadata row, then
 * projects the combined row through an
 * [[org.apache.spark.sql.catalyst.expressions.UnsafeProjection]] to produce a compact unsafe row.
 *
 * @param reader
 *   the underlying reader that produces data + partition value rows
 * @param baseSchema
 *   the combined schema of data columns and partition columns
 * @param metadataSchema
 *   the schema of the metadata columns being appended
 * @param metadataValues
 *   the constant metadata values to append to every row
 */
private[shapefile] class PartitionReaderWithMetadata(
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
