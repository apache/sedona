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
package org.apache.spark.sql.execution.datasources.v2.geoparquet.metadata

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetMetaData
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, render}

case class GeoParquetMetadataPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: FileSourceOptions,
    filters: Seq[Filter])
    extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val iter = GeoParquetMetadataPartitionReaderFactory.readFile(
      broadcastedConf.value.value,
      partitionedFile,
      readDataSchema)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(
      fileReader,
      readDataSchema,
      partitionSchema,
      partitionedFile.partitionValues)
  }
}

object GeoParquetMetadataPartitionReaderFactory {
  private def readFile(
      configuration: Configuration,
      partitionedFile: PartitionedFile,
      readDataSchema: StructType): Iterator[InternalRow] = {
    val filePath = partitionedFile.toPath.toString
    val metadata = ParquetFileReader
      .open(HadoopInputFile.fromPath(partitionedFile.toPath, configuration))
      .getFooter
      .getFileMetaData
      .getKeyValueMetaData
    val row = GeoParquetMetaData.parseKeyValueMetaData(metadata) match {
      case Some(geo) =>
        val geoColumnsMap = geo.columns.map { case (columnName, columnMetadata) =>
          implicit val formats: org.json4s.Formats = DefaultFormats
          import org.json4s.jackson.Serialization
          val columnMetadataFields: Array[Any] = Array(
            UTF8String.fromString(columnMetadata.encoding),
            new GenericArrayData(columnMetadata.geometryTypes.map(UTF8String.fromString).toArray),
            new GenericArrayData(columnMetadata.bbox.toArray),
            columnMetadata.crs
              .map(projjson => UTF8String.fromString(compact(render(projjson))))
              .getOrElse(UTF8String.fromString("")),
            columnMetadata.covering
              .map(covering => UTF8String.fromString(Serialization.write(covering)))
              .orNull)
          val columnMetadataStruct = new GenericInternalRow(columnMetadataFields)
          UTF8String.fromString(columnName) -> columnMetadataStruct
        }
        val fields: Array[Any] = Array(
          UTF8String.fromString(filePath),
          UTF8String.fromString(geo.version.orNull),
          UTF8String.fromString(geo.primaryColumn),
          ArrayBasedMapData(geoColumnsMap))
        new GenericInternalRow(fields)
      case None =>
        // Not a GeoParquet file, return a row with null metadata values.
        val fields: Array[Any] = Array(UTF8String.fromString(filePath), null, null, null)
        new GenericInternalRow(fields)
    }
    Iterator(pruneBySchema(row, GeoParquetMetadataTable.schema, readDataSchema))
  }

  private def pruneBySchema(
      row: InternalRow,
      schema: StructType,
      readDataSchema: StructType): InternalRow = {
    // Projection push down for nested fields is not enabled, so this very simple implementation is enough.
    val values: Array[Any] = readDataSchema.fields.map { field =>
      val index = schema.fieldIndex(field.name)
      row.get(index, field.dataType)
    }
    new GenericInternalRow(values)
  }
}
