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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderWithPartitionValues
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class ShapefilePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
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
    new PartitionReaderWithPartitionValues(
      fileReader,
      readDataSchema,
      partitionSchema,
      partitionedFiles.head.partitionValues)
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
