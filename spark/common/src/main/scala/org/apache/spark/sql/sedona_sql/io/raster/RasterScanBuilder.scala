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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsPushDownLimit
import org.apache.spark.sql.connector.read.SupportsPushDownTableSample
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

case class RasterScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    rasterOptions: RasterOptions)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownTableSample
    with SupportsPushDownLimit {

  private var pushedTableSample: Option[TableSampleInfo] = None
  private var pushedLimit: Option[Int] = None

  override def build(): Scan = {
    RasterScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      rasterOptions,
      pushedDataFilters,
      partitionFilters,
      dataFilters,
      pushedTableSample,
      pushedLimit)
  }

  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = {
    if (withReplacement || rasterOptions.retile) {
      false
    } else {
      pushedTableSample = Some(TableSampleInfo(lowerBound, upperBound, withReplacement, seed))
      true
    }
  }

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
  }

  override def isPartiallyPushed: Boolean = rasterOptions.retile
}

case class RasterScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    rasterOptions: RasterOptions,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    pushedTableSample: Option[TableSampleInfo] = None,
    pushedLimit: Option[Int] = None)
    extends FileScan
    with Batch {

  private lazy val inputPartitions = {
    var partitions = super.planInputPartitions()

    // Sample the files based on the table sample
    pushedTableSample.foreach { tableSample =>
      val r = new Random(tableSample.seed)
      var partitionIndex = 0
      partitions = partitions.flatMap {
        case filePartition: FilePartition =>
          val files = filePartition.files
          val sampledFiles = files.filter(_ => r.nextDouble() < tableSample.upperBound)
          if (sampledFiles.nonEmpty) {
            val index = partitionIndex
            partitionIndex += 1
            Some(FilePartition(index, sampledFiles))
          } else {
            None
          }
        case partition =>
          throw new IllegalArgumentException(
            s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
      }
    }

    // Limit the number of files to read
    pushedLimit.foreach { limit =>
      var remaining = limit
      partitions = partitions.iterator
        .takeWhile(_ => remaining > 0)
        .map { partition =>
          val filePartition = partition.asInstanceOf[FilePartition]
          val files = filePartition.files
          if (files.length <= remaining) {
            remaining -= files.length
            filePartition
          } else {
            val selectedFiles = files.take(remaining)
            remaining = 0
            FilePartition(filePartition.index, selectedFiles)
          }
        }
        .toArray
    }

    partitions
  }

  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions.map {
      case filePartition: FilePartition =>
        RasterInputPartition(filePartition.index, filePartition.files)
      case partition =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    RasterPartitionReaderFactory(
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      rasterOptions,
      pushedFilters)
  }
}
