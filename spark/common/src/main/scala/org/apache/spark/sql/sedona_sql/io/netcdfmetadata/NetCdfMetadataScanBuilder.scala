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
package org.apache.spark.sql.sedona_sql.io.netcdfmetadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsPushDownLimit
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sedona_sql.io.raster.RasterInputPartition
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class NetCdfMetadataScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownLimit {

  private var pushedLimit: Option[Int] = None

  override def build(): Scan = {
    NetCdfMetadataScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      pushedDataFilters,
      partitionFilters,
      dataFilters,
      pushedLimit)
  }

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
  }

  override def isPartiallyPushed: Boolean = false
}

case class NetCdfMetadataScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    pushedLimit: Option[Int] = None)
    extends FileScan
    with Batch {

  // FileScan defines concrete equals/hashCode (comparing only fileIndex, read schema, and
  // normalized filters), which suppresses the case-class-synthesized versions — so `pushedLimit`
  // and `options` would not participate in equality. Since the pushed limit is enforced by the
  // scan alone (the Limit operator is removed when isPartiallyPushed = false), two scans that
  // differ only by limit must not compare equal, or exchange/subquery reuse could serve a
  // limited scan's result for an unlimited one. Mirror how Spark's own pushdown-carrying scans
  // (e.g. CSVScan) override equality.
  override def equals(obj: Any): Boolean = obj match {
    case o: NetCdfMetadataScan =>
      super.equals(o) && options == o.options && pushedLimit == o.pushedLimit
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def isSplitable(path: org.apache.hadoop.fs.Path): Boolean = false

  private lazy val inputPartitions = {
    var partitions = super.planInputPartitions()

    // Limit the number of files to read
    pushedLimit.foreach { limit =>
      var remaining = limit
      partitions = partitions.iterator
        .takeWhile(_ => remaining > 0)
        .map {
          case filePartition: FilePartition =>
            val files = filePartition.files
            if (files.length <= remaining) {
              remaining -= files.length
              filePartition
            } else {
              val selectedFiles = files.take(remaining)
              remaining = 0
              FilePartition(filePartition.index, selectedFiles)
            }
          case partition =>
            throw new IllegalArgumentException(
              s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
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

    NetCdfMetadataPartitionReaderFactory(
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema)
  }
}
