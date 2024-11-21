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
package org.apache.sedona.sql.datasources.spider

import org.apache.sedona.common.spider.GeneratorFactory
import org.apache.sedona.common.spider.ParcelGenerator
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Random

private class SpiderScanBuilder(
    distribution: String,
    numPartitions: Int,
    numRows: Long,
    opts: CaseInsensitiveStringMap,
    seed: Long,
    transform: AffineTransform)
    extends ScanBuilder {
  override def build(): Scan = new Scan {
    override def readSchema(): StructType = SpiderTable.SCHEMA

    override def toBatch: Batch = new Batch {
      override def planInputPartitions(): Array[InputPartition] = {
        if (distribution != "parcel") {
          val finalNumPartitions = Math.min(numPartitions, numRows).toInt
          val partitionInfo = computePartitionRanges(finalNumPartitions, numRows)
          val optsMap = opts.asCaseSensitiveMap()
          partitionInfo.zipWithIndex.map { case ((startIndex, partitionRows), iPartition) =>
            SpiderPartition(
              iPartition,
              startIndex,
              partitionRows,
              seed + iPartition,
              distribution,
              transform,
              optsMap)
          }.toArray
        } else {
          // Parcel distribution requires special partition generation to ensure that the records are
          // non overlapping.
          val maxPartitionBoxes = Math.min(numPartitions, numRows)
          if (maxPartitionBoxes == 0) {
            return Array.empty[InputPartition]
          }

          // The actual number of partitions is a power of 4 that is less than or equal to the number of
          // requested partitions. This is for distributing the records evenly across the partitions.
          val finalNumPartitions =
            Math.pow(4, Math.floor(Math.log(maxPartitionBoxes) / Math.log(4))).toInt

          // Generate the partitions using the parcel generator but set dithering to zero since dithering
          // should only be applied on the final records and not the partitions
          val conf = new util.HashMap[String, String]()
          conf.putAll(opts.asCaseSensitiveMap())
          conf.put("dither", "0")
          conf.put("cardinality", finalNumPartitions.toString)
          val random = new Random(seed - 1)
          val parcelGenerator =
            GeneratorFactory.create("parcel", random, conf).asInstanceOf[ParcelGenerator]

          // Generate the partitions
          val partitionInfo = computePartitionRanges(finalNumPartitions, numRows)
          partitionInfo.zipWithIndex.map { case ((startIndex, partitionRows), iPartition) =>
            val partitionBox = parcelGenerator.generateBox()
            val partitionTransform = transform.transform(partitionBox)
            val partitionOpts = new util.HashMap[String, String]()
            partitionOpts.putAll(opts.asCaseSensitiveMap())
            partitionOpts.put("cardinality", partitionRows.toString)
            SpiderPartition(
              iPartition,
              startIndex,
              partitionRows,
              seed + iPartition,
              distribution,
              partitionTransform,
              partitionOpts)
          }.toArray
        }
      }

      /**
       * Computes the start index and number of rows for each partition
       *
       * @param numPartitions
       *   The number of partitions to create
       * @param totalRows
       *   The total number of rows to distribute
       * @return
       *   Sequence of (startIndex, numRows) for each partition
       */
      private def computePartitionRanges(
          numPartitions: Int,
          totalRows: Long): Seq[(Long, Long)] = {
        var recordsRemaining = totalRows
        (0 until numPartitions).map { iPartition =>
          val startIndex = totalRows - recordsRemaining
          val partitionRows = recordsRemaining / (numPartitions - iPartition)
          recordsRemaining -= partitionRows
          (startIndex, partitionRows)
        }
      }

      override def createReaderFactory(): PartitionReaderFactory = {
        (partition: InputPartition) =>
          {
            new SpiderPartitionReader(partition.asInstanceOf[SpiderPartition])
          }
      }
    }
  }
}
