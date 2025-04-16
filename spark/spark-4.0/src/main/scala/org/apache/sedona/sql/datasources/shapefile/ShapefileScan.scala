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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.sedona.sql.datasources.shapefile.ShapefileScan.logger
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.{Logger, LoggerFactory}

import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

case class ShapefileScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
    extends FileScan {

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    ShapefilePartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      ShapefileReadOptions.parse(options),
      pushedFilters)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    // Simply use the default implementation to compute input partitions for all files
    val allFilePartitions = super.planInputPartitions().flatMap {
      case filePartition: FilePartition =>
        filePartition.files
      case partition =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${partition.getClass.getCanonicalName}")
    }

    // Group shapefiles by their main path (without the extension)
    val shapefileGroups: mutable.Map[String, mutable.Map[String, PartitionedFile]] =
      mutable.Map.empty
    allFilePartitions.foreach { partitionedFile =>
      val path = partitionedFile.filePath.toPath
      val fileName = path.getName
      val pos = fileName.lastIndexOf('.')
      if (pos == -1) None
      else {
        val mainName = fileName.substring(0, pos)
        val extension = fileName.substring(pos + 1).toLowerCase(Locale.ROOT)
        if (ShapefileUtils.shapeFileExtensions.contains(extension)) {
          val key = new Path(path.getParent, mainName).toString
          val group = shapefileGroups.getOrElseUpdate(key, mutable.Map.empty)
          group += (extension -> partitionedFile)
        }
      }
    }

    // Create a partition for each group
    shapefileGroups.zipWithIndex.flatMap { case ((key, group), index) =>
      // Check if the group has all the necessary files
      val suffixes = group.keys.toSet
      val hasMissingFiles = ShapefileUtils.mandatoryFileExtensions.exists { suffix =>
        if (!suffixes.contains(suffix)) {
          logger.warn(s"Shapefile $key is missing a $suffix file")
          true
        } else false
      }
      if (!hasMissingFiles) {
        Some(ShapefilePartition(index, group.values.toArray))
      } else {
        None
      }
    }.toArray
  }
}

object ShapefileScan {
  val logger: Logger = LoggerFactory.getLogger(classOf[ShapefileScan])
}
