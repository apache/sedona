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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Helper for creating a [[PartitioningAwareFileIndex]] without going through the
 * [[org.apache.spark.sql.execution.streaming.FileStreamSink.hasMetadata]] check in
 * [[org.apache.spark.sql.execution.datasources.v2.FileTable.fileIndex]].
 *
 * <p>The streaming metadata check can produce spurious [[java.io.FileNotFoundException]] warnings
 * when reading from cloud storage (e.g., S3) because it attempts to stat the path as a directory.
 * For non-streaming, read-only file tables such as Shapefile and GeoPackage, this check is
 * unnecessary and can be safely bypassed.
 */
object SedonaFileIndexHelper {

  /**
   * Build an [[InMemoryFileIndex]] for the given paths, resolving globs if necessary, without the
   * streaming metadata directory check.
   */
  def createFileIndex(
      sparkSession: SparkSession,
      options: CaseInsensitiveStringMap,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType]): PartitioningAwareFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val globPathsEnabled =
      Option(options.get("globPaths")).map(v => java.lang.Boolean.parseBoolean(v)).getOrElse(true)
    val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(
      paths,
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = true,
      enableGlobbing = globPathsEnabled)
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(
      sparkSession,
      rootPathsSpecified,
      caseSensitiveMap,
      userSpecifiedSchema,
      fileStatusCache)
  }
}
