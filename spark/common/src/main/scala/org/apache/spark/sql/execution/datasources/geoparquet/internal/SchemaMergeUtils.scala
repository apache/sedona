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
package org.apache.spark.sql.execution.datasources.geoparquet.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.util.{Failure, Success, Try}

object SchemaMergeUtils extends Logging {

  /**
   * Lazily initialized reflection check for merge method with caseSensitive parameter. Returns
   * Some(method) if available, None otherwise.
   */
  private lazy val mergeMethodWithCaseSensitive: Option[java.lang.reflect.Method] = {
    Try {
      classOf[StructType].getMethod("merge", classOf[StructType], classOf[Boolean])
    } match {
      case Success(method) => Some(method)
      case Failure(_) => None
    }
  }

  /**
   * Safely merges two StructType schemas using cached reflection to check if caseSensitive
   * parameter is supported. Falls back to merge(schema) if merge(schema, caseSensitive) is not
   * available.
   */
  private def safeMerge(
      schema1: StructType,
      schema2: StructType,
      caseSensitive: Boolean): StructType = {
    mergeMethodWithCaseSensitive match {
      case Some(method) =>
        // Use the cached method with caseSensitive parameter (Spark 3.5 or later)
        method
          .invoke(schema1, schema2, caseSensitive.asInstanceOf[AnyRef])
          .asInstanceOf[StructType]
      case None =>
        // Fall back to merge without caseSensitive parameter (Spark 3.4)
        schema1.merge(schema2)
    }
  }

  /**
   * Figures out a merged Parquet/ORC schema with a distributed Spark job.
   */
  def mergeSchemasInParallel(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus],
      schemaReader: (Seq[FileStatus], Configuration, Boolean) => Seq[StructType])
      : Option[StructType] = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(parameters)
    // Workaround "The file might have been updated during query execution" on Databricks
    hadoopConf.set("spark.databricks.scan.modTimeCheck.enabled", "false")
    val serializedConf = new SerializableConfiguration(hadoopConf)

    // !! HACK ALERT !!
    // Here is a hack for Parquet, but it can be used by Orc as well.
    //
    // Parquet requires `FileStatus`es to read footers.
    // Here we try to send cached `FileStatus`es to executor side to avoid fetching them again.
    // However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = files.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of orc files.
    val numParallelism = Math.min(
      Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    val ignoreCorruptFiles = CaseInsensitiveMap(parameters)
      .get("ignoreCorruptFiles")
      .map(_.toBoolean)
      .getOrElse(PortableSQLConf.get.ignoreCorruptFiles)
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

    // Issues a Spark job to read Parquet/ORC schema in parallel.
    val partiallyMergedSchemas =
      sparkSession.sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          val schemas = schemaReader(fakeFileStatuses, serializedConf.value, ignoreCorruptFiles)

          if (schemas.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema = schemas.head
            schemas.tail.foreach { schema =>
              try {
                mergedSchema = safeMerge(mergedSchema, schema, caseSensitive)
              } catch {
                case cause: SparkException =>
                  throw QueryExecutionErrors.failedMergingSchemaError(mergedSchema, schema, cause)
              }
            }
            Iterator.single(mergedSchema)
          }
        }
        .collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        try {
          finalSchema = safeMerge(finalSchema, schema, caseSensitive)
        } catch {
          case cause: SparkException =>
            throw QueryExecutionErrors.failedMergingSchemaError(finalSchema, schema, cause)
        }
      }
      Some(finalSchema)
    }
  }
}
