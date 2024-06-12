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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class GeoParquetMetadataScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {
  override def build(): Scan = {
    GeoParquetMetadataScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      getPushedDataFilters,
      getPartitionFilters,
      getDataFilters)
  }

  // The following methods uses reflection to address compatibility issues for Spark 3.0 ~ 3.2

  private def getPushedDataFilters: Array[Filter] = {
    try {
      val field = classOf[FileScanBuilder].getDeclaredField("pushedDataFilters")
      field.setAccessible(true)
      field.get(this).asInstanceOf[Array[Filter]]
    } catch {
      case _: NoSuchFieldException =>
        Array.empty
    }
  }

  private def getPartitionFilters: Seq[Expression] = {
    try {
      val field = classOf[FileScanBuilder].getDeclaredField("partitionFilters")
      field.setAccessible(true)
      field.get(this).asInstanceOf[Seq[Expression]]
    } catch {
      case _: NoSuchFieldException =>
        Seq.empty
    }
  }

  private def getDataFilters: Seq[Expression] = {
    try {
      val field = classOf[FileScanBuilder].getDeclaredField("dataFilters")
      field.setAccessible(true)
      field.get(this).asInstanceOf[Seq[Expression]]
    } catch {
      case _: NoSuchFieldException =>
        Seq.empty
    }
  }
}
