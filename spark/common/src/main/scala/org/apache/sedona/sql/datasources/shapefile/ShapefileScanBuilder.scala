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

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ShapefileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  /**
   * Tracks any metadata fields (e.g., from `_metadata`) requested in the query. Populated by
   * [[pruneColumns]] when Spark pushes down column projections.
   */
  private var _requiredMetadataSchema: StructType = StructType(Seq.empty)

  /**
   * Intercepts Spark's column pruning to separate metadata columns from data/partition columns.
   * Fields in [[requiredSchema]] that do not belong to the data schema or partition schema are
   * assumed to be metadata fields (e.g., `_metadata`). These are captured in
   * [[_requiredMetadataSchema]] so the scan can include them in the output.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    val resolver = sparkSession.sessionState.conf.resolver
    val metaFields = requiredSchema.fields.filter { field =>
      !dataSchema.fields.exists(df => resolver(df.name, field.name)) &&
      !fileIndex.partitionSchema.fields.exists(pf => resolver(pf.name, field.name))
    }
    _requiredMetadataSchema = StructType(metaFields)
    super.pruneColumns(requiredSchema)
  }

  override def build(): Scan = {
    ShapefileScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      _requiredMetadataSchema,
      options,
      pushedDataFilters,
      partitionFilters,
      dataFilters)
  }
}
