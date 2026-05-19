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
package org.apache.sedona.sql.datasources.geopackage

import org.apache.sedona.sql.datasources.geopackage.model.GeoPackageOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.jdk.CollectionConverters._

class GeoPackageScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    loadOptions: GeoPackageOptions,
    userDefinedSchema: Option[StructType] = None)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  private var _requiredMetadataSchema: StructType = StructType(Seq.empty)

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
    val paths = fileIndex.allFiles().map(_.getPath.toString)

    val fileIndexAdjusted =
      if (loadOptions.showMetadata)
        new InMemoryFileIndex(
          sparkSession,
          paths.slice(0, 1).map(new org.apache.hadoop.fs.Path(_)),
          options.asCaseSensitiveMap.asScala.toMap,
          userDefinedSchema)
      else fileIndex

    GeoPackageScan(
      dataSchema,
      sparkSession,
      fileIndexAdjusted,
      dataSchema,
      readPartitionSchema(),
      _requiredMetadataSchema,
      options,
      loadOptions)
  }
}
