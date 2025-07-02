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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.jdk.CollectionConverters._

case class GeoPackageScan(
    dataSchema: StructType,
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    loadOptions: GeoPackageOptions)
    extends FileScan {

  override def partitionFilters: Seq[Expression] = {
    Seq.empty
  }

  override def dataFilters: Seq[Expression] = {
    Seq.empty
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    GeoPackagePartitionReaderFactory(sparkSession, broadcastedConf, loadOptions, dataSchema)
  }
}
