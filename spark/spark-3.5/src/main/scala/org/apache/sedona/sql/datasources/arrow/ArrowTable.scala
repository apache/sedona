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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import org.apache.sedona.sql.datasources.arrow.ArrowWriteBuilder

case class ArrowTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    null
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new ArrowWriteBuilder(info)
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    None
  }

  override def formatName: String = {
    "Arrow Stream"
  }

  override def fallbackFileFormat: Class[_ <: FileFormat] = {
    null
  }

}
