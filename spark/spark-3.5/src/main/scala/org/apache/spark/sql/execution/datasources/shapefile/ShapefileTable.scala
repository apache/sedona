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
package org.apache.spark.sql.execution.datasources.shapefile

import org.apache.hadoop.fs.FileStatus
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.shapefile.ShapefileUtils.fieldDescriptorsToSchema
import org.apache.spark.sql.execution.datasources.shapefile.ShapefileUtils.mergeSchemas
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.JavaConverters._

case class ShapefileTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def formatName: String = "Shapefile"

  override def capabilities: java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) None
    else {
      def isDbfFile(file: FileStatus): Boolean = {
        val name = file.getPath.getName.toLowerCase(Locale.ROOT)
        name.endsWith(".dbf")
      }

      def isShpFile(file: FileStatus): Boolean = {
        val name = file.getPath.getName.toLowerCase(Locale.ROOT)
        name.endsWith(".shp")
      }

      if (!files.exists(isShpFile)) None
      else {
        val geometryFieldName = ShapefileReadOptions.parse(options).geometryFieldName
        val dbfFiles = files.filter(isDbfFile)
        if (dbfFiles.isEmpty) {
          Some(StructType(StructField(geometryFieldName, GeometryUDT) :: Nil))
        } else {
          val serializableConf = new SerializableConfiguration(
            sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap))
          val resolver = sparkSession.sessionState.conf.resolver
          val partiallyMergedSchemas = sparkSession.sparkContext
            .parallelize(dbfFiles)
            .mapPartitions { iter =>
              val schemas = iter.map { stat =>
                val fs = stat.getPath.getFileSystem(serializableConf.value)
                val stream = fs.open(stat.getPath)
                try {
                  val dbfParser = new DbfParseUtil()
                  dbfParser.parseFileHead(stream)
                  val fieldDescriptors = dbfParser.getFieldDescriptors
                  fieldDescriptorsToSchema(
                    fieldDescriptors.asScala.toSeq,
                    geometryFieldName,
                    resolver)
                } finally {
                  stream.close()
                }
              }.toSeq
              mergeSchemas(schemas).iterator
            }
            .collect()
          mergeSchemas(partiallyMergedSchemas)
        }
      }
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    ShapefileScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = null
}
