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
package org.apache.spark.sql.sedona_sql.io.geojson

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json._
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * This is taken from [[org.apache.spark.sql.execution.datasources.json.JsonFileFormat]] with
 * slight modification to support GeoJSON and read/write geometry values.
 */
class GeoJSONFileFormat extends TextBasedFileFormat with DataSourceRegister {
  override val shortName: String = "geojson"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    val jsonDataSource = JsonDataSource(parsedOptions)
    jsonDataSource.isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {

    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    // Use existing logic to infer the full schema first
    val fullSchemaOption =
      JsonDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)

    fullSchemaOption.map { fullSchema =>
      // Replace 'geometry' field type with GeometryUDT
      val newFields = GeoJSONUtils.updateGeometrySchema(fullSchema, GeometryUDT)
      StructType(newFields)
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val conf = job.getConfiguration
    val parsedOptions = new JSONOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }
    val geometryColumnName = options.getOrElse("geometry.column", "geometry")
    SparkCompatUtil.findNestedField(
      dataSchema,
      geometryColumnName.split('.'),
      resolver = SQLConf.get.resolver) match {
      case Some(StructField(_, dataType, _, _)) =>
        if (!dataType.acceptsType(GeometryUDT)) {
          throw new IllegalArgumentException(s"$geometryColumnName is not a geometry column")
        }
      case None =>
        throw new IllegalArgumentException(s"Column $geometryColumnName not found in the schema")
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new GeoJSONOutputWriter(path, parsedOptions, dataSchema, geometryColumnName, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".json" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val actualSchema =
      StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    val alteredSchema = GeoJSONUtils.updateGeometrySchema(actualSchema, StringType)

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new IllegalArgumentException(
        "referenced columns only include the internal corrupt record column, this is not allowed")
    }

    (file: PartitionedFile) => {
      val parser = SparkCompatUtil.constructJacksonParser(
        alteredSchema,
        parsedOptions,
        allowArrayAsStructs = true)
      val dataSource = JsonDataSource(parsedOptions)

      dataSource
        .readFile(broadcastedHadoopConf.value.value, file, parser, actualSchema)
        .map(row => {
          val newRow = GeoJSONUtils.convertGeoJsonToGeometry(row, alteredSchema)
          newRow
        })
    }
  }

  override def toString: String = "GEOJSON"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[GeoJSONFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case GeometryUDT => true
    case RasterUDT => false
    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }
}
