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

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.sedona.common.FunctionsGeoTools
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.DbfFileReader
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.PrimitiveShape
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.ShapeFileReader
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.ShxFileReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.sedona.sql.datasources.shapefile.ShapefilePartitionReader.logger
import org.apache.sedona.sql.datasources.shapefile.ShapefilePartitionReader.openStream
import org.apache.sedona.sql.datasources.shapefile.ShapefilePartitionReader.tryOpenStream
import org.apache.sedona.sql.datasources.shapefile.ShapefileUtils.baseSchema
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.StructType
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.PrecisionModel
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import java.util.Locale
import scala.util.Try

class ShapefilePartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    readDataSchema: StructType,
    options: ShapefileReadOptions)
    extends PartitionReader[InternalRow] {

  private val partitionedFilesMap: Map[String, Path] = partitionedFiles.map { file =>
    val fileName = file.filePath.toPath.getName
    val extension = FilenameUtils.getExtension(fileName).toLowerCase(Locale.ROOT)
    extension -> file.filePath.toPath
  }.toMap

  private val cpg = options.charset.orElse {
    // No charset option or sedona.global.charset system property specified, infer charset
    // from the cpg file.
    tryOpenStream(partitionedFilesMap, "cpg", configuration)
      .flatMap { stream =>
        try {
          val lineIter = IOUtils.lineIterator(stream, StandardCharsets.UTF_8)
          if (lineIter.hasNext) {
            Some(lineIter.next().trim())
          } else {
            None
          }
        } finally {
          stream.close()
        }
      }
      .orElse {
        // Cannot infer charset from cpg file. If sedona.global.charset is set to "utf8", use UTF-8 as
        // the default charset. This is for compatibility with the behavior of the RDD API.
        val charset = System.getProperty("sedona.global.charset", "default")
        val utf8flag = charset.equalsIgnoreCase("utf8")
        if (utf8flag) Some("UTF-8") else None
      }
  }

  private val prj = tryOpenStream(partitionedFilesMap, "prj", configuration).map { stream =>
    try {
      IOUtils.toString(stream, StandardCharsets.UTF_8)
    } finally {
      stream.close()
    }
  }

  private val shpReader: ShapeFileReader = {
    val reader = tryOpenStream(partitionedFilesMap, "shx", configuration) match {
      case Some(shxStream) =>
        try {
          val index = ShxFileReader.readAll(shxStream)
          new ShapeFileReader(index)
        } finally {
          shxStream.close()
        }
      case None => new ShapeFileReader()
    }
    val stream = openStream(partitionedFilesMap, "shp", configuration)
    reader.initialize(stream)
    reader
  }

  private val dbfReader =
    tryOpenStream(partitionedFilesMap, "dbf", configuration).map { stream =>
      val reader = new DbfFileReader()
      reader.initialize(stream)
      reader
    }

  private val geometryField = readDataSchema.filter(_.dataType.isInstanceOf[GeometryUDT]) match {
    case Seq(geoField) => Some(geoField)
    case Seq() => None
    case _ => throw new IllegalArgumentException("Only one geometry field is allowed")
  }

  private val shpSchema: StructType = {
    val dbfFields = dbfReader
      .map { reader =>
        ShapefileUtils.fieldDescriptorsToStructFields(reader.getFieldDescriptors.asScala.toSeq)
      }
      .getOrElse(Seq.empty)
    StructType(baseSchema(options).fields ++ dbfFields)
  }

  // projection from shpSchema to readDataSchema
  private val projection = {
    val expressions = readDataSchema.map { field =>
      val index = Try(shpSchema.fieldIndex(field.name)).getOrElse(-1)
      if (index >= 0) {
        val sourceField = shpSchema.fields(index)
        val refExpr = BoundReference(index, sourceField.dataType, sourceField.nullable)
        if (sourceField.dataType == field.dataType) refExpr
        else {
          Cast(refExpr, field.dataType)
        }
      } else {
        if (field.nullable) {
          Literal(null)
        } else {
          // This usually won't happen, since all fields of readDataSchema are nullable for most
          // of the time. See org.apache.spark.sql.execution.datasources.v2.FileTable#dataSchema
          // for more details.
          val dbfPath = partitionedFilesMap.get("dbf").orNull
          throw new IllegalArgumentException(
            s"Field ${field.name} not found in shapefile $dbfPath")
        }
      }
    }
    UnsafeProjection.create(expressions)
  }

  // Convert DBF field values to SQL values
  private val fieldValueConverters: Seq[Array[Byte] => Any] = dbfReader
    .map { reader =>
      reader.getFieldDescriptors.asScala.map { field =>
        val index = Try(readDataSchema.fieldIndex(field.getFieldName)).getOrElse(-1)
        if (index >= 0) {
          ShapefileUtils.fieldValueConverter(field, cpg)
        } else { (_: Array[Byte]) =>
          null
        }
      }.toSeq
    }
    .getOrElse(Seq.empty)

  private val geometryFactory = prj match {
    case Some(wkt) =>
      val srid =
        try {
          FunctionsGeoTools.wktCRSToSRID(wkt)
        } catch {
          case e: Throwable =>
            val prjPath = partitionedFilesMap.get("prj").orNull
            logger.warn(s"Failed to parse SRID from .prj file $prjPath", e)
            0
        }
      new GeometryFactory(new PrecisionModel, srid)
    case None => new GeometryFactory()
  }

  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (shpReader.nextKeyValue()) {
      val key = shpReader.getCurrentKey
      val id = key.getIndex

      val attributesOpt = dbfReader.flatMap { reader =>
        if (reader.nextKeyValue()) {
          val value = reader.getCurrentFieldBytes
          Option(value)
        } else {
          val dbfPath = partitionedFilesMap.get("dbf").orNull
          logger.warn("Shape record loses attributes in .dbf file {} at ID={}", dbfPath, id)
          None
        }
      }

      val value = shpReader.getCurrentValue
      val geometry = geometryField.flatMap { _ =>
        if (value.getType.isSupported) {
          val shape = new PrimitiveShape(value)
          Some(shape.getShape(geometryFactory))
        } else {
          logger.warn(
            "Shape type {} is not supported, geometry value will be null",
            value.getType.name())
          None
        }
      }

      val attrValues = attributesOpt match {
        case Some(fieldBytesList) =>
          // Convert attributes to SQL values
          fieldBytesList.asScala.zip(fieldValueConverters).map { case (fieldBytes, converter) =>
            converter(fieldBytes)
          }
        case None =>
          // No attributes, fill with nulls
          Seq.fill(fieldValueConverters.length)(null)
      }

      val serializedGeom = geometry.map(GeometryUDT.serialize).orNull
      val shpRow = if (options.keyFieldName.isDefined) {
        InternalRow.fromSeq(serializedGeom +: key.getIndex +: attrValues.toSeq)
      } else {
        InternalRow.fromSeq(serializedGeom +: attrValues.toSeq)
      }
      currentRow = projection(shpRow)
      true
    } else {
      dbfReader.foreach { reader =>
        if (reader.nextKeyValue()) {
          val dbfPath = partitionedFilesMap.get("dbf").orNull
          logger.warn("Redundant attributes in {} exists", dbfPath)
        }
      }
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    dbfReader.foreach(_.close())
    shpReader.close()
  }
}

object ShapefilePartitionReader {
  val logger: Logger = LoggerFactory.getLogger(classOf[ShapefilePartitionReader])

  private def openStream(
      partitionedFilesMap: Map[String, Path],
      extension: String,
      configuration: Configuration): FSDataInputStream = {
    tryOpenStream(partitionedFilesMap, extension, configuration).getOrElse {
      val path = partitionedFilesMap.head._2
      val baseName = FilenameUtils.getBaseName(path.getName)
      throw new IllegalArgumentException(
        s"No $extension file found for shapefile $baseName in ${path.getParent}")
    }
  }

  private def tryOpenStream(
      partitionedFilesMap: Map[String, Path],
      extension: String,
      configuration: Configuration): Option[FSDataInputStream] = {
    partitionedFilesMap.get(extension).map { path =>
      val fs = path.getFileSystem(configuration)
      fs.open(path)
    }
  }
}
