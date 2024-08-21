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

import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.SqlApiAnalysis.Resolver
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

object ShapefileUtils {

  /**
   * shp: main file for storing shapes shx: index file for the main file dbf: attribute file cpg:
   * code page file prj: projection file
   */
  val shapeFileExtensions: Set[String] = Set("shp", "shx", "dbf", "cpg", "prj")

  /**
   * The mandatory file extensions for a shapefile. We don't require the dbf file and shx file for
   * being consistent with the behavior of the RDD API ShapefileReader.readToGeometryRDD
   */
  val mandatoryFileExtensions: Set[String] = Set("shp")

  def mergeSchemas(schemas: Seq[StructType]): Option[StructType] = {
    if (schemas.isEmpty) {
      None
    } else {
      var mergedSchema = schemas.head
      schemas.tail.foreach { schema =>
        try {
          mergedSchema = mergedSchema.merge(schema)
        } catch {
          case cause: SparkException =>
            throw new IllegalArgumentException(
              s"Failed to merge schema $mergedSchema with $schema",
              cause)
        }
      }
      Some(mergedSchema)
    }
  }

  def fieldDescriptorsToStructFields(fieldDescriptors: Seq[FieldDescriptor]): Seq[StructField] = {
    fieldDescriptors.map { desc =>
      val name = desc.getFieldName
      val dataType = desc.getFieldType match {
        case 'C' => StringType
        case 'N' | 'F' =>
          val scale = desc.getFieldDecimalCount
          if (scale == 0) LongType
          else {
            val precision = desc.getFieldLength
            DecimalType(precision, scale)
          }
        case 'L' => BooleanType
        case 'D' => DateType
        case _ =>
          throw new IllegalArgumentException(s"Unsupported field type ${desc.getFieldType}")
      }
      StructField(name, dataType, nullable = true)
    }
  }

  def fieldDescriptorsToSchema(fieldDescriptors: Seq[FieldDescriptor]): StructType = {
    val structFields = fieldDescriptorsToStructFields(fieldDescriptors)
    StructType(structFields)
  }

  def fieldDescriptorsToSchema(
      fieldDescriptors: Seq[FieldDescriptor],
      geometryFieldName: String,
      resolver: Resolver): StructType = {
    val structFields = fieldDescriptorsToStructFields(fieldDescriptors)
    if (structFields.exists(f => resolver(f.name, geometryFieldName))) {
      throw new IllegalArgumentException(
        s"Field name $geometryFieldName is reserved for geometry but appears in non-spatial attributes. " +
          "Please specify a different field name for geometry using the 'geometry.name' option.")
    }
    StructType(StructField(geometryFieldName, GeometryUDT) +: structFields)
  }

  def fieldValueConverter(desc: FieldDescriptor, cpg: Option[String]): Array[Byte] => Any = {
    desc.getFieldType match {
      case 'C' =>
        val charset = System.getProperty("sedona.global.charset", "default")
        val utf8flag = charset.equalsIgnoreCase("utf8")
        val encoding = if (utf8flag) "UTF-8" else cpg.getOrElse("ISO-8859-1")
        if (encoding.toLowerCase(Locale.ROOT) == "utf-8") { (bytes: Array[Byte]) =>
          UTF8String.fromBytes(bytes).trimRight()
        } else { (bytes: Array[Byte]) =>
          {
            val str = new String(bytes, encoding)
            UTF8String.fromString(str).trimRight()
          }
        }
      case 'N' | 'F' =>
        val scale = desc.getFieldDecimalCount
        if (scale == 0) { (bytes: Array[Byte]) =>
          try {
            new String(bytes, StandardCharsets.ISO_8859_1).trim.toLong
          } catch {
            case _: Exception => null
          }
        } else { (bytes: Array[Byte]) =>
          try {
            Decimal.fromString(UTF8String.fromBytes(bytes))
          } catch {
            case _: Exception => null
          }
        }
      case 'L' =>
        (bytes: Array[Byte]) =>
          if (bytes.isEmpty) null
          else {
            bytes.head match {
              case 'T' | 't' | 'Y' | 'y' => true
              case 'F' | 'f' | 'N' | 'n' => false
              case _ => null
            }
          }
      case 'D' =>
        (bytes: Array[Byte]) => {
          try {
            val dateString = new String(bytes, StandardCharsets.ISO_8859_1)
            val formatter = DateTimeFormatter.BASIC_ISO_DATE
            val date = LocalDate.parse(dateString, formatter)
            date.toEpochDay.toInt
          } catch {
            case _: Exception => null
          }
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported field type ${desc.getFieldType}")
    }
  }
}
