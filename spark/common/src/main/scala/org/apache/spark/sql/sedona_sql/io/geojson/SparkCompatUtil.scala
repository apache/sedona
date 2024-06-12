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

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonParser}
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.LegacyDateFormat
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.reflect.runtime.{universe => ru}
import java.time.ZoneId
import java.util.Locale

/**
 * Making GeoJSONFileFormat and GeoJSONOutputWriter classes compatible with Spark 3.0.x.
 */
object SparkCompatUtil {
  // Here we are defining our own findNestedField method instead of using StructType.findNestedField method for
  // compatibility with Spark 3.0.x
  def findNestedField(
      schema: StructType,
      path: Array[String],
      resolver: Resolver): Option[StructField] = {
    path match {
      case Array(part) =>
        schema.find(f => resolver(f.name, part))
      case Array(head, tail @ _*) =>
        schema.find(f => resolver(f.name, head)).flatMap { f =>
          f.dataType match {
            case st: StructType => findNestedField(st, tail.toArray, resolver)
            case _ => None
          }
        }
      case _ => None
    }
  }

  def constructTimestampFormatter(
      options: JSONOptions,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): TimestampFormatter = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val timestampFormatTerm = ru.typeOf[JSONOptions].decl(ru.TermName("timestampFormat"))
    val timestampFormatInWriteTerm =
      ru.typeOf[JSONOptions].decl(ru.TermName("timestampFormatInWrite"))
    val instanceMirror = mirror.reflect(options)
    val format = if (timestampFormatTerm.isMethod) {
      instanceMirror.reflectMethod(timestampFormatTerm.asMethod).apply().asInstanceOf[String]
    } else if (timestampFormatInWriteTerm.isMethod) {
      instanceMirror
        .reflectMethod(timestampFormatInWriteTerm.asMethod)
        .apply()
        .asInstanceOf[String]
    } else {
      throw new Exception(
        "Neither timestampFormat nor timestampFormatInWrite found in JSONOptions")
    }
    TimestampFormatter(format, zoneId, locale, legacyFormat, isParsing)
  }

  def constructDateFormatter(
      options: JSONOptions,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): DateFormatter = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    val dateFormatTerm = ru.typeOf[JSONOptions].decl(ru.TermName("dateFormat"))
    val dateFormatInWriteTerm = ru.typeOf[JSONOptions].decl(ru.TermName("dateFormatInWrite"))
    val instanceMirror = mirror.reflect(options)
    val format = if (dateFormatTerm.isMethod) {
      instanceMirror.reflectMethod(dateFormatTerm.asMethod).apply().asInstanceOf[String]
    } else if (dateFormatInWriteTerm.isMethod) {
      instanceMirror.reflectMethod(dateFormatInWriteTerm.asMethod).apply().asInstanceOf[String]
    } else {
      throw new Exception("Neither dateFormat nor dateFormatInWrite found in JSONOptions")
    }

    val dateFormatterClass =
      mirror.staticClass("org.apache.spark.sql.catalyst.util.DateFormatter$")
    val dateFormatterModule = mirror.staticModule(dateFormatterClass.fullName)
    val dateFormatterInstance = mirror.reflectModule(dateFormatterModule)
    val applyMethods =
      dateFormatterClass.toType.members.filter(_.name.decodedName.toString == "apply")
    applyMethods.find(_.typeSignature.paramLists.flatten.size == 5) match {
      case Some(applyMethod) =>
        mirror
          .reflect(dateFormatterInstance.instance)
          .reflectMethod(applyMethod.asMethod)(format, zoneId, locale, legacyFormat, isParsing)
          .asInstanceOf[DateFormatter]
      case None =>
        applyMethods.find { method =>
          val params = method.typeSignature.paramLists.flatten
          params.size == 4 &&
          // get rid of the variant taking Option[String] as first parameter
          params.head.typeSignature <:< ru.typeOf[String]
        } match {
          case Some(applyMethod) =>
            mirror
              .reflect(dateFormatterInstance.instance)
              .reflectMethod(applyMethod.asMethod)(format, locale, legacyFormat, isParsing)
              .asInstanceOf[DateFormatter]
          case None =>
            throw new Exception("No suitable apply method found in DateFormatter")
        }
    }
  }

  def constructJacksonParser(
      schema: DataType,
      options: JSONOptions,
      allowArrayAsStructs: Boolean): JacksonParser = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val jacksonParserClass =
      mirror.staticClass("org.apache.spark.sql.catalyst.json.JacksonParser")

    val constructorMethods = jacksonParserClass.toType.members.filter(_.isConstructor)

    constructorMethods.find(_.typeSignature.paramLists.flatten.size == 3) match {
      case Some(constructorMethod) =>
        mirror
          .reflectClass(jacksonParserClass)
          .reflectConstructor(constructorMethod.asMethod)(schema, options, allowArrayAsStructs)
          .asInstanceOf[JacksonParser]
      case None =>
        constructorMethods.find(_.typeSignature.paramLists.flatten.size == 4) match {
          case Some(constructorMethod) =>
            mirror
              .reflectClass(jacksonParserClass)
              .reflectConstructor(constructorMethod.asMethod)(
                schema,
                options,
                allowArrayAsStructs,
                Seq.empty)
              .asInstanceOf[JacksonParser]
          case None =>
            throw new Exception("No suitable constructor found in JacksonParser")
        }
    }
  }
}
