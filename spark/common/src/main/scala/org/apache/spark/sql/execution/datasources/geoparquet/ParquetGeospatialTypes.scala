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
package org.apache.spark.sql.execution.datasources.geoparquet

import java.util.Locale

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.parquet.schema.{LogicalTypeAnnotation, Type}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse

/**
 * A column annotated with the GEOMETRY or GEOGRAPHY logical type of the Parquet format.
 *
 * @param isGeography
 *   true for GEOGRAPHY, false for GEOMETRY
 * @param crs
 *   the `crs` property of the logical type, None when the file leaves it out (OGC:CRS84)
 * @param edges
 *   the edge interpolation algorithm of a GEOGRAPHY column in lower case, None when the file
 *   leaves it out (spherical)
 */
case class ParquetGeospatialColumn(
    isGeography: Boolean,
    crs: Option[String],
    edges: Option[String])

/**
 * The GEOMETRY and GEOGRAPHY logical types of the Parquet format, which GeoParquet 2.0 is built
 * on. parquet-java has them since 1.16.0, the version Spark 4.1 ships. Sedona also compiles
 * against the older parquet-java of Spark 3.5 and 4.0, so the types are reached by reflection and
 * [[supported]] reflects the library found at run time.
 */
object ParquetGeospatialTypes {
  private val annotation = "org.apache.parquet.schema.LogicalTypeAnnotation"

  private def loadClass(name: String): Option[Class[_]] =
    Try[Class[_]](
      Class.forName(name, false, classOf[LogicalTypeAnnotation].getClassLoader)).toOption

  private val geometryClass = loadClass(annotation + "$GeometryLogicalTypeAnnotation")
  private val geographyClass = loadClass(annotation + "$GeographyLogicalTypeAnnotation")
  private val algorithmClass =
    loadClass("org.apache.parquet.column.schema.EdgeInterpolationAlgorithm")

  /** Whether the Parquet library on the classpath can read and write the geospatial types. */
  val supported: Boolean =
    geometryClass.isDefined && geographyClass.isDefined && algorithmClass.isDefined

  /**
   * The value of the `crs` property that GeoParquet 2.0 assigns to an undefined or unknown CRS.
   */
  val UNKNOWN_CRS = "srid:0"

  /** The edge interpolation algorithm the Parquet format assumes when a column names none. */
  val DEFAULT_EDGES = "spherical"

  /** The geospatial annotation of `parquetType`, None for any other kind of field. */
  def describe(parquetType: Type): Option[ParquetGeospatialColumn] = {
    if (!supported || !parquetType.isPrimitive) return None
    val logicalType = parquetType.getLogicalTypeAnnotation
    if (logicalType == null) {
      None
    } else if (geometryClass.get.isInstance(logicalType)) {
      Some(ParquetGeospatialColumn(isGeography = false, crsOf(logicalType), None))
    } else if (geographyClass.get.isInstance(logicalType)) {
      val edges = Option(logicalType.getClass.getMethod("getAlgorithm").invoke(logicalType))
        .map(_.toString.toLowerCase(Locale.ROOT))
      Some(ParquetGeospatialColumn(isGeography = true, crsOf(logicalType), edges))
    } else {
      None
    }
  }

  private def crsOf(logicalType: LogicalTypeAnnotation): Option[String] =
    Option(logicalType.getClass.getMethod("getCrs").invoke(logicalType))
      .map(_.toString)
      .filter(_.trim.nonEmpty)

  /** A GEOMETRY annotation. A `crs` of None leaves the property out, which means OGC:CRS84. */
  def geometry(crs: Option[String]): LogicalTypeAnnotation = {
    requireSupported()
    classOf[LogicalTypeAnnotation]
      .getMethod("geometryType", classOf[String])
      .invoke(null, crs.orNull)
      .asInstanceOf[LogicalTypeAnnotation]
  }

  /**
   * A GEOGRAPHY annotation. A `crs` of None leaves the property out, which means OGC:CRS84.
   * `edges` is one of the algorithm names of the Parquet format, in any case.
   */
  def geography(crs: Option[String], edges: String): LogicalTypeAnnotation = {
    requireSupported()
    val algorithm = algorithmClass.get.getEnumConstants
      .find(_.toString.equalsIgnoreCase(edges))
      .getOrElse(throw new IllegalArgumentException(
        s"Unknown edge interpolation algorithm '$edges' for a Parquet GEOGRAPHY column"))
    classOf[LogicalTypeAnnotation]
      .getMethod("geographyType", classOf[String], algorithmClass.get)
      .invoke(null, crs.orNull, algorithm.asInstanceOf[AnyRef])
      .asInstanceOf[LogicalTypeAnnotation]
  }

  private def requireSupported(): Unit = {
    if (!supported) {
      throw new UnsupportedOperationException(
        "The Parquet library on the classpath has no GEOMETRY and GEOGRAPHY logical types; " +
          "they need parquet-java 1.16.0 or newer, which ships with Spark 4.1 and later")
    }
  }

  /** Whether the `crs` property states that the CRS is undefined or unknown. */
  def isUnknownCrs(crs: Option[String]): Boolean =
    crs.exists(_.trim.equalsIgnoreCase(UNKNOWN_CRS))

  private val sridForm = "(?i)srid:(\\d+)".r
  private val projjsonKeyForm = "(?i)projjson:(.+)".r
  private val authorityCodeForm = "([^:\\s]+):([^:\\s]+)".r

  /**
   * Resolves the `crs` property of a geospatial logical type to an SRID. The Parquet format
   * allows inline PROJJSON, `authority:code`, `srid:identifier` and `projjson:key`, where `key`
   * names a file metadata entry that holds PROJJSON.
   *
   * @return
   *   4326 when the property is absent, the EPSG code when the CRS names one, and 0 when the CRS
   *   is unknown or names no EPSG code
   */
  def crsToSrid(crs: Option[String], keyValueMetaData: java.util.Map[String, String]): Int = {
    crs.map(_.trim) match {
      case None => GeoParquetMetaData.DEFAULT_SRID
      case Some(projjson) if projjson.startsWith("{") => projjsonToSrid(projjson)
      case Some(sridForm(srid)) => Try(srid.toInt).getOrElse(0)
      case Some(projjsonKeyForm(key)) =>
        Option(keyValueMetaData)
          .flatMap(m => Option(m.get(key.trim)))
          .map(projjsonToSrid)
          .getOrElse(0)
      case Some(authorityCodeForm(authority, code)) =>
        GeoParquetMetaData.identifierSrid(authority, code)
      case _ => 0
    }
  }

  private def projjsonToSrid(projjson: String): Int = {
    val parsed: Option[JValue] =
      try Some(parse(projjson))
      catch { case NonFatal(_) => None }
    parsed.map(json => GeoParquetMetaData.extractSridFromCrs(Some(json))).getOrElse(0)
  }
}
