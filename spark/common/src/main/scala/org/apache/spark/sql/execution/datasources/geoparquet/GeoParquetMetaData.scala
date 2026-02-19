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

import scala.util.control.NonFatal

import org.apache.spark.sql.types.{DoubleType, FloatType, StructType}
import org.datasyslab.proj4sedona.core.Proj
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.compactJson
import org.json4s.{DefaultFormats, Extraction, JField, JNothing, JNull, JObject, JValue}

/**
 * A case class that holds the metadata of geometry column in GeoParquet metadata
 * @param encoding
 *   Name of the geometry encoding format. Currently only "WKB" is supported
 * @param geometryTypes
 *   The geometry types of all geometries, or an empty array if they are not known.
 * @param bbox
 *   Bounding Box of the geometries in the file, formatted according to RFC 7946, section 5.
 * @param crs
 *   The CRS of the geometries in the file. None if crs metadata is absent, Some(JNull) if crs is
 *   null, Some(value) if the crs is present and not null.
 * @param covering
 *   Object containing bounding box column names to help accelerate spatial data retrieval
 */
case class GeometryFieldMetaData(
    encoding: String,
    geometryTypes: Seq[String],
    bbox: Seq[Double],
    crs: Option[JValue] = None,
    covering: Option[Covering] = None)

case class Covering(bbox: CoveringBBox)

case class CoveringBBox(
    xmin: Seq[String],
    ymin: Seq[String],
    zmin: Option[Seq[String]],
    xmax: Seq[String],
    ymax: Seq[String],
    zmax: Option[Seq[String]])

/**
 * A case class that holds the metadata of GeoParquet file
 * @param version
 *   The version identifier for the GeoParquet specification.
 * @param primaryColumn
 *   The name of the "primary" geometry column.
 * @param columns
 *   Metadata about geometry columns.
 */
case class GeoParquetMetaData(
    version: Option[String], // defined as optional for compatibility with old GeoParquet specs
    primaryColumn: String,
    columns: Map[String, GeometryFieldMetaData])

object GeoParquetMetaData {
  // We're conforming to version 1.1.0 of the GeoParquet specification, please refer to
  // https://geoparquet.org/releases/v1.1.0/ for more details.
  val VERSION = "1.1.0"

  /**
   * Configuration key for overriding the version field in GeoParquet file metadata.
   */
  val GEOPARQUET_VERSION_KEY = "geoparquet.version"

  /**
   * Configuration key for setting the CRS of the geometries in GeoParquet column metadata. This
   * is applied to all geometry columns in the file.
   */
  val GEOPARQUET_CRS_KEY = "geoparquet.crs"

  /**
   * Configuration key prefix for setting the covering columns of the geometries in GeoParquet
   * column metadata. The configuration key for geometry column named `x` is
   * `geoparquet.covering.x`. If the parquet file contains only one geometry column, we can omit
   * the column name and use `geoparquet.covering` directly.
   */
  val GEOPARQUET_COVERING_KEY = "geoparquet.covering"

  def parseKeyValueMetaData(
      keyValueMetaData: java.util.Map[String, String]): Option[GeoParquetMetaData] = {
    Option(keyValueMetaData.get("geo")).map { geo =>
      implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
      val geoObject = parse(geo)
      val metadata = geoObject.camelizeKeys.extract[GeoParquetMetaData]
      val columns =
        (geoObject \ "columns").extract[Map[String, JValue]].map { case (name, columnObject) =>
          val fieldMetadata = columnObject.camelizeKeys.extract[GeometryFieldMetaData]
          // Postprocess to distinguish between null (JNull) and missing field (JNothing).
          columnObject \ "crs" match {
            case JNothing => name -> fieldMetadata.copy(crs = None)
            case JNull => name -> fieldMetadata.copy(crs = Some(JNull))
            case _ => name -> fieldMetadata
          }
        }
      metadata.copy(columns = columns)
    }
  }

  def toJson(geoParquetMetadata: GeoParquetMetaData): String = {
    implicit val formats: org.json4s.Formats = DefaultFormats
    val geoObject = Extraction.decompose(geoParquetMetadata)

    // Make sure that the keys of columns are not transformed to camel case, so we use the columns map with
    // original keys to replace the transformed columns map.
    val columnsMap =
      (geoObject \ "columns").extract[Map[String, JValue]].map { case (name, columnObject) =>
        name -> columnObject.underscoreKeys
      }

    // We are not using transformField here for binary compatibility with various json4s versions shipped with
    // Spark 3.0.x ~ Spark 3.5.x
    val serializedGeoObject = geoObject.underscoreKeys mapField {
      case field @ (jField: JField) =>
        if (jField._1 == "columns") {
          JField("columns", JObject(columnsMap.toList))
        } else {
          field
        }
      case field: Any => field
    }
    compactJson(serializedGeoObject)
  }

  /**
   * Default SRID for GeoParquet files where the CRS field is omitted. Per the GeoParquet spec,
   * omitting the CRS implies OGC:CRS84, which is equivalent to EPSG:4326.
   */
  val DEFAULT_SRID: Int = 4326

  /**
   * Extract SRID from a GeoParquet CRS metadata value.
   *
   * Per the GeoParquet specification:
   *   - If the CRS field is absent (None), the CRS is OGC:CRS84 (EPSG:4326).
   *   - If the CRS field is explicitly null, the CRS is unknown (SRID 0).
   *   - If the CRS field is a PROJJSON object with an "id" containing "authority" and "code", the
   *     EPSG code is used as the SRID.
   *
   * @param crs
   *   The CRS field from GeoParquet column metadata.
   * @return
   *   The SRID corresponding to the CRS. Returns 4326 for omitted CRS, 0 for null or unrecognized
   *   CRS, and the EPSG code for PROJJSON with an EPSG identifier.
   */
  def extractSridFromCrs(crs: Option[JValue]): Int = {
    crs match {
      case None =>
        // CRS omitted: default to OGC:CRS84 (EPSG:4326) per GeoParquet spec
        DEFAULT_SRID
      case Some(JNull) =>
        // CRS explicitly null: unknown CRS
        0
      case Some(projjson) =>
        // Use proj4sedona to extract authority and code from PROJJSON
        try {
          val jsonStr = compactJson(projjson)
          val result = new Proj(jsonStr).toAuthority()
          if (result != null && result.length == 2) {
            result(0) match {
              case "EPSG" =>
                try { result(1).toInt }
                catch { case _: NumberFormatException => 0 }
              case "OGC" if result(1) == "CRS84" => DEFAULT_SRID
              case _ => 0
            }
          } else {
            0
          }
        } catch {
          case NonFatal(_) => 0
        }
    }
  }

  def createCoveringColumnMetadata(coveringColumnName: String, schema: StructType): Covering = {
    val coveringColumnIndex = schema.fieldIndex(coveringColumnName)
    schema(coveringColumnIndex).dataType match {
      case coveringColumnType: StructType =>
        coveringColumnTypeToCovering(coveringColumnName, coveringColumnType)
      case _ =>
        throw new IllegalArgumentException(
          s"Covering column $coveringColumnName is not a struct type")
    }
  }

  private def coveringColumnTypeToCovering(
      coveringColumnName: String,
      coveringColumnType: StructType): Covering = {
    def validateField(fieldName: String): Unit = {
      val index = coveringColumnType.fieldIndex(fieldName)
      val fieldType = coveringColumnType(index).dataType
      if (fieldType != FloatType && fieldType != DoubleType) {
        throw new IllegalArgumentException(
          s"`$fieldName` in covering column `$coveringColumnName` is not float or double type")
      }
    }
    // We only validate the existence and types of the fields here. Although the order of the fields is required to be
    // xmin, ymin, [zmin], xmax, ymax, [zmax] (see https://github.com/opengeospatial/geoparquet/pull/202), but we don't
    // validate it. The requirement on user provided covering column is quite lenient, it is up to the user to provide
    // the covering column strictly follow the ordering requirement.
    validateField("xmin")
    validateField("ymin")
    validateField("xmax")
    validateField("ymax")
    coveringColumnType.find(_.name == "zmin") match {
      case Some(_) =>
        validateField("zmin")
        validateField("zmax")
        Covering(
          CoveringBBox(
            Seq(coveringColumnName, "xmin"),
            Seq(coveringColumnName, "ymin"),
            Some(Seq(coveringColumnName, "zmin")),
            Seq(coveringColumnName, "xmax"),
            Seq(coveringColumnName, "ymax"),
            Some(Seq(coveringColumnName, "zmax"))))
      case None =>
        if (coveringColumnType.fieldNames.contains("zmax")) {
          throw new IllegalArgumentException(
            s"zmax should not present in covering column `$coveringColumnName` since zmin is not present")
        }
        Covering(
          CoveringBBox(
            Seq(coveringColumnName, "xmin"),
            Seq(coveringColumnName, "ymin"),
            None,
            Seq(coveringColumnName, "xmax"),
            Seq(coveringColumnName, "ymax"),
            None))
    }
  }
}
