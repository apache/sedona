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
package org.apache.spark.sql.execution.datasources.parquet

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
 */
case class GeometryFieldMetaData(
    encoding: String,
    geometryTypes: Seq[String],
    bbox: Seq[Double],
    crs: Option[JValue] = None)

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
  // We're conforming to version 1.0.0 of the GeoParquet specification, please refer to
  // https://geoparquet.org/releases/v1.0.0/ for more details.
  val VERSION = "1.0.0"

  /**
   * Configuration key for overriding the version field in GeoParquet file metadata.
   */
  val GEOPARQUET_VERSION_KEY = "geoparquet.version"

  /**
   * Configuration key for setting the CRS of the geometries in GeoParquet column metadata. This
   * is applied to all geometry columns in the file.
   */
  val GEOPARQUET_CRS_KEY = "geoparquet.crs"

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
}
