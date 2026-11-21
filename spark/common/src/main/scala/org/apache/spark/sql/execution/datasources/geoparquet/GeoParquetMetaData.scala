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

import scala.util.control.NonFatal

import org.apache.spark.sql.sedona_sql.UDT.Box2DUDT
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{DoubleType, FloatType, StructType}
import org.datasyslab.proj4sedona.core.Proj
import org.datasyslab.proj4sedona.parser.CRSSerializer
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.compactJson
import org.json4s.{DefaultFormats, Extraction, JArray, JField, JInt, JNothing, JNull, JObject, JString, JValue}

/**
 * A case class that holds the metadata of geometry column in GeoParquet metadata
 * @param encoding
 *   Name of the geometry encoding format. Currently only "WKB" is supported
 * @param geometryTypes
 *   The geometry types of all geometries, or an empty array if they are not known.
 * @param bbox
 *   Bounding Box of the geometries in the file, formatted according to RFC 7946, section 5. None
 *   if the file contains no geometries (per the GeoParquet 1.1 spec, bbox is optional and should
 *   be omitted when there is no extent to describe).
 * @param crs
 *   The CRS of the geometries in the file. None if crs metadata is absent, Some(JNull) if crs is
 *   null, Some(value) if the crs is present and not null.
 * @param covering
 *   Object containing bounding box column names to help accelerate spatial data retrieval
 * @param edges
 *   How the edges of the geometries are interpreted. None stands for planar edges, which a
 *   geometry column has. A geography column has "spherical" edges.
 */
case class GeometryFieldMetaData(
    encoding: String,
    geometryTypes: Seq[String],
    bbox: Option[Seq[Double]],
    crs: Option[JValue] = None,
    covering: Option[Covering] = None,
    edges: Option[String] = None)

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
  // The version of the GeoParquet specification that is written where the Parquet library has no
  // geospatial logical types, please refer to https://geoparquet.org/releases/v1.1.0/ for more
  // details.
  val VERSION = "1.1.0"

  // The version that is written where the Parquet library has the geospatial logical types,
  // please refer to https://geoparquet.org/releases/v2.0.0/ for more details.
  val VERSION_2 = "2.0.0"

  /**
   * The GeoParquet version of a write that states none: [[VERSION_2]] where the Parquet library
   * can write the geospatial logical types, which Spark has from 4.1, and [[VERSION]] elsewhere.
   */
  def defaultVersion: String = if (ParquetGeospatialTypes.supported) VERSION_2 else VERSION

  /**
   * Session setting for the GeoParquet version of the writes that have no
   * [[GEOPARQUET_VERSION_KEY]] option.
   */
  val GEOPARQUET_VERSION_SESSION_KEY = "spark.sedona.geoparquet.version"

  /**
   * Configuration key for the GeoParquet version to write. A 2.x version stores geometries in the
   * Parquet GEOMETRY logical type. Any other value is written to the file metadata as given, with
   * geometries in plain binary columns.
   */
  val GEOPARQUET_VERSION_KEY = "geoparquet.version"

  /**
   * Whether a file of this GeoParquet version stores its geometries in the geospatial logical
   * types of the Parquet format, which is what GeoParquet 2 requires.
   */
  def usesGeospatialTypes(version: String): Boolean =
    version.trim.takeWhile(_ != '.') == "2"

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

  /**
   * Configuration key for controlling default covering behavior.
   *
   * Supported values:
   *   - `auto`: automatically generate/reuse `<geometryColumnName>_bbox` covering columns for
   *     GeoParquet 1.1.0 when explicit covering options are not provided.
   *   - `legacy`: disable automatic covering generation and keep legacy behavior.
   */
  val GEOPARQUET_COVERING_MODE_KEY = "geoparquet.covering.mode"
  val GEOPARQUET_COVERING_MODE_AUTO = "auto"
  val GEOPARQUET_COVERING_MODE_LEGACY = "legacy"

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
            case crs => name -> fieldMetadata.copy(crs = Some(crs))
          }
        }
      metadata.copy(columns = columns)
    }
  }

  def toJson(geoParquetMetadata: GeoParquetMetaData): String = {
    implicit val formats: org.json4s.Formats = DefaultFormats
    val geoObject = Extraction.decompose(geoParquetMetadata)

    // Make sure that the keys of columns are not transformed to camel case, so we use the columns map with
    // original keys to replace the transformed columns map. Preserve the raw CRS value separately because
    // PROJJSON keys are independent of the GeoParquet metadata case classes.
    val columnsMap =
      (geoObject \ "columns").extract[Map[String, JValue]].map { case (name, columnObject) =>
        val serializedColumn = columnObject.underscoreKeys
        val rawCrs = geoParquetMetadata.columns(name).crs
        name -> preserveRawCrs(serializedColumn, rawCrs)
      }

    // We are not using transformField here for binary compatibility with various json4s versions shipped with
    // Spark 3.0.x ~ Spark 3.5.x
    val serializedGeoObject = geoObject.underscoreKeys mapField { case field @ (jField: JField) =>
      if (jField._1 == "columns") {
        JField("columns", JObject(columnsMap.toList))
      } else {
        field
      }
    }
    compactJson(serializedGeoObject)
  }

  private def preserveRawCrs(columnObject: JValue, rawCrs: Option[JValue]): JValue = {
    (columnObject, rawCrs) match {
      case (JObject(fields), Some(crs)) =>
        JObject(fields.map { field =>
          if (field._1 == "crs") JField("crs", crs) else field
        })
      case _ => columnObject
    }
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
   *   - If the CRS field is a PROJJSON object with an "id" or "ids" containing "authority" and
   *     "code", the first recognized identifier is used as the SRID.
   *
   * @param crs
   *   The CRS field from GeoParquet column metadata.
   * @return
   *   The SRID corresponding to the CRS. Returns 4326 for omitted CRS, 0 for null or unrecognized
   *   CRS, and the EPSG code for PROJJSON with an EPSG identifier. An identifier of another
   *   authority, such as ESRI, gives the SRID that Spark's registry of coordinate reference
   *   systems assigns to it, which Spark has since 4.2.
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
        // GeoParquet uses a declared top-level PROJJSON identifier as the SRID.
        // Reading the identifier directly also supports CRS objects that omit the
        // optional definition fields that a projection engine would need.
        sridIdentifier(projjson).map(_._2).getOrElse(0)
    }
  }

  /**
   * Whether the CRS of a PROJJSON object is a geographic one, with longitudes and latitudes as
   * its coordinates. The `type` of the object says so. An unknown CRS, which is null, and an
   * object without a type that this method knows are taken for geographic ones: there is nothing
   * to tell that they are not.
   */
  private[geoparquet] def isGeographicCrs(projjson: JValue): Boolean = projjson \ "type" match {
    case JString("ProjectedCRS") | JString("DerivedProjectedCRS") | JString("EngineeringCRS") |
        JString("VerticalCRS") | JString("TemporalCRS") | JString("ParametricCRS") =>
      false
    case JString("GeodeticCRS") =>
      // Geocentric coordinates are distances from the centre of the earth
      projjson \ "coordinate_system" \ "subtype" != JString("Cartesian")
    case JString("BoundCRS") => isGeographicCrs(projjson \ "source_crs")
    case JString("CompoundCRS") =>
      projjson \ "components" match {
        case JArray(horizontal :: _) => isGeographicCrs(horizontal)
        case _ => true
      }
    case _ => true
  }

  /** What a message calls the CRS of a PROJJSON object: its name, or else its identifier. */
  private[geoparquet] def crsName(projjson: JValue): String =
    (projjson \ "name", projjson \ "id" \ "authority", projjson \ "id" \ "code") match {
      case (JString(name), _, _) => name
      case (_, JString(authority), JInt(code)) => s"$authority:$code"
      case (_, JString(authority), JString(code)) => s"$authority:$code"
      case _ => "a CRS without a name"
    }

  /**
   * The identifier of a PROJJSON object that gives its SRID, together with that SRID: `id` when
   * it names a known CRS, otherwise the first entry of `ids` that does. None when no identifier
   * names a known CRS.
   */
  private[geoparquet] def sridIdentifier(projjson: JValue): Option[(JValue, Int)] = {
    val identifiers = (projjson \ "id") +: (projjson \ "ids" match {
      case JArray(ids) => ids
      case _ => Nil
    })
    identifiers.iterator
      .map(identifier => identifier -> identifierSrid(identifier))
      .find(_._2 != 0)
  }

  private def identifierSrid(identifier: JValue): Int = {
    val authority = identifier \ "authority"
    val code = identifier \ "code"
    (authority, code) match {
      case (JString(value), _) if value.equalsIgnoreCase("EPSG") => positiveInt(code)
      case (JString(value), JString(codeValue))
          if value.equalsIgnoreCase("OGC") && codeValue.equalsIgnoreCase("CRS84") =>
        DEFAULT_SRID
      case (JString(value), _) => registeredSrid(value, code)
      case _ => 0
    }
  }

  /**
   * SRID of a CRS of an authority other than EPSG, such as ESRI:102100. Spark has a registry of
   * such identifiers since 4.2, which is also where Sedona uses Spark's native spatial types. The
   * SRID is 0 when Spark has no registry or the registry does not know the identifier.
   *
   * The code of such an identifier is never taken as the SRID on its own. Sedona reads an SRID as
   * an EPSG code, and another authority can use the number of an EPSG code for a different
   * system: ESRI:4812 is not EPSG:4812. Spark's registry gives every SRID one owner and leaves
   * such identifiers out.
   */
  private def registeredSrid(authority: String, code: JValue): Int = {
    val codeText = code match {
      case JInt(value) => Some(value.toString)
      case JString(value) if value.trim.nonEmpty => Some(value.trim)
      case _ => None
    }
    codeText
      .flatMap { text =>
        SpatialTypeSupport.registeredSrid(s"${authority.trim.toUpperCase(Locale.ROOT)}:$text")
      }
      .getOrElse(0)
  }

  /**
   * SRID of a CRS given as an `authority:code` pair, 0 when neither Sedona nor Spark's registry
   * of coordinate reference systems knows it.
   */
  private[geoparquet] def identifierSrid(authority: String, code: String): Int =
    identifierSrid(
      JObject(JField("authority", JString(authority)), JField("code", JString(code))))

  private def positiveInt(value: JValue): Int = {
    value match {
      case JInt(code) if code.isValidInt && code > 0 => code.toInt
      case JString(code) =>
        try {
          val parsed = code.trim.toInt
          if (parsed > 0) parsed else 0
        } catch {
          case _: NumberFormatException => 0
        }
      case _ => 0
    }
  }

  /**
   * Convert an SRID to a PROJJSON JValue using proj4sedona.
   *
   * The generated PROJJSON includes an `id` field with the EPSG authority and code, which enables
   * round-trip SRID preservation when reading the GeoParquet file back.
   *
   * @param srid
   *   The SRID to convert (e.g., 4326 for WGS 84).
   * @return
   *   Some(JValue) containing the PROJJSON if conversion succeeds, None if the SRID is 0
   *   (unknown), 4326 (GeoParquet default CRS), or if conversion fails.
   */
  def sridToProjJson(srid: Int): Option[JValue] = {
    if (srid == 0 || srid == DEFAULT_SRID) return None
    try {
      val proj = new Proj("EPSG:" + srid)
      val projjsonStr = CRSSerializer.toProjJson(proj)
      if (projjsonStr != null && projjsonStr.nonEmpty) {
        Some(parse(projjsonStr))
      } else {
        None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  def createCoveringColumnMetadata(coveringColumnName: String, schema: StructType): Covering = {
    val coveringColumnIndex = schema.fieldIndex(coveringColumnName)
    schema(coveringColumnIndex).dataType match {
      case coveringColumnType: StructType =>
        coveringColumnTypeToCovering(coveringColumnName, coveringColumnType)
      case udt: Box2DUDT =>
        // Box2DUDT exposes a struct<xmin, ymin, xmax, ymax: double> sqlType, which is the exact
        // shape required by GeoParquet 1.1 bbox covering columns. Treat the underlying struct as
        // the covering struct so users can write a Box2D column and have it referenced as a
        // covering column in GeoParquet metadata without any manual struct construction.
        udt.sqlType match {
          case structType: StructType =>
            coveringColumnTypeToCovering(coveringColumnName, structType)
          case other =>
            throw new IllegalStateException(
              s"Box2DUDT.sqlType is expected to be a StructType, got $other")
        }
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
