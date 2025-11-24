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
package org.apache.spark.sql.sedona_sql.io.stac

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.locationtech.jts.geom.Envelope

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source

object StacUtils {

  // Function to load JSON from URL or service
  def loadStacCollectionToJson(opts: Map[String, String]): String = {
    val urlFull: String = getFullCollectionUrl(opts)
    val headers: Map[String, String] = parseHeaders(opts)

    loadStacCollectionToJson(urlFull, headers)
  }

  /**
   * Parse headers from the options map.
   *
   * Headers can be provided as a JSON string in the "headers" option.
   *
   * @param opts
   *   The options map that may contain a "headers" key with JSON-encoded headers
   * @return
   *   Map of header names to values
   */
  def parseHeaders(opts: Map[String, String]): Map[String, String] = {
    opts.get("headers") match {
      case Some(headersJson) =>
        try {
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val headersMap = mapper.readValue(headersJson, classOf[Map[String, String]])
          headersMap
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Failed to parse headers JSON: ${e.getMessage}",
              e)
        }
      case None => Map.empty[String, String]
    }
  }

  def getFullCollectionUrl(opts: Map[String, String]) = {
    val url = opts.getOrElse(
      "path",
      opts.getOrElse(
        "service",
        throw new IllegalArgumentException("Either 'path' or 'service' must be provided")))
    val urlFinal = if (url.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")) url else s"file://$url"
    urlFinal
  }

  // Function to load JSON from URL or service with optional headers
  def loadStacCollectionToJson(
      url: String,
      headers: Map[String, String] = Map.empty,
      maxRetries: Int = 3): String = {
    var retries = 0
    var success = false
    var result: String = ""

    while (retries < maxRetries && !success) {
      try {
        result = if (url.startsWith("s3://") || url.startsWith("s3a://")) {
          // S3 URLs are handled by Spark
          SparkSession.active.read.textFile(url).collect().mkString("\n")
        } else if (headers.isEmpty) {
          // No headers - use the simple Source.fromURL approach for backward compatibility
          Source.fromURL(url).mkString
        } else {
          // Headers provided - use URLConnection to set custom headers
          val connection = new java.net.URL(url).openConnection()

          // Set all custom headers
          headers.foreach { case (key, value) =>
            connection.setRequestProperty(key, value)
          }

          // Read the response
          val source = Source.fromInputStream(connection.getInputStream)
          try {
            source.mkString
          } finally {
            source.close()
          }
        }
        success = true
      } catch {
        case e: Exception =>
          retries += 1
          if (retries >= maxRetries) {
            throw new RuntimeException(
              s"Failed to load STAC collection from $url after $maxRetries attempts",
              e)
          }
      }
    }

    result
  }

  // Overloaded version for backward compatibility
  def loadStacCollectionToJson(url: String, maxRetries: Int): String = {
    loadStacCollectionToJson(url, Map.empty[String, String], maxRetries)
  }

  // Function to get the base URL from the collection URL or service
  def getStacCollectionBasePath(opts: Map[String, String]): String = {
    val ref = opts.getOrElse(
      "path",
      opts.getOrElse(
        "service",
        throw new IllegalArgumentException("Either 'path' or 'service' must be provided")))
    getStacCollectionBasePath(ref)
  }

  // Function to get the base URL from the collection URL or service
  def getStacCollectionBasePath(collectionUrl: String): String = {
    val urlPattern = "(https?://[^/]+/|http://[^/]+/).*".r
    val filePattern = "(file:///.*/|/.*/).*".r

    collectionUrl match {
      case urlPattern(baseUrl) => baseUrl
      case filePattern(basePath) =>
        if (basePath.startsWith("file://")) basePath else s"file://$basePath"
      case _ => throw new IllegalArgumentException(s"Invalid URL or file path: $collectionUrl")
    }
  }

  /**
   * Infer the schema of the STAC data source table.
   *
   * This method checks if a cached schema exists for the given data source options. If not, it
   * processes the STAC collection and saves it as a GeoJson file. The schema is then inferred
   * from this GeoJson file.
   *
   * @param opts
   *   Mapping of data source options, which should include either 'url' or 'service'.
   * @return
   *   The inferred schema of the STAC data source table.
   * @throws IllegalArgumentException
   *   If neither 'url' nor 'service' are provided.
   */
  def inferStacSchema(opts: Map[String, String]): StructType = {
    val stacCollectionJsonString = loadStacCollectionToJson(opts)

    // Create the ObjectMapper
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Parse the STAC collection JSON
    val collection = mapper.readTree(stacCollectionJsonString)

    // Extract the stac_version
    val stacVersion = collection.get("stac_version").asText()

    // Return the corresponding schema based on the stac_version
    val coreSchema = stacVersion match {
      case "1.0.0" => StacTable.SCHEMA_V1_0_0
      case version if version.matches("1\\.[1-9]\\d*\\.\\d*") => StacTable.SCHEMA_V1_1_0
      // Add more cases here for other versions if needed
      case _ => throw new IllegalArgumentException(s"Unsupported STAC version: $stacVersion")
    }
    val extensions = StacExtension.getStacExtensionDefinitions()
    val schemaWithExtensions = addExtensionFieldsToSchema(coreSchema, extensions)
    schemaWithExtensions
  }

  /**
   * Adds STAC extension fields to the properties field in the schema
   *
   * @param schema
   *   The base STAC schema to enhance
   * @param extensions
   *   Array of STAC extension definitions
   * @return
   *   Enhanced schema with extension fields added to the properties struct
   */
  def addExtensionFieldsToSchema(
      schema: StructType,
      extensions: Array[StacExtension]): StructType = {
    // Find the properties field in the schema
    val propertiesFieldOpt = schema.fields.find(_.name == "properties")

    if (propertiesFieldOpt.isEmpty) {
      // If there's no properties field, return the original schema
      return schema
    }

    // Get the properties field and its struct type
    val propertiesField = propertiesFieldOpt.get
    val propertiesStruct = propertiesField.dataType.asInstanceOf[StructType]

    // Create extension fields with metadata indicating their source
    val extensionFields = extensions.flatMap { extension =>
      extension.schema.fields.map { field =>
        StructField(
          field.name,
          field.dataType,
          field.nullable,
          new MetadataBuilder()
            .withMetadata(field.metadata)
            .putString("stac_extension", extension.name)
            .build())
      }
    }

    // Create a new properties struct that includes the extension fields
    val updatedPropertiesStruct = StructType(propertiesStruct.fields ++ extensionFields)

    // Create a new properties field with the updated struct
    val updatedPropertiesField = StructField(
      propertiesField.name,
      updatedPropertiesStruct,
      propertiesField.nullable,
      propertiesField.metadata)

    // Replace the properties field in the schema
    val updatedFields = schema.fields.map {
      case field if field.name == "properties" => updatedPropertiesField
      case field => field
    }

    // Return the schema with the updated properties field
    StructType(updatedFields)
  }

  /**
   * Promote the properties field to the top level of the row.
   */
  def promotePropertiesToTop(row: InternalRow, schema: StructType): InternalRow = {
    val propertiesIndex = schema.fieldIndex("properties")
    val propertiesStruct = schema("properties").dataType.asInstanceOf[StructType]
    val propertiesRow = row.getStruct(propertiesIndex, propertiesStruct.fields.length)

    val newValues = schema.fields.zipWithIndex.foldLeft(Seq.empty[Any]) {
      case (acc, (field, index)) if field.name == "properties" =>
        acc ++ propertiesStruct.fields.zipWithIndex.map { case (propField, propIndex) =>
          propertiesRow.get(propIndex, propField.dataType)
        }
      case (acc, (_, index)) =>
        acc :+ row.get(index, schema(index).dataType)
    }

    InternalRow.fromSeq(newValues)
  }

  def updatePropertiesPromotedSchema(schema: StructType): StructType = {
    val propertiesIndex = schema.fieldIndex("properties")
    val propertiesStruct = schema("properties").dataType.asInstanceOf[StructType]

    val newFields = schema.fields.foldLeft(Seq.empty[StructField]) {
      case (acc, StructField("properties", _, _, _)) =>
        acc ++ propertiesStruct.fields
      case (acc, other) =>
        acc :+ other
    }

    StructType(newFields)
  }

  /**
   * Returns the number of partitions to use for reading the data.
   *
   * The number of partitions is determined based on the number of items, the number of partitions
   * requested, the maximum number of item files per partition, and the default parallelism.
   *
   * @param itemCount
   *   The number of items in the collection.
   * @param numPartitions
   *   The number of partitions requested.
   * @param maxPartitionItemFiles
   *   The maximum number of item files per partition.
   * @param defaultParallelism
   *   The default parallelism.
   * @return
   *   The number of partitions to use for reading the data.
   */
  def getNumPartitions(
      itemCount: Int,
      numPartitions: Int,
      maxPartitionItemFiles: Int,
      defaultParallelism: Int): Int = {
    if (numPartitions > 0) {
      numPartitions
    } else {
      val maxSplitFiles = if (maxPartitionItemFiles > 0) {
        Math.min(maxPartitionItemFiles, Math.ceil(itemCount.toDouble / defaultParallelism).toInt)
      } else {
        Math.ceil(itemCount.toDouble / defaultParallelism).toInt
      }
      Math.max(1, Math.ceil(itemCount.toDouble / maxSplitFiles).toInt)
    }
  }

  /** Returns the temporal filter string based on the temporal filter. */
  def getFilterBBox(filter: GeoParquetSpatialFilter): String = {
    def calculateUnionBBox(filter: GeoParquetSpatialFilter): Envelope = {
      filter match {
        case GeoParquetSpatialFilter.AndFilter(left, right) =>
          val leftEnvelope = calculateUnionBBox(left)
          val rightEnvelope = calculateUnionBBox(right)
          leftEnvelope.expandToInclude(rightEnvelope)
          leftEnvelope
        case GeoParquetSpatialFilter.OrFilter(left, right) =>
          val leftEnvelope = calculateUnionBBox(left)
          val rightEnvelope = calculateUnionBBox(right)
          leftEnvelope.expandToInclude(rightEnvelope)
          leftEnvelope
        case leaf: GeoParquetSpatialFilter.LeafFilter =>
          leaf.queryWindow.getEnvelopeInternal
      }
    }

    val unionEnvelope = calculateUnionBBox(filter)
    s"bbox=${unionEnvelope.getMinX}%2C${unionEnvelope.getMinY}%2C${unionEnvelope.getMaxX}%2C${unionEnvelope.getMaxY}"
  }

  /** Returns the temporal filter string based on the temporal filter. */
  def getFilterTemporal(filter: TemporalFilter): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    def formatDateTime(dateTime: LocalDateTime): String = {
      if (dateTime == null) ".." else dateTime.format(formatter)
    }

    def calculateUnionTemporal(filter: TemporalFilter): (LocalDateTime, LocalDateTime) = {
      filter match {
        case TemporalFilter.AndFilter(left, right) =>
          val (leftStart, leftEnd) = calculateUnionTemporal(left)
          val (rightStart, rightEnd) = calculateUnionTemporal(right)
          val start =
            if (leftStart == null || (rightStart != null && rightStart.isBefore(leftStart)))
              rightStart
            else leftStart
          val end =
            if (leftEnd == null || (rightEnd != null && rightEnd.isAfter(leftEnd))) rightEnd
            else leftEnd
          (start, end)
        case TemporalFilter.OrFilter(left, right) =>
          val (leftStart, leftEnd) = calculateUnionTemporal(left)
          val (rightStart, rightEnd) = calculateUnionTemporal(right)
          val start =
            if (leftStart == null || (rightStart != null && rightStart.isBefore(leftStart)))
              rightStart
            else leftStart
          val end =
            if (leftEnd == null || (rightEnd != null && rightEnd.isAfter(leftEnd))) rightEnd
            else leftEnd
          (start, end)
        case TemporalFilter.LessThanFilter(_, value) =>
          (null, value)
        case TemporalFilter.GreaterThanFilter(_, value) =>
          (value, null)
        case TemporalFilter.EqualFilter(_, value) =>
          (value, value)
      }
    }

    val (start, end) = calculateUnionTemporal(filter)
    if (end == null) s"datetime=${formatDateTime(start)}/.."
    else s"datetime=${formatDateTime(start)}/${formatDateTime(end)}"
  }

  /** Adds the spatial and temporal filters to the base URL. */
  def addFiltersToUrl(
      baseUrl: String,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): String = {
    val spatialFilterStr = spatialFilter.map(StacUtils.getFilterBBox).getOrElse("")
    val temporalFilterStr = temporalFilter.map(StacUtils.getFilterTemporal).getOrElse("")

    val filters = Seq(spatialFilterStr, temporalFilterStr).filter(_.nonEmpty).mkString("&")
    val urlWithFilters = if (filters.nonEmpty) s"&$filters" else ""
    s"$baseUrl$urlWithFilters"
  }
}
