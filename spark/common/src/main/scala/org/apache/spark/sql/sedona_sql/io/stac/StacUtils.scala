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
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}

import scala.io.Source

object StacUtils {

  // Function to load JSON from URL or service
  def loadStacCollectionToJson(opts: Map[String, String]): String = {
    val urlFull: String = getFullCollectionUrl(opts)

    loadStacCollectionToJson(urlFull)
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

  // Function to load JSON from URL or service
  def loadStacCollectionToJson(url: String, maxRetries: Int = 3): String = {
    var retries = 0
    var success = false
    var result: String = ""

    while (retries < maxRetries && !success) {
      try {
        result = if (url.startsWith("s3://") || url.startsWith("s3a://")) {
          SparkSession.active.read.textFile(url).collect().mkString("\n")
        } else {
          Source.fromURL(url).mkString
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
    stacVersion match {
      case "1.0.0" => StacTable.SCHEMA_V1_0_0
      case version if version.matches("1\\.[1-9]\\d*\\.\\d*") => StacTable.SCHEMA_V1_1_0
      // Add more cases here for other versions if needed
      case _ => throw new IllegalArgumentException(s"Unsupported STAC version: $stacVersion")
    }
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
   * Builds the output row with the raster field in the assets map.
   *
   * @param row
   *   The input row.
   * @param schema
   *   The schema of the input row.
   * @return
   *   The output row with the raster field in the assets map.
   */
  def buildOutDbRasterFields(row: InternalRow, schema: StructType): InternalRow = {
    val newValues = new Array[Any](schema.fields.length)

    schema.fields.zipWithIndex.foreach {
      case (StructField("assets", MapType(StringType, valueType: StructType, _), _, _), index) =>
        val assetsMap = row.getMap(index)
        if (assetsMap != null) {
          val updatedAssets = assetsMap
            .keyArray()
            .array
            .zip(assetsMap.valueArray().array)
            .map { case (key, value) =>
              val assetRow = value.asInstanceOf[InternalRow]
              if (assetRow != null) {
                key -> assetRow
              } else {
                key -> null
              }
            }
            .toMap
          newValues(index) = ArrayBasedMapData(updatedAssets)
        } else {
          newValues(index) = null
        }
      case (_, index) =>
        newValues(index) = row.get(index, schema.fields(index).dataType)
    }

    InternalRow.fromSeq(newValues)
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
}
