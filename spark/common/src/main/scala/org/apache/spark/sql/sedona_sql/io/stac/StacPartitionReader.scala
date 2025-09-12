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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter
import org.apache.spark.sql.sedona_sql.io.geojson.{GeoJSONUtils, SparkCompatUtil}
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.promotePropertiesToTop
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.io.{File, PrintWriter}
import java.lang.reflect.Constructor
import scala.io.Source

class StacPartitionReader(
    broadcast: Broadcast[SerializableConfiguration],
    partition: StacPartition,
    schema: StructType,
    opts: Map[String, String],
    spatialFilter: Option[GeoParquetSpatialFilter],
    temporalFilter: Option[TemporalFilter])
    extends PartitionReader[InternalRow] {

  private val itemsIterator = partition.items.iterator
  private var currentItem: String = _
  private var currentFile: File = _
  private var featureIterator: Iterator[InternalRow] = Iterator.empty
  private val mapper = new ObjectMapper()

  override def next(): Boolean = {
    if (featureIterator.hasNext) {
      true
    } else if (itemsIterator.hasNext) {
      currentItem = itemsIterator.next()
      if (currentItem.startsWith("http://") || currentItem.startsWith("https://") || currentItem
          .startsWith("file://")) {
        val url = new java.net.URL(currentItem)

        // Download the file to a local temp file
        val tempFile = File.createTempFile("stac_item_", ".json")
        val writer = new PrintWriter(tempFile)
        try {
          val fileContent = fetchContentWithRetry(url)
          val rootNode = mapper.readTree(fileContent)
          val nodeType = rootNode.get("type").asText()

          nodeType match {
            case "Feature" =>
              // Write the content as a single line JSON
              val content = mapper.writeValueAsString(rootNode)
              writer.write(content)
            case "FeatureCollection" =>
              // Write each feature in the features array to a multi-line JSON file
              val features = rootNode.get("features")
              val featureIterator = features.elements()
              while (featureIterator.hasNext) {
                val feature = featureIterator.next()
                val content = mapper.writeValueAsString(feature)
                writer.write(content)
                writer.write("\n")
              }
            case _ =>
              throw new IllegalArgumentException(s"Unsupported type for item: $nodeType")
          }

        } finally {
          writer.close()
        }
        checkAndDeleteTempFile(currentFile)
        currentFile = tempFile
      } else {
        throw new IllegalArgumentException(s"Unsupported protocol for item: $currentItem")
      }

      // Parse the current file and extract features
      featureIterator = if (currentFile.exists()) {

        val parsedOptions = new JSONOptionsInRead(
          opts,
          opts.getOrElse("sessionLocalTimeZone", "UTC"),
          opts.getOrElse("columnNameOfCorruptRecord", "_corrupt_record"))
        val dataSource = JsonDataSource(parsedOptions)

        val alteredSchema = GeoJSONUtils.updateGeometrySchema(schema, StringType)

        val parser = SparkCompatUtil.constructJacksonParser(
          alteredSchema,
          parsedOptions,
          allowArrayAsStructs = true)

        val rows = SparkCompatUtil
          .readFile(
            dataSource,
            new Configuration(),
            createPartitionedFile(currentFile),
            parser,
            schema)

        rows.map(row => {
          val geometryConvertedRow = GeoJSONUtils.convertGeoJsonToGeometry(row, alteredSchema)
          val propertiesPromotedRow = promotePropertiesToTop(geometryConvertedRow, alteredSchema)
          propertiesPromotedRow
        })
      } else {
        Iterator.empty
      }

      next()
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    featureIterator.next()
  }

  override def close(): Unit = {
    checkAndDeleteTempFile(currentFile)
  }

  private def checkAndDeleteTempFile(file: File): Unit = {
    // Delete the local file if it was downloaded to tmp
    if (file != null && file.exists() && file.getAbsolutePath.startsWith(
        System.getProperty("java.io.tmpdir"))) {
      file.delete()
    }
  }

  def fetchContentWithRetry(url: java.net.URL, maxRetries: Int = 3): String = {
    var attempt = 0
    var success = false
    var fileContent: String = ""

    while (attempt < maxRetries && !success) {
      try {
        fileContent = Source.fromURL(url).mkString
        success = true
      } catch {
        case e: Exception =>
          attempt += 1
          if (attempt >= maxRetries) {
            throw new RuntimeException(
              s"Failed to fetch content from URL after $maxRetries attempts",
              e)
          }
      }
    }

    fileContent
  }

  /**
   * Create a PartitionedFile instance using reflection. The constructor parameters differ between
   * these versions, so we need to handle both cases. For Spark 3.4 and below, the constructor has
   * 7 parameters, while for Spark 3.5 and above, it has 8 parameters. Additionally, the type of
   * the second parameter may be `SparkPath` in some cases, which requires special handling.
   *
   * @param currentFile
   *   The file to create the PartitionedFile for.
   * @return
   *   The created PartitionedFile instance.
   * @throws NoSuchMethodException
   *   If no suitable constructor is found.
   */
  def createPartitionedFile(currentFile: File): PartitionedFile = {
    val partitionedFileClass =
      Class.forName("org.apache.spark.sql.execution.datasources.PartitionedFile")
    val constructors = partitionedFileClass.getConstructors
    val constructor = constructors
      .find(_.getParameterCount == 7)
      .getOrElse(
        constructors
          .find(_.getParameterCount == 8)
          .getOrElse(
            throw new NoSuchMethodException("No constructor with 7 or 8 parameters found")))

    val params = if (constructor.getParameterCount == 7) {
      val secondParamType = constructor.getParameterTypes()(1)
      if (secondParamType.getName == "org.apache.spark.paths.SparkPath") {
        Array(
          null,
          createSparkPath(currentFile.getPath),
          java.lang.Long.valueOf(0L),
          java.lang.Long.valueOf(currentFile.length()),
          Array.empty[String],
          java.lang.Long.valueOf(0L),
          java.lang.Long.valueOf(0L))
      } else {
        Array(
          null,
          currentFile.getPath,
          java.lang.Long.valueOf(0L),
          java.lang.Long.valueOf(currentFile.length()),
          Array.empty[String],
          java.lang.Long.valueOf(0L),
          java.lang.Long.valueOf(0L))
      }
    } else {
      Array(
        null,
        createSparkPath(currentFile.getPath),
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(currentFile.length()),
        Array.empty[String],
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(0L),
        null)
    }

    constructor.newInstance(params: _*).asInstanceOf[PartitionedFile]
  }

  /**
   * Create a SparkPath instance using reflection. This is needed to support both Spark 3.3 and
   * below and Spark 3.4 and above.
   *
   * @param pathString
   *   The path to create the SparkPath for.
   * @return
   *   The created SparkPath instance.
   */
  def createSparkPath(pathString: String): Object = {
    val sparkPathClass = Class.forName("org.apache.spark.paths.SparkPath")
    val constructor: Constructor[_] = sparkPathClass.getDeclaredConstructor(classOf[String])
    constructor.setAccessible(true) // Make the private constructor accessible
    constructor.newInstance(pathString).asInstanceOf[Object]
  }
}
