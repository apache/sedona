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
import org.apache.spark.paths.SparkPath
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
  private val headers = StacUtils.parseHeaders(opts)

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
        if (headers.isEmpty) {
          fileContent = Source.fromURL(url).mkString
        } else {
          val connection = url.openConnection()
          var inputStream: java.io.InputStream = null
          var source: Source = null

          try {
            headers.foreach { case (key, value) =>
              connection.setRequestProperty(key, value)
            }
            inputStream = connection.getInputStream
            source = Source.fromInputStream(inputStream)
            fileContent = source.mkString
          } finally {
            // Close resources in reverse order
            if (source != null) {
              try source.close()
              catch { case _: Throwable => }
            }
            if (inputStream != null) {
              try inputStream.close()
              catch { case _: Throwable => }
            }
            // Disconnect HTTP connection if applicable
            connection match {
              case httpConn: java.net.HttpURLConnection =>
                try httpConn.disconnect()
                catch { case _: Throwable => }
              case _ =>
            }
          }
        }
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
   * Create a PartitionedFile instance for a locally staged STAC item file.
   *
   * @param currentFile
   *   The file to create the PartitionedFile for.
   * @return
   *   The created PartitionedFile instance.
   */
  def createPartitionedFile(currentFile: File): PartitionedFile =
    PartitionedFile(
      partitionValues = null,
      filePath = SparkPath.fromPathString(currentFile.getPath),
      start = 0L,
      length = currentFile.length())
}
