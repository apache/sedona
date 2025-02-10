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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.parquet.{GeoParquetSpatialFilter, GeometryFieldMetaData}
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.getNumPartitions
import org.apache.spark.sql.types.StructType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import scala.jdk.CollectionConverters._

/**
 * The `StacBatch` class represents a batch of partitions for reading data in the SpatioTemporal
 * Asset Catalog (STAC) data source. It implements the `Batch` interface from Apache Spark's data
 * source API.
 *
 * This class provides methods to plan input partitions and create a partition reader factory,
 * which are necessary for batch data processing.
 */
case class StacBatch(
    stacCollectionUrl: String,
    stacCollectionJson: String,
    schema: StructType,
    opts: Map[String, String],
    spatialFilter: Option[GeoParquetSpatialFilter],
    temporalFilter: Option[TemporalFilter])
    extends Batch {

  val mapper = new ObjectMapper()

  /**
   * Plans the input partitions for reading data from the STAC data source.
   *
   * @return
   *   An array of input partitions for reading STAC data.
   */
  override def planInputPartitions(): Array[InputPartition] = {
    val stacCollectionBasePath = StacUtils.getStacCollectionBasePath(stacCollectionUrl)

    // Initialize the itemLinks array
    val itemLinks = scala.collection.mutable.ArrayBuffer[String]()

    // Start the recursive collection of item links
    collectItemLinks(stacCollectionBasePath, stacCollectionJson, itemLinks)

    // Handle when the number of items is less than 1
    if (itemLinks.isEmpty) {
      return Array.empty[InputPartition]
    }

    val numPartitions = getNumPartitions(
      itemLinks.length,
      opts.getOrElse("numPartitions", "-1").toInt,
      opts.getOrElse("maxPartitionItemFiles", "-1").toInt,
      opts.getOrElse("defaultParallelism", "1").toInt)

    // Handle when the number of items is less than the number of partitions
    if (itemLinks.length < numPartitions) {
      return itemLinks.zipWithIndex.map { case (item, index) =>
        StacPartition(index, Array(item), new java.util.HashMap[String, String]())
      }.toArray
    }

    // Determine how many items to put in each partition
    val partitionSize = Math.ceil(itemLinks.length.toDouble / numPartitions).toInt

    // Group the item links into partitions
    itemLinks
      .grouped(partitionSize)
      .zipWithIndex
      .map { case (items, index) =>
        // Create a StacPartition for each group of items
        StacPartition(index, items.toArray, new java.util.HashMap[String, String]())
      }
      .toArray
  }

  /**
   * Recursively processes collections and collects item links.
   *
   * @param collectionBasePath
   *   The base path of the STAC collection.
   * @param collectionJson
   *   The JSON string representation of the STAC collection.
   * @param itemLinks
   *   The list of item links to populate.
   */
  private def collectItemLinks(
      collectionBasePath: String,
      collectionJson: String,
      itemLinks: scala.collection.mutable.ArrayBuffer[String]): Unit = {
    // Parse the JSON string into a JsonNode (tree representation of JSON)
    val rootNode: JsonNode = mapper.readTree(collectionJson)

    // Extract item links from the "links" array
    val linksNode = rootNode.get("links")
    val iterator = linksNode.elements()
    while (iterator.hasNext) {
      val linkNode = iterator.next()
      val rel = linkNode.get("rel").asText()
      val href = linkNode.get("href").asText()

      // item links are identified by the "rel" value of "item" or "items"
      if (rel == "item" || rel == "items") {
        // need to handle relative paths and local file paths
        val itemUrl = if (href.startsWith("http") || href.startsWith("file")) {
          href
        } else {
          collectionBasePath + href
        }
        itemLinks += itemUrl // Add the item URL to the list
      } else if (rel == "child") {
        val childUrl = if (href.startsWith("http") || href.startsWith("file")) {
          href
        } else {
          collectionBasePath + href
        }
        // Recursively process the linked collection
        val linkedCollectionJson = StacUtils.loadStacCollectionToJson(childUrl)
        val nestedCollectionBasePath = StacUtils.getStacCollectionBasePath(childUrl)
        val collectionFiltered =
          filterCollection(linkedCollectionJson, spatialFilter, temporalFilter)

        if (!collectionFiltered) {
          collectItemLinks(nestedCollectionBasePath, linkedCollectionJson, itemLinks)
        }
      }
    }
  }

  /**
   * Filters a collection based on the provided spatial and temporal filters.
   *
   * @param collectionJson
   *   The JSON string representation of the STAC collection.
   * @param spatialFilter
   *   The spatial filter to apply to the collection.
   * @param temporalFilter
   *   The temporal filter to apply to the collection.
   * @return
   *   `true` if the collection is filtered out, `false` otherwise.
   */
  def filterCollection(
      collectionJson: String,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): Boolean = {

    val mapper = new ObjectMapper()
    val rootNode: JsonNode = mapper.readTree(collectionJson)

    // Filter based on spatial extent
    val spatialFiltered = spatialFilter match {
      case Some(filter) =>
        val extentNode = rootNode.path("extent").path("spatial").path("bbox")
        if (extentNode.isMissingNode) {
          false
        } else {
          val bbox = extentNode
            .elements()
            .asScala
            .map { bboxNode =>
              val minX = bboxNode.get(0).asDouble()
              val minY = bboxNode.get(1).asDouble()
              val maxX = bboxNode.get(2).asDouble()
              val maxY = bboxNode.get(3).asDouble()
              (minX, minY, maxX, maxY)
            }
            .toList

          !bbox.exists { case (minX, minY, maxX, maxY) =>
            val geometryTypes = Seq("Polygon")
            val bbox = Seq(minX, minY, maxX, maxY)

            val geometryFieldMetaData = GeometryFieldMetaData(
              encoding = "WKB",
              geometryTypes = geometryTypes,
              bbox = bbox,
              crs = None,
              covering = None)

            filter.evaluate(Map("geometry" -> geometryFieldMetaData))
          }
        }
      case None => false
    }

    // Filter based on temporal extent
    val temporalFiltered = temporalFilter match {
      case Some(filter) =>
        val extentNode = rootNode.path("extent").path("temporal").path("interval")
        if (extentNode.isMissingNode) {
          // if extent is missing, we assume the collection is not filtered
          true
        } else {
          // parse the temporal intervals
          val formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .appendPattern("'Z'")
            .toFormatter()

          val intervals = extentNode
            .elements()
            .asScala
            .map { intervalNode =>
              val start = LocalDateTime.parse(intervalNode.get(0).asText(), formatter)
              val end = LocalDateTime.parse(intervalNode.get(1).asText(), formatter)
              (start, end)
            }
            .toList

          // check if the filter evaluates to true for any of the interval start or end times
          !intervals.exists { case (start, end) =>
            filter.evaluate(Map("datetime" -> start)) ||
            filter.evaluate(Map("datetime" -> end))
          }
        }
      // if the collection is not filtered, return false
      case None => false
    }

    spatialFiltered || temporalFiltered
  }

  /**
   * Creates a partition reader factory for reading data from the STAC data source.
   *
   * @return
   *   A partition reader factory for reading STAC data.
   */
  override def createReaderFactory(): PartitionReaderFactory = { (partition: InputPartition) =>
    {
      new StacPartitionReader(
        partition.asInstanceOf[StacPartition],
        schema,
        opts,
        spatialFilter,
        temporalFilter)
    }
  }
}
