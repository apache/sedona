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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.geoparquet.{GeoParquetSpatialFilter, GeometryFieldMetaData}
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.getNumPartitions
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.Breaks.breakable

/**
 * The `StacBatch` class represents a batch of partitions for reading data in the SpatioTemporal
 * Asset Catalog (STAC) data source. It implements the `Batch` interface from Apache Spark's data
 * source API.
 *
 * This class provides methods to plan input partitions and create a partition reader factory,
 * which are necessary for batch data processing.
 */
case class StacBatch(
    broadcastConf: Broadcast[SerializableConfiguration],
    stacCollectionUrl: String,
    stacCollectionJson: String,
    schema: StructType,
    opts: Map[String, String],
    spatialFilter: Option[GeoParquetSpatialFilter],
    temporalFilter: Option[TemporalFilter],
    limitFilter: Option[Int])
    extends Batch {

  private val defaultItemsLimitPerRequest: Int = {
    val itemsLimitMax = opts.getOrElse("itemsLimitMax", "-1").toInt
    val limitPerRequest = opts.getOrElse("itemsLimitPerRequest", "10").toInt
    if (itemsLimitMax > 0 && limitPerRequest > itemsLimitMax) itemsLimitMax else limitPerRequest
  }
  private val itemsLoadProcessReportThreshold =
    opts.getOrElse("itemsLoadProcessReportThreshold", "1000000").toInt
  private var itemMaxLeft: Int = -1
  private var lastReportCount: Int = 0

  val mapper = new ObjectMapper()

  // Parse headers from options for authenticated requests
  private val headers: Map[String, String] = StacUtils.parseHeaders(opts)

  /**
   * Sets the maximum number of items left to process.
   *
   * @param value
   *   The maximum number of items left.
   */
  def setItemMaxLeft(value: Int): Unit = {
    itemMaxLeft = value
  }

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

    // Get the maximum number of items to process
    val itemsLimitMax = limitFilter match {
      case Some(limit) if limit >= 0 => limit
      case _ => opts.getOrElse("itemsLimitMax", "-1").toInt
    }
    val checkItemsLimitMax = itemsLimitMax > 0

    // Start the recursive collection of item links
    setItemMaxLeft(itemsLimitMax)

    collectItemLinks(stacCollectionBasePath, stacCollectionJson, itemLinks, checkItemsLimitMax)

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

    // Group the item links into partitions, but randomize first for better load balancing
    Random
      .shuffle(itemLinks)
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
  def collectItemLinks(
      collectionBasePath: String,
      collectionJson: String,
      itemLinks: scala.collection.mutable.ArrayBuffer[String],
      needCountNextItems: Boolean): Unit = {

    // end early if there are no more items to process
    if (needCountNextItems && itemMaxLeft <= 0) return

    if (itemLinks.size - lastReportCount >= itemsLoadProcessReportThreshold) {
      Console.out.println(s"Searched or partitioned ${itemLinks.size} items so far.")
      lastReportCount = itemLinks.size
    }

    // Parse the JSON string into a JsonNode (tree representation of JSON)
    val rootNode: JsonNode = mapper.readTree(collectionJson)

    // Extract item links from the "links" array
    val linksNode = rootNode.get("links")
    val iterator = linksNode.elements()

    def iterateItemsWithLimit(itemUrl: String, needCountNextItems: Boolean): Boolean = {
      // Load the item URL and process the response
      var nextUrl: Option[String] = Some(itemUrl)
      breakable {
        while (nextUrl.isDefined) {
          val itemJson = StacUtils.loadStacCollectionToJson(nextUrl.get, headers)
          val itemRootNode = mapper.readTree(itemJson)
          // Check if there exists a "next" link
          val itemLinksNode = itemRootNode.get("links")
          if (itemLinksNode == null) {
            return true
          }
          val itemIterator = itemLinksNode.elements()
          nextUrl = None
          while (itemIterator.hasNext) {
            val itemLinkNode = itemIterator.next()
            val itemRel = itemLinkNode.get("rel").asText()
            val itemHref = itemLinkNode.get("href").asText()
            if (itemRel == "next") {
              // Only check the number of items returned if there are more items to process
              val numberReturnedNode = itemRootNode.get("numberReturned")
              val numberReturned = if (numberReturnedNode == null) {
                // From STAC API Spec:
                // The optional limit parameter limits the number of
                // items that are presented in the response document.
                // The default value is 10.
                defaultItemsLimitPerRequest
              } else {
                numberReturnedNode.asInt()
              }
              // count the number of items returned and left to be processed
              itemMaxLeft = itemMaxLeft - numberReturned
              // early exit if there are no more items to process
              if (needCountNextItems && itemMaxLeft <= 0) {
                return true
              }
              nextUrl = Some(if (itemHref.startsWith("http") || itemHref.startsWith("file")) {
                itemHref
              } else {
                collectionBasePath + itemHref
              })
            }
          }
          if (nextUrl.isDefined) {
            itemLinks += nextUrl.get
          }
        }
      }
      false
    }

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
        if (rel == "items" && href.startsWith("http")) {
          itemLinks += (itemUrl + "?limit=" + defaultItemsLimitPerRequest)
        } else {
          itemLinks += itemUrl
        }
        if (needCountNextItems && itemMaxLeft <= 0) {
          return
        } else {
          if (rel == "item" && needCountNextItems) {
            // count the number of items returned and left to be processed
            itemMaxLeft = itemMaxLeft - 1
          } else if (rel == "items" && href.startsWith("http")) {
            // iterate through the items and check if the limit is reached (if needed)
            if (iterateItemsWithLimit(
                getItemLink(itemUrl, defaultItemsLimitPerRequest, spatialFilter, temporalFilter),
                needCountNextItems)) return
          }
        }
      } else if (rel == "child") {
        val childUrl = if (href.startsWith("http") || href.startsWith("file")) {
          href
        } else {
          collectionBasePath + href
        }
        // Recursively process the linked collection
        val linkedCollectionJson = StacUtils.loadStacCollectionToJson(childUrl, headers)
        val nestedCollectionBasePath = StacUtils.getStacCollectionBasePath(childUrl)
        val collectionFiltered =
          filterCollection(linkedCollectionJson, spatialFilter, temporalFilter)

        if (!collectionFiltered) {
          collectItemLinks(
            nestedCollectionBasePath,
            linkedCollectionJson,
            itemLinks,
            needCountNextItems)
        }
      }
    }
  }

  /** Adds an item link to the list of item links. */
  def getItemLink(
      itemUrl: String,
      defaultItemsLimitPerRequest: Int,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): String = {
    val baseUrl = itemUrl + "?limit=" + defaultItemsLimitPerRequest
    val urlWithFilters = StacUtils.addFiltersToUrl(baseUrl, spatialFilter, temporalFilter)
    urlWithFilters
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
        broadcastConf,
        partition.asInstanceOf[StacPartition],
        schema,
        opts,
        spatialFilter,
        temporalFilter)
    }
  }
}
