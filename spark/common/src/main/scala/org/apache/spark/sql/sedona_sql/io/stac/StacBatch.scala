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

import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.Try

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

  private val configuredItemsLimitMax = opts.getOrElse("itemsLimitMax", "-1").toInt
  private val effectiveItemsLimitMax: Option[Int] = {
    val configuredLimit = Option(configuredItemsLimitMax).filter(_ > 0)
    val sqlLimit = limitFilter.filter(_ >= 0)
    (configuredLimit, sqlLimit) match {
      case (Some(configured), Some(sql)) => Some(math.min(configured, sql))
      case (configured @ Some(_), None) => configured
      case (None, sql @ Some(_)) => sql
      case _ => None
    }
  }

  private val defaultItemsLimitPerRequest: Int = {
    val limitPerRequest = opts.getOrElse("itemsLimitPerRequest", "10").toInt
    effectiveItemsLimitMax.map(math.min(limitPerRequest, _)).getOrElse(limitPerRequest)
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
    // Initialize the itemLinks array
    val itemLinks = scala.collection.mutable.ArrayBuffer[String]()

    // Get the maximum number of items to process
    val itemsLimitMax = effectiveItemsLimitMax.getOrElse(-1)
    val checkItemsLimitMax = effectiveItemsLimitMax.isDefined

    // Start the recursive collection of item links
    setItemMaxLeft(itemsLimitMax)

    collectItemLinks(stacCollectionUrl, stacCollectionJson, itemLinks, checkItemsLimitMax)

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
   * @param collectionUrl
   *   The URL of the current STAC collection document.
   * @param collectionJson
   *   The JSON string representation of the STAC collection.
   * @param itemLinks
   *   The list of item links to populate.
   */
  def collectItemLinks(
      collectionUrl: String,
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

    def isAbsoluteLink(href: String): Boolean =
      href.startsWith("http") || href.startsWith("file")

    def normalizeFileUrl(url: String): String = {
      if (url.startsWith("file:/") && !url.startsWith("file://")) {
        "file:///" + url.stripPrefix("file:/")
      } else url
    }

    def resolveLink(baseUrl: String, href: String): String = {
      if (href.startsWith("file")) normalizeFileUrl(href)
      else if (isAbsoluteLink(href)) href
      else if (baseUrl.startsWith("http")) {
        Try {
          if (href.startsWith("?")) {
            baseUrl.takeWhile(character => character != '?' && character != '#') + href
          } else if (href.startsWith("#")) {
            baseUrl.takeWhile(_ != '#') + href
          } else new URI(baseUrl).resolve(href).toString
        }.getOrElse(StacUtils.getStacCollectionBasePath(baseUrl) + href)
      } else if (baseUrl.startsWith("file")) {
        normalizeFileUrl(new URI(baseUrl).resolve(href).toString)
      } else StacUtils.getStacCollectionBasePath(baseUrl) + href
    }

    def getReturnedItemCount(itemRootNode: JsonNode): Int = {
      val featureCount =
        Option(itemRootNode.get("features")).filter(_.isArray).map(_.size())
      val reportedCount = Option(itemRootNode.get("numberReturned"))
        .filter(_.isIntegralNumber)
        .map(_.asInt())
        .filter(_ >= 0)
      featureCount.orElse(reportedCount).getOrElse(0)
    }

    def iterateItemsWithLimit(itemUrl: String, needCountNextItems: Boolean): Boolean = {
      // Load the item URL and process the response
      var nextUrl: Option[String] = Some(itemUrl)
      while (nextUrl.isDefined) {
        val currentUrl = nextUrl.get
        val itemJson = StacUtils.loadStacCollectionToJson(currentUrl, headers)
        val itemRootNode = mapper.readTree(itemJson)

        if (needCountNextItems) {
          itemMaxLeft = itemMaxLeft - getReturnedItemCount(itemRootNode)
          if (itemMaxLeft <= 0) {
            return true
          }
        }

        val itemLinksNode = itemRootNode.get("links")
        if (itemLinksNode == null || !itemLinksNode.isArray) {
          return false
        }

        val itemIterator = itemLinksNode.elements()
        var nextHref: Option[String] = None
        while (itemIterator.hasNext && nextHref.isEmpty) {
          val itemLinkNode = itemIterator.next()
          if (itemLinkNode.path("rel").asText() == "next") {
            val href = itemLinkNode.get("href")
            if (href != null && href.isTextual) {
              nextHref = Some(href.asText())
            }
          }
        }

        // Pagination links are authoritative and may contain opaque cursor state. Changing their
        // page size can alter page-number offsets or invalidate a signed URL.
        nextUrl = nextHref.map(href => resolveLink(currentUrl, href))
        if (nextUrl.isDefined) {
          itemLinks += nextUrl.get
        }
      }
      false
    }

    while (iterator.hasNext) {
      if (needCountNextItems && itemMaxLeft <= 0) return

      val linkNode = iterator.next()
      val rel = linkNode.get("rel").asText()
      val href = linkNode.get("href").asText()

      // item links are identified by the "rel" value of "item" or "items"
      if (rel == "item" || rel == "items") {
        // need to handle relative paths and local file paths
        val itemUrl = resolveLink(collectionUrl, href)
        val firstPageUrl = if (rel == "items" && itemUrl.startsWith("http")) {
          getItemLink(itemUrl, defaultItemsLimitPerRequest, spatialFilter, temporalFilter)
        } else itemUrl
        itemLinks += firstPageUrl

        if (rel == "item" && needCountNextItems) {
          // count the number of items returned and left to be processed
          itemMaxLeft = itemMaxLeft - 1
        } else if (rel == "items" && itemUrl.startsWith("http")) {
          // iterate through the items and check if the limit is reached (if needed)
          if (iterateItemsWithLimit(firstPageUrl, needCountNextItems)) return
        }
      } else if (rel == "child") {
        val childUrl = resolveLink(collectionUrl, href)
        // Recursively process the linked collection
        val linkedCollectionJson = StacUtils.loadStacCollectionToJson(childUrl, headers)
        val collectionFiltered =
          filterCollection(linkedCollectionJson, spatialFilter, temporalFilter)

        if (!collectionFiltered) {
          collectItemLinks(childUrl, linkedCollectionJson, itemLinks, needCountNextItems)
        }
      }
    }
  }

  private def setLimitParameter(itemUrl: String, limit: Int): String = {
    val fragmentIndex = itemUrl.indexOf('#')
    val (urlWithoutFragment, fragment) = if (fragmentIndex >= 0) {
      (itemUrl.substring(0, fragmentIndex), itemUrl.substring(fragmentIndex))
    } else (itemUrl, "")
    val queryIndex = urlWithoutFragment.indexOf('?')
    val (path, existingQuery) = if (queryIndex >= 0) {
      (urlWithoutFragment.substring(0, queryIndex), urlWithoutFragment.substring(queryIndex + 1))
    } else (urlWithoutFragment, "")
    val retainedParameters = existingQuery
      .split("&", -1)
      .filter(_.nonEmpty)
      .filterNot(_.takeWhile(_ != '=') == "limit")
    val query = (retainedParameters :+ s"limit=$limit").mkString("&")
    s"$path?$query$fragment"
  }

  /** Adds an item link to the list of item links. */
  def getItemLink(
      itemUrl: String,
      defaultItemsLimitPerRequest: Int,
      spatialFilter: Option[GeoParquetSpatialFilter],
      temporalFilter: Option[TemporalFilter]): String = {
    val urlWithLimit = setLimitParameter(itemUrl, defaultItemsLimitPerRequest)
    val fragmentIndex = urlWithLimit.indexOf('#')
    val (urlWithoutFragment, fragment) = if (fragmentIndex >= 0) {
      (urlWithLimit.substring(0, fragmentIndex), urlWithLimit.substring(fragmentIndex))
    } else (urlWithLimit, "")
    val urlWithFilters =
      StacUtils.addFiltersToUrl(urlWithoutFragment, spatialFilter, temporalFilter)
    urlWithFilters + fragment
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
              bbox = Some(bbox),
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
        StacUtils.calculateTemporalBounds(filter) match {
          // A proven-empty predicate cannot match any collection.
          case None => true
          // An unbounded envelope cannot safely prune a collection.
          case Some(bounds) if bounds.isUnbounded => false
          case Some(bounds) =>
            val extentNode = rootNode.path("extent").path("temporal").path("interval")
            if (extentNode.isMissingNode || !extentNode.isArray || extentNode.size() == 0) {
              // Missing or unusable extent metadata cannot prove that a collection is disjoint.
              false
            } else {
              val formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .appendPattern("'Z'")
                .toFormatter()

              def parseEndpoint(node: JsonNode): Option[Option[LocalDateTime]] = {
                if (node == null || node.isNull) Some(None)
                else Try(LocalDateTime.parse(node.asText(), formatter)).toOption.map(Some(_))
              }

              val intervals = extentNode
                .elements()
                .asScala
                .map { intervalNode =>
                  for {
                    start <- parseEndpoint(intervalNode.get(0))
                    end <- parseEndpoint(intervalNode.get(1))
                  } yield (start, end)
                }
                .toList

              // Malformed metadata is not evidence that the collection is outside the query.
              if (intervals.exists(_.isEmpty)) false
              else {
                !intervals.flatten.exists { case (start, end) =>
                  val invalidInterval =
                    start.exists(startValue =>
                      end.exists(endValue => startValue.isAfter(endValue)))
                  invalidInterval || bounds.overlaps(start, end)
                }
              }
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
