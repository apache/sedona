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

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.types.StructType

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.collection.mutable
import scala.io.Source

class StacBatchTest extends TestBaseScala {

  private def withHttpServer(responseFor: HttpExchange => String)(test: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      exchange => {
        val response = responseFor(exchange).getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/geo+json")
        exchange.sendResponseHeaders(200, response.length)
        val body = exchange.getResponseBody
        try body.write(response)
        finally body.close()
      })
    server.start()
    try test(s"http://127.0.0.1:${server.getAddress.getPort}")
    finally server.stop(0)
  }

  def loadJsonFromResource(resourceFilePath: String): String = {
    Source.fromResource(resourceFilePath).getLines().mkString("\n")
  }

  it("collectItemLinks should collect correct item links") {
    val collectionUrl =
      StacTestUtils.getFileUrlOfResource("stac/collections/sentinel-2-pre-c1-l2a.json")
    val stacCollectionJson = StacUtils.loadStacCollectionToJson(collectionUrl)
    val opts = mutable
      .Map(
        "itemsLimitMax" -> "1000",
        "itemsLimitPerRequest" -> "200",
        "itemsLoadProcessReportThreshold" -> "1000000")
      .toMap

    val stacBatch =
      StacBatch(
        null,
        collectionUrl,
        stacCollectionJson,
        StructType(Seq()),
        opts,
        None,
        None,
        None)
    stacBatch.setItemMaxLeft(1000)
    val itemLinks = mutable.ArrayBuffer[String]()
    val needCountNextItems = true

    val startTime = System.nanoTime()
    stacBatch.collectItemLinks(collectionUrl, stacCollectionJson, itemLinks, needCountNextItems)
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e6 // Convert to milliseconds

    assert(itemLinks.nonEmpty)
    assert(itemLinks.length == 5)
    assert(duration > 0)
  }

  it("collectItemLinks should schedule the filtered first items page") {
    val requests = mutable.ArrayBuffer[String]()
    withHttpServer(exchange => {
      requests.synchronized(requests += exchange.getRequestURI.toString)
      """{"features":[]}"""
    }) { serverUrl =>
      val collectionJson =
        """{"links":[{"rel":"items","href":"items?token=x"},{"rel":"item","href":"after.json"}]}"""
      val temporalFilter =
        TemporalFilter.GreaterThanFilter("datetime", LocalDateTime.parse("2025-03-06T00:00:00"))
      val stacBatch = StacBatch(
        null,
        s"$serverUrl/api/collections/c",
        collectionJson,
        StructType(Seq()),
        Map("itemsLimitPerRequest" -> "2"),
        None,
        Some(temporalFilter),
        None)
      val itemLinks = mutable.ArrayBuffer[String]()

      stacBatch.collectItemLinks(
        s"$serverUrl/api/collections/c",
        collectionJson,
        itemLinks,
        needCountNextItems = false)

      val expectedFirstPage =
        s"$serverUrl/api/collections/items?token=x&limit=2&datetime=2025-03-06T00:00:00.000000000Z/.."
      assert(itemLinks == Seq(expectedFirstPage, s"$serverUrl/api/collections/after.json"))
      val expectedUri = new URI(expectedFirstPage)
      assert(requests == Seq(s"${expectedUri.getRawPath}?${expectedUri.getRawQuery}"))
    }
  }

  it("collectItemLinks should count actual features and preserve next links") {
    val requests = mutable.ArrayBuffer[String]()
    withHttpServer(exchange => {
      val requestUri = exchange.getRequestURI.toString
      requests.synchronized(requests += requestUri)
      if (requestUri.contains("page=1")) {
        """{"features":[{}],"links":[{"rel":"next","href":"?page=2&limit=7"}]}"""
      } else if (requestUri.contains("page=2")) {
        """{"features":[{}],"links":[{"rel":"next","href":"?page=3&limit=7"}]}"""
      } else {
        """{"features":[{}],"links":[]}"""
      }
    }) { serverUrl =>
      val collectionJson = """{"links":[{"rel":"items","href":"items?page=1"}]}"""
      val stacBatch = StacBatch(
        null,
        s"$serverUrl/api/collection",
        collectionJson,
        StructType(Seq()),
        Map("itemsLimitPerRequest" -> "2"),
        None,
        None,
        None)
      stacBatch.setItemMaxLeft(3)
      val itemLinks = mutable.ArrayBuffer[String]()

      stacBatch.collectItemLinks(
        s"$serverUrl/api/collection",
        collectionJson,
        itemLinks,
        needCountNextItems = true)

      assert(
        itemLinks == Seq(
          s"$serverUrl/api/items?page=1&limit=2",
          s"$serverUrl/api/items?page=2&limit=7",
          s"$serverUrl/api/items?page=3&limit=7"))
      assert(
        requests == Seq(
          "/api/items?page=1&limit=2",
          "/api/items?page=2&limit=7",
          "/api/items?page=3&limit=7"))
    }
  }

  it("collectItemLinks should stop at the direct item limit without adding an extra link") {
    val collectionJson =
      """{"links":[{"rel":"item","href":"file:/one.json"},{"rel":"item","href":"two.json"}]}"""
    val stacBatch = StacBatch(
      null,
      "file:///collection.json",
      collectionJson,
      StructType(Seq()),
      Map.empty,
      None,
      None,
      None)
    stacBatch.setItemMaxLeft(1)
    val itemLinks = mutable.ArrayBuffer[String]()

    stacBatch.collectItemLinks(
      "file:///collection.json",
      collectionJson,
      itemLinks,
      needCountNextItems = true)

    assert(itemLinks == Seq("file:///one.json"))
  }

  it("getItemLink should preserve existing queries and fragments") {
    assert(
      batch.getItemLink("https://example.test/items?token=x&limit=100#results", 2, None, None) ==
        "https://example.test/items?token=x&limit=2#results")
  }

  it("planInputPartitions should use the smaller configured and SQL limits") {
    val collectionJson =
      """{"links":[{"rel":"item","href":"one.json"},{"rel":"item","href":"two.json"}]}"""
    val stacBatch = StacBatch(
      null,
      "file:///collection.json",
      collectionJson,
      StructType(Seq()),
      Map("itemsLimitMax" -> "1"),
      None,
      None,
      Some(2))

    val plannedItems = stacBatch
      .planInputPartitions()
      .flatMap(_.asInstanceOf[StacPartition].items)

    assert(plannedItems.length == 1)
  }

  it("planInputPartitions should create correct number of partitions") {
    val stacCollectionJson =
      """
        |{
        |  "stac_version": "1.0.0",
        |  "id": "sample-collection",
        |  "description": "A sample STAC collection",
        |  "links": [
        |    {"rel": "item", "href": "mock-item-1.json"},
        |    {"rel": "item", "href": "mock-item-2.json"},
        |    {"rel": "item", "href": "mock-item-3.json"}
        |  ]
        |}
      """.stripMargin

    val opts = mutable.Map("numPartitions" -> "2", "itemsLimitMax" -> "20").toMap
    val collectionUrl =
      StacTestUtils.getFileUrlOfResource("stac/collections/vegetation-collection.json")

    val stacBatch =
      StacBatch(
        null,
        collectionUrl,
        stacCollectionJson,
        StructType(Seq()),
        opts,
        None,
        None,
        None)
    val partitions: Array[InputPartition] = stacBatch.planInputPartitions()

    assert(partitions.length == 2)
    assert(partitions(0).asInstanceOf[StacPartition].items.length == 2)
    assert(partitions(1).asInstanceOf[StacPartition].items.length == 1)
  }

  it("planInputPartitions should handle empty links array") {
    val stacCollectionJson =
      """
        |{
        |  "links": []
        |}
      """.stripMargin

    val opts = mutable.Map("numPartitions" -> "2", "itemsLimitMax" -> "20").toMap
    val collectionUrl =
      StacTestUtils.getFileUrlOfResource("stac/collections/vegetation-collection.json")

    val stacBatch =
      StacBatch(
        null,
        collectionUrl,
        stacCollectionJson,
        StructType(Seq()),
        opts,
        None,
        None,
        None)
    val partitions: Array[InputPartition] = stacBatch.planInputPartitions()

    assert(partitions.isEmpty)
  }

  it("planInputPartitions should create correct number of partitions with real collection.json") {
    val rootJsonFile = "datasource_stac/collection.json"
    val stacCollectionJson = loadJsonFromResource(rootJsonFile)
    val opts = mutable.Map("numPartitions" -> "3", "itemsLimitMax" -> "20").toMap
    val collectionUrl = StacTestUtils.getFileUrlOfResource(rootJsonFile)

    val stacBatch =
      StacBatch(
        null,
        collectionUrl,
        stacCollectionJson,
        StructType(Seq()),
        opts,
        None,
        None,
        None)
    val partitions: Array[InputPartition] = stacBatch.planInputPartitions()

    assert(partitions.length == 3)
    assert(partitions(0).asInstanceOf[StacPartition].items.length == 2)
    assert(partitions(1).asInstanceOf[StacPartition].items.length == 2)
    assert(partitions(2).asInstanceOf[StacPartition].items.length == 1)
  }

  private val batch = StacBatch(
    broadcastConf = null,
    stacCollectionUrl = "",
    stacCollectionJson = "",
    schema = new StructType(),
    opts = Map.empty,
    spatialFilter = None,
    temporalFilter = None,
    limitFilter = None)

  private def collectionIsFilteredOut(intervalsJson: String, filter: TemporalFilter): Boolean = {
    val collectionJson = s"""{"extent":{"temporal":{"interval":$intervalsJson}}}"""
    batch.filterCollection(collectionJson, None, Some(filter))
  }

  it("temporal pruning keeps a collection containing an equality inside its extent") {
    val filter = TemporalFilter.OrFilter(
      TemporalFilter.EqualFilter("datetime", LocalDateTime.parse("2025-03-06T00:00:00")),
      TemporalFilter.GreaterThanFilter("datetime", LocalDateTime.parse("2025-03-10T00:00:00")))

    assert(
      !collectionIsFilteredOut("""[["2025-03-04T00:00:00Z","2025-03-08T00:00:00Z"]]""", filter))
  }

  it("temporal pruning checks interval overlap instead of only collection endpoints") {
    val filter = TemporalFilter.AndFilter(
      TemporalFilter.GreaterThanFilter("datetime", LocalDateTime.parse("2025-03-05T00:00:00")),
      TemporalFilter.LessThanFilter("datetime", LocalDateTime.parse("2025-03-06T00:00:00")))

    assert(
      !collectionIsFilteredOut("""[["2025-03-01T00:00:00Z","2025-03-10T00:00:00Z"]]""", filter))
    assert(
      collectionIsFilteredOut("""[["2025-03-01T00:00:00Z","2025-03-04T00:00:00Z"]]""", filter))
  }

  it("temporal pruning retains collections with missing, open, or malformed extents") {
    val filter =
      TemporalFilter.EqualFilter("datetime", LocalDateTime.parse("2025-03-05T00:00:00"))

    assert(!batch.filterCollection("{}", None, Some(filter)))
    assert(!collectionIsFilteredOut("[[null,null]]", filter))
    assert(!collectionIsFilteredOut("""[["not-a-timestamp",null]]""", filter))
  }

  it("temporal pruning accepts nanosecond collection extents in the final Spark microsecond") {
    val filter =
      TemporalFilter.LessThanFilter("datetime", LocalDateTime.parse("2020-05-31T23:59:59.999999"))

    assert(
      !collectionIsFilteredOut(
        """[["2020-05-31T23:59:59.999999500Z","2020-05-31T23:59:59.999999999Z"]]""",
        filter))
  }

  it("temporal pruning filters every collection for a proven-empty envelope") {
    val filter = TemporalFilter.AndFilter(
      TemporalFilter.GreaterThanFilter("datetime", LocalDateTime.parse("2025-03-07T00:00:00")),
      TemporalFilter.LessThanFilter("datetime", LocalDateTime.parse("2025-03-06T00:00:00")))

    assert(batch.filterCollection("{}", None, Some(filter)))
  }
}
