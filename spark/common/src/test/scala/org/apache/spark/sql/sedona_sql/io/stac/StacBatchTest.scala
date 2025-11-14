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
import org.apache.spark.sql.types.StructType

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import scala.io.Source
import scala.collection.mutable

class StacBatchTest extends TestBaseScala {

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
}
