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

import scala.io.Source
import scala.collection.mutable

class StacBatchTest extends TestBaseScala {

  def loadJsonFromResource(resourceFilePath: String): String = {
    Source.fromResource(resourceFilePath).getLines().mkString("\n")
  }

  def getAbsolutePathOfResource(resourceFilePath: String): String = {
    val resourceUrl = getClass.getClassLoader.getResource(resourceFilePath)
    if (resourceUrl != null) {
      resourceUrl.getPath
    } else {
      throw new IllegalArgumentException(s"Resource not found: $resourceFilePath")
    }
  }

  it("planInputPartitions should create correct number of partitions") {
    val stacCollectionJson =
      """
        |{
        |  "stac_version": "1.0.0",
        |  "id": "sample-collection",
        |  "description": "A sample STAC collection",
        |  "links": [
        |    {"rel": "item", "href": "https://path/to/item1.json"},
        |    {"rel": "item", "href": "https://path/to/item2.json"},
        |    {"rel": "item", "href": "https://path/to/item3.json"}
        |  ]
        |}
      """.stripMargin

    val opts = mutable.Map("numPartitions" -> "2").toMap
    val collectionUrl = "https://path/to/collection.json"

    val stacBatch =
      StacBatch(collectionUrl, stacCollectionJson, StructType(Seq()), opts, None, None)
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

    val opts = mutable.Map("numPartitions" -> "2").toMap
    val collectionUrl = "https://path/to/collection.json"

    val stacBatch =
      StacBatch(collectionUrl, stacCollectionJson, StructType(Seq()), opts, None, None)
    val partitions: Array[InputPartition] = stacBatch.planInputPartitions()

    assert(partitions.isEmpty)
  }

  it("planInputPartitions should create correct number of partitions with real collection.json") {
    val rootJsonFile = "datasource_stac/collection.json"
    val stacCollectionJson = loadJsonFromResource(rootJsonFile)
    val opts = mutable.Map("numPartitions" -> "3").toMap
    val collectionUrl = getAbsolutePathOfResource(rootJsonFile)

    val stacBatch =
      StacBatch(collectionUrl, stacCollectionJson, StructType(Seq()), opts, None, None)
    val partitions: Array[InputPartition] = stacBatch.planInputPartitions()

    assert(partitions.length == 3)
    assert(partitions(0).asInstanceOf[StacPartition].items.length == 2)
    assert(partitions(1).asInstanceOf[StacPartition].items.length == 2)
    assert(partitions(2).asInstanceOf[StacPartition].items.length == 1)
  }
}
