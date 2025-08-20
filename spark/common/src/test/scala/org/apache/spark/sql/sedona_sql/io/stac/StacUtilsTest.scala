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
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.{getFilterBBox, getFilterTemporal, getNumPartitions}
import org.locationtech.jts.geom.{Envelope, GeometryFactory, Polygon}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import scala.io.Source
import scala.jdk.CollectionConverters._

class StacUtilsTest extends AnyFunSuite {

  val geometryFactory = new GeometryFactory()
  val wktReader = new WKTReader(geometryFactory)

  test("getStacCollectionBasePath should return base URL for HTTP URL") {
    val opts = Map("path" -> "https://service_url/collections/collection.json")
    val result = StacUtils.getStacCollectionBasePath(opts)
    assert(result == "https://service_url/")
  }

  test("getStacCollectionBasePath should return base URL for HTTPS URL") {
    val opts = Map("path" -> "https://service_url/collections/collection.json")
    val result = StacUtils.getStacCollectionBasePath(opts)
    assert(result == "https://service_url/")
  }

  test("getStacCollectionBasePath should return base path for file URL") {
    val opts = Map("path" -> "file:///usr/opt/collection.json")
    val result = StacUtils.getStacCollectionBasePath(opts)
    assert(result == "file:///usr/opt/")
  }

  test("getStacCollectionBasePath should return base path for local file path") {
    val opts = Map("path" -> "/usr/opt/collection.json")
    val result = StacUtils.getStacCollectionBasePath(opts)
    assert(result == "file:///usr/opt/")
  }

  test(
    "getStacCollectionBasePath should throw IllegalArgumentException if neither url nor service is provided") {
    val opts = Map.empty[String, String]
    assertThrows[IllegalArgumentException] {
      StacUtils.getStacCollectionBasePath(opts)
    }
  }

  test(
    "getStacCollectionBasePath should throw IllegalArgumentException for invalid URL or file path") {
    val opts = Map("path" -> "invalid_path")
    assertThrows[IllegalArgumentException] {
      StacUtils.getStacCollectionBasePath(opts)
    }
  }

  test("getNumPartitions should return numPartitions if it is greater than 0") {
    assert(
      getNumPartitions(
        itemCount = 100,
        numPartitions = 5,
        maxPartitionItemFiles = 10,
        defaultParallelism = 4) == 5)
  }

  test(
    "getNumPartitions should calculate partitions based on maxPartitionItemFiles and defaultParallelism") {
    assert(
      getNumPartitions(
        itemCount = 100,
        numPartitions = 0,
        maxPartitionItemFiles = 10,
        defaultParallelism = 4) == 10)
  }

  test(
    "getNumPartitions should handle case when maxPartitionItemFiles is less than sum of files / defaultParallelism") {
    assert(
      getNumPartitions(
        itemCount = 100,
        numPartitions = 0,
        maxPartitionItemFiles = 5,
        defaultParallelism = 4) == 20)
  }

  test("getNumPartitions should handle case when maxPartitionItemFiles is 0") {
    assert(
      getNumPartitions(
        itemCount = 100,
        numPartitions = 0,
        maxPartitionItemFiles = 0,
        defaultParallelism = 4) == 4)
  }

  test("getNumPartitions should handle case when defaultParallelism is 1") {
    assert(
      getNumPartitions(
        itemCount = 100,
        numPartitions = 0,
        maxPartitionItemFiles = 10,
        defaultParallelism = 1) == 10)
  }

  test("getNumPartitions should return at least 1 partition") {
    assert(
      getNumPartitions(
        itemCount = 0,
        numPartitions = 0,
        maxPartitionItemFiles = 10,
        defaultParallelism = 4) == 1)
  }

  test(
    "processStacCollection should process STAC collection from JSON string and save features to output file") {
    val spark = SparkSession.builder().master("local").appName("StacUtilsTest").getOrCreate()
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Create a temporary STAC collection JSON file
    val stacCollectionJson =
      """
        |{
        |  "stac_version": "1.0.0",
        |  "id": "sample-collection",
        |  "description": "A sample STAC collection",
        |  "links": [
        |    {"rel": "item", "href": "file:///tmp/item1.json"},
        |    {"rel": "item", "href": "file:///tmp/item2.json"}
        |  ]
        |}
      """.stripMargin
    val stacCollectionPath = new Path("/tmp/collection.json")
    val stacCollectionWriter = new PrintWriter(new File(stacCollectionPath.toString))
    stacCollectionWriter.write(stacCollectionJson)
    stacCollectionWriter.close()

    // Create temporary item JSON files
    val item1Json =
      """
        |{
        |  "stac_version": "1.1.0",
        |  "stac_extensions": [],
        |  "type": "Feature",
        |  "id": "20201211_223832_CS2_item1",
        |  "bbox": [
        |    172.91173669923782,
        |    1.3438851951615003,
        |    172.95469614953714,
        |    1.3690476620161975
        |  ],
        |  "geometry": {
        |    "type": "Polygon",
        |    "coordinates": [
        |      [
        |        [
        |          172.91173669923782,
        |          1.3438851951615003
        |        ],
        |        [
        |          172.95469614953714,
        |          1.3438851951615003
        |        ],
        |        [
        |          172.95469614953714,
        |          1.3690476620161975
        |        ],
        |        [
        |          172.91173669923782,
        |          1.3690476620161975
        |        ],
        |        [
        |          172.91173669923782,
        |          1.3438851951615003
        |        ]
        |      ]
        |    ]
        |  },
        |  "properties": {
        |    "title": "Item 1",
        |    "description": "A sample STAC Item 1 that includes examples of all common metadata",
        |    "datetime": null,
        |    "start_datetime": "2020-12-11T22:38:32.125Z",
        |    "end_datetime": "2020-12-11T22:38:32.327Z",
        |    "created": "2020-12-12T01:48:13.725Z",
        |    "updated": "2020-12-12T01:48:13.725Z",
        |    "platform": "cool_sat1",
        |    "instruments": [
        |      "cool_sensor_v1"
        |    ],
        |    "constellation": "ion",
        |    "mission": "collection 5624",
        |    "gsd": 0.512
        |  },
        |  "collection": "simple-collection",
        |  "links": [
        |    {
        |      "rel": "collection",
        |      "href": "./collection.json",
        |      "type": "application/json",
        |      "title": "Simple Example Collection"
        |    },
        |    {
        |      "rel": "root",
        |      "href": "./collection.json",
        |      "type": "application/json",
        |      "title": "Simple Example Collection"
        |    },
        |    {
        |      "rel": "parent",
        |      "href": "./collection.json",
        |      "type": "application/json",
        |      "title": "Simple Example Collection"
        |    },
        |    {
        |      "rel": "alternate",
        |      "type": "text/html",
        |      "href": "https://remotedata.io/catalog/20201211_223832_CS2_item1/index.html",
        |      "title": "HTML version of this STAC Item"
        |    }
        |  ],
        |  "assets": {
        |    "analytic": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item1_analytic.tif",
        |      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        |      "title": "4-Band Analytic",
        |      "roles": [
        |        "data"
        |      ]
        |    },
        |    "thumbnail": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item1.jpg",
        |      "title": "Thumbnail",
        |      "type": "image/png",
        |      "roles": [
        |        "thumbnail"
        |      ]
        |    },
        |    "visual": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item1.tif",
        |      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        |      "title": "3-Band Visual",
        |      "roles": [
        |        "visual"
        |      ]
        |    },
        |    "udm": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item1_analytic_udm.tif",
        |      "title": "Unusable Data Mask",
        |      "type": "image/tiff; application=geotiff"
        |    },
        |    "json-metadata": {
        |      "href": "https://remotedata.io/catalog/20201211_223832_CS2_item1/extended-metadata.json",
        |      "title": "Extended Metadata",
        |      "type": "application/json",
        |      "roles": [
        |        "metadata"
        |      ]
        |    },
        |    "ephemeris": {
        |      "href": "https://cool-sat.com/catalog/20201211_223832_CS2_item1/20201211_223832_CS2_item1.EPH",
        |      "title": "Satellite Ephemeris Metadata"
        |    }
        |  }
        |}
      """.stripMargin
    val item1Path = new Path("/tmp/item1.json")
    val item1Writer = new PrintWriter(new File(item1Path.toString))
    item1Writer.write(item1Json)
    item1Writer.close()

    val item2Json =
      """
        |{
        |  "stac_version": "1.1.0",
        |  "stac_extensions": [],
        |  "type": "Feature",
        |  "id": "20201211_223832_CS2_item2",
        |  "bbox": [
        |    173.91173669923782,
        |    2.3438851951615003,
        |    173.95469614953714,
        |    2.3690476620161975
        |  ],
        |  "geometry": {
        |    "type": "Polygon",
        |    "coordinates": [
        |      [
        |        [
        |          173.91173669923782,
        |          2.3438851951615003
        |        ],
        |        [
        |          173.95469614953714,
        |          2.3438851951615003
        |        ],
        |        [
        |          173.95469614953714,
        |          2.3690476620161975
        |        ],
        |        [
        |          173.91173669923782,
        |          2.3690476620161975
        |        ],
        |        [
        |          173.91173669923782,
        |          2.3438851951615003
        |        ]
        |      ]
        |    ]
        |  },
        |  "properties": {
        |    "title": "Item 2",
        |    "description": "A different sample STAC Item 2 that includes examples of all common metadata",
        |    "datetime": null,
        |    "start_datetime": "2020-12-12T22:38:32.125Z",
        |    "end_datetime": "2020-12-12T22:38:32.327Z",
        |    "created": "2020-12-13T01:48:13.725Z",
        |    "updated": "2020-12-13T01:48:13.725Z",
        |    "platform": "cool_sat2",
        |    "instruments": [
        |      "cool_sensor_v2"
        |    ],
        |    "constellation": "ion",
        |    "mission": "collection 5625",
        |    "gsd": 0.512
        |  },
        |  "collection": "simple-collection",
        |  "links": [
        |    {
        |      "rel": "collection",
        |      "href": "./collection.json",
        |      "type": "application/json",
        |      "title": "Simple Example Collection"
        |    },
        |    {
        |      "rel": "root",
        |      "href": "./collection.json",
        |      "type": "application/json",
        |      "title": "Simple Example Collection"
        |    },
        |    {
        |      "rel": "parent",
        |      "href": "./collection.json",
        |      "type": "application/json",
        |      "title": "Simple Example Collection"
        |    },
        |    {
        |      "rel": "alternate",
        |      "type": "text/html",
        |      "href": "https://remotedata.io/catalog/20201211_223832_CS2_item2/index.html",
        |      "title": "HTML version of this STAC Item"
        |    }
        |  ],
        |  "assets": {
        |    "analytic": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item2_analytic.tif",
        |      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        |      "title": "4-Band Analytic",
        |      "roles": [
        |        "data"
        |      ]
        |    },
        |    "thumbnail": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item2.jpg",
        |      "title": "Thumbnail",
        |      "type": "image/png",
        |      "roles": [
        |        "thumbnail"
        |      ]
        |    },
        |    "visual": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item2.tif",
        |      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        |      "title": "3-Band Visual",
        |      "roles": [
        |        "visual"
        |      ]
        |    },
        |    "udm": {
        |      "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2_item2_analytic_udm.tif",
        |      "title": "Unusable Data Mask",
        |      "type": "image/tiff; application=geotiff"
        |    },
        |    "json-metadata": {
        |      "href": "https://remotedata.io/catalog/20201211_223832_CS2_item2/extended-metadata.json",
        |      "title": "Extended Metadata",
        |      "type": "application/json",
        |      "roles": [
        |        "metadata"
        |      ]
        |    },
        |    "ephemeris": {
        |      "href": "https://cool-sat.com/catalog/20201211_223832_CS2_item2/20201211_223832_CS2_item2.EPH",
        |      "title": "Satellite Ephemeris Metadata"
        |    }
        |  }
        |}
  """.stripMargin
    val item2Path = new Path("/tmp/item2.json")
    val item2Writer = new PrintWriter(new File(item2Path.toString))
    item2Writer.write(item2Json)
    item2Writer.close()

    // Load the STAC collection JSON
    val opts = Map("path" -> "file:///tmp/collection.json")
    val stacCollectionJsonString = StacUtils.loadStacCollectionToJson(opts)
    val outputPath = "/tmp/output.json"

    // Call the function to process the STAC collection
    saveStacCollection(stacCollectionJsonString, outputPath)

    // Verify the output file
    val outputFile = new File(outputPath)
    assert(outputFile.exists())

    val outputContent = Source.fromFile(outputFile).getLines().mkString("\n")
    assert(outputContent.contains("item1"))
    assert(outputContent.contains("item2"))

    // Clean up temporary files
    fs.delete(stacCollectionPath, false)
    fs.delete(item1Path, false)
    fs.delete(item2Path, false)
    outputFile.delete()
  }

  test(
    "processStacCollection should process STAC collection with mixed 'item' and 'items' rels and save features to output file") {
    val spark = SparkSession.builder().master("local").appName("StacUtilsTest").getOrCreate()
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Create a temporary STAC collection JSON file
    val stacCollectionJson =
      """
        |{
        |  "stac_version": "1.0.0",
        |  "id": "sample-collection",
        |  "description": "A sample STAC collection",
        |  "links": [
        |    {"rel": "item", "href": "file:///tmp/item1.json"},
        |    {"rel": "items", "href": "file:///tmp/items.json"}
        |  ]
        |}
      """.stripMargin
    val stacCollectionPath = new Path("/tmp/collection.json")
    val stacCollectionWriter = new PrintWriter(new File(stacCollectionPath.toString))
    stacCollectionWriter.write(stacCollectionJson)
    stacCollectionWriter.close()

    // Create temporary item JSON files
    val item1Json =
      """
        |{
        |  "type": "Feature",
        |  "id": "item1",
        |  "geometry": {
        |    "type": "Point",
        |    "coordinates": [100.0, 0.0]
        |  },
        |  "properties": {
        |    "title": "Item 1"
        |  }
        |}
      """.stripMargin
    val item1Path = new Path("/tmp/item1.json")
    val item1Writer = new PrintWriter(new File(item1Path.toString))
    item1Writer.write(item1Json)
    item1Writer.close()

    val itemsJson =
      """
        |{
        |  "type": "FeatureCollection",
        |  "features": [
        |    {
        |      "type": "Feature",
        |      "id": "item2",
        |      "geometry": {
        |        "type": "Point",
        |        "coordinates": [101.0, 1.0]
        |      },
        |      "properties": {
        |        "title": "Item 2"
        |      }
        |    },
        |    {
        |      "type": "Feature",
        |      "id": "item3",
        |      "geometry": {
        |        "type": "Point",
        |        "coordinates": [102.0, 2.0]
        |      },
        |      "properties": {
        |        "title": "Item 3"
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val itemsPath = new Path("/tmp/items.json")
    val itemsWriter = new PrintWriter(new File(itemsPath.toString))
    itemsWriter.write(itemsJson)
    itemsWriter.close()

    // Load the STAC collection JSON
    val opts = Map("path" -> "file:///tmp/collection.json")
    val stacCollectionJsonString = StacUtils.loadStacCollectionToJson(opts)
    val outputPath = "/tmp/output.json"

    // Call the function to process the STAC collection
    saveStacCollection(stacCollectionJsonString, outputPath)

    // Verify the output file
    val outputFile = new File(outputPath)
    assert(outputFile.exists())

    val outputContent = Source.fromFile(outputFile).getLines().mkString("\n")
    assert(outputContent.contains("item1"))
    assert(outputContent.contains("item2"))
    assert(outputContent.contains("item3"))

    // Clean up temporary files
    fs.delete(stacCollectionPath, false)
    fs.delete(item1Path, false)
    fs.delete(itemsPath, false)
    outputFile.delete()
  }

  // Function to process STAC collection
  def saveStacCollection(stacCollectionJson: String, outputPath: String): Unit = {
    // Create the ObjectMapper
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Parse the STAC collection JSON
    val collection: JsonNode = mapper.readTree(stacCollectionJson)

    // Extract item and items links
    val itemLinks = collection.get("links").elements().asScala.filter { link =>
      val rel = link.get("rel").asText()
      rel == "item" || rel == "items"
    }

    // Open a writer for the output multiline JSON file
    val writer = new PrintWriter(new File(outputPath))

    try {
      // Iterate over each item link
      itemLinks.foreach { link =>
        val itemUrl = link.get("href").asText()

        // Fetch the item JSON
        val itemJson = Source.fromURL(itemUrl).mkString

        // Parse the item JSON
        val itemCollection: JsonNode = mapper.readTree(itemJson)

        // Check if the link is of type "items"
        if (link.get("rel").asText() == "items") {
          // Iterate over each feature in the item collection
          val features = itemCollection.get("features").elements().asScala
          features.foreach { feature =>
            // Write each feature JSON as a single line in the output file
            writer.println(mapper.writeValueAsString(feature))
          }
        } else {
          // Write the item JSON as a single line in the output file
          writer.println(mapper.writeValueAsString(itemCollection))
        }
      }
    } finally {
      // Close the writer
      writer.close()
    }
  }

  test("getFilterBBox with LeafFilter") {
    val queryWindow: Polygon =
      wktReader.read("POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))").asInstanceOf[Polygon]
    val leafFilter =
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow)
    val bbox = getFilterBBox(leafFilter)
    assert(bbox == "bbox=10.0%2C10.0%2C20.0%2C20.0")
  }

  test("getFilterBBox with AndFilter") {
    val queryWindow1: Polygon =
      wktReader.read("POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))").asInstanceOf[Polygon]
    val queryWindow2: Polygon =
      wktReader.read("POLYGON((30 30, 40 30, 40 40, 30 40, 30 30))").asInstanceOf[Polygon]
    val leafFilter1 =
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow1)
    val leafFilter2 =
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow2)
    val andFilter = GeoParquetSpatialFilter.AndFilter(leafFilter1, leafFilter2)
    val bbox = getFilterBBox(andFilter)
    assert(bbox == "bbox=10.0%2C10.0%2C40.0%2C40.0")
  }

  test("getFilterBBox with OrFilter") {
    val queryWindow1: Polygon =
      wktReader.read("POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))").asInstanceOf[Polygon]
    val queryWindow2: Polygon =
      wktReader.read("POLYGON((30 30, 40 30, 40 40, 30 40, 30 30))").asInstanceOf[Polygon]
    val leafFilter1 =
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow1)
    val leafFilter2 =
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow2)
    val orFilter = GeoParquetSpatialFilter.OrFilter(leafFilter1, leafFilter2)
    val bbox = getFilterBBox(orFilter)
    assert(bbox == "bbox=10.0%2C10.0%2C40.0%2C40.0")
  }

  test("getFilterTemporal with LessThanFilter") {
    val dateTime = LocalDateTime.parse("2025-03-07T00:00:00")
    val filter = TemporalFilter.LessThanFilter("timestamp", dateTime)
    val result = getFilterTemporal(filter)
    assert(result == "datetime=../2025-03-07T00:00:00.000Z")
  }

  test("getFilterTemporal with GreaterThanFilter") {
    val dateTime = LocalDateTime.parse("2025-03-06T00:00:00")
    val filter = TemporalFilter.GreaterThanFilter("timestamp", dateTime)
    val result = getFilterTemporal(filter)
    assert(result == "datetime=2025-03-06T00:00:00.000Z/..")
  }

  test("getFilterTemporal with EqualFilter") {
    val dateTime = LocalDateTime.parse("2025-03-06T00:00:00")
    val filter = TemporalFilter.EqualFilter("timestamp", dateTime)
    val result = getFilterTemporal(filter)
    assert(result == "datetime=2025-03-06T00:00:00.000Z/2025-03-06T00:00:00.000Z")
  }

  test("getFilterTemporal with AndFilter") {
    val dateTime1 = LocalDateTime.parse("2025-03-06T00:00:00")
    val dateTime2 = LocalDateTime.parse("2025-03-07T00:00:00")
    val filter1 = TemporalFilter.GreaterThanFilter("timestamp", dateTime1)
    val filter2 = TemporalFilter.LessThanFilter("timestamp", dateTime2)
    val andFilter = TemporalFilter.AndFilter(filter1, filter2)
    val result = getFilterTemporal(andFilter)
    assert(result == "datetime=2025-03-06T00:00:00.000Z/2025-03-07T00:00:00.000Z")
  }

  test("getFilterTemporal with OrFilter") {
    val dateTime1 = LocalDateTime.parse("2025-03-06T00:00:00")
    val dateTime2 = LocalDateTime.parse("2025-03-07T00:00:00")
    val filter1 = TemporalFilter.GreaterThanFilter("timestamp", dateTime1)
    val filter2 = TemporalFilter.LessThanFilter("timestamp", dateTime2)
    val orFilter = TemporalFilter.OrFilter(filter1, filter2)
    val result = getFilterTemporal(orFilter)
    assert(result == "datetime=2025-03-06T00:00:00.000Z/2025-03-07T00:00:00.000Z")
  }

  test("addFiltersToUrl with no filters") {
    val baseUrl = "http://example.com/stac"
    val result = StacUtils.addFiltersToUrl(baseUrl, None, None)
    assert(result == "http://example.com/stac")
  }

  test("addFiltersToUrl with spatial filter") {
    val baseUrl = "http://example.com/stac"
    val envelope = new Envelope(1.0, 2.0, 3.0, 4.0)
    val queryWindow = geometryFactory.toGeometry(envelope)
    val spatialFilter = Some(
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow))
    val result = StacUtils.addFiltersToUrl(baseUrl, spatialFilter, None)
    val expectedUrl = s"$baseUrl&bbox=1.0%2C3.0%2C2.0%2C4.0"
    assert(result == expectedUrl)
  }

  test("addFiltersToUrl with temporal filter") {
    val baseUrl = "http://example.com/stac"
    val temporalFilter = Some(
      TemporalFilter.GreaterThanFilter("timestamp", LocalDateTime.parse("2025-03-06T00:00:00")))
    val result = StacUtils.addFiltersToUrl(baseUrl, None, temporalFilter)
    val expectedUrl = s"$baseUrl&datetime=2025-03-06T00:00:00.000Z/.."
    assert(result == expectedUrl)
  }

  test("addFiltersToUrl with both spatial and temporal filters") {
    val baseUrl = "http://example.com/stac"
    val envelope = new Envelope(1.0, 2.0, 3.0, 4.0)
    val queryWindow = geometryFactory.toGeometry(envelope)
    val spatialFilter = Some(
      GeoParquetSpatialFilter.LeafFilter("geometry", SpatialPredicate.INTERSECTS, queryWindow))
    val temporalFilter = Some(
      TemporalFilter.GreaterThanFilter("timestamp", LocalDateTime.parse("2025-03-06T00:00:00")))
    val result = StacUtils.addFiltersToUrl(baseUrl, spatialFilter, temporalFilter)
    val expectedUrl = s"$baseUrl&bbox=1.0%2C3.0%2C2.0%2C4.0&datetime=2025-03-06T00:00:00.000Z/.."
    assert(result == expectedUrl)
  }
}
