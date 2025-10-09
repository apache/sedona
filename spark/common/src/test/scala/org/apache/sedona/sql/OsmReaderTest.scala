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
package org.apache.sedona.sql

import io.minio.{ListObjectsArgs, MakeBucketArgs, MinioClient}
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.MinIOContainer

import java.io.FileInputStream

case class Node(id: Long, latitude: Double, longitude: Double, tags: Map[String, String])

class OsmReaderTest extends TestBaseScala with Matchers {
  val monacoPath: String = resourceFolder + "osmpbf/monaco-latest.osm.pbf"
  val densePath: String = resourceFolder + "osmpbf/dense.pbf"
  val nodesPath: String = resourceFolder + "osmpbf/nodes.pbf"
  val planetOsmPath: String = resourceFolder + "osmpbf/planetosm.pbf"

  import sparkSession.implicits._

  describe("Loading OSM data") {
    it("should load OSM data from local file") {
      sparkSession.read
        .format("osmpbf")
        .load(monacoPath)
        .createOrReplaceTempView("osm")

      val cnt = sparkSession
        .sql("SELECT * FROM osm")
        .count()

      assert(cnt > 0)
    }

    it("should be able to process planet osm files") {
      val numberOfUniques = sparkSession.read
        .format("osmpbf")
        .load(planetOsmPath)
        .dropDuplicates("id")
        .count

      val numberOfElements = sparkSession.read
        .format("osmpbf")
        .load(planetOsmPath)
        .count

      numberOfUniques shouldBe 64000
      numberOfElements shouldBe 64000

      val idsToVerify = Seq(64949611, 64955092, 64949580, 64949694, 64949868, 64958096, 64958295)

      val elementsCount = sparkSession.read
        .format("osmpbf")
        .load(planetOsmPath)
        .where($"id".isin(idsToVerify: _*))
        .count

      elementsCount shouldBe idsToVerify.length
    }

    it("should parse normal nodes") {
      sparkSession.read
        .format("osmpbf")
        .load(nodesPath)
        .select("id", "location.*", "tags")
        .selectExpr(
          "id",
          "ROUND(latitude, 2) AS latitude",
          "ROUND(longitude, 2) AS longitude",
          "tags")
        .as[Node]
        .collect() should contain theSameElementsAs Array(
        Node(1002, 48.86, 2.35, Map("amenity" -> "cafe", "name" -> "Cafe de Paris")),
        Node(1003, 30.12, 22.23, Map("amenity" -> "bakery", "name" -> "Delicious Pastries")),
        Node(1001, 52.52, 13.41, Map("amenity" -> "restaurant", "name" -> "Curry 36")))
    }

    it("should parse dense nodes") {
      sparkSession.read
        .format("osmpbf")
        .load(densePath)
        .select("id", "location.*", "tags")
        .selectExpr(
          "id",
          "ROUND(latitude, 2) AS latitude",
          "ROUND(longitude, 2) AS longitude",
          "tags")
        .as[Node]
        .collect() should contain theSameElementsAs Array(
        Node(1002, 48.86, 2.35, Map("amenity" -> "cafe", "name" -> "Cafe de Paris")),
        Node(1003, 30.12, 22.23, Map("amenity" -> "bakery", "name" -> "Delicious Pastries")),
        Node(1001, 52.52, 13.41, Map("amenity" -> "restaurant", "name" -> "Curry 36")))
    }

    it("should be able to read from osm file on s3") {
      val container = new MinIOContainer("minio/minio:latest")

      container.start()

      val minioClient = createMinioClient(container)
      val makeBucketRequest = MakeBucketArgs
        .builder()
        .bucket("sedona-osm")
        .build()

      minioClient.makeBucket(makeBucketRequest)

      adjustSparkSession(sparkSessionMinio, container)

      val inputPath: String = prepareFile("monaco-latest.osm.pbf", monacoPath, minioClient)

      minioClient
        .listObjects(ListObjectsArgs.builder().bucket("sedona-osm").build())
        .forEach(obj => println(obj.get().objectName()))

      sparkSessionMinio.read
        .format("osmpbf")
        .load(inputPath)
        .createOrReplaceTempView("osm")

      val cnt = sparkSessionMinio
        .sql("SELECT * FROM osm")
        .count()

      assert(cnt > 0)
    }

    it("should properly assign values") {
      val osmData = sparkSession.read
        .format("osmpbf")
        .load(monacoPath)

      osmData
        .groupBy("kind")
        .count()
        .select("count")
        .collect()
        .map(_.getLong(0)) shouldEqual (Array(309, 5777, 39587))

      osmData
        .filter("tags is not null")
        .count() shouldEqual (45673)

      osmData
        .selectExpr("min(location.longitude)", "max(location.latitude)")
        .collect()
        .flatMap(row => Array(row.get(0), row.get(1))) shouldEqual (Array(7.2081882, 43.7594835))

      osmData
        .where("id == 4098197")
        .select("tags")
        .as[Map[String, String]]
        .head() shouldEqual (Map(
        "name" -> "Boulevard d'Italie",
        "surface" -> "asphalt",
        "highway" -> "tertiary",
        "lit" -> "yes",
        "lanes" -> "2",
        "smoothness" -> "excellent"))

      // make sure the nodes match with refs
      val nodes = osmData.where("kind == 'node'")
      val ways = osmData.where("kind == 'way'")
      val relations = osmData.where("kind == 'relation'")

      ways
        .selectExpr("explode(refs) AS ref")
        .alias("w")
        .join(nodes, col("w.ref") === nodes("id"))
        .count() shouldEqual (47812)

      ways
        .selectExpr("explode(refs) AS ref", "id")
        .alias("w")
        .join(nodes, col("w.ref") === nodes("id"))
        .groupBy("w.id")
        .count()
        .count() shouldEqual (ways.count())

      relations
        .selectExpr("explode(refs) AS ref", "id")
        .alias("r")
        .join(nodes, col("r.ref") === nodes("id"))
        .groupBy("r.id")
        .count()
        .count() shouldEqual (162)

      relations
        .selectExpr("explode(refs) AS ref", "id")
        .alias("r")
        .join(ways, col("r.ref") === ways("id"))
        .groupBy("r.id")
        .count()
        .count() shouldEqual (261)

      relations
        .selectExpr("explode(refs) AS ref", "id")
        .alias("r1")
        .join(relations.as("r2"), col("r1.ref") === col("r2.id"))
        .groupBy("r1.id")
        .count()
        .count() shouldEqual (54)

      val relationsList = relations
        .where("id == 7360676")
        .selectExpr("refs")
        .as[Seq[String]]
        .collect()
        .head

      val expectedRelationsList =
        Seq("252356770", "503642591", "4939150452", "1373711177", "4939150459", "503642592")

      relationsList.length shouldEqual (expectedRelationsList.length)
      relationsList should contain theSameElementsAs expectedRelationsList
    }

    it("should not lose precision due to float to double conversion") {
      // Test for accuracy loss bug in NodeExtractor and DenseNodeExtractor
      val node = sparkSession.read
        .format("osmpbf")
        .load(nodesPath)
        .where("kind == 'node'")
        .select("location.latitude", "location.longitude")
        .first()

      val latitude = node.getDouble(0)
      val longitude = node.getDouble(1)

      // Check that coordinates maintain precision beyond float limits
      val latAsFloat = latitude.toFloat
      val lonAsFloat = longitude.toFloat

      // If there's a difference, it indicates potential precision loss from float arithmetic
      val latDiff = Math.abs(latitude - latAsFloat)
      val lonDiff = Math.abs(longitude - lonAsFloat)

      // For high-precision coordinates, there should be some difference
      (latDiff > 1e-10 || lonDiff > 1e-10) shouldBe true
    }
  }

  private def prepareFile(name: String, path: String, minioClient: MinioClient): String = {
    val fis = new FileInputStream(path);
    putFileIntoBucket("sedona-osm", name, fis, minioClient)

    s"s3a://sedona-osm/$name"
  }

}
