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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, explode, expr}
import org.locationtech.jts.geom.{Geometry, MultiLineString, Point, Polygon}
import org.scalatest.BeforeAndAfterAll

import java.io.File

class geojsonIOTests extends TestBaseScala with BeforeAndAfterAll {
  val geojsondatalocation1: String = resourceFolder + "geojson/test1*"
  val geojsondatalocation2: String = resourceFolder + "geojson/geojson_feature-collection.json"
  val geojsondatalocation3: String = resourceFolder + "geojson/core-item.json"
  val geojsondatalocation4: String = resourceFolder + "geojson/test2.json"
  val geojsonoutputlocation: String = resourceFolder + "geojson/geojson_output/"

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(geojsonoutputlocation))

  describe("GeoJSON IO tests") {
    it("GeoJSON Test - Simple DataFrame writing and reading") {
      val df = sparkSession
        .range(0, 10)
        .toDF("id")
        .withColumn("geometry", expr("ST_Point(id, id)"))
        .withColumn("text", expr("concat('test', id)"))
      df.write
        .format("geojson")
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      // Read the GeoJSON back using JSON reader
      val schema = "type string, geometry string, properties struct<id:int, text:string>"
      val dfJson = sparkSession.read
        .schema(schema)
        .format("json")
        .load(geojsonoutputlocation + "/geojson_write.json")
      dfJson.collect().foreach { row =>
        assert(row.getAs("geometry").toString.startsWith("{\"type\":\"Point\""))
        assert(
          row.getAs[GenericRowWithSchema]("properties").getAs("text").toString.startsWith("test"))
      }

      // Read the GeoJSON back using the GeoJSON reader
      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      dfW.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        val properties = row.getAs[GenericRowWithSchema]("properties")
        properties.getAs("text").toString.startsWith("test")
      }
    }

    it("GeoJSON Test - Specifying geometry column other than geometry") {
      val df = sparkSession
        .range(0, 10)
        .toDF("id")
        .withColumn("point", expr("ST_Point(id, id)"))
        .withColumn("geom", expr("ST_MakeLine(ST_Point(id, id), ST_Point(id, id + 1))"))
        .withColumn("text", expr("concat('test', id)"))
      df.write
        .format("geojson")
        .option("geometry.column", "point")
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      // Read the GeoJSON back using JSON reader
      val schema =
        "type string, geometry string, properties struct<id:int, text:string, geom:string>"
      val dfJson = sparkSession.read
        .schema(schema)
        .format("json")
        .load(geojsonoutputlocation + "/geojson_write.json")
      dfJson.collect().foreach { row =>
        assert(row.getAs("geometry").toString.startsWith("{\"type\":\"Point\""))
        val properties = row.getAs[GenericRowWithSchema]("properties")
        assert(properties.getAs("text").toString.startsWith("test"))
        assert(properties.getAs("geom").toString.startsWith("LINESTRING"))
      }

      // Read the GeoJSON back using the GeoJSON reader
      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      dfW.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        val properties = row.getAs[GenericRowWithSchema]("properties")
        properties.getAs("text").toString.startsWith("test")
        properties.getAs("geom").toString.startsWith("LINESTRING")
      }
    }

    it("GeoJSON Test - Specifying geometry column in a nested struct column") {
      val df = sparkSession
        .range(0, 10)
        .toDF("id")
        .withColumn("text_outer", expr("concat('test_outer', id)"))
        .withColumn(
          "nested",
          expr("struct(id, concat('test_inner', id) AS text_inner, ST_Point(id, id) AS geom)"))
      df.write
        .format("geojson")
        .option("geometry.column", "nested.geom")
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      // Read the GeoJSON back using JSON reader
      val schema =
        "type string, geometry string, properties struct<text_outer:string, nested:struct<id:int, text_inner:string>>"
      val dfJson = sparkSession.read
        .schema(schema)
        .format("json")
        .load(geojsonoutputlocation + "/geojson_write.json")
      dfJson.collect().foreach { row =>
        assert(row.getAs("geometry").toString.startsWith("{\"type\":\"Point\""))
        val properties = row.getAs[GenericRowWithSchema]("properties")
        assert(properties.getAs("text_outer").toString.startsWith("test_outer"))
        val nested = properties.getAs[GenericRowWithSchema]("nested")
        assert(nested.getAs("text_inner").toString.startsWith("test_inner"))
      }

      // Read the GeoJSON back using the GeoJSON reader
      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      dfW.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        val properties = row.getAs[GenericRowWithSchema]("properties")
        properties.getAs("text_outer").toString.startsWith("test_outer")
        val nested = properties.getAs[GenericRowWithSchema]("nested")
        assert(nested.getAs("text_inner").toString.startsWith("test_inner"))
      }
    }

    it("GeoJSON Test - DataFrame containing properties column") {
      val df = sparkSession
        .range(0, 10)
        .toDF("id")
        .withColumn("point", expr("ST_Point(id, id)"))
        .withColumn("test_outer", expr("concat('test_outer', id)"))
        .withColumn("properties", expr("struct(id, concat('test', id) AS text)"))
      df.write
        .format("geojson")
        .option("geometry.column", "point")
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      // Read the GeoJSON back using JSON reader
      val schema =
        "type string, geometry string, test_outer string, properties struct<id:int, text:string>"
      val dfJson = sparkSession.read
        .schema(schema)
        .format("json")
        .load(geojsonoutputlocation + "/geojson_write.json")
      dfJson.collect().foreach { row =>
        assert(row.getAs("geometry").toString.startsWith("{\"type\":\"Point\""))
        assert(row.getAs[String]("test_outer").startsWith("test_outer"))
        val properties = row.getAs[GenericRowWithSchema]("properties")
        assert(properties.getAs("text").toString.startsWith("test"))
      }

      // Read the GeoJSON back using the GeoJSON reader
      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      dfW.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        assert(row.getAs[String]("test_outer").startsWith("test_outer"))
        val properties = row.getAs[GenericRowWithSchema]("properties")
        assert(properties.getAs("text").toString.startsWith("test"))
      }
    }

    it("GeoJSON Test - Read and Write multiline GeoJSON file") {
      val dfR =
        sparkSession.read.format("geojson").option("multiLine", true).load(geojsondatalocation1)
      val rowsR = dfR.collect()(0)

      assert((rowsR.getAs[GenericRowWithSchema]("assets") != null) == true)
      assert(rowsR.getAs[String]("type") == "Feature")
      assert(
        rowsR
          .getAs[GenericRowWithSchema]("properties")
          .getString(0) == "2020-12-12T01:48:13.725Z")
      assert(
        rowsR
          .getAs[GenericRowWithSchema]("properties")
          .getString(1) == "A sample STAC Item that includes examples of all common metadata")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(5) == "Core Item")
      assert(
        rowsR
          .getAs[Polygon]("geometry")
          .toString == "POLYGON ((172.91173669923782 1.3438851951615003, 172.95469614953714 1.3438851951615003, 172.95469614953714 1.3690476620161975, 172.91173669923782 1.3690476620161975, 172.91173669923782 1.3438851951615003))")

      dfR.write
        .format("geojson")
        .option("multiLine", true)
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      val rowsW = dfW.collect()(0)
      assert(
        rowsR.getAs[GenericRowWithSchema]("properties") == rowsW.getAs[GenericRowWithSchema](
          "properties"))
      assert(rowsR.getAs[Polygon]("geometry") == rowsW.getAs[Polygon]("geometry"))
      assert(rowsR.getAs[String]("type") == rowsW.getAs[String]("type"))
    }
    it("GeoJSON Test - Read and Write MultilineString geometry") {
      val dfR =
        sparkSession.read.format("geojson").option("multiLine", true).load(geojsondatalocation4)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("type") == "Feature")
      assert(
        rowsR
          .getAs[GenericRowWithSchema]("properties")
          .getString(0) == "2020-12-12T01:48:13.725Z")
      assert(
        rowsR
          .getAs[GenericRowWithSchema]("properties")
          .getString(1) == "A sample STAC Item that includes examples of all common metadata")
      assert(rowsR.getAs[GenericRowWithSchema]("properties").getString(5) == "Core Item")
      assert(
        rowsR
          .getAs[MultiLineString]("geometry")
          .toString == "MULTILINESTRING ((170 45, 180 45), (-180 45, -170 45))")

      dfR.write
        .format("geojson")
        .option("multiLine", true)
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      val rowsW = dfW.collect()(0)
      assert(
        rowsR.getAs[GenericRowWithSchema]("properties") == rowsW.getAs[GenericRowWithSchema](
          "properties"))
      assert(rowsR.getAs[MultiLineString]("geometry") == rowsW.getAs[MultiLineString]("geometry"))
      assert(rowsR.getAs[String]("type") == rowsW.getAs[String]("type"))
    }
    it("GeoJSON Test - feature collection test") {
      val dfR =
        sparkSession.read.format("geojson").option("multiLine", true).load(geojsondatalocation2)
      val rowsR = dfR.collect()(0)

      assert(rowsR.getAs[Seq[Row]]("features")(0).get(0).toString == "POINT (102 0.5)")
      assert(
        rowsR
          .getAs[Seq[Row]]("features")(1)
          .get(0)
          .toString == "LINESTRING (102 0, 103 1, 104 0, 105 1)")
      assert(
        rowsR
          .getAs[Seq[Row]]("features")(2)
          .get(0)
          .toString == "POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))")
      assert(
        rowsR
          .getAs[Seq[Row]]("features")(3)
          .get(0)
          .toString == "MULTILINESTRING ((170 45, 180 45), (-180 45, -170 45))")
      assert(
        rowsR
          .getAs[Seq[Row]]("features")(4)
          .get(0)
          .toString == "MULTIPOLYGON (((180 40, 180 50, 170 50, 170 40, 180 40)), ((-170 40, -170 50, -180 50, -180 40, -170 40)))")

      val df = dfR.select(explode(col("features")).alias("feature")).selectExpr("feature.*")
      df.write
        .format("geojson")
        .option("multiLine", true)
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")
      val rowsW = dfW.collect()
      assert(rowsW.length == 6)
      assert(rowsW(0).getAs("geometry").toString == "POINT (102 0.5)")
      assert(rowsW(1).getAs("geometry").toString == "LINESTRING (102 0, 103 1, 104 0, 105 1)")
      assert(
        rowsW(2).getAs("geometry").toString == "POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))")
      assert(
        rowsW(3)
          .getAs("geometry")
          .toString == "MULTILINESTRING ((170 45, 180 45), (-180 45, -170 45))")
      assert(
        rowsW(4)
          .getAs("geometry")
          .toString == "MULTIPOLYGON (((180 40, 180 50, 170 50, 170 40, 180 40)), ((-170 40, -170 50, -180 50, -180 40, -170 40)))")
    }
    it("GeoJSON Test - read and write single line STAC item") {
      val dfR = sparkSession.read.format("geojson").load(geojsondatalocation3)
      val rowsR = dfR.collect()(0)
      assert(rowsR.getAs[String]("type") == "Feature")
      assert(rowsR.getAs[String]("stac_version") == "1.0.0")
      assert(
        rowsR
          .getAs[Polygon]("geometry")
          .toString == "POLYGON ((172.91173669923782 1.3438851951615003, 172.95469614953714 1.3438851951615003, 172.95469614953714 1.3690476620161975, 172.91173669923782 1.3690476620161975, 172.91173669923782 1.3438851951615003))")

      dfR.write
        .format("geojson")
        .mode(SaveMode.Overwrite)
        .save(geojsonoutputlocation + "/geojson_write.json")

      val dfW =
        sparkSession.read.format("geojson").load(geojsonoutputlocation + "/geojson_write.json")

      val rowsW = dfW.collect()(0)
      assert(
        rowsR.getAs[GenericRowWithSchema]("assets") == rowsW.getAs[GenericRowWithSchema](
          "assets"))
      assert(rowsR.getAs[String]("stac_version") == rowsW.getAs[String]("stac_version"))
      assert(rowsR.getAs[Polygon]("geometry") == rowsW.getAs[Polygon]("geometry"))
    }
  }
}
