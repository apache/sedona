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

import java.io.File
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{GeometryType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class NativeGeoParquetCrsSuite extends FunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val point = "0101000000000000000000F03F0000000000000040"
  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("native-geoparquet-crs")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.geospatial.enabled", "true")
      .getOrCreate()
  }
  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally { super.afterAll() }
  }
  private def withPath(run: String => Unit): Unit = {
    val directory = Files.createTempDirectory("native-geoparquet-crs").toFile
    try { run(new File(directory, "data").getAbsolutePath) }
    finally { FileUtils.deleteDirectory(directory) }
  }
  private def pointExpression(srid: Int): String = s"ST_GeomFromWKB(unhex('$point'), $srid)"

  test("fixed geometry CRS survives all-null and empty files") {
    for (srid <- Seq(0, 4326, 3857); empty <- Seq(false, true)) withPath { path =>
      val where = if (empty) " WHERE false" else ""
      spark
        .sql(s"SELECT CAST(NULL AS GEOMETRY($srid)) AS geometry$where")
        .coalesce(1)
        .write
        .format("geoparquet")
        .save(path)
      val result = spark.read.format("geoparquet").load(path)
      assert(result.schema("geometry").dataType == GeometryType(srid))
      assert(result.count() == (if (empty) 0 else 1))
    }
  }

  test("native ANY writes are rejected even when observed values share a CRS") {
    for (values <- Seq("(4326), (4326)", "(4326), (3857)")) withPath { path =>
      val source = spark.sql(
        s"SELECT ST_GeomFromWKB(unhex('$point'), srid) AS geometry FROM VALUES $values AS t(srid)")
      assert(source.schema("geometry").dataType == GeometryType("ANY"))
      intercept[Exception] { source.write.format("geoparquet").save(path) }
    }
  }

  test("native CRS options cannot relabel a fixed geometry column") {
    withPath { path =>
      val source = spark.sql(s"SELECT ${pointExpression(3857)} AS geometry")
      intercept[Exception] {
        source.write.format("geoparquet").option("geoparquet.crs", "").save(path)
      }
    }
  }

  test(
    "every file's CRS is checked with mergeSchema disabled or enabled, including null-only files") {
    for (merge <- Seq(false, true); nullOnly <- Seq(false, true)) withPath { path =>
      val first = if (nullOnly) "CAST(NULL AS GEOMETRY(4326))" else pointExpression(4326)
      spark.sql(s"SELECT $first AS geometry").coalesce(1).write.format("geoparquet").save(path)
      spark
        .sql(s"SELECT ${pointExpression(3857)} AS geometry")
        .coalesce(1)
        .write
        .mode("append")
        .format("geoparquet")
        .save(path)
      intercept[Exception] {
        spark.read.format("geoparquet").option("mergeSchema", merge).load(path).collect()
      }
    }
  }

  test("fixed schema mismatch is rejected even for an all-null source column") {
    withPath { path =>
      spark
        .sql("SELECT CAST(NULL AS GEOMETRY(3857)) AS geometry")
        .write
        .format("geoparquet")
        .save(path)
      val schema = StructType(Seq(StructField("geometry", GeometryType(4326))))
      intercept[Exception] { spark.read.schema(schema).format("geoparquet").load(path).collect() }
    }
  }

  test("unrepresentable CRS is rejected natively while explicit UDT behavior remains available") {
    withPath { path =>
      val geometry = new org.locationtech.jts.io.WKTReader().read("POINT (1 2)")
      val schema = StructType(Seq(StructField("geometry", GeometryUDT())))
      spark
        .createDataFrame(java.util.Arrays.asList(Row(geometry)), schema)
        .write
        .format("geoparquet")
        .option("geoparquet.crs", """{"name":"custom CRS"}""")
        .save(path)
      intercept[Exception] { spark.read.format("geoparquet").load(path).collect() }
      val legacy =
        spark.read.schema(schema).format("geoparquet").load(path).head().getAs[Geometry](0)
      assert(legacy.equalsExact(geometry))
      assert(legacy.getSRID == 0)
    }
  }

  test("separate geometry columns may use different fixed CRSs") {
    withPath { path =>
      spark
        .sql(s"SELECT ${pointExpression(3857)} AS projected, ${pointExpression(4326)} AS angular")
        .write
        .format("geoparquet")
        .save(path)
      val result = spark.read.format("geoparquet").load(path)
      assert(result.schema("projected").dataType == GeometryType(3857))
      assert(result.schema("angular").dataType == GeometryType(4326))
      assert(
        result.selectExpr("ST_SRID(projected)", "ST_SRID(angular)").head() == Row(3857, 4326))
    }
  }

  test("an explicit scalar-only schema does not convert an unused geometry CRS") {
    withPath { path =>
      val geometry = new org.locationtech.jts.io.WKTReader().read("POINT (1 2)")
      val schema = StructType(
        Seq(
          StructField("id", org.apache.spark.sql.types.IntegerType),
          StructField("geometry", GeometryUDT())))
      spark
        .createDataFrame(java.util.Arrays.asList(Row(7, geometry)), schema)
        .write
        .format("geoparquet")
        .option("geoparquet.crs", """{"name":"custom CRS"}""")
        .save(path)
      val scalarSchema = StructType(Seq(schema("id")))
      assert(
        spark.read.schema(scalarSchema).format("geoparquet").load(path).collect().toSeq ==
          Seq(Row(7)))
    }
  }

  test("explicit UDT and native ANY schemas retain per-file CRSs") {
    withPath { path =>
      Seq(4326, 3857).foreach { srid =>
        spark
          .sql(s"SELECT ${pointExpression(srid)} AS geometry")
          .coalesce(1)
          .write
          .mode("append")
          .format("geoparquet")
          .save(path)
      }
      val schema = StructType(Seq(StructField("geometry", GeometryUDT())))
      val values = spark.read.schema(schema).format("geoparquet").load(path).collect()
      assert(values.length == 2)
      assert(values.map(_.getAs[Geometry](0).getSRID).toSet == Set(4326, 3857))
      val anySchema = StructType(Seq(StructField("geometry", GeometryType("ANY"))))
      val nativeSrids = spark.read
        .schema(anySchema)
        .format("geoparquet")
        .load(path)
        .selectExpr("ST_SRID(geometry)")
        .collect()
        .map(_.getInt(0))
      assert(nativeSrids.length == 2)
      assert(nativeSrids.toSet == Set(4326, 3857))
    }
  }
}
