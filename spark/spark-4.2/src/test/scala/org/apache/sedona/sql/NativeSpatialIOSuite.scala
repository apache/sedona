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

import java.nio.file.Files
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.GeometryType
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class NativeSpatialIOSuite extends FunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val pointWkb = "0101000000000000000000F03F0000000000000040"
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("native-spatial-io")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.geospatial.enabled", "true")
      .getOrCreate()
  }
  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally { super.afterAll() }
  }

  Seq("geoparquet", "geojson").foreach { format =>
    test(s"$format accepts native geometry and returns native geometry including nulls") {
      val directory = Files.createTempDirectory(s"sedona-native-$format").toFile
      val path = new java.io.File(directory, "data").getAbsolutePath
      try {
        // Use Spark's built-ins, without registering Sedona's SQL implementations.
        val srid = if (format == "geoparquet") 3857 else 4326
        val input = spark.sql(
          s"SELECT 1 AS id, ST_GeomFromWKB(unhex('$pointWkb'), $srid) AS geometry UNION ALL SELECT 2, CAST(NULL AS GEOMETRY($srid))")
        input.coalesce(1).write.format(format).save(path)
        val output = spark.read.format(format).load(path)
        assert(output.schema("geometry").dataType.isInstanceOf[GeometryType])
        if (format == "geoparquet") {
          assert(output.schema("geometry").dataType == GeometryType(3857))
        }
        output.createOrReplaceTempView("native_io_result")
        val geometries = spark
          .sql("SELECT hex(ST_AsBinary(geometry)), ST_Srid(geometry) FROM native_io_result WHERE geometry IS NOT NULL")
          .collect()
        assert(geometries.length == 1)
        assert(geometries.head.getString(0) == pointWkb)
        // GeoJSON has no CRS metadata; its existing reader uses SRID 0.
        assert(geometries.head.getInt(1) == (if (format == "geoparquet") 3857 else 0))
        assert(output.filter("geometry IS NULL").count() == 1)
        // An explicit fixed-SRID schema must reject incompatible file values, not mislabel them.
        val wrongSchema = org.apache.spark.sql.types.StructType(output.schema.fields.map {
          case field if field.name == "geometry" => field.copy(dataType = GeometryType(4269))
          case field => field
        })
        val error = intercept[Exception] {
          spark.read.schema(wrongSchema).format(format).load(path).collect()
        }
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toSeq
        assert(causes.exists(cause =>
          Option(cause.getMessage).exists(message =>
            message.contains("SRID") && message.contains("4269"))))
      } finally {
        spark.catalog.dropTempView("native_io_result")
        org.apache.commons.io.FileUtils.deleteDirectory(directory)
      }
    }
  }
  test("Spider exposes native geometry values") {
    val result = spark.read.format("spider").option("N", "4").option("numPartitions", "1").load()
    assert(result.schema("geometry").dataType.isInstanceOf[GeometryType])
    val geometries = result.selectExpr("ST_AsBinary(geometry)", "ST_Srid(geometry)").collect()
    assert(geometries.length == 4)
    assert(geometries.forall(row => row.getAs[Array[Byte]](0).nonEmpty && row.getInt(1) == 0))
  }

  Seq(
    ("shapefile", "shapefiles/gis_osm_pois_free_1", Map.empty[String, String]),
    ("geopackage", "geopackage/example.gpkg", Map("tableName" -> "point1")),
    ("stac", "datasource_stac/collection.json", Map.empty[String, String])).foreach {
    case (format, resource, options) =>
      test(s"$format reader returns native values usable by Spark built-ins") {
        val path =
          new java.io.File(getClass.getClassLoader.getResource(resource).toURI).getAbsolutePath
        val result = spark.read.format(format).options(options).load(path)
        val geometryField = result.schema.fields.find(_.dataType.isInstanceOf[GeometryType]).get
        val rows = result
          .selectExpr(
            s"ST_AsBinary(`${geometryField.name}`)",
            s"ST_Srid(`${geometryField.name}`)")
          .limit(4)
          .collect()
        assert(rows.nonEmpty)
        assert(rows.exists(row => !row.isNullAt(0) && row.getAs[Array[Byte]](0).nonEmpty))
      }
  }

  Seq("geoparquet", "geojson").foreach { format =>
    test(s"$format also accepts existing GeometryUDT data") {
      val directory = Files.createTempDirectory(s"sedona-legacy-$format").toFile
      try {
        val geometry = new org.locationtech.jts.io.WKTReader().read("POINT (1 2)")
        geometry.setSRID(4326)
        val schema = org.apache.spark.sql.types.StructType(Seq(org.apache.spark.sql.types
          .StructField("geometry", org.apache.spark.sql.sedona_sql.UDT.GeometryUDT())))
        val input = spark.createDataFrame(
          java.util.Arrays.asList(org.apache.spark.sql.Row(geometry)),
          schema)
        val path = new java.io.File(directory, "data").getAbsolutePath
        input.write.format(format).save(path)
        val result = spark.read.format(format).load(path)
        assert(result.schema("geometry").dataType.isInstanceOf[GeometryType])
        assert(result.selectExpr("hex(ST_AsBinary(geometry))").head().getString(0) == pointWkb)
        if (format == "geojson") {
          val legacy = spark.read.schema(schema).format(format).load(path)
          assert(legacy.head().getAs[org.locationtech.jts.geom.Geometry](0).equalsExact(geometry))
        }
      } finally { org.apache.commons.io.FileUtils.deleteDirectory(directory) }
    }
  }

}
