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

import java.nio.{ByteBuffer, ByteOrder}
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
  Seq(false, true).foreach { allEmpty =>
    test(
      s"GeoParquet preserves native XYZ and projects M with matching metadata (allEmpty=$allEmpty)") {
      def isoWkb(kind: Int, coordinates: Double*): Array[Byte] = {
        val line = kind % 1000 == 2
        val dimension = if (kind / 1000 == 3) 4 else if (kind / 1000 == 0) 2 else 3
        val buffer = ByteBuffer
          .allocate(5 + (if (line) 4 else 0) + coordinates.size * 8)
          .order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(1.toByte).putInt(kind)
        if (line) buffer.putInt(coordinates.size / dimension)
        coordinates.foreach(buffer.putDouble)
        buffer.array()
      }
      val samples = Seq(
        isoWkb(1, 10, 20) -> isoWkb(1, 10, 20),
        isoWkb(1001, 10, 20, 30) -> isoWkb(1001, 10, 20, 30),
        isoWkb(1001, Double.NaN, Double.NaN, Double.NaN) ->
          isoWkb(1001, Double.NaN, Double.NaN, Double.NaN),
        isoWkb(1002) -> isoWkb(1002),
        isoWkb(1002, 1, 2, 3, 3, 4, 5) ->
          isoWkb(1002, 1, 2, 3, 3, 4, 5),
        isoWkb(2001, 10, 20, 99) -> isoWkb(1, 10, 20),
        isoWkb(3001, 10, 20, 30, 99) -> isoWkb(1001, 10, 20, 30),
        isoWkb(3002) -> isoWkb(1002))
      val reader = org.datasyslab.jts.io.WKBReader.forDeclaredDimensions()
      val selected = samples.filter { case (input, _) => !allEmpty || reader.read(input).isEmpty }
      val directory = Files.createTempDirectory("sedona-native-dimensions").toFile
      try {
        val query = selected.zipWithIndex
          .map { case ((input, _), id) =>
            val hex = org.locationtech.jts.io.WKBWriter.toHex(input)
            s"SELECT $id AS id, ST_GeomFromWKB(unhex('$hex'), 4326) AS geometry"
          }
          .mkString(" UNION ALL ")
        val path = new java.io.File(directory, "data").getAbsolutePath
        spark.sql(query).coalesce(1).write.format("geoparquet").save(path)
        val raw = spark.read.parquet(path).orderBy("id").collect()
        val restored = spark.read.format("geoparquet").load(path)
        assert(restored.schema("geometry").dataType == GeometryType(4326))
        val geometries = restored.orderBy("id").selectExpr("ST_AsBinary(geometry)").collect()
        selected.zipWithIndex.foreach { case ((_, expected), id) =>
          Seq(raw(id).getAs[Array[Byte]](1), geometries(id).getAs[Array[Byte]](0)).foreach {
            actual =>
              assert(
                org.apache.sedona.common.utils.GeometryEquality
                  .equalsIdentical(reader.read(actual), reader.read(expected)),
                s"coordinate layout or ordinates changed for row $id")
          }
        }
        val file = new java.io.File(path).listFiles().find(_.getName.endsWith(".parquet")).get
        val parquetReader = org.apache.parquet.hadoop.ParquetFileReader.open(
          org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toURI),
            new org.apache.hadoop.conf.Configuration()))
        val metadata =
          try {
            org.json4s.jackson.parseJson(
              parquetReader.getFooter.getFileMetaData.getKeyValueMetaData.get("geo")) \
              "columns" \ "geometry"
          } finally parquetReader.close()
        implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
        val expectedTypes =
          if (allEmpty) Set("Point Z", "LineString Z")
          else Set("Point", "Point Z", "LineString Z")
        assert((metadata \ "geometry_types").extract[Seq[String]].toSet == expectedTypes)
        if (allEmpty) assert((metadata \ "bbox") == org.json4s.JNothing)
        else assert((metadata \ "bbox").extract[Seq[Double]] == Seq(1, 2, 10, 20))
      } finally { org.apache.commons.io.FileUtils.deleteDirectory(directory) }
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
