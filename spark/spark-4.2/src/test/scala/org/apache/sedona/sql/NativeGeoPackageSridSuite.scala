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
import java.sql.DriverManager
import org.apache.sedona.sql.datasources.geopackage.transform.GeometryReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.GeometryType
import org.locationtech.jts.io.WKBReader
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class NativeGeoPackageSridSuite extends FunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val point = WKBReader.hexToBytes("0101000000000000000000F03F0000000000000040")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("native-geopackage-srid")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.geospatial.enabled", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally { super.afterAll() }
  }

  private def blob(srid: Int): Array[Byte] = ByteBuffer
    .allocate(8 + point.length)
    .order(ByteOrder.LITTLE_ENDIAN)
    .put('G'.toByte)
    .put('P'.toByte)
    .put(0.toByte)
    .put(1.toByte)
    .putInt(srid)
    .put(point)
    .array()

  private def invalidSrid(error: Throwable): Boolean =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .exists(cause =>
        Option(cause.getMessage).exists(message =>
          message.contains("ST_INVALID_SRID_VALUE") && message.contains("-1")))

  test("GeoPackage undefined SRID is preserved by explicit UDT utilities") {
    val bytes = blob(-1)
    val explicit = GeometryUDT.deserialize(GeometryReader.extractGeometry(bytes, GeometryUDT()))
    assert(explicit.getSRID == -1)
    assert(explicit.getFactory.getSRID == -1)
    assert(explicit.toText == "POINT (1 2)")
    assert(GeometryUDT.deserialize(GeometryReader.extractWKB(bytes)).getSRID == -1)
    assert(GeometryReader.extractGeometry(null) == null)
    assert(invalidSrid(intercept[Exception](GeometryReader.extractGeometry(bytes))))
    val nativeZero = GeometryReader.extractGeometry(blob(0))
    assert(SpatialTypeSupport.deserializeGeometry(nativeZero, GeometryType("ANY")).getSRID == 0)
  }

  test("GeoPackage native reader rejects undefined SRID without relabeling it") {
    val directory = Files.createTempDirectory("sedona-native-gpkg-srid").toFile
    val file = new java.io.File(directory, "undefined.gpkg")
    val fixture = getClass.getClassLoader.getResource("geopackage/example.gpkg")
    Files.copy(new java.io.File(fixture.toURI).toPath, file.toPath)
    val connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath)
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(
          "INSERT INTO gpkg_contents " +
            "(table_name, data_type, identifier, srs_id) VALUES " +
            "('native_unknown_crs', 'features', 'native_unknown_crs', -1)")
        statement.execute(
          "INSERT INTO gpkg_geometry_columns VALUES " +
            "('native_unknown_crs', 'geometry', 'POINT', -1, 0, 0)")
        statement.execute(
          "CREATE TABLE native_unknown_crs (id INTEGER PRIMARY KEY, geometry POINT)")
      } finally statement.close()
      val insert = connection.prepareStatement("INSERT INTO native_unknown_crs VALUES (1, ?)")
      try { insert.setBytes(1, blob(-1)); insert.executeUpdate() }
      finally insert.close()
    } finally connection.close()

    try {
      val frame = spark.read
        .format("geopackage")
        .option("tableName", "native_unknown_crs")
        .load(file.getAbsolutePath)
      assert(frame.schema("geometry").dataType == GeometryType("ANY"))
      assert(invalidSrid(intercept[Exception](frame.select("geometry").collect())))
    } finally org.apache.commons.io.FileUtils.deleteDirectory(directory)
  }
}
