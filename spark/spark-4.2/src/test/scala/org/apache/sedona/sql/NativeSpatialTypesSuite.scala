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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{GeographyType, GeometryType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** Verifies the Spark contract used by Sedona's native spatial type adapter. */
class NativeSpatialTypesSuite extends FunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val pointWkb = "0101000000000000000000F03F0000000000000040"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // No Sedona extensions or registration: these must be Spark's own implementations.
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("native-spatial-types")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "")
      .config("spark.sql.geospatial.enabled", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }
  }

  test("Spark registers the five native spatial functions before Sedona registration") {
    Seq("st_asbinary", "st_geomfromwkb", "st_geogfromwkb", "st_srid", "st_setsrid")
      .foreach { name =>
        val info = spark.catalog.getFunction(name)
        assert(info.className.startsWith("org.apache.spark.sql.catalyst.expressions."))
        assert(!info.className.contains("sedona"))
      }
  }

  test("native geometry and geography survive shuffle with their WKB and SRID") {
    val values = spark
      .sql(s"""
      SELECT ST_GeomFromWKB(unhex('$pointWkb'), 3857) AS geom,
             ST_GeogFromWKB(unhex('$pointWkb')) AS geog
    """)
      .repartition(2)
    assert(values.schema("geom").dataType.isInstanceOf[GeometryType])
    assert(values.schema("geog").dataType.isInstanceOf[GeographyType])
    values.createOrReplaceTempView("native_spatial_round_trip")
    try {
      val row = spark
        .sql("""
        SELECT hex(ST_AsBinary(geom)), ST_Srid(geom),
               hex(ST_AsBinary(geog)), ST_Srid(geog),
               ST_Srid(ST_SetSrid(geom, 4326))
        FROM native_spatial_round_trip
      """)
        .head()
      assert(row.getString(0) == pointWkb)
      assert(row.getInt(1) == 3857)
      assert(row.getString(2) == pointWkb)
      assert(row.getInt(3) == 4326)
      assert(row.getInt(4) == 4326)
    } finally {
      spark.catalog.dropTempView("native_spatial_round_trip")
    }
  }
}
