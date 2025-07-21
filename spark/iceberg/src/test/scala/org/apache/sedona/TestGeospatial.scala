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
package org.apache.sedona

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class TestGeospatial extends AnyFunSuite with BeforeAndAfterAll {

  private var warehouse: File = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouse = File.createTempFile("warehouse", null)
    warehouse.delete()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    warehouse.delete()
  }

  test("test geospatial") {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehouse.getAbsolutePath)
      .config(
        "spark.sql.extensions",
        "org.apache.sedona.sql.SedonaSqlExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sedona.enableParserExtension", "false")
      .getOrCreate()

    spark
      .sql(
        "CREATE OR REPLACE TABLE local.tmp.geom_table (id INT, geom GEOMETRY) USING iceberg TBLPROPERTIES ('format-version' = '3')")
      .show()
    spark
      .sql("""
             INSERT INTO local.tmp.geom_table VALUES
             (1, ST_GeomFromText('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))')),
             (2, ST_GeomFromText('POINT (100 40)'))
             """)
      .show()
    spark.sql("SELECT * FROM local.tmp.geom_table").show()
    spark
      .sql("""
             SELECT * FROM local.tmp.geom_table WHERE ST_Intersects(geom, ST_GeomFromText('POINT (1 2)'))
             """)
      .show()

    spark
      .sql(
        "CREATE OR REPLACE TABLE local.tmp.geog_table (id INT, geog GEOGRAPHY) USING iceberg TBLPROPERTIES ('format-version' = '3')")
      .show()
    spark
      .sql("""
             INSERT INTO local.tmp.geog_table VALUES
             (1, ST_GeogFromWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))')),
             (2, ST_GeogFromWKT('POINT (100 40)'))
             """)
      .show()
    spark.sql("SELECT * FROM local.tmp.geog_table").show()
  }
}
