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
package org.apache.spark.sql.udf

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.udf.ScalarUDF.{geoPandasScalaFunction, sedonaDBGeometryToGeometryFunction}
import org.locationtech.jts.io.WKTReader
import org.scalatest.matchers.should.Matchers

class StrategySuite extends TestBaseScala with Matchers {
  val wktReader = new WKTReader()

  val spark: SparkSession = {
    sparkSession.sparkContext.setLogLevel("ALL")
    sparkSession
  }

  import spark.implicits._

  it("sedona geospatial UDF - geopandas") {
    val df = Seq(
      (1, "value", wktReader.read("POINT(21 52)")),
      (2, "value1", wktReader.read("POINT(20 50)")),
      (3, "value2", wktReader.read("POINT(20 49)")),
      (4, "value3", wktReader.read("POINT(20 48)")),
      (5, "value4", wktReader.read("POINT(20 47)")))
      .toDF("id", "value", "geom")

    val geopandasUDFDF = df
      .withColumn("geom_buffer", geoPandasScalaFunction(col("geom")))

    geopandasUDFDF.count shouldEqual 5

    geopandasUDFDF
      .selectExpr("ST_AsText(ST_ReducePrecision(geom_buffer, 2))")
      .as[String]
      .collect() should contain theSameElementsAs Seq(
      "POLYGON ((20 51, 20 53, 22 53, 22 51, 20 51))",
      "POLYGON ((19 49, 19 51, 21 51, 21 49, 19 49))",
      "POLYGON ((19 48, 19 50, 21 50, 21 48, 19 48))",
      "POLYGON ((19 47, 19 49, 21 49, 21 47, 19 47))",
      "POLYGON ((19 46, 19 48, 21 48, 21 46, 19 46))")
  }

  it("sedona geospatial UDF - sedona db") {
    val df = Seq(
      (1, "value", wktReader.read("POINT(21 52)")),
      (2, "value1", wktReader.read("POINT(20 50)")),
      (3, "value2", wktReader.read("POINT(20 49)")),
      (4, "value3", wktReader.read("POINT(20 48)")),
      (5, "value4", wktReader.read("POINT(20 47)")))
      .toDF("id", "value", "geom")

    val dfVectorized = df
      .withColumn("geometry", expr("ST_SetSRID(geom, '4326')"))
      .select(sedonaDBGeometryToGeometryFunction(col("geometry"), lit(100)).alias("geom"))

    dfVectorized.selectExpr("ST_X(ST_Centroid(geom)) AS x")
      .selectExpr("sum(x)")
      .as[Double]
      .collect().head shouldEqual 101
//
//    val dfCopied = sparkSession.read
//      .format("geoparquet")
//      .load(
//        "/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/book/source_data/transportation_barcelona/barcelona.geoparquet")
//
//    val values = dfCopied
//      .select(sedonaDBGeometryToGeometryFunction(col("geometry"), lit(10)).alias("geom"))
//      .selectExpr("ST_Area(geom) as area")
//      .selectExpr("Sum(area) as total_area")
//
//    values.show()
  }
}
