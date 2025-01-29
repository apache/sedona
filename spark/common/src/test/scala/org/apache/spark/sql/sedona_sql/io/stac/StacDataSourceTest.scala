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

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.types.{ArrayType, DoubleType, MapType, StringType, StructField, StructType, TimestampType}
import org.scalatest.BeforeAndAfterAll

import java.util.TimeZone

class StacDataSourceTest extends TestBaseScala {

  val STAC_COLLECTION_LOCAL: String = resourceFolder + "datasource_stac/collection.json"
  val STAC_ITEM_LOCAL: String = resourceFolder + "geojson/core-item.json"

  val STAC_COLLECTION_REMOTE: List[String] = List(
    "https://earth-search.aws.element84.com/v1/collections/sentinel-2-pre-c1-l2a",
    "https://storage.googleapis.com/cfo-public/vegetation/collection.json",
    "https://storage.googleapis.com/cfo-public/wildfire/collection.json",
    "https://earthdatahub.destine.eu/api/stac/v1/collections/copernicus-dem",
    "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip")

  it("basic df load from local file should work") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.show(false)
  }

  it("basic df load from remote service endpoints should work") {
    STAC_COLLECTION_REMOTE.foreach { endpoint =>
      val dfStac = sparkSession.read.format("stac").load(endpoint)
      assertSchema(dfStac.schema)
    }
  }

  it("normal select SQL without any filter") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect =
      sparkSession.sql("SELECT id, datetime as dt, geometry, bbox FROM STACTBL")

    assert(dfSelect.schema.fieldNames.contains("id"))
    assert(dfSelect.schema.fieldNames.contains("dt"))
    assert(dfSelect.schema.fieldNames.contains("geometry"))
    assert(dfSelect.schema.fieldNames.contains("bbox"))

    val rowCount = dfSelect.count()
    assert(rowCount == 6)
  }

  it("select SQL with filter on datetime") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect = sparkSession.sql(
      "SELECT id, datetime as dt, geometry, bbox " +
        "FROM STACTBL " +
        "WHERE datetime BETWEEN '2020-01-01T00:00:00Z' AND '2020-12-13T00:00:00Z'")

    dfSelect.explain(true)

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains(
      "PushedTemporalFilters -> AndFilter(GreaterThanFilter(datetime,2020-01-01T00:00),LessThanFilter(datetime,2020-12-13T00:00))"))

    val rowCount = dfSelect.count()
    assert(rowCount == 4)
  }

  it("select SQL with spatial filter") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect = sparkSession.sql(
      "SELECT id, geometry " +
        "FROM STACTBL " +
        "WHERE st_contains(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)")

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains(
      "PushedSpatialFilters -> LeafFilter(geometry,INTERSECTS,POLYGON ((17 10, 18 10, 18 11, 17 11, 17 10)))"))

    val rowCount = dfSelect.count()
    assert(rowCount == 3)
  }

  it("select SQL with both spatial and temporal filters") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect = sparkSession.sql("SELECT id, datetime as dt, geometry, bbox " +
      "FROM STACTBL " +
      "WHERE datetime BETWEEN '2020-01-01T00:00:00Z' AND '2020-12-13T00:00:00Z' " +
      "AND st_contains(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)")

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains(
      "PushedSpatialFilters -> LeafFilter(geometry,INTERSECTS,POLYGON ((17 10, 18 10, 18 11, 17 11, 17 10)))"))
    assert(physicalPlan.contains(
      "PushedTemporalFilters -> AndFilter(GreaterThanFilter(datetime,2020-01-01T00:00),LessThanFilter(datetime,2020-12-13T00:00))"))

    val rowCount = dfSelect.count()
    assert(rowCount == 3)
  }

  it("select SQL with regular filter on id") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect = sparkSession.sql(
      "SELECT id, datetime as dt, geometry, bbox " +
        "FROM STACTBL " +
        "WHERE id = 'some-id'")

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains("PushedSpatialFilters -> None, PushedTemporalFilters -> None"))

    val rowCount = dfSelect.count()
    assert(rowCount == 0)
  }

  it("select SQL with regular, spatial, and temporal filters") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect = sparkSession.sql("SELECT id, datetime as dt, geometry, bbox " +
      "FROM STACTBL " +
      "WHERE id = 'some-id' " +
      "AND datetime BETWEEN '2020-01-01T00:00:00Z' AND '2020-12-13T00:00:00Z' " +
      "AND st_contains(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)")

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains(
      "PushedSpatialFilters -> LeafFilter(geometry,INTERSECTS,POLYGON ((17 10, 18 10, 18 11, 17 11, 17 10)))"))
    assert(physicalPlan.contains(
      "PushedTemporalFilters -> AndFilter(GreaterThanFilter(datetime,2020-01-01T00:00),LessThanFilter(datetime,2020-12-13T00:00))"))

    val rowCount = dfSelect.count()
    assert(rowCount == 0)
  }

  def assertSchema(actualSchema: StructType): Unit = {
    val expectedSchema = StructType(
      Seq(
        StructField("stac_version", StringType, nullable = false),
        StructField(
          "stac_extensions",
          ArrayType(StringType, containsNull = true),
          nullable = true),
        StructField("type", StringType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("bbox", ArrayType(DoubleType, containsNull = true), nullable = true),
        StructField("geometry", new GeometryUDT(), nullable = true),
        StructField("title", StringType, nullable = true),
        StructField("description", StringType, nullable = true),
        StructField("datetime", TimestampType, nullable = true),
        StructField("start_datetime", TimestampType, nullable = true),
        StructField("end_datetime", TimestampType, nullable = true),
        StructField("created", TimestampType, nullable = true),
        StructField("updated", TimestampType, nullable = true),
        StructField("platform", StringType, nullable = true),
        StructField("instruments", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("constellation", StringType, nullable = true),
        StructField("mission", StringType, nullable = true),
        StructField("gsd", DoubleType, nullable = true),
        StructField("collection", StringType, nullable = true),
        StructField(
          "links",
          ArrayType(
            StructType(Seq(
              StructField("rel", StringType, nullable = true),
              StructField("href", StringType, nullable = true),
              StructField("type", StringType, nullable = true),
              StructField("title", StringType, nullable = true))),
            containsNull = true),
          nullable = true),
        StructField(
          "assets",
          MapType(
            StringType,
            StructType(Seq(
              StructField("href", StringType, nullable = true),
              StructField("type", StringType, nullable = true),
              StructField("title", StringType, nullable = true),
              StructField("roles", ArrayType(StringType, containsNull = true), nullable = true))),
            valueContainsNull = true),
          nullable = true)))

    assert(
      actualSchema == expectedSchema,
      s"Schema does not match. Expected: $expectedSchema, Actual: $actualSchema")
  }
}
