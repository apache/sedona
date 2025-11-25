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
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

class StacDataSourceTest extends TestBaseScala {

  val STAC_COLLECTION_LOCAL: String = resourceFolder + "datasource_stac/collection.json"
  val STAC_ITEM_LOCAL: String = resourceFolder + "geojson/core-item.json"

  val STAC_COLLECTION_MOCK: List[String] = List(
    StacTestUtils.getFileUrlOfResource("stac/collections/sentinel-2-pre-c1-l2a.json"),
    StacTestUtils.getFileUrlOfResource("stac/collections/vegetation-collection.json"),
    StacTestUtils.getFileUrlOfResource("stac/collections/wildfire-collection.json"),
    StacTestUtils.getFileUrlOfResource("stac/collections/copernicus-dem.json"),
    StacTestUtils.getFileUrlOfResource("stac/collections/naip.json"),
    StacTestUtils.getFileUrlOfResource("stac/collections/earthview-catalog.json"))

  it("basic df load from local file should work") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    val rowCount = dfStac.count()
    assert(rowCount > 0)
  }

  it("basic df load from local file with extensions should work") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    // Filter rows where grid:code equals "MSIN-2506"
    val filteredDf = dfStac.filter(dfStac.col("grid:code") === "MSIN-2506")
    val rowCount = filteredDf.count()
    assert(rowCount > 0)
  }

  it("basic df load from mock service endpoints should work") {
    STAC_COLLECTION_MOCK.foreach { endpoint =>
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
        "WHERE st_intersects(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)")

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains(
      "PushedSpatialFilters -> LeafFilter(geometry,INTERSECTS,POLYGON ((17 10, 18 10, 18 11, 17 11, 17 10)))"))

    val rowCount = dfSelect.count()
    assert(rowCount == 3)
  }

  it("select SQL with both spatial and temporal filters") {
    val dfStac = sparkSession.read.format("stac").load(STAC_COLLECTION_LOCAL)
    dfStac.createOrReplaceTempView("STACTBL")

    val dfSelect = sparkSession.sql(
      "SELECT id, datetime as dt, geometry, bbox " +
        "FROM STACTBL " +
        "WHERE datetime BETWEEN '2020-01-01T00:00:00Z' AND '2020-12-13T00:00:00Z' " +
        "AND st_intersects(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)")

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

    val dfSelect = sparkSession.sql(
      "SELECT id, datetime as dt, geometry, bbox " +
        "FROM STACTBL " +
        "WHERE id = 'some-id' " +
        "AND datetime BETWEEN '2020-01-01T00:00:00Z' AND '2020-12-13T00:00:00Z' " +
        "AND st_intersects(ST_GeomFromText('POLYGON((17 10, 18 10, 18 11, 17 11, 17 10))'), geometry)")

    val physicalPlan = dfSelect.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains(
      "PushedSpatialFilters -> LeafFilter(geometry,INTERSECTS,POLYGON ((17 10, 18 10, 18 11, 17 11, 17 10)))"))
    assert(physicalPlan.contains(
      "PushedTemporalFilters -> AndFilter(GreaterThanFilter(datetime,2020-01-01T00:00),LessThanFilter(datetime,2020-12-13T00:00))"))

    val rowCount = dfSelect.count()
    assert(rowCount == 0)
  }

  it("should load STAC data with public service without authentication") {
    // This test verifies backward compatibility with public STAC services
    // Set STAC_PUBLIC_URL environment variable to test (e.g., Microsoft Planetary Computer)
    // Example: https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip
    val publicUrl = sys.env.get("STAC_PUBLIC_URL")

    if (publicUrl.isDefined) {
      val dfStac = sparkSession.read
        .format("stac")
        .load(publicUrl.get)
        .limit(10)

      // Verify we can load data
      assert(dfStac.count() >= 0, "Failed to load data from public STAC service")
    } else {
      // Skip test if environment variable is not set
      cancel(
        "Skipping public STAC service test - set STAC_PUBLIC_URL to run (e.g., https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip)")
    }
  }

  // Authentication tests for remote services
  it("should load STAC data with bearer token authentication") {
    // NOTE: Planet Labs API requires Basic Auth (not Bearer token) for collections
    // Bearer token authentication works with other STAC services
    // Set STAC_AUTH_URL and STAC_BEARER_TOKEN environment variables to test with a service that supports Bearer tokens
    val authUrl = sys.env.get("STAC_AUTH_URL")
    val bearerToken = sys.env.get("STAC_BEARER_TOKEN")

    if (authUrl.isDefined && bearerToken.isDefined) {
      val headersJson = s"""{"Authorization":"Bearer ${bearerToken.get}"}"""

      val dfStac = sparkSession.read
        .format("stac")
        .option("headers", headersJson)
        .load(authUrl.get)
        .limit(10)

      // Verify we can load data
      assert(dfStac.count() >= 0, "Failed to load data with bearer token authentication")
    } else {
      // Skip test if environment variables are not set
      cancel(
        "Skipping bearer token authentication test - set STAC_AUTH_URL and STAC_BEARER_TOKEN to run")
    }
  }

  it("should load STAC data with basic authentication") {
    // This test requires environment variables to be set:
    // STAC_AUTH_URL - The URL of the authenticated STAC service
    // STAC_USERNAME - Username or API key
    // STAC_PASSWORD - Password (can be empty for API keys)
    val authUrl = sys.env.get("STAC_AUTH_URL")
    val username = sys.env.get("STAC_USERNAME")
    val password = sys.env.get("STAC_PASSWORD").getOrElse("")

    if (authUrl.isDefined && username.isDefined) {
      // Encode credentials as Base64
      val credentials = s"${username.get}:$password"
      val base64Credentials =
        java.util.Base64.getEncoder.encodeToString(credentials.getBytes("UTF-8"))
      val headersJson = s"""{"Authorization":"Basic $base64Credentials"}"""

      val dfStac = sparkSession.read
        .format("stac")
        .option("headers", headersJson)
        .load(authUrl.get)
        .limit(10)

      // Verify we can load data
      assert(dfStac.count() >= 0, "Failed to load data with basic authentication")
      assertSchema(dfStac.schema)
    } else {
      // Skip test if environment variables are not set
      cancel("Skipping basic authentication test - set STAC_AUTH_URL and STAC_USERNAME to run")
    }
  }

  it("should fail gracefully when authentication is required but not provided") {
    // This test verifies that we get a proper error when accessing
    // an authenticated endpoint without credentials
    val authUrl = sys.env.get("STAC_AUTH_URL_REQUIRE_AUTH")

    if (authUrl.isDefined) {
      val exception = intercept[Exception] {
        val dfStac = sparkSession.read
          .format("stac")
          .load(authUrl.get)
          .limit(10)
        dfStac.count()
      }

      // Verify we get an authentication-related error
      val errorMessage = exception.getMessage.toLowerCase
      assert(
        errorMessage.contains("401") ||
          errorMessage.contains("unauthorized") ||
          errorMessage.contains("403") ||
          errorMessage.contains("forbidden"),
        s"Expected authentication error, but got: ${exception.getMessage}")
    } else {
      cancel("Skipping authentication failure test - set STAC_AUTH_URL_REQUIRE_AUTH to run")
    }
  }

  it("should parse headers JSON correctly") {
    // Test that headers are correctly parsed from JSON
    val headersJson = """{"Authorization":"Bearer test_token","X-Custom":"value"}"""
    val headers = StacUtils.parseHeaders(Map("headers" -> headersJson))

    assert(headers.size == 2, "Headers should contain 2 entries")
    assert(headers("Authorization") == "Bearer test_token", "Authorization header should match")
    assert(headers("X-Custom") == "value", "Custom header should match")
  }

  it("should handle empty headers JSON") {
    val headersJson = """{}"""
    val headers = StacUtils.parseHeaders(Map("headers" -> headersJson))
    assert(headers.isEmpty, "Empty JSON should result in empty headers map")
  }

  it("should handle missing headers option") {
    val headers = StacUtils.parseHeaders(Map.empty[String, String])
    assert(headers.isEmpty, "Missing headers option should result in empty headers map")
  }

  it("should throw error for invalid headers JSON") {
    val invalidJson = """{"Authorization":"Bearer token", invalid}"""
    assertThrows[IllegalArgumentException] {
      StacUtils.parseHeaders(Map("headers" -> invalidJson))
    }
  }

  def assertSchema(actualSchema: StructType): Unit = {
    // Base STAC fields that should always be present
    val baseFields = Seq(
      StructField("stac_version", StringType, nullable = false),
      StructField("stac_extensions", ArrayType(StringType, containsNull = true), nullable = true),
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
          StructType(
            Seq(
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
          StructType(
            Seq(
              StructField("href", StringType, nullable = true),
              StructField("type", StringType, nullable = true),
              StructField("title", StringType, nullable = true),
              StructField("roles", ArrayType(StringType, containsNull = true), nullable = true))),
          valueContainsNull = true),
        nullable = true))

    // Extension fields that may be present
    val extensionFields = Seq(
      // Grid extension fields
      StructField("grid:code", StringType, nullable = true)
      // Add other extension fields as needed
    )

    // Check that all base fields are present with correct types
    baseFields.foreach { expectedField =>
      val actualField = actualSchema.fields.find(_.name == expectedField.name)
      assert(actualField.isDefined, s"Required field ${expectedField.name} not found in schema")
      assert(
        actualField.get.dataType == expectedField.dataType,
        s"Field ${expectedField.name} has wrong type. Expected: ${expectedField.dataType}, Actual: ${actualField.get.dataType}")
      assert(
        actualField.get.nullable == expectedField.nullable,
        s"Field ${expectedField.name} has wrong nullability. Expected: ${expectedField.nullable}, Actual: ${actualField.get.nullable}")
    }

    // Check extension fields if they are present
    val actualFieldNames = actualSchema.fields.map(_.name).toSet
    extensionFields.foreach { extensionField =>
      if (actualFieldNames.contains(extensionField.name)) {
        val actualField = actualSchema.fields.find(_.name == extensionField.name).get
        assert(
          actualField.dataType == extensionField.dataType,
          s"Extension field ${extensionField.name} has wrong type. Expected: ${extensionField.dataType}, Actual: ${actualField.dataType}")
      }
    }

    // Check that there are no unexpected fields
    val expectedFieldNames = (baseFields ++ extensionFields).map(_.name).toSet
    val unexpectedFields = actualFieldNames.diff(expectedFieldNames)
    assert(unexpectedFields.isEmpty, s"Schema contains unexpected fields: $unexpectedFields")
  }
}
