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
package org.apache.spark.sql.execution.datasources.geoparquet

import java.util.Collections

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.json4s.JNull
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.FunSuite

class ParquetGeospatialTypesTest extends FunSuite {
  private val noMetaData = Collections.emptyMap[String, String]()
  private val utm10n = """{"type":"ProjectedCRS","id":{"authority":"EPSG","code":32610}}"""
  private val webMercator = """{"type":"ProjectedCRS","id":{"authority":"EPSG","code":3857}}"""

  /** The SRID of a CRS that only Spark's registry knows, which Spark has since 4.2. */
  private def registered(srid: Int): Int = if (SpatialTypeSupport.usesNativeTypes) srid else 0

  private def srid(crs: String, metaData: java.util.Map[String, String] = noMetaData): Int =
    ParquetGeospatialTypes.crsToSrid(Option(crs), metaData)

  test("an absent crs property means OGC:CRS84") {
    assert(srid(null) == 4326)
  }

  test("authority:code names an EPSG code or OGC:CRS84") {
    assert(srid("EPSG:3857") == 3857)
    assert(srid("epsg:3857") == 3857)
    assert(srid(" EPSG:3857 ") == 3857)
    assert(srid("OGC:CRS84") == 4326)
    assert(srid("EPSG:Pseudo-Mercator") == 0)
  }

  test("other authorities resolve through Spark's CRS registry where Spark has one") {
    assert(srid("ESRI:102100") == registered(102100))
    assert(srid("esri:102100") == registered(102100))
    assert(srid("OGC:CRS83") == registered(4269))
    assert(
      srid("""{"type":"ProjectedCRS","id":{"authority":"ESRI","code":102100}}""") ==
        registered(102100))
    assert(
      srid("""{"type":"ProjectedCRS","id":{"authority":"ESRI","code":"102100"}}""") ==
        registered(102100))
    assert(srid("ESRI:999999999") == 0)
    assert(srid("IAU_2015:49900") == 0)
    assert(srid("ESRI:ANY") == 0)
  }

  test("the number of an ESRI code is not an SRID on its own") {
    // ESRI:4812 and EPSG:4812 are different systems, and Sedona reads an SRID as an EPSG code.
    // Spark's registry keeps 4812 for EPSG, so no Spark version gives this identifier an SRID.
    assert(srid("ESRI:4812") == 0)
    assert(srid("""{"type":"GeographicCRS","id":{"authority":"ESRI","code":4812}}""") == 0)
    assert(srid("EPSG:4812") == 4812)
  }

  test("srid:identifier is the SRID, in any case") {
    assert(srid("srid:32610") == 32610)
    assert(srid("SRID:0") == 0)
    assert(ParquetGeospatialTypes.isUnknownCrs(Some("srid:0")))
    assert(ParquetGeospatialTypes.isUnknownCrs(Some("SRID:0")))
    assert(!ParquetGeospatialTypes.isUnknownCrs(Some("srid:32610")))
    assert(!ParquetGeospatialTypes.isUnknownCrs(None))
  }

  test("inline PROJJSON resolves through its identifier") {
    assert(srid(utm10n) == 32610)
    assert(srid("""{"type":"ProjectedCRS","name":"local grid"}""") == 0)
    assert(srid("""{"type":"ProjectedCRS",""") == 0)
  }

  test("projjson:key resolves through the file metadata entry it names") {
    assert(srid("projjson:my_crs", Collections.singletonMap("my_crs", utm10n)) == 32610)
    assert(srid("projjson:my_crs") == 0)
    assert(srid("projjson:my_crs", null) == 0)
  }

  test("projjson:key names the file metadata entry exactly as written") {
    val metaData = new java.util.HashMap[String, String]()
    metaData.put(" utm", utm10n)
    metaData.put("utm", webMercator)
    assert(srid("projjson: utm", metaData) == 32610)
    assert(srid("projjson:utm", metaData) == 3857)
    assert(srid("projjson:utm ", metaData) == 0)
    assert(srid("projjson: my_crs", Collections.singletonMap("my_crs", utm10n)) == 0)
  }

  test("text in no known form is an unknown CRS") {
    assert(srid("WGS 84") == 0)
  }

  test("fields without a geospatial annotation are not geospatial columns") {
    val string = Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
    val binary = Types.optional(BINARY).named("payload")
    val group = Types.optionalGroup().addField(binary).named("nested")
    assert(ParquetGeospatialTypes.describe(string).isEmpty)
    assert(ParquetGeospatialTypes.describe(binary).isEmpty)
    assert(ParquetGeospatialTypes.describe(group).isEmpty)
  }

  test("geospatial annotations round trip when the Parquet library has them") {
    if (ParquetGeospatialTypes.supported) {
      val geometry = Types
        .optional(BINARY)
        .as(ParquetGeospatialTypes.geometry(Some("EPSG:3857")))
        .named("geometry")
      assert(
        ParquetGeospatialTypes.describe(geometry) ==
          Some(ParquetGeospatialColumn(isGeography = false, Some("EPSG:3857"), None)))
      val defaultCrs = Types
        .optional(BINARY)
        .as(ParquetGeospatialTypes.geometry(None))
        .named("geometry")
      assert(ParquetGeospatialTypes.describe(defaultCrs).exists(_.crs.isEmpty))
      val geography = Types
        .optional(BINARY)
        .as(ParquetGeospatialTypes.geography(None, "Karney"))
        .named("geography")
      assert(
        ParquetGeospatialTypes.describe(geography) ==
          Some(ParquetGeospatialColumn(isGeography = true, None, Some("karney"))))
      intercept[IllegalArgumentException](ParquetGeospatialTypes.geography(None, "rhumb"))
    } else {
      intercept[UnsupportedOperationException](ParquetGeospatialTypes.geometry(None))
      intercept[UnsupportedOperationException](ParquetGeospatialTypes.geography(None, "karney"))
    }
  }

  private def property(projjson: String): Option[String] =
    ParquetGeospatialTypes.crsProperty(Option(projjson).map(parse(_)))

  test("a CRS becomes the shortest crs property that still identifies it") {
    assert(property(null).isEmpty)
    assert(ParquetGeospatialTypes.crsProperty(Some(JNull)).contains("srid:0"))
    assert(property(utm10n).contains("EPSG:32610"))
    assert(
      property("""{"type":"GeographicCRS","id":{"authority":"OGC","code":"CRS84"}}""")
        .contains("OGC:CRS84"))
    val localGrid = """{"type":"EngineeringCRS","name":"local grid"}"""
    assert(property(localGrid).contains(localGrid))
    // An identifier that is not a plain authority:code pair cannot be written in that form
    val spaced = """{"name":"x","id":{"authority":"MY ORG","code":7}}"""
    assert(property(spaced).contains(spaced))
  }

  test("an identifier with a registry version keeps its PROJJSON") {
    // PROJ writes IAU_2015:49900 as authority IAU, code 49900 and version 2015. IAU:49900 names
    // no CRS, and a registry may give a code another meaning in another version.
    val mars =
      """{"type":"GeodeticCRS","name":"Mars (2015) - Sphere","id":{"authority":"IAU","code":49900,"version":2015}}"""
    assert(property(mars).contains(mars))
    val textVersion = """{"name":"x","id":{"authority":"EPSG","code":4812,"version":"4.4"}}"""
    assert(property(textVersion).contains(textVersion))
    val decimalVersion = """{"name":"x","id":{"authority":"EPSG","code":4812,"version":4.4}}"""
    assert(property(decimalVersion).contains(decimalVersion))
  }

  test("only GeoParquet 2.x versions use the geospatial logical types") {
    Seq("2.0.0", "2.1.0", " 2.0.0 ", "2").foreach { version =>
      assert(GeoParquetMetaData.usesGeospatialTypes(version), version)
    }
    // Any other version string keeps the plain binary layout it has always had
    Seq("1.0.0", "1.1.0", "10.9.8", "20.0.0", "", "two").foreach { version =>
      assert(!GeoParquetMetaData.usesGeospatialTypes(version), version)
    }
  }
}
