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
import org.scalatest.FunSuite

class ParquetGeospatialTypesTest extends FunSuite {
  private val noMetaData = Collections.emptyMap[String, String]()
  private val utm10n = """{"type":"ProjectedCRS","id":{"authority":"EPSG","code":32610}}"""

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
    assert(srid("ESRI:102100") == 0)
    assert(srid("EPSG:Pseudo-Mercator") == 0)
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
}
