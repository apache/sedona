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

import org.json4s.{JNull, JValue}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.FunSuite

class GeoParquetNativeCrsTest extends FunSuite {
  private val epsg3857 = parse("""{"id":{"authority":"EPSG","code":3857}}""")

  test("native CRS distinguishes absent, explicit null, and recognized identifiers") {
    assert(GeoParquetNativeCrs.readSrid("g", None) == 4326)
    assert(GeoParquetNativeCrs.readSrid("g", Some(JNull)) == 0)
    assert(GeoParquetNativeCrs.readSrid("g", Some(epsg3857)) == 3857)
    val custom = Some(parse("""{"name":"custom CRS"}"""))
    assert(GeoParquetMetaData.extractSridFromCrs(custom) == 0)
    intercept[IllegalArgumentException] { GeoParquetNativeCrs.readSrid("g", custom) }
  }

  test("fixed native type supplies CRS even when a file has no non-null geometry") {
    assert(GeoParquetNativeCrs.writeCrs("g", 4326, None).isEmpty)
    assert(GeoParquetNativeCrs.writeCrs("g", 0, None).contains(JNull))
    val crs = GeoParquetNativeCrs.writeCrs("g", 3857, None)
    assert(GeoParquetNativeCrs.readSrid("g", crs) == 3857)
  }

  test("explicit native CRS options must agree with the fixed schema") {
    assert(GeoParquetNativeCrs.writeCrs("g", 3857, Some(Some(epsg3857))).contains(epsg3857))
    val mismatches: Seq[Option[JValue]] = Seq(None, Some(JNull))
    mismatches.foreach { crs =>
      intercept[IllegalArgumentException] { GeoParquetNativeCrs.writeCrs("g", 3857, Some(crs)) }
    }
    intercept[IllegalArgumentException] { GeoParquetNativeCrs.requireSrid("g", 4326, 3857) }
  }
}
