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

import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class NativeNestedGeoParquetGuardSuite extends AnyFunSuite {
  test("native geometry inside structs, arrays and maps is rejected before a write") {
    val nestedTypes = Seq(
      StructType(Seq(StructField("g", GeometryType(3857)))),
      ArrayType(GeometryType(4326)),
      MapType(StringType, GeometryType(0)))
    nestedTypes.foreach { nested =>
      val schema =
        StructType(Seq(StructField("top", GeometryType(4326)), StructField("nested", nested)))
      val error = intercept[IllegalArgumentException] {
        GeoParquetWriteSupport.requireGeometryColumn(schema)
      }
      assert(error.getMessage.contains("root of the schema"))
    }
  }

  test("root native columns and explicit nested legacy UDT controls keep their behavior") {
    GeoParquetWriteSupport.requireGeometryColumn(
      StructType(Seq(StructField("g", GeometryType(4326)), StructField("h", GeometryType(3857)))))
    GeoParquetWriteSupport.requireGeometryColumn(
      StructType(
        Seq(
          StructField("top", GeometryUDT()),
          StructField("nested", StructType(Seq(StructField("g", GeometryUDT())))))))
  }
}
