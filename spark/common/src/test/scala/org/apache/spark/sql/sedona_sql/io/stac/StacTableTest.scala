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

import org.apache.spark.sql.sedona_sql.io.stac.StacTable.{SCHEMA_GEOPARQUET, addAssetStruct, addAssetsStruct}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class StacTableTest extends AnyFunSuite {

  test("addAssetStruct should add a new asset to an existing assets struct") {
    val initialSchema = StructType(
      Seq(
        StructField("id", StringType, nullable = false),
        StructField(
          "assets",
          StructType(Seq(StructField(
            "image",
            StructType(Seq(
              StructField("href", StringType, nullable = true),
              StructField("roles", ArrayType(StringType), nullable = true),
              StructField("title", StringType, nullable = true),
              StructField("type", StringType, nullable = true))),
            nullable = true))),
          nullable = true)))

    val updatedSchema = addAssetStruct(initialSchema, "thumbnail")

    assert(updatedSchema.fieldNames.contains("assets"))
    val assetsField = updatedSchema("assets").dataType.asInstanceOf[StructType]
    assert(assetsField.fieldNames.contains("thumbnail"))
  }

  test("addAssetStruct should create assets struct if it doesn't exist") {
    val initialSchema = StructType(Seq(StructField("id", StringType, nullable = false)))

    val updatedSchema1 = addAssetStruct(initialSchema, "image")
    val updatedSchema2 = addAssetStruct(updatedSchema1, "rast")

    assert(updatedSchema2.fieldNames.contains("assets"))
    val assetsField = updatedSchema2("assets").dataType.asInstanceOf[StructType]
    assert(assetsField.fieldNames.contains("image"))
    assert(assetsField.fieldNames.contains("rast"))
  }

  test("addAssetStruct should not modify other fields") {
    val initialSchema = SCHEMA_GEOPARQUET
    val updatedSchema = addAssetsStruct(initialSchema, Array("thumbnail", "preview"))

    assert(updatedSchema.fieldNames.contains("id"))
    assert(updatedSchema.fieldNames.contains("stac_version"))
    assert(updatedSchema.fieldNames.contains("stac_extensions"))
    assert(updatedSchema.fieldNames.contains("bbox"))
    assert(updatedSchema.fieldNames.contains("geometry"))
    assert(updatedSchema.fieldNames.contains("assets"))
  }
}
