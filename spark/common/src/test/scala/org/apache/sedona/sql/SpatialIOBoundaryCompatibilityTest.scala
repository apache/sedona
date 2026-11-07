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

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.sedona.sql.datasources.geopackage.transform.GeometryReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.io.geojson.GeoJSONUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.WKBReader
import org.scalatest.FunSuite

class SpatialIOBoundaryCompatibilityTest extends FunSuite {
  test("existing GeoJSON binary utility overloads remain explicitly UDT-based") {
    val json = """{"type":"Point","coordinates":[1.0,2.0]}"""
    val oldSignature: Array[Byte] = GeoJSONUtils.geoJsonToGeometry(json)
    val typed = GeoJSONUtils.geoJsonToGeometry(json, GeometryUDT()).asInstanceOf[Array[Byte]]
    assert(oldSignature.sameElements(typed))
    assert(GeometryUDT.deserialize(oldSignature).getCoordinate.x == 1)
    assert(
      GeoJSONUtils
        .geometryToGeoJson(oldSignature) == GeoJSONUtils.geometryToGeoJson(typed, GeometryUDT()))
    val input = InternalRow(UTF8String.fromString(json))
    val inputSchema = StructType(Seq(StructField("geometry", StringType)))
    val outputSchema = StructType(Seq(StructField("geometry", GeometryUDT())))
    val output = GeoJSONUtils.convertGeoJsonToGeometry(input, inputSchema, outputSchema)
    assert(GeometryUDT.deserialize(output.getBinary(0)).getCoordinate.y == 2)
  }

  test("GeoPackage typed reads preserve the public binary overload and null behavior") {
    val wkb = WKBReader.hexToBytes("0101000000000000000000F03F0000000000000040")
    val bytes = ByteBuffer
      .allocate(8 + wkb.length)
      .order(ByteOrder.LITTLE_ENDIAN)
      .put('G'.toByte)
      .put('P'.toByte)
      .put(0.toByte)
      .put(1.toByte)
      .putInt(3857)
      .put(wkb)
      .array()
    val oldSignature: Array[Byte] = GeometryReader.extractWKB(bytes)
    val typed = GeometryReader.extractGeometry(bytes, GeometryUDT()).asInstanceOf[Array[Byte]]
    assert(oldSignature.sameElements(typed))
    assert(GeometryUDT.deserialize(typed).getSRID == 3857)
    assert(GeometryReader.extractWKB(null) == null)
    assert(GeometryReader.extractGeometry(null, GeometryUDT()) == null)
  }
}
