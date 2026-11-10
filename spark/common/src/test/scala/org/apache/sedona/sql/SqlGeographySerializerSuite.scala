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
import java.util.Arrays

import org.apache.sedona.common.S2Geography.{GeographyWKBSerializer, WKBGeography}
import org.apache.sedona.common.geography.Constructors
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.datasyslab.jts.io.WKBReader
import org.scalatest.funsuite.AnyFunSuite

class SqlGeographySerializerSuite extends AnyFunSuite {
  private val point = WKBReader.hexToBytes("0101000000000000000000F03F0000000000000040")
  private def header(srid: Int) =
    ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(srid).array()
  private def decode(bytes: Array[Byte]) =
    GeographyUDT.deserialize(bytes).asInstanceOf[WKBGeography]

  test("SQL geography writes a little endian SRID without changing common geography bytes") {
    Seq(0, 1, 4326, Int.MaxValue).foreach { srid =>
      val geography = WKBGeography.fromWKB(point.clone(), srid)
      val bytes = GeographyUDT.serialize(geography)
      assert(bytes.sameElements(header(srid) ++ point))
      assert(decode(bytes).getSRID == srid)
      val common = GeographyWKBSerializer.serialize(geography)
      assert(ByteBuffer.wrap(common).getInt == srid)
      assert(common.drop(4).sameElements(point))
      assert(GeographyWKBSerializer.deserialize(common).getSRID == srid)
    }
  }

  test("SQL geography reads independent ISO point fixtures with either WKB byte order") {
    Seq(
      "E61000000101000000000000000000F03F0000000000000040",
      "E610000000000000013FF00000000000004000000000000000").foreach { hex =>
      val result = decode(WKBReader.hexToBytes(hex))
      assert(result.getSRID == 4326)
      assert(result.getPointX == 1.0 && result.getPointY == 2.0)
    }
  }

  test("SQL geography normalizes EWKB headers without parsing lazy S2 values") {
    val input = WKBReader.hexToBytes(
      "01010000E0110F0000000000000000F03F000000000000004000000000000008400000000000001040")
    val expected = WKBReader.hexToBytes(
      "01B90B0000000000000000F03F000000000000004000000000000008400000000000001040")
    val geography = WKBGeography.fromWKB(input, 4326)
    val bytes = GeographyUDT.serialize(geography)
    assert(bytes.sameElements(header(4326) ++ expected))
    Seq("jtsGeometry", "s2Geography", "shapeIndexGeography").foreach { name =>
      val field = classOf[WKBGeography].getDeclaredField(name)
      field.setAccessible(true)
      assert(field.get(geography) == null)
    }
    assert(
      input.sameElements(WKBReader.hexToBytes(
        "01010000E0110F0000000000000000F03F000000000000004000000000000008400000000000001040")))
    val result = decode(bytes)
    assert(result.getSRID == 4326 && result.getWKBBytes.sameElements(expected))
    val coords = result.getJTSGeometry.getCoordinates.head
    assert(coords.getZ == 3.0 && coords.getM == 4.0)
  }

  test("SQL geography keeps independent collection layouts and dimensional empties") {
    val xy = point
    val zmEmpty = WKBReader.hexToBytes(
      "01B90B0000000000000000F87F000000000000F87F000000000000F87F000000000000F87F")
    val collection = WKBReader.hexToBytes("010700000002000000") ++ xy ++ zmEmpty
    Seq(collection, zmEmpty, WKBReader.hexToBytes("01BA0B000000000000")).foreach { wkb =>
      val restored = decode(GeographyUDT.serialize(WKBGeography.fromWKB(wkb, 4326)))
      assert(restored.getWKBBytes.sameElements(wkb))
      assert(restored.getSRID == 4326)
    }
  }

  test("SQL geography owns decoded and encoded buffers") {
    val source = point.clone()
    val geography = WKBGeography.fromWKB(source, 4326)
    val encoded = GeographyUDT.serialize(geography)
    val restored = decode(encoded)
    Arrays.fill(source, 0.toByte)
    assert(encoded.drop(4).sameElements(point))
    Arrays.fill(encoded, 0.toByte)
    assert(restored.getWKBBytes.sameElements(point))
    assert(restored.getPointX == 1.0 && restored.getSRID == 4326)
  }

  test("SQL geography retains computed S2 values and empty lines") {
    Seq("POINT (1 2)", "POLYGON ((0 0, 2 0, 0 2, 0 0))", "LINESTRING EMPTY")
      .foreach { wkt =>
        val lazyValue = Constructors.geogFromWKT(wkt, 4326).asInstanceOf[WKBGeography]
        val computed = lazyValue.getS2Geography
        val encoded = GeographyUDT.serialize(computed)
        assert(ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN).getInt == 4326)
        val result = decode(encoded)
        assert(result.getSRID == 4326)
        assert(result.getJTSGeometry.equalsExact(lazyValue.getJTSGeometry, 1e-12))
      }
  }

  test("SQL geography retains eager S2 initialization and rejects negative SRIDs") {
    val previous = WKBGeography.isEagerShapeIndex
    try {
      WKBGeography.setEagerShapeIndex(true)
      val result = decode(header(4326) ++ point)
      val field = classOf[WKBGeography].getDeclaredField("shapeIndexGeography")
      field.setAccessible(true)
      assert(field.get(result) != null)
    } finally WKBGeography.setEagerShapeIndex(previous)
    intercept[IllegalArgumentException] { decode(header(-1) ++ point) }
    (0 until 9).foreach { length =>
      intercept[IllegalArgumentException] { decode((header(4326) ++ point).take(length)) }
    }
  }
}
