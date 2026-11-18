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
package org.apache.spark.sql.sedona_sql.types

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.ByteOrder.{BIG_ENDIAN, LITTLE_ENDIAN}

import org.scalatest.FunSuite

class PublicWkbHeaderAdapterSuite extends FunSuite {
  private val ewkbZ = 0x80000000
  private val ewkbM = 0x40000000
  private val ewkbSrid = 0x20000000

  private def wkb(order: ByteOrder, size: Int)(fill: ByteBuffer => Unit): Array[Byte] = {
    val buffer = ByteBuffer.allocate(5 + size).order(order)
    buffer.put(if (order == LITTLE_ENDIAN) 1.toByte else 0.toByte)
    fill(buffer)
    buffer.array()
  }

  private def point(order: ByteOrder, code: Int, ordinates: Double*): Array[Byte] =
    wkb(order, ordinates.size * 8) { buffer =>
      buffer.putInt(code)
      ordinates.foreach(buffer.putDouble)
    }

  private def line(order: ByteOrder, code: Int, points: Int, ordinates: Double*): Array[Byte] =
    wkb(order, 4 + ordinates.size * 8) { buffer =>
      buffer.putInt(code).putInt(points)
      ordinates.foreach(buffer.putDouble)
    }

  private def collection(order: ByteOrder, code: Int, members: Array[Byte]*): Array[Byte] =
    wkb(order, 4 + members.map(_.length).sum) { buffer =>
      buffer.putInt(code).putInt(members.size)
      members.foreach(buffer.put)
    }

  private def typeCode(wkb: Array[Byte], offset: Int = 0): Int =
    ByteBuffer
      .wrap(wkb)
      .order(if (wkb(offset) == 1) LITTLE_ENDIAN else BIG_ENDIAN)
      .getInt(offset + 1)

  test("toIsoWkbInPlace writes the ISO type word into the array it was given") {
    val source = line(LITTLE_ENDIAN, ewkbZ | 2, 2, 1, 2, 3, 4, 5, 6)
    val expected = PublicWkbHeaderAdapter.toIsoWkb(source.clone())
    val result = PublicWkbHeaderAdapter.toIsoWkbInPlace(source)
    assert(result eq source)
    assert(typeCode(result) == 1002)
    assert(result.toSeq == expected.toSeq)
  }

  test("toIsoWkbInPlace rewrites the members of a collection in both byte orders") {
    val nested = collection(
      BIG_ENDIAN,
      ewkbZ | 7,
      point(LITTLE_ENDIAN, ewkbZ | 1, 1, 2, 3),
      line(BIG_ENDIAN, ewkbZ | ewkbM | 2, 1, 1, 2, 3, 4))
    val source = collection(
      LITTLE_ENDIAN,
      ewkbZ | 7,
      point(BIG_ENDIAN, ewkbZ | 1, 1, 2, 3),
      nested,
      point(LITTLE_ENDIAN, ewkbM | 1, 1, 2, 3))
    val expected = PublicWkbHeaderAdapter.toIsoWkb(source.clone())
    val result = PublicWkbHeaderAdapter.toIsoWkbInPlace(source)
    assert(result eq source)
    assert(typeCode(result) == 1007)
    // The first member starts after the byte order, type and member count of the collection
    assert(typeCode(result, 9) == 1001)
    assert(result.toSeq == expected.toSeq)
  }

  test("toIsoWkbInPlace leaves WKB without a Z or M coordinate, and ISO WKB, as it is") {
    Seq(
      point(LITTLE_ENDIAN, 1, 1, 2),
      point(BIG_ENDIAN, 1001, 1, 2, 3),
      collection(LITTLE_ENDIAN, 1007, line(BIG_ENDIAN, 1002, 1, 1, 2, 3))).foreach { source =>
      val before = source.clone()
      assert(PublicWkbHeaderAdapter.toIsoWkbInPlace(source) eq source)
      assert(source.toSeq == before.toSeq)
    }
  }

  test("toIsoWkbInPlace returns a new array for WKB with an embedded SRID") {
    // ISO WKB has no place for the SRID, so the result is four bytes shorter than the source
    val source = wkb(LITTLE_ENDIAN, 4 + 24) { buffer =>
      buffer.putInt(ewkbZ | ewkbSrid | 1).putInt(4326).putDouble(1).putDouble(2).putDouble(3)
    }
    val before = source.clone()
    val result = PublicWkbHeaderAdapter.toIsoWkbInPlace(source)
    assert(result.toSeq == point(LITTLE_ENDIAN, 1001, 1, 2, 3).toSeq)
    assert(source.toSeq == before.toSeq)
  }

  test("toIsoWkbInPlace rejects invalid WKB without changing it") {
    // A valid Point Z in front of a member with a type that does not exist
    val source = collection(
      LITTLE_ENDIAN,
      ewkbZ | 7,
      point(LITTLE_ENDIAN, ewkbZ | 1, 1, 2, 3),
      point(LITTLE_ENDIAN, 9, 1, 2))
    val before = source.clone()
    intercept[IllegalArgumentException](PublicWkbHeaderAdapter.toIsoWkbInPlace(source))
    assert(source.toSeq == before.toSeq)
  }
}
