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
import org.apache.spark.sql.catalyst.util.{Geometry => NativeGeometry, Geography => NativeGeography}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.BinaryView
import org.scalatest.funsuite.AnyFunSuite
import scala.util.Try

class NativeSpatialValidatorSuite extends AnyFunSuite {
  private val orders = Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN)
  private def ints(order: ByteOrder, values: Int*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.size * 4).order(order)
    values.foreach(buffer.putInt)
    buffer.array()
  }
  private def doubles(order: ByteOrder, values: Seq[Double]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.size * 8).order(order)
    values.foreach(buffer.putDouble)
    buffer.array()
  }
  private def header(kind: Int, order: ByteOrder): Array[Byte] =
    Array(if (order == ByteOrder.LITTLE_ENDIAN) 1.toByte else 0.toByte) ++ ints(order, kind)
  private def point(offset: Int, coordinates: Seq[Double], order: ByteOrder): Array[Byte] =
    header(offset + 1, order) ++ doubles(order, coordinates)
  private def line(offset: Int, coordinates: Seq[Seq[Double]], order: ByteOrder): Array[Byte] =
    header(offset + 2, order) ++ ints(order, coordinates.size) ++ doubles(
      order,
      coordinates.flatten)
  private def polygon(offset: Int, rings: Seq[Seq[Seq[Double]]], order: ByteOrder): Array[Byte] =
    header(offset + 3, order) ++ ints(order, rings.size) ++ rings.flatMap(ring =>
      ints(order, ring.size) ++ doubles(order, ring.flatten))
  private def collection(kind: Int, children: Seq[Array[Byte]], order: ByteOrder): Array[Byte] =
    header(kind, order) ++ ints(order, children.size) ++ children.flatten
  private def native(wkb: Array[Byte]): BinaryView =
    BinaryView.fromBytes(ints(ByteOrder.LITTLE_ENDIAN, 4326) ++ wkb)
  private def reference(wkb: Array[Byte], geography: Boolean): Unit = {
    if (geography) NativeGeography.fromWkb(wkb, 4326) else NativeGeometry.fromWkb(wkb, 4326)
  }
  private def assertParity(wkb: Array[Byte], geography: Boolean): Unit = {
    val expected = Try(reference(wkb, geography))
    val actual = Try(NativeSpatialValidator.validate(native(wkb), geography))
    assert(
      actual.isSuccess == expected.isSuccess,
      s"acceptance mismatch geography=$geography wkb=${wkb.map(b => f"${b & 255}%02x").mkString}")
    if (expected.isFailure) assert(actual.failed.get.getMessage == expected.failed.get.getMessage)
  }
  private def validSamples: Seq[Array[Byte]] = for {
    order <- orders
    offset <- Seq(0, 1000, 2000, 3000)
    dim = if (offset == 0) 2 else if (offset == 3000) 4 else 3
    a = Seq(1d, 2d, 3d, 4d).take(dim)
    b = Seq(2d, 3d, 4d, 5d).take(dim)
    c = Seq(3d, 1d, 5d, 6d).take(dim)
    p = point(offset, a, order)
    l = line(offset, Seq(a, b), order)
    poly = polygon(offset, Seq(Seq(a, b, c, a)), order)
    sample <- Seq(
      p,
      point(offset, Seq.fill(dim)(Double.NaN), order),
      l,
      line(offset, Seq.empty, order),
      poly,
      polygon(offset, Seq.empty, order),
      collection(offset + 4, Seq(p), order),
      collection(offset + 5, Seq(l), order),
      collection(offset + 6, Seq(poly), order),
      collection(offset + 7, Seq(p, l, poly), order),
      collection(offset + 7, Seq.empty, order),
      collection(offset + 7, Seq(point(offset, a, orders.find(_ != order).get)), order))
  } yield sample

  test("matches Spark for all ISO types, dimensions, endian orders and empty values") {
    validSamples.foreach { wkb =>
      Seq(false, true).foreach(geography => assertParity(wkb, geography))
      assert(NativeSpatialValidator.validate(native(wkb), false) == wkb.length)
    }
  }
  test("matches Spark coordinate validation including partial NaN points and geography bounds") {
    for {
      order <- orders
      offset <- Seq(0, 1000, 2000, 3000)
      dim = if (offset == 0) 2 else if (offset == 3000) 4 else 3
      coordinate <- 0 until dim
      bad <- Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity, 181d, -91d)
    } {
      val values = Seq.fill(dim)(0d).updated(coordinate, bad)
      Seq(false, true).foreach { geography =>
        assertParity(point(offset, values, order), geography)
        assertParity(line(offset, Seq(values, Seq.fill(dim)(1d)), order), geography)
      }
    }
    assertParity(point(1000, Seq(181d, 91d, Double.NaN), orders.head), true)
  }
  test("matches Spark structural errors, count bounds and error offsets") {
    for (order <- orders) {
      val p = point(0, Seq(1d, 2d), order)
      val cases = Seq(
        Array.emptyByteArray,
        Array[Byte](1),
        Array[Byte](2, 1, 0, 0, 0),
        header(0, order),
        header(4001, order),
        header(0x80000001, order),
        header(2, order) ++ ints(order, -1),
        header(2, order) ++ ints(order, Int.MaxValue),
        line(0, Seq(Seq(1d, 2d)), order),
        polygon(0, Seq(Seq.empty), order),
        polygon(0, Seq(Seq(Seq(0d, 0d), Seq(1d, 1d), Seq(0d, 0d))), order),
        polygon(0, Seq(Seq(Seq(0d, 0d), Seq(1d, 0d), Seq(1d, 1d), Seq(0d, 2d))), order),
        collection(4, Seq(line(0, Seq.empty, order)), order),
        collection(5, Seq(p), order),
        collection(6, Seq(p), order),
        collection(1007, Seq(p), order),
        collection(7, Seq(point(1000, Seq(1d, 2d, 3d), order)), order))
      cases.foreach(wkb => Seq(false, true).foreach(geography => assertParity(wkb, geography)))
    }
    validSamples.foreach(wkb =>
      (0 until wkb.length).foreach(size => assertParity(wkb.take(size), false)))
  }
  test("ring closure compares XY rather than Z or M") {
    val wkb = polygon(
      3000,
      Seq(
        Seq(Seq(0d, 0d, 1d, 1d), Seq(1d, 0d, 1d, 1d), Seq(1d, 1d, 1d, 1d), Seq(0d, 0d, 2d, 2d))),
      orders.head)
    assertParity(wkb, false)
    assert(NativeSpatialValidator.validate(native(wkb), false) == wkb.length)
  }
  test("returns only the consumed payload and supports sliced and offheap views") {
    val wkb = validSamples.head
    val data = native(wkb ++ Array[Byte](9, 8, 7)).getBytes
    val padded = Array.fill[Byte](7)(-1) ++ data ++ Array.fill[Byte](9)(-1)
    assert(
      NativeSpatialValidator
        .validate(BinaryView.fromBytes(padded, 7, data.length), false) == wkb.length)
    val address = Platform.allocateMemory(data.length)
    try {
      Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, null, address, data.length)
      assert(
        NativeSpatialValidator
          .validate(BinaryView.fromAddress(null, address, data.length), true) == wkb.length)
    } finally Platform.freeMemory(address)
    assert(data.sameElements(native(wkb ++ Array[Byte](9, 8, 7)).getBytes))
  }
  test("matches Spark on bounded deterministic mutations") {
    val random = new scala.util.Random(73)
    for (_ <- 0 until 500) {
      val wkb = validSamples(random.nextInt(validSamples.size)).clone()
      wkb(random.nextInt(wkb.length)) = random.nextInt(256).toByte
      Seq(false, true).foreach(geography => assertParity(wkb, geography))
    }
  }
}
