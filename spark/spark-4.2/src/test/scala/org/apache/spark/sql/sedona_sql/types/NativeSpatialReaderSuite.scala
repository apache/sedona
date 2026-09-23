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
import java.util.Arrays

import org.apache.spark.sql.catalyst.util.{Geometry => NativeGeometry}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.BinaryView
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, Point, Polygon}
import org.scalatest.funsuite.AnyFunSuite

class NativeSpatialReaderSuite extends AnyFunSuite {
  private def pointWkb(order: ByteOrder, layout: Int, empty: Boolean): Array[Byte] = {
    val dimensions = if (layout == 3000) 4 else if (layout == 0) 2 else 3
    val buffer = ByteBuffer.allocate(5 + dimensions * 8).order(order)
    buffer.put(if (order == ByteOrder.LITTLE_ENDIAN) 1.toByte else 0.toByte)
    buffer.putInt(1 + layout)
    (1 to dimensions).foreach(i => buffer.putDouble(if (empty) Double.NaN else i.toDouble))
    buffer.array()
  }

  private def assertFactorySrid(geom: Geometry): Unit = {
    assert(geom.getSRID == 4326)
    assert(geom.getFactory.getSRID == 4326)
    geom match {
      case collection: GeometryCollection =>
        (0 until collection.getNumGeometries).foreach(i =>
          assertFactorySrid(collection.getGeometryN(i)))
      case polygon: Polygon =>
        assertFactorySrid(polygon.getExteriorRing)
        (0 until polygon.getNumInteriorRing).foreach(i =>
          assertFactorySrid(polygon.getInteriorRingN(i)))
      case _ =>
    }
  }

  test(
    "reads native XY, Z, M and ZM points and empties with their declared dimensions and SRID") {
    for (order <- Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN);
      layout <- Seq(0, 1000, 2000, 3000);
      empty <- Seq(false, true)) {
      val input = NativeGeometry.fromWkb(pointWkb(order, layout, empty), 4326).getValue
      val point = NativeSpatialReader.geometry(input).asInstanceOf[Point]
      val sequence = point.getCoordinateSequence
      assertFactorySrid(point)
      assert(sequence.hasZ == (layout == 1000 || layout == 3000))
      assert(sequence.hasM == (layout == 2000 || layout == 3000))
      assert(point.isEmpty == empty)
      if (!empty) {
        assert(sequence.getX(0) == 1d)
        assert(sequence.getY(0) == 2d)
        if (sequence.hasZ) assert(sequence.getZ(0) == 3d)
        if (sequence.hasM) assert(sequence.getM(0) == (if (sequence.hasZ) 4d else 3d))
      }
    }
  }

  test("reads nested mixed-endian native WKB with one SRID factory for every component") {
    val point = pointWkb(ByteOrder.BIG_ENDIAN, 0, false)
    val line = ByteBuffer
      .allocate(41)
      .order(ByteOrder.BIG_ENDIAN)
      .put(0.toByte)
      .putInt(2)
      .putInt(2)
      .putDouble(0d)
      .putDouble(0d)
      .putDouble(3d)
      .putDouble(4d)
      .array()
    val wkb = ByteBuffer
      .allocate(9 + point.length + line.length)
      .order(ByteOrder.LITTLE_ENDIAN)
      .put(1.toByte)
      .putInt(7)
      .putInt(2)
      .put(point)
      .put(line)
      .array()
    val geometry = NativeSpatialReader.geometry(NativeGeometry.fromWkb(wkb, 4326).getValue)
    assertFactorySrid(geometry)
    assert(geometry.getGeometryN(0).asInstanceOf[Point].getX == 1d)
    assert(geometry.getGeometryN(1).asInstanceOf[LineString].getLength == 5d)
  }

  test("decoded geometry and geography own data from sliced and off-heap native values") {
    val bytes = NativeGeometry.fromWkb(pointWkb(ByteOrder.BIG_ENDIAN, 3000, false), 4326).getBytes
    val backing = Array.fill[Byte](bytes.length + 24)(0)
    System.arraycopy(bytes, 0, backing, 11, bytes.length)
    val sliced = BinaryView.fromBytes(backing).slice(11, bytes.length)
    val slicedGeometry = NativeSpatialReader.geometry(sliced).asInstanceOf[Point]
    val slicedGeography = NativeSpatialReader.geography(sliced)
    Arrays.fill(backing, 0.toByte)
    assert(slicedGeometry.getCoordinateSequence.getM(0) == 4d)
    assert(slicedGeography.getJTSGeometry.getCoordinate.getX == 1d)
    assert(slicedGeography.getSRID == 4326)
    val address = Platform.allocateMemory(bytes.length)
    try {
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length)
      val view = BinaryView.fromAddress(null, address, bytes.length)
      val geometry = NativeSpatialReader.geometry(view).asInstanceOf[Point]
      val geography = NativeSpatialReader.geography(view)
      Platform.setMemory(address, 0.toByte, bytes.length)
      assert(geometry.getCoordinateSequence.getM(0) == 4d)
      assert(geography.getJTSGeometry.getCoordinate.getX == 1d)
      assert(geography.getSRID == 4326)
    } finally Platform.freeMemory(address)
  }

  test("rejects truncated native geometry without reading beyond the buffer") {
    intercept[org.apache.spark.SparkIllegalArgumentException] {
      NativeSpatialReader.geometry(BinaryView.fromBytes(Array[Byte](0, 0, 0)))
    }
    val bytes = NativeGeometry.fromWkb(pointWkb(ByteOrder.LITTLE_ENDIAN, 0, false), 4326).getBytes
    intercept[org.apache.spark.SparkIllegalArgumentException] {
      NativeSpatialReader.geometry(BinaryView.fromBytes(bytes).slice(0, bytes.length - 1))
    }
  }

  test("empty native lines, polygons and collections retain their declared layout") {
    for (base <- 2 to 7; layout <- Seq(0, 1000, 2000, 3000)) {
      val wkb = ByteBuffer
        .allocate(9)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put(1.toByte)
        .putInt(base + layout)
        .putInt(0)
        .array()
      val input = NativeGeometry.fromWkb(wkb, 4326).getValue
      val actual = NativeSpatialReader.geometry(input)
      assert(actual.isEmpty)
      assertFactorySrid(actual)
      val previous = org.apache.sedona.common.Constructors.geomFromWKB(wkb, 4326)
      assert(
        org.apache.sedona.common.Functions
          .asWKB(actual)
          .sameElements(org.apache.sedona.common.Functions.asWKB(previous)))
    }
  }

  test("native geography accepts both byte orders and discards trailing bytes") {
    for (order <- Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN)) {
      val wkb = pointWkb(order, 3000, false)
      val extended = Arrays.copyOf(wkb, wkb.length + 8)
      val input = NativeGeometry.fromWkb(extended, 4326).getValue
      val geography = NativeSpatialReader.geography(input)
      assert(geography.getWKBBytes.length == wkb.length)
      val point = geography.getJTSGeometry.asInstanceOf[Point]
      assert(point.getCoordinateSequence.getZ(0) == 3d)
      assert(point.getCoordinateSequence.getM(0) == 4d)
      assert(geography.getSRID == 4326)
    }
  }

}
