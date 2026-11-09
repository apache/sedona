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

import org.apache.sedona.sql.utils.GeometrySerializer
import org.datasyslab.jts.io.{WKBReader, WKTReader}
import org.locationtech.jts.geom.{Coordinate, GeometryCollection, GeometryFactory, LineString, Point}
import org.scalatest.funsuite.AnyFunSuite

class SqlGeometrySerializerSuite extends AnyFunSuite {
  private def read(text: String) = {
    val reader = WKTReader.forDeclaredDimensions()
    reader.setIsOldJtsCoordinateSyntaxAllowed(false)
    reader.read(text)
  }
  private def decode(bytes: Array[Byte]) = {
    val srid = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
    WKBReader.forDeclaredDimensions(srid).read(bytes.drop(4))
  }

  test("SQL geometry bytes are a little endian SRID followed by ISO WKB") {
    val point = read("POINT (1 2)")
    point.setSRID(4326)
    val bytes = GeometrySerializer.serialize(point)
    assert(
      bytes.sameElements(
        WKBReader.hexToBytes("E61000000101000000000000000000F03F0000000000000040")))
    val decoded = decode(bytes).asInstanceOf[Point]
    assert(decoded.getX == 1 && decoded.getY == 2)
    assert(decoded.getSRID == 4326)
    assert(decoded.getCoordinateSequence.getDimension == 2)
  }

  test("SQL geometry reads independent ISO fixtures with either WKB byte order") {
    val fixtures = Seq(
      "E61000000101000000000000000000F03F0000000000000040",
      "E610000000000000013FF00000000000004000000000000000")
    fixtures.foreach { hex =>
      val point = GeometrySerializer.deserialize(WKBReader.hexToBytes(hex)).asInstanceOf[Point]
      assert(point.getX == 1 && point.getY == 2)
      assert(point.getSRID == 4326)
    }
  }

  test("SQL geometry prefix overrides embedded EWKB SRIDs in either byte order") {
    Seq(Int.MinValue, -1, 0, 4326, Int.MaxValue).foreach { srid =>
      val header = java.nio.ByteBuffer
        .allocate(4)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .putInt(srid)
        .array()
      Seq(
        "0101000020110F0000000000000000F03F0000000000000040",
        "002000000100000F113FF00000000000004000000000000000").foreach { hex =>
        val point = GeometrySerializer
          .deserialize(header ++ WKBReader.hexToBytes(hex))
          .asInstanceOf[Point]
        assert(point.getSRID == srid && point.getFactory.getSRID == srid)
        assert(point.getCentroid.getSRID == srid)
        assert(point.getX == 1 && point.getY == 2)
      }
    }
  }

  test("SQL geometry prefix overrides nested EWKB SRIDs while preserving independent layouts") {
    val members =
      "00A0000001000010AD3FF000000000000040000000000000004008000000000000" +
        "0020000007000010AD00000001" +
        "0101000040000000000000F03F00000000000000400000000000000840"
    Seq(0, -1, 4326).foreach { srid =>
      val header = java.nio.ByteBuffer
        .allocate(4)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .putInt(srid)
        .array()
      // A root without EWKB SRID must still override SRIDs declared by descendants.
      Seq("0107000020110F000002000000", "010700000002000000").foreach { root =>
        val bytes = header ++ WKBReader.hexToBytes(root + members)
        val original = bytes.clone()
        val result = GeometrySerializer.deserialize(bytes)
        val first = result.getGeometryN(0).asInstanceOf[Point]
        val nested = result.getGeometryN(1)
        val last = nested.getGeometryN(0).asInstanceOf[Point]
        Seq(result, first, nested, last).foreach { geometry =>
          assert(geometry.getSRID == srid && geometry.getFactory.getSRID == srid)
          assert(geometry.getCentroid.getSRID == srid)
        }
        Seq(first -> 0, last -> 1).foreach { case (point, measures) =>
          val sequence = point.getCoordinateSequence
          assert(sequence.getDimension == 3 && sequence.getMeasures == measures)
          (0 until 3).foreach(i => assert(sequence.getOrdinate(0, i) == i + 1.0))
        }
        assert(bytes.sameElements(original))
      }
    }
  }

  Seq(("", 2, 0), (" Z", 3, 0), (" M", 3, 1), (" ZM", 4, 1)).foreach {
    case (layout, dimensions, measures) =>
      val values = layout match {
        case " ZM" => "1 2 3 4"
        case " Z" | " M" => "1 2 3"
        case _ => "1 2"
      }
      test(s"SQL geometry preserves declared $dimensions/$measures point ordinates") {
        val bytes = GeometrySerializer.serialize(read(s"POINT$layout ($values)"))
        val point = decode(bytes).asInstanceOf[Point]
        val seq = point.getCoordinateSequence
        assert(seq.getDimension == dimensions && seq.getMeasures == measures)
        (0 until dimensions).foreach(i => assert(seq.getOrdinate(0, i) == i + 1.0))
        val roundTrip = GeometrySerializer.deserialize(bytes).asInstanceOf[Point]
        assert(roundTrip.getCoordinateSequence.getDimension == dimensions)
        assert(roundTrip.getCoordinateSequence.getMeasures == measures)
      }
      test(s"SQL geometry preserves declared $dimensions/$measures empty point and line") {
        Seq("POINT", "LINESTRING").foreach { kind =>
          val restored = decode(GeometrySerializer.serialize(read(s"$kind$layout EMPTY")))
          assert(restored.isEmpty && restored.getGeometryType.toUpperCase == kind)
          val seq = restored match {
            case p: Point => p.getCoordinateSequence
            case l: LineString => l.getCoordinateSequence
          }
          assert(seq.getDimension == dimensions && seq.getMeasures == measures)
        }
      }
  }

  test("SQL geometry preserves independent geometry collection layouts") {
    val factory = new GeometryFactory()
    val collection = factory.createGeometryCollection(
      Array(read("POINT (1 2)"), read("POINT Z (3 4 5)"), read("POINT M (6 7 8)")))
    val result = decode(GeometrySerializer.serialize(collection))
    val layouts = (0 until result.getNumGeometries).map { i =>
      val seq = result.getGeometryN(i).asInstanceOf[Point].getCoordinateSequence
      (seq.getDimension, seq.getMeasures)
    }
    assert(layouts == Seq((2, 0), (3, 0), (3, 1)))
  }

  test("SQL geometry rejects explicitly heterogeneous multipart layouts") {
    val factory = new GeometryFactory()
    val points =
      Array(read("POINT (1 2)").asInstanceOf[Point], read("POINT Z (3 4 5)").asInstanceOf[Point])
    intercept[IllegalArgumentException] {
      GeometrySerializer.serialize(factory.createMultiPoint(points))
    }
  }

  test("SQL geometry keeps ambiguous unmarked JTS coordinates compact") {
    val point = new GeometryFactory().createPoint(new Coordinate(1, 2))
    val result = decode(GeometrySerializer.serialize(point)).asInstanceOf[Point]
    assert(result.getCoordinateSequence.getDimension == 2)
    assert(result.getX == 1 && result.getY == 2)
  }

  test("SQL multipart geometry retains ordinates established beside ambiguous JTS sequences") {
    val factory = new GeometryFactory()
    val multi = factory.createMultiPoint(
      Array(
        factory.createPoint(new Coordinate(1, 2)),
        read("POINT Z (3 4 5)").asInstanceOf[Point]))
    val result = decode(GeometrySerializer.serialize(multi))
    assert(result.getGeometryN(0).asInstanceOf[Point].getX == 1)
    assert(result.getGeometryN(1).getCoordinate.getZ == 5)
  }

  test("SQL geometry preserves the factory SRID used by derived geometries") {
    val polygon = read("POLYGON ((0 0, 4 0, 0 4, 0 0))")
    polygon.setSRID(4326)
    val result = GeometrySerializer.deserialize(GeometrySerializer.serialize(polygon))
    assert(result.getSRID == 4326)
    assert(result.getFactory.getSRID == 4326)
    assert(result.getCentroid.getSRID == 4326)
    val collection = new GeometryFactory().createGeometryCollection(Array(polygon))
    collection.setSRID(4326)
    val restored = GeometrySerializer.deserialize(GeometrySerializer.serialize(collection))
    assert(restored.getGeometryN(0).getFactory.getSRID == 4326)
  }

  test("SQL geometry owns decoded coordinates and retains null join sentinel") {
    val bytes = WKBReader.hexToBytes("000000000101000000000000000000F03F0000000000000040")
    val point = GeometrySerializer.deserialize(bytes).asInstanceOf[Point]
    java.util.Arrays.fill(bytes, 0.toByte)
    assert(point.getX == 1 && point.getY == 2)
    val empty = GeometrySerializer.deserialize(null)
    assert(empty.isInstanceOf[GeometryCollection] && empty.isEmpty)
  }
  test("SQL geometry retains the full signed SRID independently of WKB byte order") {
    Seq(Int.MinValue, -1, 0, 4326, 16777216, Int.MaxValue).foreach { srid =>
      Seq(
        "0101000000000000000000F03F0000000000000040",
        "00000000013FF00000000000004000000000000000").foreach { hex =>
        val header = java.nio.ByteBuffer
          .allocate(4)
          .order(java.nio.ByteOrder.LITTLE_ENDIAN)
          .putInt(srid)
          .array()
        val point = GeometrySerializer.deserialize(header ++ WKBReader.hexToBytes(hex))
        assert(point.getSRID == srid && point.getFactory.getSRID == srid)
        val bytes = GeometrySerializer.serialize(point)
        assert(bytes.take(4).sameElements(header))
        assert(
          bytes
            .drop(4)
            .sameElements(WKBReader.hexToBytes("0101000000000000000000F03F0000000000000040")))
      }
    }
  }

  test("SQL geometry emits ISO dimension offsets rather than EWKB flags") {
    Seq(("", 1, "1 2"), (" Z", 1001, "1 2 3"), (" M", 2001, "1 2 3"), (" ZM", 3001, "1 2 3 4"))
      .foreach { case (layout, typeCode, values) =>
        val bytes = GeometrySerializer.serialize(read(s"POINT$layout ($values)"))
        assert(bytes(4) == 1)
        assert(
          java.nio.ByteBuffer
            .wrap(bytes, 5, 4)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getInt == typeCode)
      }
  }

  test("SQL geometry rejects a truncated SRID header and incomplete point payload") {
    val complete = WKBReader.hexToBytes("E61000000101000000000000000000F03F0000000000000040")
    (0 until complete.length).foreach { length =>
      intercept[IllegalArgumentException] {
        GeometrySerializer.deserialize(complete.take(length))
      }
    }
    assert(GeometrySerializer.serialize(null) == null)
  }

  test("SQL geometry rejects oversized counts before allocating coordinates or members") {
    // Tiny external payloads must not allocate arrays based on an unchecked count.
    (2 to 7).foreach { kind =>
      val bytes = java.nio.ByteBuffer
        .allocate(13)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .putInt(4326)
        .put(1.toByte)
        .putInt(kind)
        .putInt(Int.MaxValue)
        .array()
      intercept[IllegalArgumentException] { GeometrySerializer.deserialize(bytes) }
    }
  }
}
