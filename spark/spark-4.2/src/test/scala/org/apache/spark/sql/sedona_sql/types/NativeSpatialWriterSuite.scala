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

import org.apache.sedona.common.{Constructors, Functions}
import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.util.{Geography => NativeGeography, Geometry => NativeGeometry}
import org.datasyslab.jts.geom.impl.DeclaredCoordinateSequence
import org.locationtech.jts.geom._
import org.scalatest.funsuite.AnyFunSuite

class NativeSpatialWriterSuite extends AnyFunSuite {
  private val factory = new GeometryFactory()

  // Independent oracle: Sedona's existing dimension-preserving WKB writer, with only
  // EWKB dimensional flags translated to Spark's ISO type codes.
  private def oracleWkb(geometry: Geometry): Array[Byte] = {
    val bytes = Functions.asWKB(geometry)
    val buffer = ByteBuffer.wrap(bytes)
    def rewrite(): Unit = {
      buffer.order(if (buffer.get() == 1) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN)
      val offset = buffer.position()
      val code = buffer.getInt()
      val z = if ((code & 0x80000000) != 0) 1 else 0
      val m = if ((code & 0x40000000) != 0) 1 else 0
      val base = code & 0x1fffffff
      buffer.putInt(offset, base + z * 1000 + m * 2000)
      def skip(count: Int): Unit = buffer.position(buffer.position() + count * (2 + z + m) * 8)
      base match {
        case 1 => skip(1)
        case 2 => skip(buffer.getInt())
        case 3 => val count = buffer.getInt(); (0 until count).foreach(_ => skip(buffer.getInt()))
        case _ => val count = buffer.getInt(); (0 until count).foreach(_ => rewrite())
      }
    }
    rewrite()
    bytes
  }

  private def check(geometry: Geometry, geography: Boolean = false): Unit = {
    val expected = oracleWkb(geometry)
    val actual = NativeSpatialWriter.serialize(geometry, 4326, geography)
    val expectedValue =
      if (geography) NativeGeography.fromWkb(expected, 4326).getValue
      else NativeGeometry.fromWkb(expected, 4326).getValue
    assert(Arrays.equals(actual.getBytes, expectedValue.getBytes))
    val exported =
      if (geography) NativeGeography.fromValue(actual).toWkb(ByteOrder.LITTLE_ENDIAN)
      else NativeGeometry.fromValue(actual).toWkb(ByteOrder.LITTLE_ENDIAN)
    assert(Arrays.equals(exported, expected))
  }

  private def rejects(geometry: Geometry, geography: Boolean = false): Unit = {
    val expected = intercept[SparkIllegalArgumentException] {
      if (geography) NativeGeography.fromWkb(oracleWkb(geometry), 4326)
      else NativeGeometry.fromWkb(oracleWkb(geometry), 4326)
    }
    val actual = intercept[SparkIllegalArgumentException] {
      NativeSpatialWriter.serialize(geometry, 4326, geography)
    }
    assert(actual.getCondition == expected.getCondition)
    assert(actual.getMessageParameters == expected.getMessageParameters)
  }

  private def sequence(dimension: Int, measures: Int, size: Int): DeclaredCoordinateSequence = {
    val result = new DeclaredCoordinateSequence(size, dimension, measures)
    var i = 0
    while (i < size) {
      var d = 0
      while (d < dimension) {
        result.setOrdinate(i, d, i + d + 1d)
        d += 1
      }
      i += 1
    }
    result
  }

  test("writes the native SRID header and preserves ordinary XY and multipart coordinates") {
    Seq(
      "POINT (1 2)",
      "LINESTRING (1 2, 3 4)",
      "POLYGON ((0 0, 5 0, 5 5, 0 0), (1 1, 2 1, 2 2, 1 1))",
      "MULTIPOINT ((1 2), EMPTY, (3 4))",
      "MULTILINESTRING ((1 2, 3 4), EMPTY)",
      "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), EMPTY)",
      "GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION (LINESTRING (1 2, 3 4)))")
      .foreach(wkt => check(Constructors.geomFromWKT(wkt, 0)))
    check(factory.createPoint(new Coordinate(1, 2))) // padded NaN Z remains XY
    assert(NativeSpatialWriter.serialize(null, 4326, false) == null)
  }

  test("preserves declared XY XYZ XYM XYZM including empty primitive layouts") {
    Seq((2, 0), (3, 0), (3, 1), (4, 1)).foreach { case (dimension, measures) =>
      check(factory.createPoint(sequence(dimension, measures, 1)))
      check(factory.createLineString(sequence(dimension, measures, 3)))
      check(factory.createPoint(sequence(dimension, measures, 0)))
      check(factory.createLineString(sequence(dimension, measures, 0)))
      check(factory.createPolygon(factory.createLinearRing(sequence(dimension, measures, 0))))
      val ring = sequence(dimension, measures, 4)
      ring.setOrdinate(3, 0, ring.getX(0))
      ring.setOrdinate(3, 1, ring.getY(0))
      check(factory.createPolygon(factory.createLinearRing(ring)))
    }
    check(factory.createGeometryCollection())
    check(factory.createMultiPoint())
    check(factory.createMultiLineString())
    check(factory.createMultiPolygon())
  }

  test("promotes multipart layouts and preserves NaN point ordinates") {
    val xy = factory.createPoint(sequence(2, 0, 1))
    val xyz = factory.createPoint(sequence(3, 0, 1))
    check(factory.createMultiPoint(Array(xy, xyz)))
    check(factory.createMultiPoint(Array(xy, xyz)), geography = true)
    val point = sequence(4, 1, 1)
    point.setOrdinate(0, 0, 999d)
    point.setOrdinate(0, 2, Double.NaN)
    check(factory.createPoint(point), geography = true) // any NaN makes a Spark point empty
  }

  test("rejects mixed geometry collection layouts including declared empty children") {
    val xy = factory.createPoint(sequence(2, 0, 1))
    val xyz = factory.createPoint(sequence(3, 0, 1))
    rejects(factory.createGeometryCollection(Array(xy, xyz)))
    rejects(factory.createGeometryCollection(Array(xy, factory.createPoint(sequence(3, 0, 0)))))
    rejects(factory.createGeometryCollection(Array(xyz, factory.createGeometryCollection())))
  }

  test("rejects infinite point ordinates and nonfinite line ordinates like Spark") {
    Seq(Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
      val point = sequence(4, 1, 1)
      point.setOrdinate(0, 2, value)
      rejects(factory.createPoint(point))
    }
    Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
      val line = sequence(4, 1, 2)
      line.setOrdinate(1, 3, value)
      rejects(factory.createLineString(line))
    }
    val xy = factory.createLineString(sequence(2, 0, 2))
    val xyz = factory.createLineString(sequence(3, 0, 2))
    rejects(factory.createMultiLineString(Array(xy, xyz))) // promotion introduces non-point NaN
  }

  test("validates geography longitude and latitude bounds but not Z or M bounds") {
    check(factory.createPoint(new CoordinateXY(-180, -90)), geography = true)
    check(factory.createPoint(new CoordinateXY(180, 90)), geography = true)
    Seq(new CoordinateXY(181, 0), new CoordinateXY(0, -91)).foreach { coordinate =>
      val point = factory.createPoint(coordinate)
      check(point)
      rejects(point, geography = true)
    }
    val line = sequence(4, 1, 2)
    line.setOrdinate(1, 0, 181d)
    rejects(factory.createLineString(line), geography = true)
    val point = sequence(4, 1, 1)
    point.setOrdinate(0, 2, 10000d)
    point.setOrdinate(0, 3, -10000d)
    check(factory.createPoint(point), geography = true)
  }

  test("ring closure is XY only and empty interior rings are rejected") {
    val ring = sequence(4, 1, 4)
    ring.setOrdinate(3, 0, ring.getX(0))
    ring.setOrdinate(3, 1, ring.getY(0))
    val polygon = factory.createPolygon(factory.createLinearRing(ring))
    check(polygon)
    ring.setOrdinate(3, 0, 42d)
    rejects(polygon)
    val shell = factory.createLinearRing(
      Array[Coordinate](
        new CoordinateXY(0, 0),
        new CoordinateXY(1, 0),
        new CoordinateXY(1, 1),
        new CoordinateXY(0, 0)))
    rejects(factory.createPolygon(shell, Array(factory.createLinearRing())))
  }

  test("matches Spark for each nonfinite ordinate in every declared layout") {
    Seq((2, 0), (3, 0), (3, 1), (4, 1)).foreach { case (dimension, measures) =>
      (0 until dimension).foreach { ordinate =>
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
          val point = sequence(dimension, measures, 1)
          point.setOrdinate(0, ordinate, value)
          Seq(false, true).foreach { geography =>
            if (java.lang.Double.isNaN(value)) check(factory.createPoint(point), geography)
            else rejects(factory.createPoint(point), geography)
          }
          val line = sequence(dimension, measures, 2)
          line.setOrdinate(1, ordinate, value)
          rejects(factory.createLineString(line))
          rejects(factory.createLineString(line), geography = true)
        }
      }
    }
  }

}
