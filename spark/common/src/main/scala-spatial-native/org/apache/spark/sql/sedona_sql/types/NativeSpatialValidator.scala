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

import java.nio.ByteOrder
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.types.BinaryView

/** Validates Spark's ISO WKB payload without materializing its geometry model. */
private[types] object NativeSpatialValidator {
  private val nativeLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
  private val cursors = new ThreadLocal[Cursor] {
    override def initialValue(): Cursor = new Cursor
  }

  /** Returns the consumed WKB length, excluding the four-byte native SRID header. */
  def validate(value: BinaryView, geography: Boolean): Int =
    cursors.get().validate(value, geography)

  // A cursor is reused per thread, but never retains a row's potentially off-heap backing memory.
  private final class Cursor {
    private var value: BinaryView = _
    private var position = 0
    private var limit = 0
    private var geography = false
    private var x = 0d
    private var y = 0d

    def validate(input: BinaryView, isGeography: Boolean): Int = {
      value = input
      position = 4
      limit = if (input == null) 0 else input.numBytes()
      geography = isGeography
      try {
        if (limit < 4) fail("Unexpected end of WKB buffer", 4)
        if (limit == 4) fail("WKB data is empty or null", 4)
        if (limit - 4 < 5) fail("Unexpected end of WKB buffer", 4)
        geometry(-1)
        position - 4
      } finally value = null
    }

    private def fail(message: String, offset: Int): Nothing =
      throw QueryExecutionErrors.wkbParseError(message, (offset - 4).toLong)

    private def requireBytes(count: Int): Unit = {
      if (limit - position < count) fail("Unexpected end of WKB buffer", position)
    }

    private def readInt(littleEndian: Boolean): Int = {
      requireBytes(4)
      val result = value.getInt(position)
      position += 4
      if (littleEndian == nativeLittleEndian) result else java.lang.Integer.reverseBytes(result)
    }

    private def readDouble(littleEndian: Boolean, allowEmpty: Boolean): Double = {
      requireBytes(8)
      val raw = value.getLong(position)
      val result = java.lang.Double.longBitsToDouble(
        if (littleEndian == nativeLittleEndian) raw else java.lang.Long.reverseBytes(raw))
      val offset = position
      position += 8
      if (java.lang.Double
          .isInfinite(result) || (!allowEmpty && java.lang.Double.isNaN(result))) {
        fail("Invalid coordinate value found", offset)
      }
      result
    }

    private def readCount(name: String, minimumBytes: Int, littleEndian: Boolean): Int = {
      val offset = position
      val count = readInt(littleEndian)
      if (count < 0) fail(s"Invalid count for $name: $count", offset)
      if (count > (limit - position) / minimumBytes) {
        fail(s"Invalid count for $name: exceeds remaining bytes", offset)
      }
      count
    }

    private def point(dimensions: Int, littleEndian: Boolean, allowEmpty: Boolean): Unit = {
      val offset = position
      x = readDouble(littleEndian, allowEmpty)
      y = readDouble(littleEndian, allowEmpty)
      var empty = java.lang.Double.isNaN(x) || java.lang.Double.isNaN(y)
      var ordinate = 2
      while (ordinate < dimensions) {
        if (java.lang.Double.isNaN(readDouble(littleEndian, allowEmpty))) empty = true
        ordinate += 1
      }
      // Spark treats any NaN ordinate of a Point as empty, even if X and Y are finite.
      if (geography && !empty && !(x >= -180 && x <= 180 && y >= -90 && y <= 90)) {
        fail("Invalid coordinate value found", offset)
      }
    }

    private def ring(dimensions: Int, littleEndian: Boolean): Unit = {
      val offset = position
      val count = readCount("ring points", dimensions * 8, littleEndian)
      var firstX = 0d
      var firstY = 0d
      var index = 0
      while (index < count) {
        point(dimensions, littleEndian, allowEmpty = false)
        if (index == 0) {
          firstX = x
          firstY = y
        }
        index += 1
      }
      if (count < 4) fail("Too few points in ring", offset)
      if (firstX != x || firstY != y) fail("Ring is not closed", offset)
    }

    private def geometry(expectedLayout: Int): Int = {
      requireBytes(1)
      val byteOrder = value.getByte(position)
      position += 1
      if (byteOrder != 0 && byteOrder != 1) {
        fail(s"Invalid byte order $byteOrder", position - 1)
      }
      val littleEndian = byteOrder == 1
      val typeOffset = position
      val kind = readInt(littleEndian)
      val base = kind % 1000
      val layout = kind / 1000
      if (kind < 0 || base < 1 || base > 7 || layout > 3 ||
        (expectedLayout >= 0 && layout != expectedLayout)) {
        fail(s"Invalid or unsupported type $kind", typeOffset)
      }
      val dimensions = if (layout == 0) 2 else if (layout == 3) 4 else 3
      base match {
        case 1 => point(dimensions, littleEndian, allowEmpty = true)
        case 2 =>
          val offset = position
          val count = readCount("LineString points", dimensions * 8, littleEndian)
          if (count == 1) fail("Too few points in linestring", offset)
          var index = 0
          while (index < count) {
            point(dimensions, littleEndian, allowEmpty = false)
            index += 1
          }
        case 3 =>
          val count = readCount("polygon rings", 4, littleEndian)
          var index = 0
          while (index < count) {
            ring(dimensions, littleEndian)
            index += 1
          }
        case _ =>
          val name = base match {
            case 4 => "MultiPoint points"
            case 5 => "MultiLineString line strings"
            case 6 => "MultiPolygon polygons"
            case 7 => "GeometryCollection geometries"
          }
          val count = readCount(name, 5, littleEndian)
          var index = 0
          while (index < count) {
            val child = geometry(layout)
            if (base != 7 && child != base - 3) {
              val message = base match {
                case 4 => "Expected Point in MultiPoint"
                case 5 => "Expected LineString in MultiLineString"
                case 6 => "Expected Polygon in MultiPolygon"
              }
              fail(message, position)
            }
            index += 1
          }
      }
      base
    }
  }
}
