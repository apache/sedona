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

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.types.BinaryView
import org.datasyslab.jts.geom.impl.DeclaredCoordinateSequence
import org.locationtech.jts.geom._

/**
 * Writes Spark 4.2's native physical representation directly from JTS: a little-endian SRID
 * followed by ISO WKB. The caller validates the SRID against the native SQL type. Coordinate
 * validation follows Spark's WkbReader, without constructing its intermediate geometry model.
 */
private[sql] object NativeSpatialWriter {
  def serialize(geometry: Geometry, srid: Int, geography: Boolean): BinaryView = {
    if (geometry == null) return null
    val dimensions = layout(geometry)
    val buffer = ByteBuffer
      .allocate(Math.addExact(4, size(geometry, dimensions)))
      .order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(srid)
    new Writer(buffer, geography).write(geometry, dimensions)
    BinaryView.fromBytes(buffer.array())
  }

  // Match Sedona's dimension-preserving WKB writer: declared sequences (including empty
  // ones) and measured sequences retain their layout; ordinary NaN-padded Z is omitted.
  private def layout(sequence: CoordinateSequence): Int = {
    if (sequence.isInstanceOf[DeclaredCoordinateSequence] || sequence.getMeasures > 0) {
      (if (sequence.hasZ) 1 else 0) | (if (sequence.hasM) 2 else 0)
    } else if (sequence.hasZ) {
      var i = 0
      while (i < sequence.size()) {
        if (!java.lang.Double.isNaN(sequence.getZ(i))) return 1
        i += 1
      }
      0
    } else 0
  }

  private def layout(geometry: Geometry): Int = geometry match {
    case point: Point => layout(point.getCoordinateSequence)
    case line: LineString => layout(line.getCoordinateSequence)
    case polygon: Polygon =>
      var result = layout(polygon.getExteriorRing.getCoordinateSequence)
      var i = 0
      while (i < polygon.getNumInteriorRing) {
        result |= layout(polygon.getInteriorRingN(i).getCoordinateSequence)
        i += 1
      }
      result
    case collection: GeometryCollection =>
      var result = 0
      var i = 0
      while (i < collection.getNumGeometries) {
        result |= layout(collection.getGeometryN(i))
        i += 1
      }
      result
    case other =>
      throw new IllegalArgumentException(s"Unsupported geometry: ${other.getGeometryType}")
  }

  private def coordinateBytes(count: Int, dimensions: Int): Int =
    Math.multiplyExact(count, (2 + Integer.bitCount(dimensions)) * 8)

  private def isMultipart(geometry: Geometry): Boolean = geometry match {
    case _: MultiPoint | _: MultiLineString | _: MultiPolygon => true
    case _ => false
  }

  private def size(geometry: Geometry, dimensions: Int): Int = geometry match {
    case _: Point => Math.addExact(5, coordinateBytes(1, dimensions))
    case line: LineString => Math.addExact(9, coordinateBytes(line.getNumPoints, dimensions))
    case polygon: Polygon =>
      if (polygon.isEmpty) 9
      else {
        var result =
          Math.addExact(13, coordinateBytes(polygon.getExteriorRing.getNumPoints, dimensions))
        var i = 0
        while (i < polygon.getNumInteriorRing) {
          result = Math.addExact(
            result,
            Math.addExact(
              4,
              coordinateBytes(polygon.getInteriorRingN(i).getNumPoints, dimensions)))
          i += 1
        }
        result
      }
    case collection: GeometryCollection =>
      var result = 9
      var i = 0
      while (i < collection.getNumGeometries) {
        val child = collection.getGeometryN(i)
        val childDimensions = if (isMultipart(collection)) dimensions else layout(child)
        result = Math.addExact(result, size(child, childDimensions))
        i += 1
      }
      result
    case other =>
      throw new IllegalArgumentException(s"Unsupported geometry: ${other.getGeometryType}")
  }

  private def geometryType(geometry: Geometry): Int = geometry match {
    case _: Point => 1
    case _: LineString => 2
    case _: Polygon => 3
    case _: MultiPoint => 4
    case _: MultiLineString => 5
    case _: MultiPolygon => 6
    case _: GeometryCollection => 7
    case other =>
      throw new IllegalArgumentException(s"Unsupported geometry: ${other.getGeometryType}")
  }

  private class Writer(buffer: ByteBuffer, geography: Boolean) {
    // Error offsets describe WKB, excluding Spark's four-byte physical SRID header.
    private def position: Int = buffer.position() - 4
    private def invalid(message: String, offset: Int): Nothing =
      throw QueryExecutionErrors.wkbParseError(message, offset.toLong)

    def write(geometry: Geometry, dimensions: Int, expectedDimensions: Int = -1): Unit = {
      buffer.put(1.toByte)
      val typePosition = position
      val code = geometryType(geometry) + (dimensions & 1) * 1000 + (dimensions & 2) * 1000
      buffer.putInt(code)
      if (expectedDimensions >= 0 && dimensions != expectedDimensions) {
        invalid(s"Invalid or unsupported type $code", typePosition)
      }
      geometry match {
        case point: Point =>
          val sequence = point.getCoordinateSequence
          if (sequence.size() == 0) {
            var i = 0
            while (i < 2 + Integer.bitCount(dimensions)) {
              buffer.putLong(java.lang.Double.doubleToLongBits(Double.NaN))
              i += 1
            }
          } else writeCoordinate(sequence, 0, dimensions, point = true)
        case line: LineString =>
          val countPosition = position
          val sequence = line.getCoordinateSequence
          buffer.putInt(sequence.size())
          if (sequence.size() == 1) invalid("Too few points in linestring", countPosition)
          writeCoordinates(sequence, dimensions)
        case polygon: Polygon =>
          if (polygon.isEmpty) buffer.putInt(0)
          else {
            buffer.putInt(polygon.getNumInteriorRing + 1)
            writeRing(polygon.getExteriorRing.getCoordinateSequence, dimensions)
            var i = 0
            while (i < polygon.getNumInteriorRing) {
              writeRing(polygon.getInteriorRingN(i).getCoordinateSequence, dimensions)
              i += 1
            }
          }
        case collection: GeometryCollection =>
          buffer.putInt(collection.getNumGeometries)
          var i = 0
          while (i < collection.getNumGeometries) {
            val child = collection.getGeometryN(i)
            val childDimensions = if (isMultipart(collection)) dimensions else layout(child)
            write(child, childDimensions, dimensions)
            i += 1
          }
      }
    }

    private def writeRing(sequence: CoordinateSequence, dimensions: Int): Unit = {
      val countPosition = position
      val count = sequence.size()
      buffer.putInt(count)
      writeCoordinates(sequence, dimensions)
      if (count < 4) invalid("Too few points in ring", countPosition)
      if (sequence.getX(0) != sequence.getX(count - 1) ||
        sequence.getY(0) != sequence.getY(count - 1)) {
        invalid("Ring is not closed", countPosition)
      }
    }

    private def writeCoordinates(sequence: CoordinateSequence, dimensions: Int): Unit = {
      var i = 0
      while (i < sequence.size()) {
        writeCoordinate(sequence, i, dimensions, point = false)
        i += 1
      }
    }

    private def writeOrdinate(value: Double, point: Boolean): Boolean = {
      val nan = java.lang.Double.isNaN(value)
      if (java.lang.Double.isInfinite(value) || (!point && nan)) {
        invalid("Invalid coordinate value found", position)
      }
      buffer.putLong(java.lang.Double.doubleToLongBits(value))
      nan
    }

    private def writeCoordinate(
        sequence: CoordinateSequence,
        index: Int,
        dimensions: Int,
        point: Boolean): Unit = {
      val start = position
      val x = sequence.getX(index)
      val y = sequence.getY(index)
      var empty = writeOrdinate(x, point)
      empty = writeOrdinate(y, point) || empty
      if ((dimensions & 1) != 0) empty = writeOrdinate(sequence.getZ(index), point) || empty
      if ((dimensions & 2) != 0) empty = writeOrdinate(sequence.getM(index), point) || empty
      if (geography && !empty && (x < -180 || x > 180 || y < -90 || y > 90)) {
        invalid("Invalid coordinate value found", start)
      }
    }
  }
}
