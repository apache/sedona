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

import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.BinaryView
import org.datasyslab.jts.io.WKBReader
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.InStream

/** Reads Spark's SRID-prefixed WKB without constructing an intermediate Spark geometry model. */
private[types] object NativeSpatialReader {
  private val HeaderSize = 4

  private def srid(value: BinaryView): Int = {
    val raw = value.getInt(0)
    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) raw
    else java.lang.Integer.reverseBytes(raw)
  }

  def geometry(value: BinaryView): Geometry = {
    val length = NativeSpatialValidator.validate(value, geography = false)
    // The validator bounds every count before JTS allocates coordinate arrays. Supplying the
    // factory SRID up front also avoids copying the parsed geometry just to change its factory.
    WKBReader
      .forDeclaredDimensions(srid(value))
      .read(new InStream {
        private val base = value.getBaseObject
        private val start = value.getBaseOffset + HeaderSize
        private var offset = 0
        override def read(bytes: Array[Byte]): Int = {
          val count = math.min(bytes.length, length - offset)
          // JTS requests one byte, one int, or one double at a time. Copy raw bits here;
          // its reader applies the WKB byte order. Avoid generic memcpy for every ordinate.
          count match {
            case 1 => bytes(0) = Platform.getByte(base, start + offset)
            case 4 =>
              Platform
                .putInt(bytes, Platform.BYTE_ARRAY_OFFSET, Platform.getInt(base, start + offset))
            case 8 =>
              Platform.putLong(
                bytes,
                Platform.BYTE_ARRAY_OFFSET,
                Platform.getLong(base, start + offset))
            case _ =>
              Platform.copyMemory(base, start + offset, bytes, Platform.BYTE_ARRAY_OFFSET, count)
          }
          offset += count
          count
        }
      })
  }

  def geography(value: BinaryView): WKBGeography = {
    val length = NativeSpatialValidator.validate(value, geography = true)
    // Geography parses lazily, so it must own the payload after the source row is reused.
    val bytes = new Array[Byte](length)
    Platform.copyMemory(
      value.getBaseObject,
      value.getBaseOffset + HeaderSize,
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      length)
    WKBGeography.fromWKB(bytes, srid(value))
  }
}
