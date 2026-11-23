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
package org.apache.sedona.sql.utils

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays

import org.apache.sedona.common.S2Geography.{Geography, WKBGeography, WKBWriter}
import org.apache.spark.sql.sedona_sql.types.PublicWkbHeaderAdapter
import org.locationtech.jts.io.ByteOrderValues

/** SQL geography transport. The common geography serializer retains its separate RDD format. */
object GeographySerializer {
  def serialize(geography: Geography): Array[Byte] = {
    if (geography == null) return null
    val source = geography match {
      // Preserve lazy coordinates and layouts without constructing JTS or S2 objects.
      case value: WKBGeography => value.getWKBBytes
      case _ => new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, false).write(geography)
    }
    serializeWkb(source, geography.getSRID)
  }

  /**
   * The SQL transport of the geography that `source` is the WKB of. The coordinates are not
   * parsed, so a geography that is only passed on costs no JTS or S2 object.
   */
  def serializeWkb(source: Array[Byte], srid: Int): Array[Byte] = {
    val wkb = PublicWkbHeaderAdapter.toIsoWkb(source)
    ByteBuffer
      .allocate(Integer.BYTES + wkb.length)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(srid)
      .put(wkb)
      .array()
  }

  def deserialize(value: Array[Byte]): Geography = {
    if (value == null) return null
    if (value.length < Integer.BYTES + 5) {
      throw new IllegalArgumentException("Truncated SQL geography payload")
    }
    // Geography retains WKB, so it must own its bytes rather than alias a mutable row buffer.
    // The public factory also retains the configured eager/lazy S2 initialization behavior.
    WKBGeography.fromWKB(Arrays.copyOfRange(value, Integer.BYTES, value.length), readSRID(value))
  }

  /** Where the WKB of a serialized geography begins, after its SRID. */
  val WKB_OFFSET: Int = Integer.BYTES

  /** The WKB and the SRID of a serialized geography. The WKB is a copy that the caller owns. */
  def wkbAndSrid(value: Array[Byte]): (Array[Byte], Int) =
    (Arrays.copyOfRange(value, WKB_OFFSET, value.length), readSRID(value))

  def readSRID(value: Array[Byte]): Int = {
    if (value.length < Integer.BYTES) {
      throw new IllegalArgumentException("Truncated spatial SRID header")
    }
    // Read without a buffer object: a writer and a reader call this for every value
    (value(0) & 0xff) | (value(1) & 0xff) << 8 | (value(2) & 0xff) << 16 | (value(3) & 0xff) << 24
  }
}
