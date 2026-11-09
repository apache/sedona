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
    val wkb = PublicWkbHeaderAdapter.toIsoWkb(source)
    ByteBuffer
      .allocate(Integer.BYTES + wkb.length)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(geography.getSRID)
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

  def readSRID(value: Array[Byte]): Int = {
    if (value.length < Integer.BYTES) {
      throw new IllegalArgumentException("Truncated spatial SRID header")
    }
    ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt
  }
}
