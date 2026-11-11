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

import org.apache.sedona.common.geometrySerde.{GeometrySerializer => LayoutPolicy}
import org.apache.spark.sql.sedona_sql.types.PublicWkbHeaderAdapter
import org.datasyslab.jts.io.{WKBReader, WKBWriter}
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.{ByteOrderValues, ParseException}

/**
 * SQL geometry transport: a signed little-endian int32 SRID followed by ISO WKB. RDD/index
 * serializers use their separate compact format.
 */
object GeometrySerializer {

  /**
   * Given a geometry returns array of bytes
   *
   * @param geometry
   *   JTS geometry
   * @return
   *   SRID-prefixed ISO WKB preserving coordinate dimensions
   */
  def serialize(geometry: Geometry): Array[Byte] = {
    if (geometry == null) return null
    validateLayout(geometry)
    val writer = new WKBWriter(4, ByteOrderValues.LITTLE_ENDIAN, false)
    writer.setPreserveCoordinateDimensions(true)
    val wkb = PublicWkbHeaderAdapter.toIsoWkb(writer.write(geometry))
    ByteBuffer
      .allocate(Integer.BYTES + wkb.length)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(geometry.getSRID)
      .put(wkb)
      .array()
  }

  // Preserve the existing layout contract without using compact byte serialization.
  // General collections have independent member layouts; polygons and multipart values do not.
  private def validateLayout(geometry: Geometry): Unit = {
    if (geometry != null) {
      if (geometry.getGeometryType == "GeometryCollection") {
        (0 until geometry.getNumGeometries).foreach(i => validateLayout(geometry.getGeometryN(i)))
      } else {
        LayoutPolicy.getCoordinateType(geometry)
      }
    }
  }

  /**
   * Given SRID-prefixed ISO WKB bytes returns Geometry
   *
   * @param value
   *   SRID-prefixed ISO WKB bytes
   * @return
   *   JTS geometry
   */
  def deserialize(value: Array[Byte]): Geometry = {
    if (value == null) {
      return new GeometryFactory().createGeometryCollection()
    }
    if (value.length < Integer.BYTES) {
      throw new IllegalArgumentException("Truncated spatial SRID header")
    }
    val srid = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt
    try {
      // The byte-array reader bounds declared counts by payload size; its stream entry point does not.
      WKBReader
        .forDeclaredDimensions(srid)
        .read(Arrays.copyOfRange(value, Integer.BYTES, value.length))
    } catch {
      case e: ParseException =>
        throw new IllegalArgumentException("Invalid serialized Geometry", e)
    }
  }
}
