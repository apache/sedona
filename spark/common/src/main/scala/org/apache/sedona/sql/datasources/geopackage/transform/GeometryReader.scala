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
package org.apache.sedona.sql.datasources.geopackage.transform

import org.apache.sedona.sql.datasources.geopackage.errors.GeopackageException
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.io.WKBReader

import java.nio.{ByteBuffer, ByteOrder}

object GeometryReader {

  def extractWKB(gpkgGeom: Array[Byte]): Array[Byte] = {
    val reader = ByteBuffer.wrap(gpkgGeom)

    val magic = new Array[Byte](2)
    reader.get(magic)

    if (magic(0) != 71 || magic(1) != 80) {
      throw new GeopackageException("Invalid GeoPackage geometry magic number")
    }

    // Read version and flags
    val versionByte = Array.ofDim[Byte](1)
    reader.get(versionByte)

    val flagsByte = Array.ofDim[Byte](1)
    reader.get(flagsByte)

    val resolvedFlags = readFlags(flagsByte(0))

    reader.order(resolvedFlags._2)

    // skip srid for now
    val srid = reader.getInt()

    skipEnvelope(resolvedFlags._1, reader)

    val wkb = new Array[Byte](reader.remaining())
    reader.get(wkb)

    val wkbReader = new WKBReader(new GeometryFactory(new PrecisionModel(), srid))
    val geom = wkbReader.read(wkb)

    // that needs rewriting
    GeometryUDT.serialize(geom)
  }

  def skipEnvelope(value: Any, buffer: ByteBuffer): Any = {
    value match {
      case 0 => null
      case 1 | 3 => buffer.get(new Array[Byte](32))
      case 2 => buffer.get(new Array[Byte](48))
      case 4 => buffer.get(new Array[Byte](64))
      case _ =>
        throw new GeopackageException(
          "Unexpected GeoPackage Geometry flags. " +
            "Envelope contents indicator must be between 0 and 4. Actual: " + value)
    }
  }

  private def readFlags(flags: Byte): (Int, ByteOrder, Boolean) = {
    val reserved7 = (flags >> 7) & 1
    val reserved6 = (flags >> 6) & 1
    if (reserved7 != 0 || reserved6 != 0)
      throw new GeopackageException(
        "Unexpected GeoPackage Geometry flags. " +
          "Flag bit 7 and 6 should both be 0, 7=" + reserved7 + ", 6=" + reserved6)
    val binaryType = (flags >> 5) & 1
    val extended = binaryType == 1
    val emptyValue = (flags >> 4) & 1
    val empty = emptyValue == 1
    val envelopeIndicator = (flags >> 1) & 7
    if (envelopeIndicator > 4)
      throw new GeopackageException(
        "Unexpected GeoPackage Geometry flags. " +
          "Envelope contents indicator must be between 0 and 4. Actual: " + envelopeIndicator)
    val byteOrderValue = flags & 1
    val byteOrder =
      if (byteOrderValue == 0) ByteOrder.BIG_ENDIAN
      else ByteOrder.LITTLE_ENDIAN

    (envelopeIndicator, byteOrder, extended)
  }

}
