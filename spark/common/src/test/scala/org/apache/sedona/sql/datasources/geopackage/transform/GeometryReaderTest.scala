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

import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.locationtech.jts.geom.{GeometryFactory, Polygon}
import org.locationtech.jts.io.{Ordinate, WKBWriter}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.{ByteBuffer, ByteOrder}
import java.util.EnumSet

class GeometryReaderTest extends AnyFunSuite {
  test("extractWKB retains an empty geometry's declared layout and GeoPackage SRID") {
    val factory = new GeometryFactory()
    Seq((3, 0), (3, 1), (4, 1)).foreach { case (dimension, measures) =>
      val sequence = factory.getCoordinateSequenceFactory.create(0, dimension, measures)
      val empty = factory.createPolygon(factory.createLinearRing(sequence))
      val writer = new WKBWriter(dimension)
      val ordinates = EnumSet.of(Ordinate.X, Ordinate.Y)
      if (dimension - measures > 2) ordinates.add(Ordinate.Z)
      if (measures > 0) ordinates.add(Ordinate.M)
      writer.setOutputOrdinates(ordinates)
      val wkb = writer.write(empty)
      val bytes = ByteBuffer
        .allocate(8 + wkb.length)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put('G'.toByte)
        .put('P'.toByte)
        .put(0.toByte)
        .put(1.toByte)
        .putInt(4326)
        .put(wkb)
        .array()

      val result = GeometryUDT.deserialize(GeometryReader.extractWKB(bytes)).asInstanceOf[Polygon]
      val resultSequence = result.getExteriorRing.getCoordinateSequence
      assert(resultSequence.getDimension == dimension)
      assert(resultSequence.getMeasures == measures)
      assert(result.getSRID == 4326)
    }
  }
}
