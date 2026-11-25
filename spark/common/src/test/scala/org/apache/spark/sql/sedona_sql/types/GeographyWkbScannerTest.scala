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

import org.locationtech.jts.io.{ByteOrderValues, WKBWriter, WKTReader}
import org.scalatest.funsuite.AnyFunSuite

class GeographyWkbScannerTest extends AnyFunSuite {
  private def wkb(
      wkt: String,
      dimensions: Int = 2,
      byteOrder: Int = ByteOrderValues.LITTLE_ENDIAN): Array[Byte] =
    new WKBWriter(dimensions, byteOrder).write(new WKTReader().read(wkt))

  private def scan(bytes: Array[Byte], offset: Int = 0): Int =
    GeographyWkbScanner.scan(bytes, offset, true, "shape")

  test("returns the ISO type code of the geography") {
    assert(scan(wkb("POINT (1 2)")) == 1)
    assert(scan(wkb("LINESTRING (0 0, 10 10)")) == 2)
    assert(scan(wkb("POLYGON ((0 0, 10 0, 10 10, 0 0))")) == 3)
    assert(scan(wkb("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))")) == 7)
    assert(scan(wkb("POINT (1 2)", byteOrder = ByteOrderValues.BIG_ENDIAN)) == 1)
  }

  test("walks the WKB from an offset") {
    val bytes = Array[Byte](9, 9, 9, 9) ++ wkb("MULTIPOINT ((1 2), (3 4))")
    assert(scan(bytes, offset = 4) == 4)
  }

  test("marks WKB whose type words carry the flags of EWKB") {
    // JTS marks a Z coordinate with a flag where ISO WKB adds 1000 to the type code
    val result = scan(wkb("POINT Z (1 2 3)", dimensions = 3))
    assert((result & GeographyWkbScanner.NOT_ISO) != 0)
    assert((result & ~GeographyWkbScanner.NOT_ISO) == 1001)
    // The point of a collection carries the flag as the collection does
    val nested = scan(wkb("GEOMETRYCOLLECTION Z (POINT Z (1 2 3))", dimensions = 3))
    assert(nested == (1007 | GeographyWkbScanner.NOT_ISO))
  }

  test("accepts the bounds of longitude and latitude, and an empty point") {
    assert(scan(wkb("LINESTRING (-180 -90, 180 90)")) == 2)
    assert(scan(wkb("POINT EMPTY")) == 1)
  }

  for {
    (wkt, coordinates) <- Seq(
      "POINT (190 10)" -> "longitude 190 and latitude 10",
      "POINT (10 -90.5)" -> "longitude 10 and latitude -90.5",
      "POLYGON ((0 0, 10 0, 10 10, 0 0), (1 1, 2 1, 2 91, 1 1))" -> "latitude 91",
      "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 0)), ((0 0, -181 0, 10 10, 0 0)))" -> "longitude -181")
    byteOrder <- Seq(ByteOrderValues.LITTLE_ENDIAN, ByteOrderValues.BIG_ENDIAN)
  } {
    test(s"rejects $wkt in byte order $byteOrder") {
      val bytes = wkb(wkt, byteOrder = byteOrder)
      val error = intercept[IllegalArgumentException](scan(bytes))
      assert(error.getMessage.contains("'shape'"))
      assert(error.getMessage.contains(coordinates))
      // A value of Spark's native geography type has been checked by Spark
      assert(GeographyWkbScanner.scan(bytes, 0, false, "shape") > 0)
    }
  }

  test("rejects WKB that ends before its coordinates") {
    val bytes = wkb("LINESTRING (0 0, 10 10)")
    intercept[IllegalArgumentException](scan(bytes.take(bytes.length - 1)))
    intercept[IllegalArgumentException](scan(bytes.take(3)))
  }
}
