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
package org.apache.sedona.sql.geography

import org.apache.sedona.common.S2Geography.{Geography, WKBReader}
import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sedona_sql.expressions.{implicits, st_constructors}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.locationtech.jts.geom.PrecisionModel

class ConstructorsDataFrameAPITest extends TestBaseScala {
  import sparkSession.implicits._

  it("Passed ST_GeogFromWKB point") {

    val wkbSeq =
      Seq(Array[Byte](1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 46, 64))

    val df = wkbSeq.toDF("wkb").select(st_constructors.ST_GeogFromWKB(col("wkb")))
    val actualResult = df.take(1)(0).get(0).asInstanceOf[Geography].toString()
    val expectedResult = "POINT (10 15)"
    assert(actualResult == expectedResult)
  }

  it("passed ST_GeogFromWKB linestring") {
    val wkbSeq = Seq[Array[Byte]](
      Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75,
        -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
    val df = wkbSeq.toDF("wkb").select(st_constructors.ST_GeogFromWKB(col("wkb")))
    val actualResult = df.take(1)(0).get(0).asInstanceOf[Geography].toString()
    val expectedResult =
      "LINESTRING (-2.1 -0.4, -1.5 -0.7)"
    assert(actualResult == expectedResult)
  }

  it("passed ST_GeogFromWKB collection") {
    val hexStr =
      "0107000020E6100000090000000101000020E61000000000000000000000000000000000F03F0101000020E61000000000000000000000000000000000F03F0101000020E6100000000000000000004000000000000008400102000020E61000000200000000000000000000400000000000000840000000000000104000000000000014400102000020E6100000020000000000000000000000000000000000F03F000000000000004000000000000008400102000020E6100000020000000000000000001040000000000000144000000000000018400000000000001C400103000020E61000000200000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000002240000000000000224000000000000022400000000000002240000000000000F03F000000000000F03F000000000000F03F0103000020E61000000200000005000000000000000000000000000000000000000000000000000000000000000000244000000000000024400000000000002440000000000000244000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000002240000000000000224000000000000022400000000000002240000000000000F03F000000000000F03F000000000000F03F0103000020E6100000010000000500000000000000000022C0000000000000000000000000000022C00000000000002440000000000000F0BF0000000000002440000000000000F0BF000000000000000000000000000022C00000000000000000";
    val array = WKBReader.hexToBytes(hexStr)
    val wkbSeq = Seq[Array[Byte]](array)
    val df = wkbSeq.toDF("wkb").select(st_constructors.ST_GeogFromWKB(col("wkb")))
    val actualResult = df.take(1)(0).get(0).asInstanceOf[Geography].toString()
    val expectedResult =
      "GEOMETRYCOLLECTION (POINT (0 1), POINT (0 1), POINT (2 3), LINESTRING (2 3, 4 5), LINESTRING (0 1, 2 3), LINESTRING (4 5, 6 7), POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (9 1, 9 9, 1 9, 1 1, 9 1)), POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (9 1, 9 9, 1 9, 1 1, 9 1)), POLYGON ((-9 0, -9 10, -1 10, -1 0, -9 0)))"
    assert(actualResult == expectedResult)
  }

}
