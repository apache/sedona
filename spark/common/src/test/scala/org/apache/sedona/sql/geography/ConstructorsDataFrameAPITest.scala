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
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sedona_sql.expressions.st_constructors
import org.junit.Assert.assertEquals
import org.locationtech.jts.geom.{Geometry, PrecisionModel}
import org.locationtech.jts.io.WKTWriter

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
    val actualResult = df.take(1)(0).get(0).asInstanceOf[Geography].toEWKT()
    val expectedResult =
      "GEOMETRYCOLLECTION (SRID=4326; POINT (0 1), SRID=4326; POINT (0 1), SRID=4326; POINT (2 3), SRID=4326; LINESTRING (2 3, 4 5), SRID=4326; LINESTRING (0 1, 2 3), SRID=4326; LINESTRING (4 5, 6 7), SRID=4326; POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (9 1, 9 9, 1 9, 1 1, 9 1)), SRID=4326; POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (9 1, 9 9, 1 9, 1 1, 9 1)), SRID=4326; POLYGON ((-9 0, -9 10, -1 10, -1 0, -9 0)))"
    assert(actualResult == expectedResult)
  }

  it("passed ST_GeogFromEWKB") {
    val wkbSeq = Seq[Array[Byte]](
      Array[Byte](1, 2, 0, 0, 32, -26, 16, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0,
        0, 0, -128, -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27,
        -65))
    val df = wkbSeq.toDF("wkb") select (st_constructors.ST_GeogFromEWKB("wkb"))
    val actualResult = df.take(1)(0).get(0).asInstanceOf[Geography].toEWKT()
    val expectedResult = {
      "SRID=4326; LINESTRING (-2.1 -0.4, -1.5 -0.7)"
    }
    assert(df.take(1)(0).get(0).asInstanceOf[Geography].getSRID == 4326)
    assert(actualResult == expectedResult)
  }

  it("passed st_geomfromewkt") {
    val df = sparkSession
      .sql("SELECT 'SRID=4269;POINT(0.0 1.0)' AS wkt")
      .select(st_constructors.ST_GeogFromEWKT("wkt"))
    val actualResult = df.take(1)(0).get(0).asInstanceOf[Geography]
    assert(actualResult.toEWKT() == "SRID=4269; POINT (0 1)")
    assert(actualResult.getSRID == 4269)
  }

  it("Passed ST_GeogFromGeoHash") {
    val df = sparkSession
      .sql("SELECT '9q9j8ue2v71y5zzy0s4q' AS geohash")
      .select(st_constructors.ST_GeogFromGeoHash("geohash", 16))
    val actualResult =
      df.take(1)(0).get(0).asInstanceOf[Geography].toText(new PrecisionModel(1e6))
    var expectedWkt =
      "POLYGON ((-122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162))"
    assertEquals(expectedWkt, actualResult)
  }

  it("passed st_geogtogeometry multipolygon") {
    val wkt =
      "MULTIPOLYGON (" +
        "((10 10, 70 10, 70 70, 10 70, 10 10), (20 20, 60 20, 60 60, 20 60, 20 20))," +
        "((30 30, 50 30, 50 50, 30 50, 30 30), (36 36, 44 36, 44 44, 36 44, 36 36))" +
        ")"

    val df = sparkSession
      .sql(s"SELECT '$wkt' AS wkt")
      .select(st_constructors.ST_GeogFromWKT(col("wkt"), lit(4326)).as("geog"))
      .select(st_constructors.ST_GeogToGeometry(col("geog")).as("geom"))

    val geom = df.head().getAs[Geometry]("geom")
    assert(geom.getGeometryType == "MultiPolygon")

    val expectedWkt =
      "MULTIPOLYGON (((10 10, 70 10, 70 70, 10 70, 10 10), " +
        "(20 20, 20 60, 60 60, 60 20, 20 20)), " +
        "((30 30, 50 30, 50 50, 30 50, 30 30), " +
        "(36 36, 36 44, 44 44, 44 36, 36 36)))"

    val writer = new WKTWriter()
    writer.setFormatted(false)
    writer.setPrecisionModel(new PrecisionModel(PrecisionModel.FIXED))

    val got = writer.write(geom)
    assert(got == expectedWkt)
    assert(geom.getSRID == 4326)
  }

  it("Passed ST_GeomToGeography multilinestring") {
    var wkt = "MULTILINESTRING " + "((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"
    val df = sparkSession
      .sql(s"SELECT '$wkt' AS wkt")
      .select(st_constructors.ST_GeomFromWKT(col("wkt")).as("geom"))
      .select(st_constructors.ST_GeomToGeography(col("geom")).as("geog"))
    val geog = df.head().getAs[Geography]("geog")
    assertEquals(wkt, geog.toString)
  }

}
