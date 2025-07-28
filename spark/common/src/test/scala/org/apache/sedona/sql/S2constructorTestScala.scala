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
package org.apache.sedona.sql

import org.apache.sedona.common.S2Geography.{S2Geography}
import org.locationtech.jts.geom.{PrecisionModel}

class S2constructorTestScala extends TestBaseScala {

  import sparkSession.implicits._
  val precisionModel: PrecisionModel = new PrecisionModel(PrecisionModel.FIXED);

  it("Passed ST_S2PointFromWKB") {
    val geometryDf = Seq(
      "0101000000000000000000F03F0000000000000040",
      "0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf",
      "010100000000000000000024400000000000002e40",
      "0103000000010000000500000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000001440000000000000f03f0000000000001440000000000000000000000000000000000000000000000000")
      .map(Tuple1.apply)
      .toDF("wkb")

    geometryDf.createOrReplaceTempView("wkbtable")

    var validPointDf = sparkSession.sql("SELECT ST_S2PointFromWKB(wkbtable.wkb) FROM wkbtable")
    var rows = validPointDf.collect()
    assert(rows.length == 4)

    var expectedPoints = Seq("POINT (1 2)", null, "POINT (10 15)", null)
    for (i <- rows.indices) {
      if (expectedPoints(i) == null) {
        assert(rows(i).isNullAt(0))
      } else {
        assert(rows(i).getAs[S2Geography](0).toString(precisionModel) == expectedPoints(i))
      }
    }
  }

  it("Passed ST_S2LinestringFromWKB") {
    val geometryDf = Seq(
      "010200000003000000000000000000000000000000000000000000000000000840000000000000084000000000000010400000000000001040",
      "0101000000000000000000F03F0000000000000040",
      "01020000000300000000000000000000c000000000000000c000000000000010400000000000001040000000000000104000000000000000c0",
      "0103000000010000000500000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000001440000000000000f03f0000000000001440000000000000000000000000000000000000000000000000")
      .map(Tuple1.apply)
      .toDF("wkb")

    geometryDf.createOrReplaceTempView("wkbtable")

    var validLineDf =
      sparkSession.sql("SELECT ST_S2LinestringFromWKB(wkbtable.wkb) FROM wkbtable")
    var rows = validLineDf.collect()
    assert(rows.length == 4)

    var expectedPoints =
      Seq("LINESTRING (0 0, 3 3, 4 4)", null, "LINESTRING (-2 -2, 4 4, 4 -2)", null)
    for (i <- rows.indices) {
      if (expectedPoints(i) == null) {
        assert(rows(i).isNullAt(0))
      } else {
        assert(rows(i).getAs[S2Geography](0).toString(precisionModel) == expectedPoints(i))
      }
    }
  }

  it("Passed ST_S2GeogFromWKB") {
    // UTF-8 encoded WKB String
    val polygonWkbDf = sparkSession.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load("spark/common/src/test/resources/county_small_wkb.tsv")
    polygonWkbDf.createOrReplaceTempView("polygontable")
    val polygonDf =
      sparkSession.sql("select ST_GeomFromWKB(polygontable._c0) as countyshape from polygontable")
    assert(polygonDf.count() == 100)
    // RAW binary array
    val wkbSeq = Seq[Array[Byte]](
      Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75,
        -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
    val rawWkbDf = wkbSeq.toDF("wkb")
    rawWkbDf.createOrReplaceTempView("rawWKBTable")
    val geometries = {
      sparkSession.sql("SELECT ST_S2GeogFromWKB(rawWKBTable.wkb) as countyshape from rawWKBTable")
    }
    val expectedGeom =
      "LINESTRING (-2.1047439575195317 -0.35482788085937506, -1.4960645437240603 -0.6676061153411864)"
    assert(
      geometries
        .first()
        .getAs[S2Geography](0)
        .toString(new PrecisionModel(1e16))
        .equals(expectedGeom))
    // null input
    val nullGeom = sparkSession.sql("SELECT ST_S2GeogFromWKB(null)")
    assert(nullGeom.first().isNullAt(0))
    // Fail on wrong input type
    intercept[Exception] {
      sparkSession.sql("SELECT ST_S2GeogFromWKB(0)").collect()
    }
  }

  it("Passed ST_S2MLineFromText") {
    var mLineDf =
      sparkSession.sql("select ST_S2MLineFromText('MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))')")
    assert(mLineDf.count() == 1)
  }

  it("Passed ST_S2MPolyFromText") {
    var mLineDf = sparkSession.sql(
      "select ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))')")
    assert(mLineDf.count() == 1)
  }

  it("Passed ST_S2MPointFromText") {
    val baseDf =
      sparkSession.sql("SELECT 'MULTIPOINT ((10 10), (20 20), (30 30))' as geom, 4326 as srid")
    var actual =
      baseDf
        .selectExpr("ST_S2MPointFromText(geom)")
        .first()
        .get(0)
        .asInstanceOf[S2Geography]
        .toText(new PrecisionModel(PrecisionModel.FIXED))
    val expected = "MULTIPOINT ((10 10), (20 20), (30 30))"
    assert(expected.equals(actual))

//    val actualGeom =
//      baseDf.selectExpr("ST_S2MPointFromText(geom, srid)").first().get(0).asInstanceOf[Geometry]
//    actual = actualGeom.toText
//    assert(expected.equals(actual))
//    val actualSrid = actualGeom.getSRID
//    assert(4326 == actualSrid)
  }

  it("Passed ST_S2GeogCollFromText") {
    val baseDf = sparkSession.sql(
      "SELECT 'GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((35 15, 45 15, 40 25, 35 15), (30 10, 40 20, 30 20, 30 10)))' as geom, 4326 as srid")
    var actual = baseDf
      .selectExpr("ST_S2GeogCollFromText(geom)")
      .first()
      .get(0)
      .asInstanceOf[S2Geography]
      .toText(new PrecisionModel(PrecisionModel.FIXED))
    println(actual)
    val expected =
      "GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((35 15, 45 15, 40 25, 35 15), (30 10, 40 20, 30 20, 30 10)))"
    assert(expected.equals(actual))
  }

}
