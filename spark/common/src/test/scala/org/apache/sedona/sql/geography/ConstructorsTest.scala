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

import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.sql.TestBaseScala
import org.locationtech.jts.geom.PrecisionModel

class ConstructorsTest extends TestBaseScala {

  import sparkSession.implicits._
  val precisionModel: PrecisionModel = new PrecisionModel(PrecisionModel.FIXED);

  it("Passed ST_GeogFromWKT") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val row = sparkSession.sql(s"SELECT ST_GeogFromWKT('$wkt', 4326) AS geog").first()
    // Write output with precisionModel
    val geoStr = row.get(0).asInstanceOf[Geography].toString()
    val geog = row.get(0).asInstanceOf[Geography]
    assert(geog.getSRID == 4326)
    assert(geog.isInstanceOf[Geography])
    assert(geoStr == wkt)
  }

  it("Passed ST_GeogFromText") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val row = sparkSession.sql(s"SELECT ST_GeogFromText('$wkt', 4326) AS geog").first()
    // Write output with precisionModel
    val geoStr = row.get(0).asInstanceOf[Geography].toString()
    val geog = row.get(0).asInstanceOf[Geography]
    assert(geog.getSRID == 4326)
    assert(geog.isInstanceOf[Geography])
    assert(geoStr == wkt)
  }

  it("Passed ST_GeogFromWKT no SRID") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val row = sparkSession.sql(s"SELECT ST_GeogFromWKT('$wkt') AS geog").first()
    // Write output with precisionModel
    val geoStr = row.get(0).asInstanceOf[Geography].toString()
    val geog = row.get(0).asInstanceOf[Geography]
    assert(geog.getSRID == 0)
    assert(geog.isInstanceOf[Geography])
    assert(geoStr == wkt)
  }

  it("Passed ST_GeogCollFromText") {
    val baseDf = sparkSession.sql(
      "SELECT 'GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((35 15, 45 15, 40 25, 35 15), (30 10, 40 20, 30 20, 30 10)))' as geom, 4326 as srid")
    var actual = baseDf
      .selectExpr("ST_GeogCollFromText(geom)")
      .first()
      .get(0)
      .asInstanceOf[Geography]
      .toString()
    val expected =
      "GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((35 15, 45 15, 40 25, 35 15), (30 10, 40 20, 30 20, 30 10)))"
    assert(expected.equals(actual))
  }

  it("Passed ST_S2PointFromWKB") {
    val geometryDf = Seq(
      "0101000000000000000000F03F0000000000000040",
      "0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf",
      "010100000000000000000024400000000000002e40",
      "0103000000010000000500000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000001440000000000000f03f0000000000001440000000000000000000000000000000000000000000000000")
      .map(Tuple1.apply)
      .toDF("wkb")

    geometryDf.createOrReplaceTempView("wkbtable")

    val validPointDf = sparkSession.sql("SELECT ST_S2PointFromWKB(wkbtable.wkb) FROM wkbtable")
    var rows = validPointDf.collect()
    assert(rows.length == 4)

    var expectedPoints = Seq("POINT (1 2)", null, "POINT (10 15)", null)
    for (i <- rows.indices) {
      if (expectedPoints(i) == null) {
        assert(rows(i).isNullAt(0))
      } else {
        assert(rows(i).getAs[Geography](0).toString(precisionModel) == expectedPoints(i))
      }
    }
  }

  it("Passed ST_S2LineFromWKB") {
    val geometryDf = Seq(
      "010200000003000000000000000000000000000000000000000000000000000840000000000000084000000000000010400000000000001040",
      "0101000000000000000000F03F0000000000000040",
      "01020000000300000000000000000000c000000000000000c000000000000010400000000000001040000000000000104000000000000000c0",
      "0103000000010000000500000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000001440000000000000f03f0000000000001440000000000000000000000000000000000000000000000000")
      .map(Tuple1.apply)
      .toDF("wkb")

    geometryDf.createOrReplaceTempView("wkbtable")

    val validLineDf =
      sparkSession.sql("SELECT ST_S2LineFromWKB(wkbtable.wkb) FROM wkbtable")
    val rows = validLineDf.collect()
    assert(rows.length == 4)

    val expectedPoints =
      Seq("LINESTRING (0 0, 3 3, 4 4)", null, "LINESTRING (-2 -2, 4 4, 4 -2)", null)
    for (i <- rows.indices) {
      if (expectedPoints(i) == null) {
        assert(rows(i).isNullAt(0))
      } else {
        assert(rows(i).getAs[Geography](0).toString(precisionModel) == expectedPoints(i))
      }
    }
  }

  it("Passed ST_S2MLineFromText") {
    val mLineDf =
      sparkSession.sql("select ST_S2MLineFromText('MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))')")
    assert(mLineDf.count() == 1)
  }

  it("Passed ST_S2MPolyFromText") {
    val mLineDf = sparkSession.sql(
      "select ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))')")
    assert(mLineDf.count() == 1)
  }

  it("Passed ST_S2MPointFromText") {
    val baseDf =
      sparkSession.sql("SELECT 'MULTIPOINT ((10 10), (20 20), (30 30))' as geom, 4326 as srid")
    val actual =
      baseDf
        .selectExpr("ST_S2MPointFromText(geom)")
        .first()
        .get(0)
        .asInstanceOf[Geography]
        .toString()
    val expected = "MULTIPOINT ((10 10), (20 20), (30 30))"
    assert(expected.equals(actual))
  }

  it("Passed ST_ToGeography") {
    val geog = "MULTIPOINT ((10 10), (20 20), (30 30))"

    // Escape into a SQL string literal
    val df = sparkSession
      .sql(s"SELECT ST_ToGeography('$geog') AS geog_col")

    val actual = df
      .first()
      .get(0)
      .asInstanceOf[Geography]
      .toString()

    val expected = "MULTIPOINT ((10 10), (20 20), (30 30))"
    assert(expected.equals(actual))
  }

}
