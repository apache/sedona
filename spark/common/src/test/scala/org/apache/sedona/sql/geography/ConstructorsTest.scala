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
import org.apache.sedona.common.geography.Constructors
import org.apache.sedona.sql.TestBaseScala
import org.junit.Assert.assertEquals
import org.locationtech.jts.geom.PrecisionModel

class ConstructorsTest extends TestBaseScala {

  import sparkSession.implicits._
  val precisionModel: PrecisionModel = new PrecisionModel(PrecisionModel.FIXED);

  it("Passed ST_GeogFromGeoHash") {
    var geohash = "9q9j8ue2v71y5zzy0s4q";
    var precision = 16;
    var row =
      sparkSession.sql(s"SELECT ST_GeogFromGeoHash('$geohash','$precision') AS geog").first()
    var geoStr = row.get(0).asInstanceOf[Geography].toText(new PrecisionModel(1e6))
    var expectedWkt =
      "SRID=4326; POLYGON ((-122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162))"
    assertEquals(expectedWkt, geoStr)

    geohash = "s00twy01mt"
    precision = 4;
    row = sparkSession.sql(s"SELECT ST_GeogFromGeoHash('$geohash','$precision') AS geog").first()
    geoStr = row.get(0).asInstanceOf[Geography].toText(new PrecisionModel(1e6))
    expectedWkt =
      "SRID=4326; POLYGON ((0.703125 0.8789062, 1.0546875 0.8789062, 1.0546875 1.0546875, 0.703125 1.0546875, 0.703125 0.8789062))"
    assertEquals(expectedWkt, geoStr)
  }

  it("Passed ST_GeogFromWKT") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val wktExpected = "SRID=4326; LINESTRING (1 2, 3 4, 5 6)"
    val row = sparkSession.sql(s"SELECT ST_GeogFromWKT('$wkt', 4326) AS geog").first()
    // Write output with precisionModel
    val geoStr = row.get(0).asInstanceOf[Geography].toString()
    val geog = row.get(0).asInstanceOf[Geography]
    assert(geog.getSRID == 4326)
    assert(geog.isInstanceOf[Geography])
    assert(geoStr == wktExpected)
  }

  it("Passed ST_GeogFromText") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val wktExpected = "SRID=4326; LINESTRING (1 2, 3 4, 5 6)"
    val row = sparkSession.sql(s"SELECT ST_GeogFromText('$wkt', 4326) AS geog").first()
    // Write output with precisionModel
    val geoStr = row.get(0).asInstanceOf[Geography].toString()
    val geog = row.get(0).asInstanceOf[Geography]
    assert(geog.getSRID == 4326)
    assert(geog.isInstanceOf[Geography])
    assert(geoStr == wktExpected)
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

  it("Passed ST_GeogFromEWKT") {
    val mixedWktGeometryInputLocation =
      getClass.getResource("/county_small.tsv").getPath
    var polygonWktDf = sparkSession.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(mixedWktGeometryInputLocation)
    polygonWktDf.createOrReplaceTempView("polygontable")
    var polygonDf = sparkSession.sql(
      "select ST_GeogFromEWKT(polygontable._c0) as countyshape from polygontable")
    assert(polygonDf.count() == 100)
    val nullGeog = sparkSession.sql("select ST_GeogFromEWKT(null)")
    assert(nullGeog.first().isNullAt(0))
    val pointDf =
      sparkSession.sql("select ST_GeogFromEWKT('SRID=4269;POINT(-71.064544 42.28787)')")
    assert(pointDf.count() == 1)
    assert(pointDf.first().getAs[Geography](0).getSRID == 4269)
  }

  it("Passed ST_GeogFromWKB") {
    // RAW binary array
    val wkbSeq = Seq[Array[Byte]](
      Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75,
        -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
    val rawWkbDf = wkbSeq.toDF("wkb")
    rawWkbDf.createOrReplaceTempView("rawWKBTable")
    val geometries = {
      sparkSession.sql("SELECT ST_GeogFromWKB(rawWKBTable.wkb) as countyshape from rawWKBTable")
    }
    val expectedGeom =
      "LINESTRING (-2.1047439575195317 -0.35482788085937506, -1.4960645437240603 -0.6676061153411864)"
    assert(
      geometries
        .first()
        .getAs[Geography](0)
        .toString(new PrecisionModel(1e16))
        .equals(expectedGeom))
    // null input
    val nullGeom = sparkSession.sql("SELECT ST_GeogFromWKB(null)")
    assert(nullGeom.first().isNullAt(0))
  }

  it("Passed ST_GeogFromEWKB") {
    // UTF-8 encoded WKB String
    val mixedWkbGeometryInputLocation =
      getClass.getResource("/county_small_wkb.tsv").getPath
    val polygonWkbDf = sparkSession.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(mixedWkbGeometryInputLocation)
    polygonWkbDf.createOrReplaceTempView("polygontable")
    val polygonDf = sparkSession.sql(
      "select ST_GeogFromEWKB(polygontable._c0) as countyshape from polygontable")
    assert(polygonDf.count() == 100)
    // RAW binary array
    val wkbSeq = Seq[Array[Byte]](
      Array[Byte](1, 2, 0, 0, 32, -26, 16, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0,
        0, 0, -128, -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27,
        -65))
    val rawWkbDf = wkbSeq.toDF("wkb")
    rawWkbDf.createOrReplaceTempView("rawWKBTable")
    val geography =
      sparkSession.sql("SELECT ST_GeogFromEWKB(rawWKBTable.wkb) as countyshape from rawWKBTable")
    val expectedGeog = {
      "SRID=4326; LINESTRING (-2.1 -0.4, -1.5 -0.7)"
    }
    assert(geography.first().getAs[Geography](0).getSRID == 4326)
    assert(geography.first().getAs[Geography](0).toString.equals(expectedGeog))
  }
}
