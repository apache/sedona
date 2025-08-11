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
      Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75,
        -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
    val rawWkbDf = wkbSeq.toDF("wkb")
    rawWkbDf.createOrReplaceTempView("rawWKBTable")
    val geometries =
      sparkSession.sql("SELECT ST_GeogFromEWKB(rawWKBTable.wkb) as countyshape from rawWKBTable")
    val expectedGeom = {
      "LINESTRING (-2.1 -0.4, -1.5 -0.7)"
    }
    assert(geometries.first().getAs[Geography](0).toString.equals(expectedGeom))
  }
}
