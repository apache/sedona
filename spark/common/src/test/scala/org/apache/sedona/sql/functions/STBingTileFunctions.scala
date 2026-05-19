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
package org.apache.sedona.sql.functions

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.functions.expr
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.mutable

// Bing Tile function tests
class STBingTileFunctions
    extends TestBaseScala
    with Matchers
    with GeometrySample
    with GivenWhenThen {
  import sparkSession.implicits._

  describe("should pass ST_BingTile") {

    it("should create tile (3,5,3) as quadkey '213'") {
      val result = sparkSession.sql("SELECT ST_BingTile(3, 5, 3)").collect()(0).getString(0)
      result should equal("213")
    }

    it("should create tile (21845,13506,15) as quadkey '123030123010121'") {
      val result =
        sparkSession.sql("SELECT ST_BingTile(21845, 13506, 15)").collect()(0).getString(0)
      result should equal("123030123010121")
    }
  }

  describe("should pass ST_BingTileAt") {

    // ST_BingTileAt(lon, lat, zoom) — longitude first
    it("should return quadkey '123030123010121' for (lon=60, lat=30.12, zoom=15)") {
      val result =
        sparkSession.sql("SELECT ST_BingTileAt(60.0, 30.12, 15)").collect()(0).getString(0)
      result should equal("123030123010121")
    }

    it("should return tile(0,1,1) for (lon=-0.002, lat=0, zoom=1)") {
      val qk = sparkSession.sql("SELECT ST_BingTileAt(-0.002, 0.0, 1)").collect()(0).getString(0)
      val x = sparkSession.sql(s"SELECT ST_BingTileX('$qk')").collect()(0).getInt(0)
      val y = sparkSession.sql(s"SELECT ST_BingTileY('$qk')").collect()(0).getInt(0)
      x should equal(0)
      y should equal(1)
    }
  }

  describe("should pass ST_BingTilesAround") {

    // ST_BingTilesAround(lon, lat, zoom) — longitude first
    it("should return all 4 tiles at zoom 1 for (lon=60, lat=30.12)") {
      val result = sparkSession
        .sql("SELECT ST_BingTilesAround(60.0, 30.12, 1)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("0", "2", "1", "3")))
    }

    it("should return 9 tiles at zoom 15 for (lon=60, lat=30.12)") {
      val result = sparkSession
        .sql("SELECT ST_BingTilesAround(60.0, 30.12, 15)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(
        mutable.WrappedArray.make(Array(
          "123030123010102",
          "123030123010120",
          "123030123010122",
          "123030123010103",
          "123030123010121",
          "123030123010123",
          "123030123010112",
          "123030123010130",
          "123030123010132")))
    }

    it("should return 4 tiles at corner (lon=-180, lat=-85.05112878, zoom=3)") {
      val result = sparkSession
        .sql("SELECT ST_BingTilesAround(-180.0, -85.05112878, 3)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("220", "222", "221", "223")))
    }

    it("should return 6 tiles at edge (lon=0, lat=-85.05112878, zoom=2)") {
      val result = sparkSession
        .sql("SELECT ST_BingTilesAround(0.0, -85.05112878, 2)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("21", "23", "30", "32", "31", "33")))
    }
  }

  describe("should pass ST_BingTileZoomLevel") {

    it("should return zoom level 3 for quadkey '213'") {
      val result =
        sparkSession.sql("SELECT ST_BingTileZoomLevel('213')").collect()(0).getInt(0)
      result should equal(3)
    }

    it("should return zoom level 15 for quadkey '123030123010121'") {
      val result = sparkSession
        .sql("SELECT ST_BingTileZoomLevel('123030123010121')")
        .collect()(0)
        .getInt(0)
      result should equal(15)
    }
  }

  describe("should pass ST_BingTileX and ST_BingTileY") {

    it("should return X=3 and Y=5 for quadkey '213'") {
      val x = sparkSession.sql("SELECT ST_BingTileX('213')").collect()(0).getInt(0)
      val y = sparkSession.sql("SELECT ST_BingTileY('213')").collect()(0).getInt(0)
      x should equal(3)
      y should equal(5)
    }

    it("should return X=21845 and Y=13506 for quadkey '123030123010121'") {
      val x =
        sparkSession.sql("SELECT ST_BingTileX('123030123010121')").collect()(0).getInt(0)
      val y =
        sparkSession.sql("SELECT ST_BingTileY('123030123010121')").collect()(0).getInt(0)
      x should equal(21845)
      y should equal(13506)
    }

    it("should round-trip via ST_BingTile and ST_BingTileX/Y") {
      val result = sparkSession
        .sql("SELECT ST_BingTile(ST_BingTileX('123030123010121'), ST_BingTileY('123030123010121'), ST_BingTileZoomLevel('123030123010121'))")
        .collect()(0)
        .getString(0)
      result should equal("123030123010121")
    }
  }

  describe("should pass ST_BingTilePolygon") {

    it("should return correct envelope for quadkey '123030123010121'") {
      val result = sparkSession
        .sql("SELECT ST_AsText(ST_BingTilePolygon('123030123010121'))")
        .collect()(0)
        .getString(0)
      result should not be null
      result should startWith("POLYGON")
      result should include("59.996337890625")
      result should include("60.00732421875")
    }

    it("should return correct envelope for tile(0,0,1)") {
      val result = sparkSession
        .sql("SELECT ST_AsText(ST_BingTilePolygon(ST_BingTile(0, 0, 1)))")
        .collect()(0)
        .getString(0)
      result should include("-180")
      result should include("85.051128")
    }
  }

  describe("should pass ST_BingTileCellIDs") {

    it("should return one tile for a point at zoom 10") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_GeomFromWKT('POINT (60 30.12)'), 10)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("1230301230")))
    }

    it("should return one tile for a point at zoom 15") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_GeomFromWKT('POINT (60 30.12)'), 15)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("123030123010121")))
    }

    it("should return 4 tiles for a 10x10 polygon at zoom 6") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_GeomFromWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), 6)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(
        mutable.WrappedArray.make(Array("122220", "122222", "122221", "122223")))
    }

    // geometry_to_bing_tiles(POLYGON((10 10, -10 10, -20 -15, 10 10)), 3)
    it("should return 3 tiles for a triangle at zoom 3") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_GeomFromWKT('POLYGON ((10 10, -10 10, -20 -15, 10 10))'), 3)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("033", "211", "122")))
    }

    it("should return empty array for empty geometry") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_GeomFromWKT('POINT EMPTY'), 10)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result.length should equal(0)
    }

    it("should round-trip tile polygon back to the same tile") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_BingTilePolygon('1230301230'), 10)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(mutable.WrappedArray.make(Array("1230301230")))
    }

    it("should return 4 child tiles when expanding tile to zoom+1") {
      val result = sparkSession
        .sql("SELECT ST_BingTileCellIDs(ST_BingTilePolygon('1230301230'), 11)")
        .collect()(0)
        .getAs[mutable.WrappedArray[String]](0)
      result should equal(
        mutable.WrappedArray.make(
          Array("12303012300", "12303012302", "12303012301", "12303012303")))
    }
  }

  describe("should pass ST_BingTileToGeom") {

    it("should convert quadkeys to polygon geometries") {
      val result = sparkSession
        .sql("SELECT ST_BingTileToGeom(array('0', '1', '2', '3'))")
        .collect()(0)
        .getAs[mutable.WrappedArray[Any]](0)
      result.length should equal(4)
    }
  }

}
