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
package org.apache.sedona.sql.functions.geohash

import org.apache.sedona.common.utils.GeometryGeoHashEncoder
import org.apache.spark.sql.sedona_sql.expressions.geohash.GeoHashDecoder
import org.locationtech.jts.geom.Geometry
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2, TableFor3, TableFor4}


object Fixtures extends TableDrivenPropertyChecks {
  val geometriesToCalculateGeoHash: TableFor4[String, String, Int, Option[String]] = Table(
    ("statement", "input geometry", "precision", "expected geohash"),
    ("Point with precision of 1", "POINT(21 52)", 1, Some("u")),
    ("Point with precision of 2", "POINT(21 52)", 2, Some("u3")),
    ("Point with precision of 4", "POINT(21 52)", 4, Some("u3nz")),
    ("Point with precision of 10", "POINT(21 52)", 10, Some("u3nzvf79zq")),
    ("Point with precision of 20", "POINT(21 52)", 20, Some("u3nzvf79zqwfmzesx7yv")),
    ("Complex Point with precision of 1", "POINT(-100.022131 -21.12314242)", 1, Some("3")),
    ("Complex Point with precision of 5", "POINT(-100.022131 -21.12314242)", 5, Some("3u0zg")),
    ("Complex Point with precision of 10", "POINT(-100.022131 -21.12314242)", 10, Some("3u0zgfwhg2")),
    ("Complex Point with precision of 21", "POINT(-100.022131 -21.12314242)", 20, Some("3u0zgfwhg2v7rs3d3ykz")),
    ("Polygon with precision of 10", "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))",
      10, Some("s03y0zh7w1")),
    ("Polygon with precision of 20", "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))", 20,
      Some("s03y0zh7w1z0gs3y0zh7")),
    ("MultiPolygon with precision of 20",
      "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35)))",
      20, Some("ss7w1z0gs3y0zh7w1z0g")),
    ("Linestring with precision of 20", "LINESTRING (30 10, 10 30, 40 40)", 20, Some("ss3y0zh7w1z0gs3y0zh7"))
  )

  val geometriesFromGeoHash: TableFor4[String, String, Int, String] = Table(
    ("statement", "input geohash", "precision", "expected geometry"),
    ("geohash with precision of 0", "u", 0, "POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))"),
    ("geohash with precision of 1", "u", 1, "POLYGON((0 45,0 90,45 90,45 45,0 45))"),
    ("geohash with precision of 2", "u3", 2, "POLYGON((11.25 50.625,11.25 56.25,22.5 56.25,22.5 50.625,11.25 50.625))"),
    ("geohash with precision of 3", "u3nz", 3, "POLYGON((19.6875 50.625,19.6875 52.03125,21.09375 52.03125,21.09375 50.625,19.6875 50.625))"),
    ("geohash with precision of 4", "u3nz", 4, "POLYGON((20.7421875 51.85546875,20.7421875 52.03125,21.09375 52.03125,21.09375 51.85546875,20.7421875 51.85546875))"),
    ("geohash with precision of 10", "3u0zgfwhg2v7rs3d3ykz", 10, "POLYGON ((-100.02213835716248 -21.123147010803223, -100.02213835716248 -21.123141646385193, -100.02212762832642 -21.123141646385193, -100.02212762832642 -21.123147010803223, -100.02213835716248 -21.123147010803223))"),
    ("geohash with precision of 20", "3u0zgfwhg2v7rs3d3ykz", 20, "POLYGON ((-100.02213100000023 -21.123142420000125, -100.02213100000023 -21.123142419999965, -100.02213099999992 -21.123142419999965, -100.02213099999992 -21.123142420000125, -100.02213100000023 -21.123142420000125))")
  )

  val invalidGeoHashes: TableFor3[String, String, Int] = Table(
    ("statement", "invalid geohash", "precision"),
    ("geohash with non asci characters", "-+=123sda", 3),
    ("negative value of precision", "asdi", -3)
  )

  def calculateGeoHash(geom: Geometry, precision: Int): Option[String] = {
    Option(GeometryGeoHashEncoder.calculate(geom, precision))
  }

  def decodeGeoHash(geohash: String, precision: Option[Int]): Geometry =
    GeoHashDecoder.decode(geohash, precision)

}