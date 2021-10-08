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

import org.apache.spark.sql.sedona_sql.expressions.geohash.PointGeoHashCalculator
import org.locationtech.jts.geom.{Geometry, Point}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}

class TestCalculatingGeoHash extends AnyFunSuite with Matchers with TableDrivenPropertyChecks with FunctionsHelper{
  for ((statement: String, inputGeometry: String, precision: Int, geoHash: Option[String]) <- Fixtures.geometriesToCalculateGeoHash) {
    test("it should simplify geometry " + statement) {
      Fixtures.calculateGeoHash(wktReader.read(inputGeometry), precision) shouldBe geoHash
    }
  }

  object Fixtures {
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
      ("Complex Point with precision of 21", "POINT(-100.022131 -21.12314242)", 20, Some("3u0zgfwhg2v7rs3d3ykz"))
    )

    def calculateGeoHash(geom: Geometry, precision: Int): Option[String] = {
      PointGeoHashCalculator.calculateGeoHash(geom.asInstanceOf[Point], precision)
    }
  }

}
