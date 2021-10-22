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

import org.apache.sedona.sql.functions.FunctionsHelper
import org.apache.spark.sql.sedona_sql.expressions.geohash.InvalidGeoHashException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class TestGeoHashDecoder extends AnyFunSuite with Matchers with TableDrivenPropertyChecks with FunctionsHelper{
  for ((statement: String, geoHash: String, precision: Int, geometry: String) <- Fixtures.geometriesFromGeoHash) {
    test("it should decode " + statement) {
      Fixtures.decodeGeoHash(geoHash, Some(precision)) shouldBe wktReader.read(geometry)
    }
  }

  for ((statement: String, geoHash: String, precision: Int) <- Fixtures.invalidGeoHashes) {
    test("it should raise InvalidGeoHashException when " + statement) {
      an[InvalidGeoHashException] shouldBe thrownBy(Fixtures.decodeGeoHash(geoHash, Some(precision)))
    }
  }
}
