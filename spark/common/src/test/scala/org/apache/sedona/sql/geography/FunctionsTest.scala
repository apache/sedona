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
import org.apache.sedona.common.geography.{Constructors, Functions}
import org.apache.sedona.sql.TestBaseScala
import org.junit.Assert.assertEquals
import org.locationtech.jts.geom.{Geometry, PrecisionModel}

class FunctionsTest extends TestBaseScala {

  import sparkSession.implicits._

  it("Passed ST_Envelope antarctica") {
    val antarctica =
      "POLYGON ((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90))"
    var row =
      sparkSession.sql(s"SELECT ST_Envelope(ST_GeogFromWKT('$antarctica'), true) AS env").first()
    var env = row.get(0).asInstanceOf[Geography]
    var expectedWKT = "POLYGON ((-180 -63.3, 180 -63.3, 180 -90, -180 -90, -180 -63.3))";
    assertEquals(expectedWKT, env.toString)
  }

  it("Passed ST_Envelope Fiji") {
    val fiji =
      "MULTIPOLYGON (" + "((177.285 -18.28799, 180 -18.28799, 180 -16.02088, 177.285 -16.02088, 177.285 -18.28799))," +
        "((-180 -18.28799, -179.7933 -18.28799, -179.7933 -16.02088, -180 -16.02088, -180 -18.28799))" + ")"

    val row =
      sparkSession.sql(s"SELECT ST_Envelope(ST_GeogFromEWKT('$fiji'), false) AS env").first()
    val env = row.get(0).asInstanceOf[Geography]
    val expectedWKT = "POLYGON ((177.3 -18.3, -179.8 -18.3, -179.8 -16, 177.3 -16, 177.3 -18.3))"
    assertEquals(expectedWKT, env.toString)
  }

  it("Passed ST_Envelope null") {
    val functionDf = sparkSession.sql("select ST_Envelope(null, false)")
    assert(functionDf.first().get(0) == null)
  }

  it("Passed ST_AsEWKT") {
    val wkt = "LINESTRING (1 2, 3 4, 5 6)"
    val wktExpected = "SRID=4326; LINESTRING (1 2, 3 4, 5 6)"
    val row = sparkSession.sql(s"SELECT ST_AsEWKT(ST_GeogFromText('$wkt', 4326)) AS geog").first()
    val geoStr = row.get(0)
    assert(geoStr == wktExpected)
  }
}
