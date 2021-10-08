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
import org.scalatest.GivenWhenThen

class TestStGeomFromGeoHash extends TestBaseScala with GeometrySample with GivenWhenThen {
  describe("should correctly create geometry from geohash"){

    import sparkSession.implicits._

    it("it should return null for empty geometries"){
      Given("geometry data frame with null values")
      val geometryDf = Seq((1, "POINT(21 52)")).map {
        case (id, geom) => (id, wktReader.read(geom))
      }.union(Seq((2, null))).toDF("id", "geom")

      When("running ST_GeomFromGeoHash function")
      geometryDf.show

      Then("null values should produce null values on output")
    }


  }
}
