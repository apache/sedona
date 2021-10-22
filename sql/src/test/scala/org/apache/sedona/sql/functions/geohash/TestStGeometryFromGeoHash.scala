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

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.functions.expr
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers

class TestStGeometryFromGeoHash extends TestBaseScala with GeometrySample with GivenWhenThen with Matchers{
  import sparkSession.implicits._

  describe("should create geometries based on hash using ST_GeomFromGeoHash function"){
    it("should create polygon geometry based on geohash"){
      Given("data frame with geohash values")
      val geoHashCollection = Seq(
        ("sd", "POLYGON ((22.5 11.25, 22.5 16.875, 33.75 16.875, 33.75 11.25, 22.5 11.25))"),
        ("s12fd", "POLYGON ((1.142578125 7.470703125, 1.142578125 7.5146484375, 1.1865234375 7.5146484375, 1.1865234375 7.470703125, 1.142578125 7.470703125))"),
        ("2131s12fd", "POLYGON ((-178.4168529510498 -37.69778251647949, -178.4168529510498 -37.697739601135254, -178.41681003570557 -37.697739601135254, -178.41681003570557 -37.69778251647949, -178.4168529510498 -37.69778251647949))")
      )

      val geoHashDf = geoHashCollection.toDF("geohash", "expected_geom")

      When("creating Geometry from geohash")
      val result = geoHashDf.selectExpr(
        "ST_AsText(ST_GeomFromGeoHash(geohash)) AS wkt"
      ).where("wkt is not null").as[String].collect().toList

      Then("result should be as expected")
      result.size shouldBe 3
      result should contain theSameElementsAs geoHashCollection.map(_._2)

    }

    it("should create geometry based on geohash and precision"){
      Given("data frame with geohash values")
      val geoHashCollection = Seq(
        (
          "2131s12fd",
          "POLYGON ((-180 -39.375, -180 -33.75, -168.75 -33.75, -168.75 -39.375, -180 -39.375))")
      )

      val geoHashDf = geoHashCollection.toDF("geohash", "expected_geom")

      When("creating Geometry from geohash")
      val result = geoHashDf.selectExpr(
        "ST_AsText(ST_GeomFromGeoHash(geohash, 2)) AS wkt"
      ).where("wkt is not null").as[String].collect().toList

      Then("result should be as expected")
      result.size shouldBe 1
      result should contain theSameElementsAs geoHashCollection.map(_._2)

    }

    it("should return null geometries when geohash or given precision value is invalid"){
      Given("data frame with geo hash")
      val invalidGeoHashDf = Seq(
        ("aidft35")
      ).toDF("geohash")

      val validGeoHash = Seq(
        ("sd")
      ).toDF("geohash")

      When("using StGeomFromGeoHash function with invalid arguments")
      val geometryDfWithInvalidGeoHash = invalidGeoHashDf
        .select(expr("ST_GeomFromGeoHash(geohash)").alias("geom"))
        .filter("geom is not null")

      val geometryDfWithInvalidPrecision = validGeoHash
        .select(expr("ST_GeomFromGeoHash(geohash, -1)").alias("geom"))
        .filter("geom is not null")

      Then("output record should have null value")
      geometryDfWithInvalidGeoHash.count shouldBe 0
      geometryDfWithInvalidPrecision.count shouldBe 0
    }

    it("should return null for null values"){
      Given("data frame with null values")
      val dataFrameWithNulls = sparkSession.sql("SELECT NULL AS geohash")

      When("using ST_GeomFromGeoHash function on null values")
      val geometryDf = dataFrameWithNulls.selectExpr("ST_GeomFromGeoHash(geohash) AS geom")
        .filter("geom IS NOT NULL")

      Then("result should be also null")
      geometryDf.count shouldBe 0

    }

  }
}
