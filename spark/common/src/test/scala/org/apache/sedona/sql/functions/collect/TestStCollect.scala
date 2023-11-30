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

package org.apache.sedona.sql.functions.collect

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.functions.expr
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers

class TestStCollect
    extends TestBaseScala
    with GeometrySample
    with GivenWhenThen
    with Matchers {

  import sparkSession.implicits._

  describe("st collect workflow") {
    it("should return null when passed geometry is also null") {
      Given("data frame with empty geometries")
      val emptyGeometryDataFrame = Seq(
        (1, null),
        (2, null),
        (3, null)
      ).toDF("id", "geom")

      When("running st collect on null elements")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn("collected_geom", expr("ST_Collect(geom)"))

      Then("result should contain all rows null")
      withCollectedGeometries
        .selectExpr("ST_AsText(collected_geom)")
        .distinct()
        .as[String]
        .collect
        .head shouldBe "GEOMETRYCOLLECTION EMPTY"

    }

    it("should return null geometry when multiple passed geometries are null") {
      Given("data frame with multiple null elements")
      val emptyGeometryDataFrame = Seq(
        (1, null, null),
        (2, null, null),
        (3, null, null)
      ).toDF("id", "geom_left", "geom_right")

      When("running st collect on null elements")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn("collected_geom", expr("ST_Collect(geom_left, geom_right)"))

      Then("result should be null")
      withCollectedGeometries
        .selectExpr("ST_AsText(collected_geom)")
        .distinct()
        .as[String]
        .collect
        .head shouldBe "GEOMETRYCOLLECTION EMPTY"
    }

    it("should not fail if any element in given array is empty") {
      Given("Dataframe with geometries and null geometries within array")
      val emptyGeometryDataFrame = Seq(
        (1, Seq(null)),
        (
          2,
          Seq(
            null,
            wktReader.read("LINESTRING(0 0, 1 1, 2 2)"),
            wktReader.read("LINESTRING(5 7, 4 3, 1 1, 0 0)")
          )
        ),
        (
          3,
          Seq(
            null,
            wktReader.read("Point(21 52)"),
            wktReader.read("POINT(45 43)")
          )
        )
      ).toDF("id", "geom")

      When("running st_collect function")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn("collected_geom", expr("ST_Collect(geom)"))

      Then(
        "only those non null element should be included in process of creating multigeometry"
      )
      val expectedGeoms = withCollectedGeometries
        .selectExpr("ST_AsText(collected_geom)")
        .as[String]
        .collect()

      expectedGeoms should contain theSameElementsAs Seq(
        "GEOMETRYCOLLECTION EMPTY",
        "MULTILINESTRING ((0 0, 1 1, 2 2), (5 7, 4 3, 1 1, 0 0))",
        "MULTIPOINT ((21 52), (45 43))"
      )

    }

    it("should return multi geometry for an array of geometries") {
      Given("Dataframe with geometries and null geometries within array")
      val emptyGeometryDataFrame = Seq(
        (
          1,
          Seq(
            "POLYGON((1 2,1 4,3 4,3 2,1 2))",
            "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))"
          ).map(wktReader.read)
        ),
        (
          2,
          Seq(
            "LINESTRING(1 2, 3 4)",
            "LINESTRING(3 4, 4 5)"
          ).map(wktReader.read)
        ),
        (
          3,
          Seq(
            "POINT(1 2)",
            "POINT(-2 3)"
          ).map(wktReader.read)
        )
      ).toDF("id", "geom")

      When("running st_collect function")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn("collected_geom", expr("ST_Collect(geom)"))

      Then(
        "only those non null element should be included in process of creating multigeometry"
      )
      val expectedGeoms = withCollectedGeometries
        .selectExpr("ST_AsText(collected_geom)")
        .as[String]
        .collect()

      expectedGeoms should contain theSameElementsAs Seq(
        "MULTIPOLYGON (((1 2, 1 4, 3 4, 3 2, 1 2)), ((0.5 0.5, 5 0, 5 5, 0 5, 0.5 0.5), (1.5 1, 4 3, 4 1, 1.5 1)))",
        "MULTILINESTRING ((1 2, 3 4), (3 4, 4 5))",
        "MULTIPOINT ((1 2), (-2 3))"
      )
    }

    it("should return multi geometry for a list of geometries") {
      Given("Data frame with more than one geometry column")
      val geometryDf = Seq(
        (1, "POINT(21 52)", "POINT(43 34)", "POINT(12 34)", "POINT(34 67)")
      ).map {
          case (id, geomFirst, geomSecond, geomThird, geomFourth) =>
            (
              id,
              wktReader.read(geomFirst),
              wktReader.read(geomSecond),
              wktReader.read(geomThird),
              wktReader.read(geomFourth)
            )
        }
        .toDF("id", "geomFirst", "geomSecond", "geomThird", "geomFourth")

      When("running st collect function")
      val stCollectResult = geometryDf
        .withColumn(
          "collected",
          expr("ST_Collect(geomFirst, geomSecond, geomThird, geomFourth)")
        )

      Then("multi geometries should be created")
      stCollectResult
        .selectExpr("ST_AsText(collected)")
        .as[String]
        .collect() should contain theSameElementsAs (
        Seq("MULTIPOINT ((21 52), (43 34), (12 34), (34 67))")
      )

    }

    it("should filter out null values if exists in any of passed column") {
      Given(
        "Data frame with more than one geometry column and some of them are filled with null"
      )
      val geometryDf = Seq(
        (1, null, "POINT(43 58)", null, "POINT(34 67)")
      ).map {
          case (id, geomFirst, geomSecond, geomThird, geomFourth) =>
            (
              id,
              geomFirst,
              wktReader.read(geomSecond),
              geomThird,
              wktReader.read(geomFourth)
            )
        }
        .toDF("id", "geomFirst", "geomSecond", "geomThird", "geomFourth")

      When("running st collect function")
      val stCollectResult = geometryDf
        .withColumn(
          "collected",
          expr("ST_Collect(geomFirst, geomSecond, geomThird, geomFourth)")
        )

      Then("multi geometries should be created")
      stCollectResult
        .selectExpr("ST_AsText(collected)")
        .as[String]
        .collect() should contain theSameElementsAs (
        Seq("MULTIPOINT ((43 58), (34 67))")
      )
    }

    it("should return multitype when only one column is passed") {
      Given("data frame with one geometry column")
      val geometryDf = Seq(
        (1, "POINT(43 54)"),
        (2, "POLYGON((1 2,1 4,3 4,3 2,1 2))"),
        (3, "LINESTRING(1 2, 3 4)")
      ).map { case (id, geom) => (id, wktReader.read(geom)) }.toDF("id", "geom")

      When("running on st collect on one geometry column")
      val geometryDfWithCollected = geometryDf
        .withColumn(
          "collected",
          expr("ST_Collect(geom)")
        )
        .selectExpr("ST_AsText(collected)")
        .as[String]

      Then("should return MultiType for each geometry")
      geometryDfWithCollected.collect() should contain theSameElementsAs
        Seq(
          "MULTIPOINT ((43 54))",
          "MULTIPOLYGON (((1 2, 1 4, 3 4, 3 2, 1 2)))",
          "MULTILINESTRING ((1 2, 3 4))"
        )
    }
  }
}
