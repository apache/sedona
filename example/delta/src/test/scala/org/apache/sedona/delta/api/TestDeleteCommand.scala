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

package org.apache.sedona.delta.api

import org.apache.sedona.delta.Base


class TestDeleteCommand extends Base {

  import spark.implicits._

  describe("testing sql delete statement lifecycle for delta lake with geospatial"){
    it("should remove elements based on geospatial conditions") {
      Given("geospatial delta lake table")
      val deltaTableName = "geospatial"
      val expectedNumberOfElements = 100

      val pointsTable = createSpatialPointDataFrame(expectedNumberOfElements)
        .unionAll(
          Seq((1, createPoint(21.0, 52.0))).toDF("index", "geom")
        )

      createTableAsSelect(pointsTable, deltaTableName)

      When("removing records based on geospatial predicate")
      spark.sql(
        s"DELETE FROM ${produceDeltaPath(deltaTableName)} WHERE ST_X(geom) == 21.0"
      )

      Then("target table should have appropriate number of records within it")
      loadDeltaTable(deltaTableName).count shouldBe expectedNumberOfElements

    }

    it("should remove elements on geospatial data based on non geospatial conditions"){
      Given("geospatial data frame")
      val deltaTableName = "geospatial_non_geospatial_predicate"
      val expectedNumberOfElements = 100
      val indexToRemove = 100000

      val pointsTable = createSpatialPointDataFrame(expectedNumberOfElements)
        .unionAll(
          Seq((indexToRemove, createPoint(21.0, 52.0))).toDF("index", "geom")
        )

      createTableAsSelect(pointsTable, deltaTableName)

      When("deleting elements from delta table based on non geospatial predicate")
      spark.sql(
        s"DELETE FROM ${produceDeltaPath(deltaTableName)} WHERE index == $indexToRemove"
      )

      Then("count in delta table should be as expected")
      loadDeltaTable(deltaTableName).count shouldBe expectedNumberOfElements
    }

    it("should remove elements based on geospatial data") {
      Given("geospatial delta table")
      val geospatialDataFrame = Seq(
        (1, wktReader.read("POINT(21.00 52.00)")),
        (2, wktReader.read("POINT(21.00 20.00)")),
        (3, wktReader.read("POINT(21.00 23.00)")),
        (4, wktReader.read("POINT(21.00 24.00)"))
      ).toDF("index", "geom")

      val expectedElementsCount = 3
      val deltaTableName = "geospatial_equality"

      createTableAsSelect(geospatialDataFrame, deltaTableName)

      When("removing elements based on geometry equality")
      spark.sql(
        s"DELETE FROM ${produceDeltaPath(deltaTableName)} WHERE geom = ST_Point(21.00, 52.00)"
      )

      Then("number of elements should be as expected")
      loadDeltaTable(deltaTableName).count shouldBe expectedElementsCount
    }

    it("should remove elements based on geospatial predicate") {
      Given("geospatial delta table")
      val gdf = Seq(
        (1, wktReader.read("POINT(21.064945 52.215713)")),
        (2, wktReader.read("POINT(15.241143 52.092268)")),
        (3, wktReader.read("POINT(21.310270 54.052040)")),
        (4, wktReader.read("POINT(16.282355 53.791566)")),
        (5, wktReader.read("POINT(8.485754 51.581370)"))
      ).toDF("index", "geom")
      val deltaTableName = "geospatial_predicate"

      createTableAsSelect(gdf, deltaTableName)

      And("extent to clip the data")
      val extentOfPoland = "POLYGON((14.368894 54.793480, 23.654518 54.903482, 24.074855 49.028561, 14.024982 49.749864, 14.368894 54.793480))"

      When("removing elements based on geospatial predicate")
      spark.sql(
        s"DELETE FROM ${produceDeltaPath(deltaTableName)} WHERE NOT ST_Within(geom, ST_GeomFromText('$extentOfPoland'))"
      )

      Then("number of records should match expected")
      val deltaTable = loadDeltaTable(deltaTableName)

      deltaTable.count shouldBe 4

      And("ids set should be as given")
      deltaTable.select("index").as[Int].collect().toList should contain theSameElementsAs List(
        1, 2, 3, 4
      )

    }

  }
}
