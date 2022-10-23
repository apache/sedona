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
import org.apache.spark.sql.functions.expr


class TestCreateTableCommand extends Base {

  describe("should create table using delta with geospatial data") {
    import spark.implicits._

    it("should save geospatial columns") {
      Given("geospatial data")
      val gdf = createSpatialPointDataFrame(20)
      val tableName = "spatial_df"

      When("saving to delta lake format")
      createTableAsSelect(gdf, tableName)

      Then("geospatial columns should be in the delta table")
      val loadedTable = loadDeltaTable(tableName)

      And("count should be as initial")
      loadedTable.count shouldBe gdf.count

      And("input dataframe should be the same as the saved one")
      val numberOfValidRecords = loadedTable
        .alias("left").join(gdf.alias("right"), $"left.index" === $"right.index")
        .withColumn("eq", expr("left.geom == right.geom"))
        .where("eq == true")
        .count

      numberOfValidRecords shouldBe gdf.count

    }

    it("should save non geospatial data only") {
      Given("data frame with non geospatial data")
      val df = Seq(
        (1, "text1"),
        (2, "text2")
      ).toDF("index", "name")
      val targetTableName = "non_geospatial"

      When("saving as delta table")
      createTableAsSelect(df, targetTableName)

      Then("result table should be the same as input table")
      val loadedTable = loadDeltaTable(targetTableName)

      loadedTable.count shouldBe df.count

    }

    it("should create geospatial table based on transformed table") {
      Given("geometry dataframe")
      val gdf = createSpatialPointDataFrame(10)
        .selectExpr(
          "index", "ST_GeoHash(geom, 6) AS geohash"
        )

      val tableName = "transformed"

      When("transforming geometry data frame and save it to delta lake")
      createTableAsSelect(gdf, tableName)

      Then("data should be properly saved")
      val loadedDf = loadDeltaTable(tableName)

      And("data should have initial count")
      loadedDf.count shouldBe gdf.count

      And("point")
      val numberOfInEqualElements = loadedDf
        .alias("left").join(gdf.alias("right"), $"left.index" === $"right.index")
        .selectExpr("left.geohash == right.geohash AS eq")
        .where("eq == false")
        .count

      numberOfInEqualElements shouldBe 0
    }
  }

}
