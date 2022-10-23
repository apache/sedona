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

class TestInsertCommand extends Base with MeasureTestDataFixture {
  import spark.implicits._

  describe("testing insert data using delta lake") {
    it("should be able to insert geospatial data") {
      Given("geospatial data frame")
      val geospatialData = produceDfFromRecords(
        Warsaw.withIndex(1).withMeasure(2.0),
        NewYork.withIndex(2).withMeasure(3.0)
      )

      val geospatialDataViewName = "geospatial_data_to_insert_view_name"
      geospatialData.createOrReplaceTempView(geospatialDataViewName)

      And("existing records")
      val existingRecords = produceDfFromRecords(
        NewYork.withIndex(3).withMeasure(3.0)
      )

      val existingRecordsTableName = "test_insert_data"

      createTableAsSelect(existingRecords, existingRecordsTableName)

      And("expected table")
      val expectedRecordsTable = produceDfFromRecords(
        Warsaw.withIndex(1).withMeasure(2.0),
        NewYork.withIndex(2).withMeasure(3.0),
        NewYork.withIndex(3).withMeasure(3.0)
      )

      When(s"inserting records to $existingRecordsTableName")
      val insertCommand = spark.sql(
        s"""
           |INSERT INTO delta.`$temporarySavePath/$existingRecordsTableName` (${measurementDataColumns.mkString(", ")})
           |    SELECT * FROM $geospatialDataViewName
           |
           |""".stripMargin
      )

      Then("result should match expected elements")
      val deltaTablAfterUpdate = loadDeltaTable(existingRecordsTableName)
        .toDF(measurementDataColumns: _*)

      compareDataFrames(
        expectedRecordsTable, deltaTablAfterUpdate
      )

    }

    it("should use optimized plan when inserting geospatial data after spatial join") {
      Given("geospatial dataset")
      val geospatialData = produceDfFromRecords(
        Warsaw.withIndex(1),
        Amsterdam.withIndex(2),
        Berlin.withIndex(3),
        NewYork.withIndex(4)
      )

      val geospatialDataViewName = "geospatial_insert"
      geospatialData.createOrReplaceTempView(geospatialDataViewName)

      val geospatialDataTableView = "geospatial_produced_records"

      geospatialData.createOrReplaceTempView(geospatialDataTableView)

      And("existing table")
      val existingTableRecords = produceDfFromRecords(
        Washington.withIndex(6)
      )

      val existingTableRecordsTableName = "geospatial_insert_initial_table"

      createTableAsSelect(existingTableRecords, existingTableRecordsTableName)

      And("europe extent")
      val europeExtentTableName = "europe_extent"
      val europeExtentData = Seq(
        (1, wktReader.read(europeExtent))
      ).toDF("id", "geom")

      europeExtentData.createOrReplaceTempView(europeExtentTableName)

      And("expected elements")
      val expectedElements = produceDfFromRecords(
        Warsaw.withIndex(1),
        Amsterdam.withIndex(2),
        Berlin.withIndex(3),
        Washington.withIndex(6)
      )

      When("inserting based on spatial join result")
      spark.sql(
        s"""
           |INSERT INTO delta.`$temporarySavePath/$existingTableRecordsTableName` (${measurementDataColumns.mkString(", ")})
           |    SELECT i.* FROM $geospatialDataViewName AS i, $europeExtentTableName AS b
           |    WHERE St_Intersects(i.geom, b.geom)
           |
           |""".stripMargin
      )

      Then("optimized query should be used for spatial join")
      val savedDataFrame = loadDeltaTable(existingTableRecordsTableName)

      verifyPlan("RangeJoin") shouldBe true

      And("data should match expected")
      compareDataFrames(expectedElements, savedDataFrame)
    }
  }


}
