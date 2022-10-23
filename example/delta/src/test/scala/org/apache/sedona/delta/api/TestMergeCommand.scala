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

class TestMergeCommand extends Base with MeasureTestDataFixture {

  describe("merge command lifecycle") {
    it("should merge based on geospatial join") {
      Given("geospatial delta table with point data")
      val measureDataTableName = "measure_data_geospatial"
      val measureDataInputViewName = "measure_data_input"

      val measureData = produceDfFromRecords(
        Cracow.withIndex(2).withMeasure(2.0),
        Amsterdam.withIndex(4).withMeasure(4.0),
        LosAngeles.withIndex(6).withMeasure(6.0),
        Washington.withIndex(7).withMeasure(8.0),
        Barcelona.withIndex(8).withMeasure(5.0)
      )

      measureData.show

      measureData.createOrReplaceTempView(measureDataInputViewName)

      And("geospatial delta table with polygons")
      val existingMeasures = produceDfFromRecords(
        Warsaw.withIndex(1).withMeasure(1.0),
        Cracow.withIndex(2).withMeasure(11.0),
        Berlin.withIndex(3).withMeasure(5.0),
        Amsterdam.withIndex(4).withMeasure(7.0),
        NewYork.withIndex(5).withMeasure(7.0),
        LosAngeles.withIndex(6).withMeasure(10.0)
      )

      createTableAsSelect(existingMeasures, measureDataTableName)

      And("Expected elements")
      val expectedElementsTable = produceDfFromRecords(
        Warsaw.withIndex(1).withMeasure(1.0),
        Cracow.withIndex(2).withMeasure(2.0),
        Berlin.withIndex(3).withMeasure(5.0),
        Amsterdam.withIndex(4).withMeasure(4.0),
        NewYork.withIndex(5).withMeasure(7.0),
        LosAngeles.withIndex(6).withMeasure(6.0),
        Washington.withIndex(7).withMeasure(8.0),
        Barcelona.withIndex(8).withMeasure(5.0)
      )

      When("merge on geospatial join approach")
      val update = spark.sql(
        s"""
           | MERGE INTO delta.`$temporarySavePath/$measureDataTableName` AS m
           | USING $measureDataInputViewName AS u
           | ON ST_Intersects(m.geom, u.geom)
           | WHEN MATCHED THEN
           | UPDATE SET
           |  measure = u.measure
           | WHEN NOT MATCHED
           | THEN INSERT (
           | index,
           | geom,
           | measure,
           | city
           | )
           | VALUES(
           |  u.index,
           |  u.geom,
           |  u.measure,
           |  u.city
           | )
           |
           |""".stripMargin
      )

      Then("optimized plan should be used")
      verifyPlan("RangeJoin") shouldBe true

      And("data should have appropriate count")
      val deltaResult = loadDeltaTable(measureDataTableName)

      And("elements should match to expected ones")
      compareDataFrames(
        deltaResult
          .toDF(measurementDataColumns:_*),
        expectedElementsTable
      ) shouldBe true

    }

    it("should merge based on geospatial predicate") {
      Given("Already saved measure data")

      val measurementData = produceDfFromRecords(
        Cracow.withIndex(2).withMeasure(2.0),
        Amsterdam.withIndex(4).withMeasure(4.0),
        LosAngeles.withIndex(6).withMeasure(6.0)
      )

      val measurementDataViewName = "measurement_data_geospatial_predicate"

      measurementData.createOrReplaceTempView(measurementDataViewName)

      val existingMeasurementsTableName = "existing_measurement_data_geospatial_predicate"

      val existingMeasures = produceDfFromRecords(
          Warsaw.withIndex(1).withMeasure(1.0),
          Cracow.withIndex(2).withMeasure(11.0),
          Berlin.withIndex(3).withMeasure(5.0)
      )

      createTableAsSelect(existingMeasures, existingMeasurementsTableName)

      val expectedDataFrame = produceDfFromRecords(
        Cracow.withIndex(2).withMeasure(2.0),
        Warsaw.withIndex(1).withMeasure(1.0),
        Berlin.withIndex(3).withMeasure(5.0)
      )

      When("updating only measures in poland")
      val update = spark.sql(
        s"""
           | MERGE INTO delta.`$temporarySavePath/$existingMeasurementsTableName` AS m
           | USING $measurementDataViewName AS u
           | ON ST_Intersects(m.geom, ST_GeomFromText('$polandExtent')) AND u.index = m.index
           | WHEN MATCHED THEN
           | UPDATE SET
           |  measure = u.measure
           |""".stripMargin
      )

      Then("only polish observations should be updated")
      val deltaResult = loadDeltaTable(existingMeasurementsTableName)

      compareDataFrames(
        deltaResult.toDF(measurementDataColumns:_*), expectedDataFrame
      )

    }

    it("should merge based on non geospatial attributes") {
      Given("measurement data with geospatial fields")
      val measurementData = produceDfFromRecords(
        Amsterdam.withIndex(1).withMeasure(2.0),
        Berlin.withIndex(2).withMeasure(4.0)
      )

      val measurementDataTempView = "measure_data"
      measurementData.createOrReplaceTempView(measurementDataTempView)

      And("already existing table")
      val existingRecords = produceDfFromRecords(
        Warsaw.withIndex(3).withMeasure(1.0),
        Berlin.withIndex(2).withMeasure(7.0)
      )

      val existingTableName = "non_geospatial_predicate_merge_command"

      createTableAsSelect(existingRecords, existingTableName)

      And("expected result table")
      val expectedRecords = produceDfFromRecords(
        Amsterdam.withIndex(1).withMeasure(2.0),
        Warsaw.withIndex(3).withMeasure(1.0),
        Berlin.withIndex(2).withMeasure(7.0)
      )

      When("merging based on non geospatial field")
      val update = spark.sql(
        s"""
           | MERGE INTO delta.`$temporarySavePath/$existingTableName` AS m
           | USING $measurementDataTempView AS u
           | ON m.index = u.index
           | WHEN MATCHED THEN
           | UPDATE SET
           |  measure = u.measure
           | WHEN NOT MATCHED
           | THEN INSERT (
           | index,
           | geom,
           | measure,
           | city
           | )
           | VALUES(
           |  u.index,
           |  u.geom,
           |  u.measure,
           |  u.city
           | )
           |
           |""".stripMargin
      )


      Then("the result should be the same as expected")
      val deltaTable = loadDeltaTable(existingTableName)

      compareDataFrames(deltaTable.toDF(measurementDataColumns:_*), expectedRecords)

    }
  }

}
