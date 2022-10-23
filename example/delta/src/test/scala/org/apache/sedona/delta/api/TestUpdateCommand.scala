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

class TestUpdateCommand extends Base with MeasureTestDataFixture {

  import spark.implicits._

  describe("update lifecycle"){

    it("should be able to update based on non geospatial column"){
      Given("initial dataframe")
      val deltaTableName = "geospatial_non_geospatial_predicate_update"
      val existingRecords = produceDfFromRecords(
        NewYork.withIndex(3).withMeasure(3.0),
        Warsaw.withIndex(4).withMeasure(4.0)
      )
      createTableAsSelect(existingRecords, deltaTableName)

      When("updating based geospatial attributes")
      spark.sql(
        s"""
          | UPDATE ${produceDeltaPath(deltaTableName)}
          | SET measure = 1.0 where index=4
          """.stripMargin).show()

      Then("table should be properly updated")
      val table = loadDeltaTable(deltaTableName)
      val elements = table.select("measure").as[Double].collect()

      elements should contain theSameElementsAs Seq(3.0, 1.0)

    }

    it("should be able to update based on geospatial column"){
      val deltaTableName = "geospatial_geospatial_predicate"
      val existingRecords = produceDfFromRecords(
        NewYork.withIndex(3).withMeasure(3.0),
        Warsaw.withIndex(4).withMeasure(4.0)
      )
      createTableAsSelect(existingRecords, deltaTableName)

      When("updating based geospatial attributes")
      spark.sql(
        s"""
           | UPDATE ${produceDeltaPath(deltaTableName)}
           | SET measure = 1.0 where ST_Distance(geom, ST_GeomFromText('POINT(21.00 52.00)')) < 1.0
          """.stripMargin).show()

      Then("table should be properly updated")
      val table = loadDeltaTable(deltaTableName)
      val elements = table.select("measure").as[Double].collect()

      elements should contain theSameElementsAs Seq(3.0, 1.0)

    }
  }


}
