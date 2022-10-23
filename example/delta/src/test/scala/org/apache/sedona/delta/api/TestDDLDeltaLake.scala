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

import io.delta.tables.DeltaTable
import org.apache.sedona.delta.Base
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT

class TestDDLDeltaLake extends Base {

  describe("ddl lifecycle"){
    it("should be able to create delta table based on delta api"){
      Given("delta table definition")
      val deltaTableName = "delta_table"

      val deltaTable = DeltaTable.create(spark)
      .location(produceDeltaTargetPath(deltaTableName))
      .addColumn("identifier", "BIGINT")
      .addColumn("geom", GeometryUDT)


      When("executing query")
      deltaTable.execute()

      Then("delta table with geometry type should be created")
      val savedDeltaTable = loadDeltaTable(deltaTableName)
      val fields = savedDeltaTable.schema.map(
        field => Field(field.dataType.simpleString, field.name)
      )

      fields should contain theSameElementsAs Seq(
        Field("geometry", "geom"), Field("bigint", "identifier")
      )

    }

  }
  protected case class Field(tp: String, name: String)
}
