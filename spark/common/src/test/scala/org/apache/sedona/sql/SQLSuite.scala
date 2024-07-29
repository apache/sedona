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
package org.apache.sedona.sql

import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks

/**
 * Test suite for testing SQLsupport in Sedona-SQL module.
 */
class SQLSuite extends TestBaseScala with TableDrivenPropertyChecks {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  describe("Hive table creation") {
    it("should throw an exception when creating a regular Hive table without Hive support") {
      val exception = intercept[org.apache.spark.sql.AnalysisException] {
        sparkSession.sql("CREATE TABLE T_TEST (INT_COL INT)")
      }
      exception.getMessage should include("Hive support is required to CREATE Hive TABLE")
    }

    it("should throw an exception when creating a geospatial Hive table without Hive support") {
      val exception = intercept[org.apache.spark.sql.AnalysisException] {
        // Create sample data
        sparkSession.sql("""
    CREATE OR REPLACE TEMP VIEW EMPTY_VIEW AS
    SELECT ST_GEOMFROMTEXT(CAST(NULL AS STRING)) AS GEOM
    WHERE 1 = 0
  """)
        sparkSession.sql("CREATE TABLE T_TEST AS (SELECT * FROM EMPTY_VIEW)")
      }
      exception.getMessage should include("Hive support is required to CREATE Hive TABLE")
    }
  }
}
