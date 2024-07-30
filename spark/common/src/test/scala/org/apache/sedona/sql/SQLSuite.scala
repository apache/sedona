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

import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks

/**
 * Test suite for testing Sedona SQL support.
 */
class SQLSuite extends TestBaseScala with TableDrivenPropertyChecks {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession.conf.set("spark.sql.legacy.createHiveTableByDefault", "false")
  }

  describe("Table creation DDL tests") {

    it("should be able to create a regular table without geometry column should work") {
      sparkSession.sql("CREATE TABLE T_TEST_REGULAR (INT_COL INT)")
      sparkSession.catalog.tableExists("T_TEST_REGULAR") should be(true)
    }

    it(
      "should be able to create a regular table with geometry column should work with a workaround") {
      sparkSession.sql("""
    CREATE OR REPLACE TEMP VIEW EMPTY_VIEW AS
    SELECT ST_GEOMFROMTEXT(CAST(NULL AS STRING)) AS GEOM
    WHERE 1 = 0
  """)
      sparkSession.sql("CREATE TABLE T_TEST_IMPLICIT_GEOMETRY AS (SELECT * FROM EMPTY_VIEW)")
      sparkSession.catalog.tableExists("T_TEST_IMPLICIT_GEOMETRY") should be(true)
    }

    ignore(
      "should be able to create a regular table with geometry column should work without a workaround") {
      sparkSession.sql("CREATE TABLE T_TEST_EXPLICIT_GEOMETRY (INT_COL GEOMETRY)")
      sparkSession.catalog.tableExists("T_TEST_EXPLICIT_GEOMETRY") should be(true)
    }
  }
}
