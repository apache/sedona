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

package org.apache.sedona.sql.ogc

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.expr
import org.scalatest.BeforeAndAfterAll

/**
 * Test for section GeometryTypes And Functions in OGC simple feature implementation specification for SQL.
 */
class GeometryTypesAndFunctionsTestScala extends TestBaseScala with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession.createDataFrame(Seq((119, "Route 75", 4, "MULTILINESTRING((10 48, 10 21, 10 0), (16 0,16 23,16 48))")))
      .toDF("fid", "name", "num_lanes", "centerlines")
      .withColumn("centerlines", expr("ST_GeomFromWKT(centerlines)"))
      .createOrReplaceTempView("divided_routes")

    // A possible bug in the test specification, INSERT specifies 'BLUE LAKE' but test use 'Blue Lake'.
    sparkSession.createDataFrame(Seq((101, "Blue Lake", "POLYGON((52 18,66 23,73 9,48 6,52 18), (59 18,67 18,67 13,59 13,59 18))")))
      .toDF("fid", "name", "shore")
      .withColumn("shore", expr ("ST_GeomFromWKT(shore)"))
      .createOrReplaceTempView("lakes")
  }

  describe("Sedona-SQL OGC Geometry types and functions") {
    ignore("T7") {
      // ST_GeometryType is SQL/MM compliant but not OGC compliant.
      // Postgis has solved this by implementing both ST_GeometryType (SQL/MM) and GeometryType (OGC).

      // A possible bug in the test specification (C.3.3.3 Geometry types and functions schema test queries).
      // Query specifies lakes but probably should be divided_routes.
      val actual = sparkSession.sql(
        """
          |SELECT GeometryType(centerlines)
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getString(0)
      assert(actual == "MULTILINESTRING")
    }
    it("T12") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_IsSimple(shore)
          |FROM lakes
          |WHERE name = 'Blue Lake';
          |""".stripMargin).first().getBoolean(0)
      assert(actual == true)
    }
    it("T30") {
      val actual = sparkSession.sql(
        """
          |SELECT ST_NumGeometries(centerlines)
          |FROM divided_routes
          |WHERE name = 'Route 75';
          |""".stripMargin).first().getInt(0)
      assert(actual == 2)
    }
  }
}