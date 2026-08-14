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
package org.apache.sedona.sql.functions

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_HilbertDistance
import org.apache.spark.sql.types.LongType
import org.scalatest.Matchers

class STHilbertDistanceFunctions extends TestBaseScala with Matchers {

  describe("ST_HilbertDistance") {

    it("encodes geometry midpoints as unsigned Hilbert addresses") {
      val row = sparkSession
        .sql("""
          |SELECT
          |  ST_HilbertDistance(ST_GeomFromWKT('POINT (0 0)'), 0.0, 0.0, 1.0, 1.0, 2),
          |  ST_HilbertDistance(ST_GeomFromWKT('POINT (0 1)'), 0.0, 0.0, 1.0, 1.0, 2),
          |  ST_HilbertDistance(ST_GeomFromWKT('POINT (1 1)'), 0.0, 0.0, 1.0, 1.0, 2),
          |  ST_HilbertDistance(ST_GeomFromWKT('POINT (1 0)'), 0.0, 0.0, 1.0, 1.0, 16)
          |""".stripMargin)
        .first()

      row.getLong(0) should equal(0L)
      row.getLong(1) should equal(5L)
      row.getLong(2) should equal(10L)
      row.getLong(3) should equal(4294967295L)
    }

    it("supports Column and String DataFrame API overloads") {
      val source = sparkSession.sql("""
          |SELECT
          |  ST_GeomFromWKT('LINESTRING (0 0, 2 2)') AS geom,
          |  0.0 AS xmin,
          |  0.0 AS ymin,
          |  2.0 AS xmax,
          |  2.0 AS ymax,
          |  2 AS level
          |""".stripMargin)

      val columnResult = source.select(
        ST_HilbertDistance(
          col("geom"),
          col("xmin"),
          col("ymin"),
          col("xmax"),
          col("ymax"),
          col("level")))
      columnResult.schema.head.dataType should equal(LongType)
      columnResult.first().getLong(0) should equal(2L)

      source
        .select(ST_HilbertDistance("geom", 0.0, 0.0, 2.0, 2.0, 2))
        .first()
        .getLong(0) should equal(2L)
    }

    it("propagates null inputs") {
      val row = sparkSession
        .sql("""
          |SELECT
          |  ST_HilbertDistance(ST_GeomFromWKT(CAST(NULL AS STRING)), 0.0, 0.0, 1.0, 1.0, 2),
          |  ST_HilbertDistance(ST_GeomFromWKT('POINT (0 0)'), CAST(NULL AS DOUBLE), 0.0, 1.0, 1.0, 2)
          |""".stripMargin)
        .first()

      row.isNullAt(0) should be(true)
      row.isNullAt(1) should be(true)
    }

    it("rejects empty geometries before collapsing non-positive levels") {
      val exception = intercept[Exception] {
        sparkSession
          .sql("""
            |SELECT ST_HilbertDistance(
            |  ST_GeomFromWKT('POINT EMPTY'), 0.0, 0.0, 1.0, 1.0, 0)
            |""".stripMargin)
          .collect()
      }

      val expected = "Hilbert distance cannot be computed for an empty geometry"
      val hasExpectedMessage = Iterator
        .iterate(exception: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .exists(error => Option(error.getMessage).exists(_.contains(expected)))
      hasExpectedMessage should be(true)
    }
  }
}
