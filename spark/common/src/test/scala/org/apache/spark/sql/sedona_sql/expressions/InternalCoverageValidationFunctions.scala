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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.locationtech.jts.geom.Geometry
import org.scalatest.Matchers

class InternalCoverageValidationFunctions extends TestBaseScala with Matchers {

  private val functionName = "__sedona_internal_coverage_invalid_edges_for_target"

  describe("internal coverage-validation expression") {

    it("registers the hidden function") {
      sparkSession.catalog.functionExists(functionName) should be(true)

      val invalidEdges = coverageSource
        .selectExpr(s"$functionName(target, matching_adjacent, 0.0) AS invalid_edges")
        .first()
        .getAs[Geometry]("invalid_edges")
      invalidEdges.isEmpty should be(true)
    }

    it("returns an empty line for a valid target and invalid edges for an overlap") {
      val row = coverageSource
        .selectExpr(
          s"$functionName(target, matching_adjacent, 0.0) AS valid",
          s"$functionName(target, overlapping_adjacent, 0.0) AS invalid")
        .first()

      val valid = row.getAs[Geometry]("valid")
      val invalid = row.getAs[Geometry]("invalid")
      valid.isEmpty should be(true)
      valid.getGeometryType should equal("LineString")
      invalid.isEmpty should be(false)
      invalid.getDimension should equal(1)
    }

    it("accepts zero and an explicit gap width") {
      val result = coverageSource.selectExpr(
        s"$functionName(target, across_gap_adjacent, 0.0) AS zero_width",
        s"$functionName(target, across_gap_adjacent, gap_width) AS explicit_width")

      result.schema.fields.foreach(_.dataType should equal(GeometryUDT()))
      val row = result.first()
      row.getAs[Geometry]("zero_width").isEmpty should be(true)
      row.getAs[Geometry]("explicit_width").isEmpty should be(false)
    }

    it("propagates null arguments and ignores null array members") {
      val row = coverageSource
        .selectExpr(
          s"$functionName(null_target, matching_adjacent, 0.0) AS null_target",
          s"$functionName(target, null_member_adjacent, 0.0) AS null_member",
          s"$functionName(target, matching_adjacent, null_gap_width) AS null_gap_width")
        .first()

      row.isNullAt(row.fieldIndex("null_target")) should be(true)
      row.getAs[Geometry]("null_member").isEmpty should be(true)
      row.isNullAt(row.fieldIndex("null_gap_width")) should be(true)
    }

    it("rejects negative and non-finite gap widths") {
      Seq(-0.1, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach {
        gapWidth =>
          val exception = withClue(s"gapWidth=$gapWidth: ") {
            intercept[Exception] {
              coverageSource
                .withColumn("invalid_gap_width", lit(gapWidth))
                .selectExpr(s"$functionName(target, matching_adjacent, invalid_gap_width)")
                .collect()
            }
          }

          val hasExpectedMessage = Iterator
            .iterate(exception: Throwable)(_.getCause)
            .takeWhile(_ != null)
            .exists(error =>
              Option(error.getMessage)
                .exists(_.contains("gapWidth must be finite and non-negative")))
          hasExpectedMessage should be(true)
      }
    }
  }

  private def coverageSource: DataFrame = sparkSession.sql("""
      |SELECT
      |  ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))') AS target,
      |  ARRAY(ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))')) AS matching_adjacent,
      |  ARRAY(ST_GeomFromWKT(
      |    'POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))')) AS overlapping_adjacent,
      |  ARRAY(ST_GeomFromWKT(
      |    'POLYGON ((1.1 0, 2.1 0, 2.1 1, 1.1 1, 1.1 0))')) AS across_gap_adjacent,
      |  ST_GeomFromWKT(CAST(NULL AS STRING)) AS null_target,
      |  ARRAY(ST_GeomFromWKT(CAST(NULL AS STRING))) AS null_member_adjacent,
      |  0.2 AS gap_width,
      |  CAST(NULL AS DOUBLE) AS null_gap_width
      |""".stripMargin)
}
