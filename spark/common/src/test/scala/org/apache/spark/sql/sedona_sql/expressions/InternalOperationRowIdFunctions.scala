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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lit, map, row_number, struct}
import org.scalatest.Matchers

class InternalOperationRowIdFunctions extends TestBaseScala with Matchers {

  private val internalFunctionName = "__sedona_internal_operation_row_id"

  describe("internal operation-row-id expression") {

    it("registers the hidden internal alias") {
      sparkSession.catalog.functionExists(internalFunctionName) should be(true)
    }

    it("anchors complex dependency lineages to an operation-local row ID") {
      val rowCount = 1000L
      val source = sparkSession
        .range(0, rowCount, 1, 8)
        .repartition(8, col("id"))
        .select(
          col("id").as("payload"),
          row_number().over(Window.orderBy(col("id").desc)).as("derived_index"),
          map(lit("id"), col("id")).as("map_index"),
          (col("id") % lit(7)).as("natural_order"))
      val dependency =
        struct(col("payload"), col("derived_index"), col("map_index"), col("natural_order"))
      val keyed = source
        .withColumn("dependency", dependency)
        .withColumn("row_key", expr(s"$internalFunctionName(dependency)"))
        .drop("dependency")
      val narrow = keyed.select("payload", "row_key")
      val restored = keyed.select("derived_index", "map_index", "natural_order", "row_key")
      val result = narrow.join(restored, "row_key")

      narrow.queryExecution.optimizedPlan.toString should include("Window")
      restored.queryExecution.optimizedPlan.toString should include("Window")
      result.count() should equal(rowCount)
      result
        .where(col("derived_index") =!= lit(rowCount) - col("payload"))
        .count() should equal(0L)
      result
        .where(col("map_index").getItem("id") =!= col("payload"))
        .count() should equal(0L)
    }
  }
}
