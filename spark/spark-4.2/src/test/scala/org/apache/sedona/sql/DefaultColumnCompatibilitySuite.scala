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

import org.apache.spark.sql.execution.datasources.geoparquet.internal.ResolveDefaultColumns
import org.apache.spark.sql.types.StringType
import org.scalatest.FunSuite

class DefaultColumnCompatibilitySuite extends FunSuite {
  test("GeoParquet default values resolve and fold Spark built-in functions") {
    val expression = ResolveDefaultColumns.analyze(
      "label",
      StringType,
      "upper(concat('geo', 'parquet'))",
      "CREATE TABLE")
    assert(expression.foldable)
    assert(expression.eval().toString == "GEOPARQUET")
  }

  test("GeoParquet default values reject unknown functions") {
    intercept[IllegalArgumentException] {
      ResolveDefaultColumns.analyze(
        "label",
        StringType,
        "missing_default_function('x')",
        "CREATE TABLE")
    }
  }
}
