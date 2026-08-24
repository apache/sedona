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
package org.apache.spark.sql.sedona_sql.io.stac

import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class StacBatchUrlTest extends AnyFunSuite {

  private val batch = StacBatch(
    null,
    "file:///collection.json",
    """{"links":[]}""",
    StructType(Seq.empty),
    Map.empty,
    None,
    None,
    None)

  test("getItemLink should not append datetime when the endpoint already provides it") {
    val temporalFilter = Some(
      TemporalFilter.GreaterThanFilter("datetime", LocalDateTime.parse("2025-03-06T00:00:00")))
    val cases = Seq(
      "https://example.test/items?token=x&datetime=2025-02-01/2025-02-28#results" ->
        "https://example.test/items?token=x&datetime=2025-02-01/2025-02-28&limit=2#results",
      "https://example.test/items?datetime=" ->
        "https://example.test/items?datetime=&limit=2",
      "https://example.test/items?datetime=%ZZ&datetime=also-bad" ->
        "https://example.test/items?datetime=%ZZ&datetime=also-bad&limit=2",
      "https://example.test/items?d%61tetime=2025-02-01/2025-02-28" ->
        "https://example.test/items?d%61tetime=2025-02-01/2025-02-28&limit=2")

    cases.foreach { case (input, expected) =>
      assert(batch.getItemLink(input, 2, None, temporalFilter) == expected)
    }
  }
}
