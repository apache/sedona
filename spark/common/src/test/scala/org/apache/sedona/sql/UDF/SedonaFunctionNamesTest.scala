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
package org.apache.sedona.sql.UDF

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class SedonaFunctionNamesTest extends AnyFunSpec with BeforeAndAfterEach {

  override def beforeEach(): Unit = SedonaFunctionNames.resetForTesting()
  override def afterEach(): Unit = SedonaFunctionNames.resetForTesting()

  describe("SedonaFunctionNames") {
    it("resolves to the plain name before any prefix is fixed") {
      assert(SedonaFunctionNames.effectivePrefix == "")
      assert(SedonaFunctionNames.resolve("ST_Buffer") == "ST_Buffer")
    }
    it("prepends the prefix verbatim to every name") {
      SedonaFunctionNames.initialize("sedona_")
      assert(SedonaFunctionNames.resolve("ST_Buffer") == "sedona_ST_Buffer")
      assert(SedonaFunctionNames.resolve("GeometryType") == "sedona_GeometryType")
    }
    it("keeps the first prefix when a later one differs") {
      SedonaFunctionNames.initialize("sedona_")
      SedonaFunctionNames.initialize("other_")
      SedonaFunctionNames.initialize("")
      assert(SedonaFunctionNames.effectivePrefix == "sedona_")
    }
    it("keeps an empty first prefix") {
      SedonaFunctionNames.initialize("")
      SedonaFunctionNames.initialize("sedona_")
      assert(SedonaFunctionNames.effectivePrefix == "")
    }
    it("accepts identifier-shaped prefixes") {
      Seq("sedona_", "_s", "S1_", "a").foreach { p =>
        SedonaFunctionNames.resetForTesting()
        SedonaFunctionNames.initialize(p)
        assert(SedonaFunctionNames.effectivePrefix == p)
      }
    }
    it("rejects other values and does not fix them") {
      Seq("1sedona", "sedona-", "sedona.", " sedona_", "sedona_ ", "se dona", "sedona_;")
        .foreach { p =>
          val e = intercept[IllegalArgumentException](SedonaFunctionNames.initialize(p))
          assert(e.getMessage.contains("spark.sedona.function.prefix"))
          assert(e.getMessage.contains(p))
          assert(SedonaFunctionNames.effectivePrefix == "")
        }
    }
  }
}
