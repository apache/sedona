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

import org.scalatest.funspec.AnyFunSpec

/**
 * Sanity checks on `Catalog.categorizedSequences`.
 *
 * `Catalog.expressions` is defined as `categorizedSequences.flatten`, so cross-checking the two
 * for "missing" or "extra" entries is tautological. The check that has independent value is the
 * uniqueness check: a function must not appear in two named category sequences. That's what this
 * test enforces.
 *
 * If you add a function to two sequences by mistake (easy to do when a function arguably fits
 * more than one docs category), this test fails and tells you which name was duplicated.
 */
class CatalogCategorizationTest extends AnyFunSpec {

  describe("Catalog categorized sequences") {

    it("each function appears in at most one named category sequence") {
      val flattenedNames = Catalog.categorizedSequences.flatten.map(_._1.funcName)
      val duplicates = flattenedNames.diff(flattenedNames.distinct).distinct.sorted
      assert(
        duplicates.isEmpty,
        s"Functions appearing in more than one named sequence: ${duplicates.mkString(", ")}")
    }
  }
}
