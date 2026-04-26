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
 * Asserts the categorization invariant for `Catalog.expressions`: every registered function
 * appears in exactly one of the named category sequences in `Catalog.categorizedSequences`.
 *
 * If you add a function to `Catalog.expressions` (typically by adding it to one of the named
 * sequences) and this test fails, it means a function ended up in zero or two+ sequences. Find
 * the missing/duplicate function and place it in the right docs category — the names of the
 * sequences mirror the categories at
 * https://sedona.apache.org/latest/api/sql/Geometry-Functions/.
 */
class CatalogCategorizationTest extends AnyFunSpec {

  describe("Catalog categorization invariant") {

    it("every registered expression appears in exactly one named sequence") {
      val flattened = Catalog.categorizedSequences.flatten.map(_._1.funcName)
      val registered = Catalog.expressions.map(_._1.funcName)

      val flattenedSet = flattened.toSet
      val registeredSet = registered.toSet

      val missing = registeredSet -- flattenedSet
      val extra = flattenedSet -- registeredSet
      val duplicates = flattened.diff(flattened.distinct).distinct

      assert(
        missing.isEmpty,
        s"Functions registered in Catalog.expressions but missing from any named sequence: " +
          missing.toSeq.sorted.mkString(", "))
      assert(
        extra.isEmpty,
        s"Functions in named sequences but not registered in Catalog.expressions: " +
          extra.toSeq.sorted.mkString(", "))
      assert(
        duplicates.isEmpty,
        s"Functions appearing in more than one named sequence: " +
          duplicates.sorted.mkString(", "))
    }

    it("Catalog.expressions order matches the flattened sequence order") {
      // Registration order is part of the public contract (used by registerAll). Refactoring
      // categorization should preserve it.
      val flattened = Catalog.categorizedSequences.flatten.map(_._1.funcName)
      val registered = Catalog.expressions.map(_._1.funcName)
      assert(registered == flattened, "registration order drifted from categorized order")
    }
  }
}
