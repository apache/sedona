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

import org.apache.sedona.common.simplify.GeometrySimplifier
import org.locationtech.jts.geom.Geometry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor5}


class TestGeometrySimplify extends AnyFunSuite with Matchers with TableDrivenPropertyChecks with FunctionsHelper {

  import Fixtures._

  for ((statement: String, inputGeometry: String, outPutGeometry: String, collapse: Boolean, epsilon: Double) <- Fixtures.geometriesToSimplify) {
    test("it should simplify geometry " + statement) {
      Fixtures.simplifyGeometries(inputGeometry, collapse, epsilon).toWkt shouldBe outPutGeometry
    }
  }


  object Fixtures {

    implicit class GeometryEnhancements(geom: Geometry) {
      def toWkt: String = wktWriter.write(geom)
    }


    val geometriesToSimplify: TableFor5[String, String, String, Boolean, Double] = Table(
      ("geometryType", "inputWkt", "expectedResult", "preserve collapsed", "epsilon"),
      ("point", "POINT (4 43)", "POINT (4 43)", true, 0.0),
      ("polygon", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", true, 0.0),
      ("linestring", "LINESTRING (1 1, 2 2, 3 2)", "LINESTRING (1 1, 2 2, 3 2)", true, 0.0),
      ("polygon with few points on the same line",
        "POLYGON ((45 45, 37.857142857142854 20, 30 20, 10 20, 15 40, 45 45), (30 20, 35 35, 20 30, 30 20))",
        "POLYGON ((45 45, 37.857142857142854 20, 10 20, 15 40, 45 45), (30 20, 35 35, 20 30, 30 20))",
        true,
        0.0),
      ("linestring with 10 epsilon", "LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)", "LINESTRING (0 0, 0 0)", true, 10.0),
      ("polygon with 10 epsilon", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON ((0 0, 1 0, 1 1, 0 0))", true, 10.0),
      ("linestring with epsilon 1.0",
        "LINESTRING (0 0, 50 1.00001, 100 0)",
        "LINESTRING (0 0, 50 1.00001, 100 0)",
        false,
        1.0
      ),
      ("another linestring with epsilon 1, 0", "LINESTRING (0 0, 50 0.99999, 100 0)", "LINESTRING (0 0, 100 0)", false, 1.0),
      (
        "polygon with multiple holes",
        "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0), (1 1, 1 5, 5 5, 5 1, 1 1), (20 20, 20 40, 40 40, 40 20, 20 20))",
        "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0), (20 20, 20 40, 40 40, 40 20, 20 20))",
        false,
        10.0
      ),
      (
        "polygon with multiple holes but reordered",
        "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0), (20 20, 20 40, 40 40, 40 20, 20 20), (1 1, 1 5, 5 5, 5 1, 1 1))",
        "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0), (20 20, 20 40, 40 40, 40 20, 20 20))",
        false,
        10.0
      ),
      (
        "geometry collection with no changes needed",
        "GEOMETRYCOLLECTION (POINT (2 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))",
        "GEOMETRYCOLLECTION (POINT (2 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))",
        false,
        0.0
      ),
      (
        "multilinestring with no changes required",
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
        false,
        0.0
      ),
      (
        "multipolygon with holes",
        "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))",
        "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))",
        false,
        0.0
      ),
      (
        "multipolygon with duplicated polygons",
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
        false,
        0.0
      ),
      (
        "multipoint",
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
        false,
        0.0
      ),
      (
        "multpoint with duplicated points",
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10), (30 10))",
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10), (30 10))",
        false,
        0.0
      ),
      (
        "multilinestring",
        "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
        "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
        false,
        0.0
      ),
      (
        "multilinestring with duplicated lines",
        "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10), (40 40, 30 30, 40 20, 30 10))",
        "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10), (40 40, 30 30, 40 20, 30 10))",
        false,
        0.0
        )
    )

    def simplifyGeometries(geom: String, preserveCollapsed: Boolean, epsilon: Double): Geometry =
      GeometrySimplifier.simplify(wktReader.read(geom), preserveCollapsed, epsilon)

  }

}
