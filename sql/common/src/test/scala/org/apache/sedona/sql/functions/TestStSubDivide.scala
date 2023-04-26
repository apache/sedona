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

import org.apache.sedona.common.subDivide.GeometrySubDivider
import org.locationtech.jts.geom.Geometry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3, TableFor4}

import java.io.{FileInputStream, InputStream}
import scala.io.Source


class TestStSubDivide extends AnyFunSuite with Matchers with TableDrivenPropertyChecks with FunctionsHelper {


  for ((statement: String, inputGeometry: String, maxVertices: Int, outPutGeometry: Seq[String]) <- Fixtures.testPolygons) {
    test("it should return subdivide geometry when input geometry is " + statement) {
      Fixtures.subDivideGeometry(inputGeometry, maxVertices).map(geom => geometryToWkt(geom)) should contain theSameElementsAs outPutGeometry
    }
  }

  for ((statement: String, inputGeometry: String, maxVertices: Int) <- Fixtures.geometriesWithNumberOfVerticesLowerThanThreshold) {
    test("it should return empty sequence when number of vertices is lower than threshold" + statement) {
      Fixtures.subDivideGeometry(inputGeometry, maxVertices).map(geom => geometryToWkt(geom)) should contain theSameElementsAs Seq()
    }
  }


  object Fixtures {

    val resourceFolder: String = System.getProperty("user.dir") + "/../../core/src/test/resources/"

    private val geometries: Seq[(String, String, Int, Seq[String])] = {

      val input = loadResourceFile(resourceFolder + "subdivide/subdivide_input_geometries.txt").map(
        line => line.split(";")
      ).map(elements => (elements.head, elements(1), elements(2), elements(3).toInt))
        .sortBy(_._1)

      val expected = loadResourceFile(resourceFolder + "subdivide/subdivide_expected_result.txt").map(
        line => line.split(";")
      ).map(elements => (elements.head, elements(1)))

      val expectedGrouped = expected.groupBy(_._1)

      expectedGrouped.map{
        case (key, group) => (key, group.map(_._2))
      }.toSeq.sortBy(_._1).zip(input)
        .map{
          case ((_, resultGeom), (_, testName, inputGeom, subdivisionVertices)) =>
            (testName, inputGeom, subdivisionVertices, resultGeom)
        }

    }

    private def loadResourceFile(fileName: String): List[String] = {
      val stream: InputStream = new FileInputStream(fileName)
      Source.fromInputStream(stream).getLines.toList
    }

    val geometriesWithNumberOfVerticesLowerThanThreshold: TableFor3[String, String, Int] = Table(
      ("geometryType", "inputWkt", "numberOfVertices"),
      ("point", "POINT(4 43)", GeometrySubDivider.minMaxVertices - 1),
      ("polygon", "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", GeometrySubDivider.minMaxVertices - 1),
      ("linestring", "LINESTRING(1 1, 2 2, 3 2)", GeometrySubDivider.minMaxVertices - 1)
    )

    val testPolygons: TableFor4[String, String, Int, Seq[String]] = Table(
      ("statement", "input", "maxVertices", "output"),
      geometries:_*
    )

    def subDivideGeometry(wkt: String, maxVertices: Int): Seq[Geometry] =
      subDivideGeometry(readGeometry(wkt), maxVertices)

    def subDivideGeometry(geom: Geometry, maxVertices: Int): Seq[Geometry] =
      GeometrySubDivider.subDivide(geom, maxVertices)
  }


}
