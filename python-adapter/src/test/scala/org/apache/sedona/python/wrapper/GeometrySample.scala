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

package org.apache.sedona.python.wrapper

import org.apache.sedona.common.geometryObjects.Circle
import org.locationtech.jts.geom.Geometry

import java.io.{FileInputStream, InputStream}
import scala.io.Source

trait GeometrySample extends PythonTestSpec {
  self: TestToPythonSerialization =>

  val resourceFolder = System.getProperty("user.dir") + "/../core/src/test/resources/"
  private[python] val samplePoints: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/samplePoints")

  private[python] val sampleCircles: List[Geometry] = samplePoints.map(new Circle(_, 1.0))

  private[python] val sampleLines: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/sampleLines")

  private[python] val samplePolygons: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/samplePolygons")

  private def loadGeometriesFromResources(fileName: String): List[Geometry] = {
    val resourceFileText = loadResourceFile(fileName)
    loadFromWktStrings(resourceFileText)
  }

  private def loadFromWktStrings(geometries: List[String]): List[Geometry] = {
    geometries.map(
      geometryWKT => wktReader.read(geometryWKT)
    )
  }

  private def loadResourceFile(fileName: String): List[String] = {
    val stream: InputStream = new FileInputStream(fileName)
    Source.fromInputStream(stream).getLines.toList
  }
}
