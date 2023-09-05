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

import org.apache.spark.sql.{Dataset, Row}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

import java.io.{FileInputStream, InputStream}


trait GeometrySample {
  self: TestBaseScala =>
  val wktReader = new WKTReader()

  import sparkSession.implicits._

  val samplePoints: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/samplePoints")
  private val sampleSimplePolygons: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/simplePolygons")
  private val sampleLines: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/sampleLines")
  private val samplePolygons: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/samplePolygons")

  def createSimplePolygons(polygonCount: Int, columnNames: String*): Dataset[Row] =
    sampleSimplePolygons.take(polygonCount).map(mapToSpark).toDF(columnNames: _*)

  def createSamplePolygonDf(polygonCount: Int, columnNames: String*): Dataset[Row] =
    samplePolygons.take(polygonCount).map(mapToSpark).toDF(columnNames: _*)

  private def mapToSpark(geometry: Geometry): Tuple1[Geometry] =
    Tuple1(geometry)

  def createSamplePointDf(pointCount: Int, columnNames: String*): Dataset[Row] =
    samplePoints.take(pointCount).map(mapToSpark).toDF(columnNames: _*)

  def createSampleLineStringsDf(lineCount: Int, columnNames: String*): Dataset[Row] =
    sampleLines.take(lineCount).map(mapToSpark).toDF(columnNames: _*)

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
    val lines: Iterator[String] = scala.io.Source.fromInputStream(stream).getLines
    lines.toList
  }
}

