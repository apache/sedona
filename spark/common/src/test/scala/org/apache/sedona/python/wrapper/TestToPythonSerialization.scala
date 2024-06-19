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
import org.apache.sedona.python.wrapper.adapters.PythonConverter
import org.apache.sedona.python.wrapper.translation.{FlatPairRddConverter, GeometryRddConverter, ListPairRddConverter, PythonGeometrySerializer}
import org.apache.sedona.python.wrapper.utils.implicits._
import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.api.java.JavaPairRDD
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.io.{FileInputStream, InputStream}
import scala.io.Source
import scala.jdk.CollectionConverters._

class TestToPythonSerialization extends TestBaseScala {

  private lazy val geometryFactory = new GeometryFactory()

  private lazy val pythonGeometrySerializer = new PythonGeometrySerializer()

  private lazy val wktReader = new WKTReader(geometryFactory)

  private val samplePoints: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/samplePoints")

  private val sampleCircles: List[Geometry] = samplePoints.map(new Circle(_, 1.0))

  private val sampleLines: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/sampleLines")

  private val samplePolygons: List[Geometry] =
    loadGeometriesFromResources(resourceFolder + "python/samplePolygons")

  private def loadGeometriesFromResources(fileName: String): List[Geometry] = {
    val resourceFileText = loadResourceFile(fileName)
    loadFromWktStrings(resourceFileText)
  }

  private def loadFromWktStrings(geometries: List[String]): List[Geometry] = {
    geometries.map(geometryWKT => wktReader.read(geometryWKT))
  }

  private def loadResourceFile(fileName: String): List[String] = {
    val stream: InputStream = new FileInputStream(fileName)
    Source.fromInputStream(stream).getLines.toList
  }

  val pointSpatialRDD = sc.parallelize(samplePoints).toJavaRDD()

  val circleSpatialRDD = sc.parallelize(sampleCircles).toJavaRDD()

  val spatialPairRDD = sc.parallelize(
    samplePoints.zip(samplePolygons).map(geometries => (geometries._1, geometries._2)))

  val spatialPairRDDWithList =
    sc.parallelize(samplePolygons.map(polygon => (polygon, samplePoints.slice(0, 2).asJava)))

  val expectedPointArray: List[List[Byte]] = samplePoints.map(point =>
    0.toByteArray().toList ++ pythonGeometrySerializer.serialize(point).toList ++ 0
      .toByteArray()
      .toList)

  val expectedPairRDDPythonArray: List[List[Byte]] = samplePoints
    .zip(samplePolygons)
    .map(geometries =>
      2.toByteArray().toList ++ pythonGeometrySerializer.serialize(geometries._1).toList
        ++ 1.toByteArray().toList ++ pythonGeometrySerializer.serialize(geometries._2).toList)

  val expectedPairRDDWithListPythonArray: List[List[Byte]] = samplePolygons.map(samplePolygon =>
    1.toByteArray().toList ++ pythonGeometrySerializer.serialize(samplePolygon).toList ++ 2
      .toByteArray() ++
      samplePoints
        .slice(0, 2)
        .flatMap(samplePoint => pythonGeometrySerializer.serialize(samplePoint)))

  describe("Sedona Python Wrapper Test") {
    it("Test Serialize To Python JavaRDD[Geometry]") {
      val convertedToPythonRDD =
        GeometryRddConverter(pointSpatialRDD, pythonGeometrySerializer).translateToPython
      val convertedToPythonArrays = convertedToPythonRDD
        .collect()
        .toArray()
        .toList
        .map(arr =>
          arr match {
            case a: Array[Byte] => a.toList
          })
      convertedToPythonArrays should contain theSameElementsAs expectedPointArray
    }

    it("Test between Circle RDD and Python RDD") {
      val rddPython = PythonConverter.translateSpatialRDDToPython(circleSpatialRDD)
      val rddJava = PythonConverter.translatePythonRDDToJava(rddPython)
      rddJava.collect() should contain theSameElementsAs sampleCircles
    }

    it("Test between Point RDD and Python RDD") {
      val rddPython = PythonConverter.translateSpatialRDDToPython(pointSpatialRDD)
      val rddJava = PythonConverter.translatePythonRDDToJava(rddPython)
      rddJava.collect() should contain theSameElementsAs samplePoints
    }

    it("Should serialize to Python JavaRDD[Geometry, Geometry]") {
      val translatedToPythonSpatialPairRdd = FlatPairRddConverter(
        JavaPairRDD.fromRDD(spatialPairRDD),
        pythonGeometrySerializer).translateToPython

      translatedToPythonSpatialPairRdd
        .collect()
        .toArray()
        .toList
        .map(arr =>
          arr match {
            case a: Array[Byte] => a.toList
          }) should contain theSameElementsAs expectedPairRDDPythonArray
    }

    it("Should serialize to Python JavaRDD[Geometry, List[Geometry]]") {
      val translatedToPythonList = ListPairRddConverter(
        JavaPairRDD.fromRDD(spatialPairRDDWithList),
        pythonGeometrySerializer).translateToPython
      val existingValues = translatedToPythonList.collect().toArray().toList.map {
        case a: Array[Byte] => a.toList
      }
      existingValues should contain theSameElementsAs expectedPairRDDWithListPythonArray
    }
  }

}
