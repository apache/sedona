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

import org.apache.sedona.python.wrapper.adapters.PythonConverter
import org.apache.sedona.python.wrapper.translation.{FlatPairRddConverter, GeometryRddConverter, ListPairRddConverter, PythonRDDToJavaConverter}
import org.apache.spark.api.java.JavaPairRDD
import org.scalatest.Matchers
import org.apache.sedona.python.wrapper.utils.implicits._

import scala.jdk.CollectionConverters._


class TestToPythonSerialization extends SparkUtil with GeometrySample with Matchers {

  test("Test Serialize To Python JavaRDD[Geometry]") {
    val convertedToPythonRDD = GeometryRddConverter(pointSpatialRDD, pythonGeometrySerializer).translateToPython
    val convertedToPythonArrays = convertedToPythonRDD.collect().toArray().toList.map(arr => arr match {
      case a: Array[Byte] => a.toList
    })
    convertedToPythonArrays should contain theSameElementsAs expectedPointArray
  }

  test("Test between Circle RDD and Python RDD") {
    val rddPython = PythonConverter.translateSpatialRDDToPython(circleSpatialRDD)
    val rddJava = PythonConverter.translatePythonRDDToJava(rddPython)
    rddJava.collect() should contain theSameElementsAs sampleCircles
  }

  test("Test between Point RDD and Python RDD") {
    val rddPython = PythonConverter.translateSpatialRDDToPython(pointSpatialRDD)
    val rddJava = PythonConverter.translatePythonRDDToJava(rddPython)
    rddJava.collect() should contain theSameElementsAs samplePoints
  }

  test("Should serialize to Python JavaRDD[Geometry, Geometry]") {
    val translatedToPythonSpatialPairRdd = FlatPairRddConverter(
      JavaPairRDD.fromRDD(spatialPairRDD), pythonGeometrySerializer).translateToPython

    translatedToPythonSpatialPairRdd.collect().toArray().toList.map(arr => arr match {
      case a: Array[Byte] => a.toList
    }) should contain theSameElementsAs expectedPairRDDPythonArray
  }

  test("Should serialize to Python JavaRDD[Geometry, List[Geometry]]") {
    val translatedToPythonList = ListPairRddConverter(
      JavaPairRDD.fromRDD(spatialPairRDDWithList), pythonGeometrySerializer).translateToPython
    val existingValues = translatedToPythonList.collect().toArray().toList.map {
      case a: Array[Byte] => a.toList
    }
    existingValues should contain theSameElementsAs expectedPairRDDWithListPythonArray
  }

  private val pointSpatialRDD = sc.parallelize(samplePoints).toJavaRDD()

  private val circleSpatialRDD = sc.parallelize(sampleCircles).toJavaRDD()

  private val spatialPairRDD = sc.parallelize(
    samplePoints.zip(samplePolygons).map(
      geometries => (geometries._1, geometries._2)
    )
  )

  private val spatialPairRDDWithList = sc.parallelize(
    samplePolygons.map(
      polygon => (polygon, samplePoints.slice(0, 2).asJava)
    )
  )

  private val expectedPointArray: List[List[Byte]] = samplePoints.map(point =>
    0.toByteArray().toList ++ pythonGeometrySerializer.serialize(point).toList ++ 0.toByteArray().toList)

  private val expectedPairRDDPythonArray: List[List[Byte]] = samplePoints.zip(samplePolygons).map(
    geometries => 2.toByteArray().toList ++ pythonGeometrySerializer.serialize(geometries._1).toList
      ++ 1.toByteArray().toList ++ pythonGeometrySerializer.serialize(geometries._2).toList
  )

  private val expectedPairRDDWithListPythonArray: List[List[Byte]] = samplePolygons.map(
    samplePolygon => 1.toByteArray().toList ++ pythonGeometrySerializer.serialize(samplePolygon).toList ++ 2.toByteArray() ++
      samplePoints.slice(0, 2).flatMap(samplePoint => pythonGeometrySerializer.serialize(samplePoint)))

}
