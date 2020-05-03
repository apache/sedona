package org.datasyslab.geospark.python

import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.python.translation.{FlatPairRddConverter, GeometryRddConverter, HashSetPairRddConverter}
import org.datasyslab.geospark.python.utils.implicits._
import org.scalatest.Matchers

class TestToPythonSerialization extends SparkUtil with GeometrySample with Matchers{

  test("Test Serialize To Python JavaRDD[Geometry]"){
    val convertedToPythonRDD = GeometryRddConverter(pointSpatialRDD, pythonGeometrySerializer).translateToPython
    val convertedToPythonArrays = convertedToPythonRDD.collect().toArray().toList.map(arr => arr match {
      case a: Array[Byte] => a.toList
    })
    convertedToPythonArrays should contain theSameElementsAs expectedPointArray

  }

  test("Should serialize to Python JavaRDD[Geometry, Geometry]"){
    val translatedToPythonSpatialPairRdd = FlatPairRddConverter(
      JavaPairRDD.fromRDD(spatialPairRDD), pythonGeometrySerializer).translateToPython

    translatedToPythonSpatialPairRdd.collect().toArray().toList.map(arr => arr match {
      case a: Array[Byte] => a.toList
    }) should contain theSameElementsAs expectedPairRDDPythonArray
  }

  test("Should serialize to Python JavaRDD[Geometry, HashSet[Geometry]]"){
    val translatedToPythonHashSet = HashSetPairRddConverter(
      JavaPairRDD.fromRDD(spatialPairRDDWithHashSet), pythonGeometrySerializer).translateToPython
    val existingValues = translatedToPythonHashSet.collect().toArray().toList.map {
      case a: Array[Byte] => a.toList
    }
    existingValues should contain theSameElementsAs expectedPairRDDWithHashSetPythonArray
  }

  private val pointSpatialRDD = sc.parallelize(
    samplePoints
  ).toJavaRDD()

  private val spatialPairRDD = sc.parallelize(
    samplePoints.zip(samplePolygons).map(
      geometries => (geometries._1, geometries._2)
    )
  )

  private val spatialPairRDDWithHashSet = sc.parallelize(
    samplePolygons.map(
      polygon => (polygon, samplePoints.slice(0,2).toJavaHashSet)
    )
  )


  private val expectedPointArray: List[List[Byte]] = samplePoints.map(point =>
    0.toByteArray().toList ++ pythonGeometrySerializer.serialize(point).toList ++ 0.toByteArray().toList)

  private val expectedPairRDDPythonArray: List[List[Byte]] = samplePoints.zip(samplePolygons).map(
    geometries => 2.toByteArray().toList ++ pythonGeometrySerializer.serialize(geometries._1).toList
      ++ 1.toByteArray().toList ++ pythonGeometrySerializer.serialize(geometries._2).toList
  )

  private val expectedPairRDDWithHashSetPythonArray: List[List[Byte]] = samplePolygons.map(
    samplePolygon => 1.toByteArray().toList ++ pythonGeometrySerializer.serialize(samplePolygon).toList ++ 2.toByteArray() ++
      samplePoints.slice(0, 2).flatMap(samplePoint => pythonGeometrySerializer.serialize(samplePoint)))

  private val samplePoint = wktReader.read("Point(20 50)")
  private val sampleUserData = "testData with special characters like .?"
  private val expectedEncodedArray = Array[Byte](21, 0, 0, 0, 40, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 52, 64,
    0, 0, 0, 0, 0, 0, 73, 64, 116, 101, 115, 116, 68, 97, 116, 97, 32, 119, 105, 116, 104, 32, 115, 112, 101, 99, 105,
    97, 108, 32, 99, 104, 97, 114, 97, 99, 116, 101, 114, 115, 32, 108, 105, 107, 101, 32, 46, 63)

}
