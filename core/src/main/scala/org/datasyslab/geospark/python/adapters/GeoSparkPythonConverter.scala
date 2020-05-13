package org.datasyslab.geospark.python.adapters

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geospark.python.translation.{FlatPairRddConverter, GeometryRddConverter, GeometrySeqToPythonConverter, HashSetPairRddConverter, PythonGeometrySerializer, PythonRDDToJavaConverter}

import scala.collection.convert.Wrappers.SeqWrapper

object GeoSparkPythonConverter extends GeomSerializer{

  def translateSpatialRDDToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] =
    GeometryRddConverter(spatialRDD, geometrySerializer).translateToPython

  def translateSpatialPairRDDToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] =
    FlatPairRddConverter(spatialRDD, geometrySerializer).translateToPython

  def translateSpatialPairRDDWithHashSetToPython(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] =
    HashSetPairRddConverter(spatialRDD, geometrySerializer).translateToPython

  def translatePythonRDDToJava(pythonRDD: JavaRDD[Array[Byte]]): JavaRDD[Geometry] =
    PythonRDDToJavaConverter(pythonRDD, geometrySerializer).translateToJava

  def translateGeometrySeqToPython(spatialData: SeqWrapper[Geometry]): Array[Array[Byte]] =
    GeometrySeqToPythonConverter(spatialData, geometrySerializer).translateToPython
}
