package org.imbruced.geo_pyspark.serializers

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryFactory, LineString, Point, Polygon}
import net.razorvine.pickle.objects.{ArrayConstructor, ByteArrayConstructor, ClassDict}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.datasyslab.geosparksql.utils.GeometrySerializer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.Input

import scala.collection.JavaConverters._
import net.razorvine.pickle.Unpickler
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeSerde
import org.datasyslab.geospark.geometryObjects.GeometrySerde.Type


object GeoSerializerData {

  var initialized = false

  def initialize(): Unit = {
    synchronized{
      if (!initialized) {
        Unpickler.registerConstructor("array", "array", new ArrayConstructor())
        Unpickler.registerConstructor("__builtin__", "bytearray", new ByteArrayConstructor())
        Unpickler.registerConstructor("builtins", "bytearray", new ByteArrayConstructor())
        Unpickler.registerConstructor("__builtin__", "bytes", new ByteArrayConstructor())
        Unpickler.registerConstructor("_codecs", "encode", new ByteArrayConstructor())
        initialized = true
      }
    }
  }

  def createEnvelopes(bytes: Array[Byte]): java.util.List[Envelope] = {
    val arrBytes = bytes.map(x => x.toByte)
    val unpickler = new Unpickler
    val pythonEnvelopes = unpickler.loads(arrBytes).asInstanceOf[java.util.ArrayList[_]].toArray
    pythonEnvelopes.map(pythonEnvelope => new Envelope(
      pythonEnvelope.asInstanceOf[ClassDict].get("minx").asInstanceOf[Double],
      pythonEnvelope.asInstanceOf[ClassDict].get("maxx").asInstanceOf[Double],
      pythonEnvelope.asInstanceOf[ClassDict].get("miny").asInstanceOf[Double],
      pythonEnvelope.asInstanceOf[ClassDict].get("maxy").asInstanceOf[Double]
    )).toList.asJava
  }

  def deserializeGeom(pythonRDD: JavaRDD[Array[Byte]]): JavaRDD[Geometry] = {

    JavaRDD.fromRDD(pythonRDD.rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        val unpickler = new Unpickler
        val obj = unpickler.loads(row)
        obj match {
          case _ => obj.asInstanceOf[java.util.ArrayList[_]].toArray.map(
            classDict => {
              val geoData = classDict.asInstanceOf[ClassDict]
              val geom = geoData.asInstanceOf[ClassDict].get("geom")
              val userData = geoData.asInstanceOf[ClassDict].get("userData")
              val geometryInstance = GeometrySerializer.deserialize(ArrayData.toArrayData(geom))
              geometryInstance.setUserData(userData)
              geometryInstance.asInstanceOf[Geometry]
            }
          )
        }
      }
    }.toJavaRDD())
  }

  def deserializeToPointRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Point] = {
    deserializeGeom(javaRDD).asInstanceOf[JavaRDD[Point]]
  }

  def deserializeToPolygonRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Polygon] = {
    deserializeGeom(javaRDD).asInstanceOf[JavaRDD[Polygon]]
  }

  def deserializeToLineStringRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[LineString] = {
    deserializeGeom(javaRDD).asInstanceOf[JavaRDD[LineString]]
  }

  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](geom =>{
      val typeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

      typeBuffer.putInt(0)

      val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      sizeBuffer.putInt(0)

      typeBuffer.array() ++ serializeGeomToPython(geom) ++ sizeBuffer.array()
    }


    ).toJavaRDD()
  }
  def serializeToPython(geometryList: scala.collection.convert.Wrappers.SeqWrapper[Geometry]): Array[Array[Byte]] = {
    val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    sizeBuffer.putInt(0)

    geometryList.toArray.map(
      geometry => serializeGeomToPython(geometry.asInstanceOf[Geometry]) ++ sizeBuffer.array()
    )
  }

  def serializeGeomToPython(geom: Geometry): Array[Byte] = {
    val userData = geom.getUserData
    geom.setUserData("")
    val serializedGeom = GeometrySerializer.serialize(geom)
    val userDataBinary = userData.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
    val userDataLengthArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    userDataLengthArray.putInt(userDataBinary.length)

    userDataLengthArray.array() ++ serializedGeom ++ userDataBinary

  }

  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {

        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        val typeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

        typeBuffer.putInt(1)
        sizeBuffer.putInt(rightGeometry.toArray.length)

        typeBuffer.array() ++ serializeGeomToPython(leftGeometry) ++
          sizeBuffer.array() ++
          rightGeometry.toArray().flatMap(geometry => serializeGeomToPython(geometry.asInstanceOf[Geometry]))
      }
    )
  }
  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>{
      val leftGeometry = pairRDD._1
      val rightGeometry = pairRDD._2
      val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      val typeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

      typeBuffer.putInt(2)
      sizeBuffer.putInt(1)
      typeBuffer.array() ++ serializeGeomToPython(leftGeometry) ++ sizeBuffer.array() ++ serializeGeomToPython(rightGeometry)
    }
    ).toJavaRDD()
  }

  def getFromPythonRawGeometryRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Geometry] = {
    javaRDD.rdd.map[Geometry](serializedGeoData =>
    {
      val byteDataLength = serializedGeoData.length

      val geoDataBytes = new ByteArrayInputStream(serializedGeoData)
      val geoDataInputBytes = new DataInputStream(geoDataBytes)
      val rddType = geoDataInputBytes.readInt()
      val userDataLength = java.lang.Integer.reverseBytes(geoDataInputBytes.readInt())

      val toReadGeometry = serializedGeoData.slice(8, byteDataLength-4)
      val geom = GeometrySerializer.deserialize(ArrayData.toArrayData(toReadGeometry))

      geom.setUserData(serializedGeoData.slice(byteDataLength-(4+userDataLength), byteDataLength-4).map(_.toChar).mkString)
      geom
    }

    )
  }

}


