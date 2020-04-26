package org.datasyslab.geospark.python.translation

import java.io.{ByteArrayInputStream, DataInputStream}

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.python.SerializationException

case class PythonRDDToJavaConverter(javaRDD: JavaRDD[Array[Byte]], geometrySerializer: PythonGeometrySerializer) {
  def translateToJava: JavaRDD[Geometry] = {
    javaRDD.rdd.map[Geometry](serializedGeoData =>
    {
      val geoDataBytes = new ByteArrayInputStream(serializedGeoData)
      val geoDataInputBytes = new DataInputStream(geoDataBytes)
      val rddType = geoDataInputBytes.readInt()
      val isCircle = geoDataInputBytes.readByte().toInt
      if (isCircle == 1){
        val radius = geoDataInputBytes.readDouble()
        val geometry = readGeometry(serializedGeoData, geoDataInputBytes, 20)
        new Circle(geometry, radius)
      }
      if (isCircle == 0){
        readGeometry(serializedGeoData, geoDataInputBytes, 12)
      }
      else throw SerializationException()


    }

    )
  }

  private def readGeometry(serializedGeoData: Array[Byte], inputStream: DataInputStream, skipBytes: Int): Geometry = {
    val geomDataLength = java.lang.Integer.reverseBytes(inputStream.readInt())
    val userDataLength = java.lang.Integer.reverseBytes(inputStream.readInt())

    val toReadGeometry = serializedGeoData.slice(skipBytes, skipBytes+geomDataLength+1)
    val geom = geometrySerializer.deserialize(toReadGeometry)

    val userData = serializedGeoData.slice(skipBytes + geomDataLength+1, skipBytes+geomDataLength+1 + userDataLength)
      .map(_.toChar).mkString

    geom.setUserData(userData)
    geom
  }
}

