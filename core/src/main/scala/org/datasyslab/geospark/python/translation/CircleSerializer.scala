package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.io.WKBWriter
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.python.utils.implicits._


case class CircleSerializer(geometry: Circle) {
  private val isCircle = Array(1.toByte)

  def serialize: Array[Byte] = {
    val wkbWriter = new WKBWriter(2, 2)
    val serializedGeom = wkbWriter.write(geometry.getCenterGeometry)
    val userDataBinary = geometry.userDataToUtf8ByteArray
    val userDataLengthArray = userDataBinary.length.toByteArray()
    val serializedGeomLength = serializedGeom.length.toByteArray()
    val radius = geometry.getRadius.toDouble
    isCircle ++ serializedGeomLength ++ userDataLengthArray  ++ serializedGeom ++ userDataBinary ++ radius.toByteArray()
  }

}
