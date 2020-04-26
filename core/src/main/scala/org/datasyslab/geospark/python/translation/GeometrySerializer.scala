package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBWriter
import org.datasyslab.geospark.python.utils.implicits._


case class GeometrySerializer(geometry: Geometry) {

  private val notCircle = Array(0.toByte)
  def serialize: Array[Byte] = {
    val wkbWriter = new WKBWriter(2, 2)
    val serializedGeom = wkbWriter.write(geometry)
    val userDataBinary = geometry.userDataToUtf8ByteArray
    val userDataLengthArray = userDataBinary.length.toByteArray()
    val serializedGeomLength = serializedGeom.length.toByteArray()
    notCircle ++ serializedGeomLength ++ userDataLengthArray ++ serializedGeom ++ userDataBinary
  }
}
