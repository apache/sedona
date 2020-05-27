package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.python.SerializationException
import java.nio.ByteBuffer


private[python] class PythonGeometrySerializer extends Serializable {

  /*
      Translates JTS geometry to byte array which then will be decoded to Python shapely geometry objects
      Process needs two steps:
      - Translate JTS Geometry to Byte array using WKBWriter
      - Translate user attributes using UTF-8 encoding
   */

  def serialize: (Geometry => Array[Byte]) = {
    case geometry: Circle => CircleSerializer(geometry).serialize
    case geometry: Geometry => GeometrySerializer(geometry).serialize

  }

  def deserialize: Array[Byte] => Geometry = (values: Array[Byte]) => {
    val reader = new WKBReader()
    val isCircle = values.head.toInt
    val valuesLength = values.length

    if (isCircle == 1){
      val geom = reader.read(values.slice(9, valuesLength))
      val radius = ByteBuffer.wrap(values.slice(1 , 9)).getDouble()
      new Circle(geom, radius)
    }
    else if (isCircle == 0){
      reader.read(values.slice(1, valuesLength))
    }
    else throw SerializationException("Can not deserialize object")

  }
}