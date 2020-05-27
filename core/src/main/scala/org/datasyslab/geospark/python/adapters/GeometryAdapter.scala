package org.datasyslab.geospark.python.adapters

import com.vividsolutions.jts.geom.Geometry
import org.datasyslab.geospark.python.translation.PythonGeometrySerializer

object GeometryAdapter extends GeomSerializer{

  def translateToJava(geometryBytes: java.util.ArrayList[Int]): Geometry = {
    val bytesData = geometryBytes.toArray.map(x => x.asInstanceOf[Int].toByte)
    geometrySerializer.deserialize(bytesData)
  }
}
