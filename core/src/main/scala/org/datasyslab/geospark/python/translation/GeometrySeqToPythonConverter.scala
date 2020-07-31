package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.Geometry
import org.datasyslab.geospark.python.utils.implicits._

case class GeometrySeqToPythonConverter(spatialData: scala.collection.convert.Wrappers.SeqWrapper[Geometry],
                                        geometrySerializer: PythonGeometrySerializer) {

  def translateToPython: Array[Array[Byte]] = {
    val sizeBuffer = 0.toByteArray()

    spatialData.toArray.map(
      geometry => geometrySerializer.serialize(geometry.asInstanceOf[Geometry]) ++ sizeBuffer
    )
  }
}
