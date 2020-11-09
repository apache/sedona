package org.apache.sedona.python.wrapper

import org.apache.sedona.python.wrapper.translation.PythonGeometrySerializer
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader

trait PythonTestSpec {
  private[python] lazy val geometryFactory = new GeometryFactory()
  private[python] lazy val pythonGeometrySerializer = new PythonGeometrySerializer()
  private[python] lazy val wktReader = new WKTReader(geometryFactory)
}
