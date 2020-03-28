package org.datasyslab.geospark.python

import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import org.datasyslab.geospark.python.translation.PythonGeometrySerializer

trait PythonTestSpec {
  private[python] lazy val geometryFactory = new GeometryFactory()
  private[python] lazy val pythonGeometrySerializer = new PythonGeometrySerializer()
  private[python] lazy val wktReader = new WKTReader(geometryFactory)
}
