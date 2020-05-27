package org.datasyslab.geospark.python.adapters

import org.datasyslab.geospark.python.translation.PythonGeometrySerializer

trait GeomSerializer {
  val geometrySerializer = new PythonGeometrySerializer()
}
