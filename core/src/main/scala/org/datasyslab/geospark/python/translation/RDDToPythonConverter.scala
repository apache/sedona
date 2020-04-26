package org.datasyslab.geospark.python.translation

import org.apache.spark.api.java.JavaRDD

private[translation] abstract class RDDToPythonConverter {
  def translateToPython: JavaRDD[Array[Byte]]
}
