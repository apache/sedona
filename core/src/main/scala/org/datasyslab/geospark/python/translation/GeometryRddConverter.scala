package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geospark.python.utils.implicits._

private[python] case class GeometryRddConverter[T <: Geometry](spatialRDD: JavaRDD[T],
                           geometrySerializer: PythonGeometrySerializer) extends RDDToPythonConverter{

  override def translateToPython: JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](geom =>{
      val typeBuffer = 0.toByteArray()
      val sizeBuffer = 0.toByteArray()
      typeBuffer ++ geometrySerializer.serialize(geom) ++ sizeBuffer
    }
    ).toJavaRDD()
  }

}
