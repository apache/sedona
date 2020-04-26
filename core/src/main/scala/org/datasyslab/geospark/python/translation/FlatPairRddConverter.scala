package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geospark.python.utils.implicits._

private[python] case class FlatPairRddConverter(spatialRDD: JavaPairRDD[Geometry, Geometry],
                           geometrySerializer: PythonGeometrySerializer
                          ) extends RDDToPythonConverter {
  override def translateToPython: JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>{
      val leftGeometry = pairRDD._1
      val rightGeometry = pairRDD._2
      val sizeBuffer = 1.toByteArray()
      val typeBuffer = 2.toByteArray()

      (typeBuffer ++ geometrySerializer.serialize(leftGeometry) ++ sizeBuffer ++
        geometrySerializer.serialize(rightGeometry))
    }
    ).toJavaRDD()
  }
}
