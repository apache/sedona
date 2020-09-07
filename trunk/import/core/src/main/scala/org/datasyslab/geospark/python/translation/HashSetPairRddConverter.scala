package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geospark.python.utils.implicits._

private[python] case class HashSetPairRddConverter(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]],
                              geometrySerializer: PythonGeometrySerializer) extends RDDToPythonConverter{
  override def translateToPython: JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {

        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        val sizeBuffer = rightGeometry.toArray.length.toByteArray()
        val typeBuffer = 1.toByteArray()

        typeBuffer ++ geometrySerializer.serialize(leftGeometry) ++
          sizeBuffer ++
          rightGeometry.toArray().flatMap(geometry => geometrySerializer.serialize(geometry.asInstanceOf[Geometry]))
      }
    )
  }
}
