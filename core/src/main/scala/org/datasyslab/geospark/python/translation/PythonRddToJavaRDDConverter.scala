package org.datasyslab.geospark.python.translation

import com.vividsolutions.jts.geom.{Geometry, LineString, Point, Polygon}
import net.razorvine.pickle.Unpickler
import net.razorvine.pickle.objects.ClassDict
import org.apache.spark.api.java.JavaRDD

private[python] case class PythonRddToJavaRDDConverter(geometrySerializer: PythonGeometrySerializer){

  def deserializeToPointRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Point] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[Point]]
  }

  def deserializeToPolygonRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Polygon] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[Polygon]]
  }

  def deserializeToLineStringRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[LineString] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[LineString]]
  }

  private def translateToJava(pythonRDD: JavaRDD[Array[Byte]]): JavaRDD[Geometry] = {
    JavaRDD.fromRDD(pythonRDD.rdd.mapPartitions { iter =>
      iter.flatMap { row =>
        val unpickler = new Unpickler
        val obj = unpickler.loads(row)
        obj match {
          case _ => obj.asInstanceOf[java.util.ArrayList[_]].toArray.map(
            classDict => {
              val geoData = classDict.asInstanceOf[ClassDict]
              val geom = geoData.asInstanceOf[ClassDict].get("geom")
              val geometryInstance = geometrySerializer.deserialize(geom.asInstanceOf[Array[Byte]])
              geometryInstance
            }
          )
        }
      }
    }.toJavaRDD())
  }
}
