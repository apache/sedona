package org.datasyslab.geospark.python.adapters

import org.locationtech.jts.geom.{Geometry}
import org.datasyslab.geospark.jts.geom.{LineString, Point, Polygon}
import net.razorvine.pickle.Unpickler
import net.razorvine.pickle.objects.ClassDict
import org.apache.spark.api.java.JavaRDD

object PythonRddToJavaRDDAdapter extends GeomSerializer{

  def deserializeToPointRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[org.locationtech.jts.geom.Point] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[org.locationtech.jts.geom.Point]]
  }

  def deserializeToPolygonRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[org.locationtech.jts.geom.Polygon] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[org.locationtech.jts.geom.Polygon]]
  }

  def deserializeToLineStringRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[org.locationtech.jts.geom.LineString] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[org.locationtech.jts.geom.LineString]]
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
              val userData = geoData.asInstanceOf[ClassDict].get("userData")
              val geometryInstance = geometrySerializer.deserialize(geom.asInstanceOf[Array[Byte]])
              geometryInstance.setUserData(userData)
              geometryInstance
            }
          )
        }
      }
    }.toJavaRDD())
  }
}
