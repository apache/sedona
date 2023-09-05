/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.python.wrapper.adapters

import net.razorvine.pickle.Unpickler
import net.razorvine.pickle.objects.ClassDict
import org.apache.spark.api.java.JavaRDD
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}

object PythonRddToJavaRDDAdapter extends GeomSerializer {

  def deserializeToPointRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Point] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[Point]]
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

  def deserializeToPolygonRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[Polygon] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[Polygon]]
  }

  def deserializeToLineStringRawRDD(javaRDD: JavaRDD[Array[Byte]]): JavaRDD[LineString] = {
    translateToJava(javaRDD).asInstanceOf[JavaRDD[LineString]]
  }
}
