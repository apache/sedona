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

package org.apache.sedona.python.wrapper.translation

import java.io.{ByteArrayInputStream, DataInputStream}

import org.apache.sedona.core.geometryObjects.Circle
import org.apache.sedona.python.wrapper.SerializationException
import org.apache.spark.api.java.JavaRDD
import org.locationtech.jts.geom.Geometry

case class PythonRDDToJavaConverter(javaRDD: JavaRDD[Array[Byte]], geometrySerializer: PythonGeometrySerializer) {
  def translateToJava: JavaRDD[Geometry] = {
    javaRDD.rdd.map[Geometry](serializedGeoData => {
      val geoDataBytes = new ByteArrayInputStream(serializedGeoData)
      val geoDataInputBytes = new DataInputStream(geoDataBytes)
      val rddType = geoDataInputBytes.readInt()
      val isCircle = geoDataInputBytes.readByte().toInt
      if (isCircle == 1) {
        val radius = geoDataInputBytes.readDouble()
        val geometry = readGeometry(serializedGeoData, geoDataInputBytes, 20)
        new Circle(geometry, radius)
      }
      if (isCircle == 0) {
        readGeometry(serializedGeoData, geoDataInputBytes, 12)
      }
      else throw SerializationException()


    }

    )
  }

  private def readGeometry(serializedGeoData: Array[Byte], inputStream: DataInputStream, skipBytes: Int): Geometry = {
    val geomDataLength = java.lang.Integer.reverseBytes(inputStream.readInt())
    val userDataLength = java.lang.Integer.reverseBytes(inputStream.readInt())

    val toReadGeometry = serializedGeoData.slice(skipBytes, skipBytes + geomDataLength + 1)
    val geom = geometrySerializer.deserialize(toReadGeometry)

    val userData = serializedGeoData.slice(skipBytes + geomDataLength + 1, skipBytes + geomDataLength + 1 + userDataLength)
      .map(_.toChar).mkString

    geom.setUserData(userData)
    geom
  }
}

