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
import org.apache.sedona.common.geometryObjects.Circle
import org.apache.sedona.python.wrapper.SerializationException
import org.apache.spark.api.java.JavaRDD
import org.locationtech.jts.geom.Geometry

import java.nio.{ByteBuffer, ByteOrder}

case class PythonRDDToJavaConverter(javaRDD: JavaRDD[Array[Byte]], geometrySerializer: PythonGeometrySerializer) {
  def translateToJava: JavaRDD[Geometry] = {
    javaRDD.rdd.map[Geometry](serializedGeoData => {
      val geoDataBytes = new ByteArrayInputStream(serializedGeoData)
      val geoDataInputBytes = new DataInputStream(geoDataBytes)
      val rddType = geoDataInputBytes.readInt()
      readGeometry(serializedGeoData, geoDataInputBytes)
    })
  }

  private def readGeometry(serializedGeoData: Array[Byte], inputStream: DataInputStream): Geometry = {
    val isCircle = inputStream.readByte().toInt
    val geomDataLength = java.lang.Integer.reverseBytes(inputStream.readInt())
    val userDataLength = java.lang.Integer.reverseBytes(inputStream.readInt())
    val skipBytes = 13  // 4 bytes for rddType, 1 byte for isCircle, 4 bytes for geomDataLength, 4 bytes for userDataLength

    // Circle will be handled specially in the next step. We don't let geometrySerializer.deserialize handle Circle
    // for us since this serialization format is a little bit different from the one used in PythonGeometrySerializer.
    val geom = geometrySerializer.deserialize(0, serializedGeoData, skipBytes)
    val userData = serializedGeoData.slice(skipBytes + geomDataLength, skipBytes + geomDataLength + userDataLength)
      .map(_.toChar).mkString

    val finalGeom = if (isCircle == 1) {
      val radiusOffset = skipBytes + geomDataLength + userDataLength
      val radiusBytes = serializedGeoData.slice(radiusOffset, radiusOffset + 8)
      val radius = ByteBuffer.wrap(radiusBytes).order(ByteOrder.LITTLE_ENDIAN).getDouble
      new Circle(geom, radius)
    } else if (isCircle == 0) {
      geom
    } else {
      throw SerializationException("Can not deserialize object")
    }
    finalGeom.setUserData(userData)
    finalGeom
  }
}

