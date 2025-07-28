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

import org.apache.sedona.common.geometryObjects.Circle
import org.apache.sedona.common.utils.GeomUtils
import org.apache.sedona.python.wrapper.utils.implicits.{DoubleImplicit, GeometryEnhancer, IntImplicit}

case class CircleSerializer(geometry: Circle) {
  private val isCircle = Array(1.toByte)

  def serialize: Array[Byte] = {
    val wkbWriter = GeomUtils.createWKBWriter(2)
    val serializedGeom = wkbWriter.write(geometry.getCenterGeometry)
    val userDataBinary = geometry.userDataToUtf8ByteArray
    val userDataLengthArray = userDataBinary.length.toByteArray()
    val serializedGeomLength = serializedGeom.length.toByteArray()
    val radius = geometry.getRadius.toDouble
    isCircle ++ serializedGeomLength ++ userDataLengthArray ++ serializedGeom ++ userDataBinary ++ radius
      .toByteArray()
  }

}
