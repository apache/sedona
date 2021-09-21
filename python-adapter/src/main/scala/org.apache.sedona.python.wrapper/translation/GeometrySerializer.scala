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

import org.apache.sedona.python.wrapper.utils.implicits._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKBWriter


case class GeometrySerializer(geometry: Geometry) {

  private val notCircle = Array(0.toByte)

  def serialize: Array[Byte] = {
    val wkbWriter = new WKBWriter(2, 2)
    val serializedGeom = wkbWriter.write(geometry)
    val userDataBinary = geometry.userDataToUtf8ByteArray
    val userDataLengthArray = userDataBinary.length.toByteArray()
    val serializedGeomLength = serializedGeom.length.toByteArray()
    notCircle ++ serializedGeomLength ++ userDataLengthArray ++ serializedGeom ++ userDataBinary
  }
}
