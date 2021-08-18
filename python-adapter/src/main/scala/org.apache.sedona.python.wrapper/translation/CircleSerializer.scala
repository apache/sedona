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

import org.apache.sedona.core.geometryObjects.Circle
import org.apache.sedona.python.wrapper.utils.implicits._
import org.apache.spark.sql.sedona_sql.userSerializerType


case class CircleSerializer(geometry: Circle) {
  private val isCircle = Array(1.toByte)
  private val serializer = GeomSerdeUtil.getSerializer(userSerializerType)
  val serializationTypeBytes: Array[Byte] = GeomSerdeUtil.getSerializationBytes(userSerializerType)

  def serialize: Array[Byte] = {
    val serializedGeometry = serializer.serialize(geom = geometry)
    val userDataBinary = geometry.userDataToUtf8ByteArray
    val userDataLengthArray = userDataBinary.length.toByteArray()
    val radius = geometry.getRadius.toDouble
    serializationTypeBytes ++ isCircle ++ userDataLengthArray ++ serializedGeometry ++ userDataBinary ++ radius.toByteArray()
  }

}
