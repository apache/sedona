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

package org.apache.sedona.python.wrapper.utils

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

import org.locationtech.jts.geom.Geometry

object implicits {

  implicit class IntImplicit(value: Int) {
    def toByteArray(byteOrder: ByteOrder = ByteOrder.LITTLE_ENDIAN): Array[Byte] = {
      val typeBuffer = ByteBuffer
        .allocate(4)
        .order(byteOrder)
      typeBuffer.putInt(value)
      typeBuffer.array()
    }
  }

  implicit class DoubleImplicit(value: Double) {
    def toByteArray(byteOrder: ByteOrder = ByteOrder.LITTLE_ENDIAN): Array[Byte] = {
      val typeBuffer = ByteBuffer
        .allocate(8)
        .order(byteOrder)
      typeBuffer.putDouble(value)
      typeBuffer.array()
    }
  }

  implicit class GeometryEnhancer(geometry: Geometry) {
    private val EMPTY_STRING = ""

    def userDataToUtf8ByteArray: Array[Byte] = {
      geometry.getUserData match {
        case null => EMPTY_STRING.getBytes(StandardCharsets.UTF_8)
        case data: String => data.getBytes(StandardCharsets.UTF_8)
      }
    }
  }

}
