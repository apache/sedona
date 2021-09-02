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


package org.apache.sedona.python.wrapper.translation.serde

import java.nio.ByteOrder

import org.apache.sedona.python.wrapper.utils.implicits.IntImplicit
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBReader, WKBWriter}

object WkbPythonGeometrySerde extends PythonGeometrySerde {

  def serialize(geometry: Geometry): Array[Byte] = {
    val writer: WKBWriter = new WKBWriter(2, 2)
    val data = writer.write(geometry)
    Array(1.toByte) ++ data.length.toByteArray(ByteOrder.BIG_ENDIAN) ++ data
  }

  def deserialize(array: Array[Byte]): Geometry = {
    val wkbReader = new WKBReader()
    wkbReader.read(array.slice(5, array.length))
  }

}
