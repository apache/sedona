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

import org.apache.sedona.core.enums.SerializerType
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeSerde
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKBWriter


abstract class PythonGeometrySerde{
  def serialize(geom: Geometry): Array[Byte]
}

class PythonShapeSerde extends PythonGeometrySerde {
  override def serialize(geom: Geometry): Array[Byte] =
    ShapeSerde.serialize(geom)
}

class PythonWkbSerde extends PythonGeometrySerde {
  private val wkbWriter = new WKBWriter(2, 2, true)
  override def serialize(geom: Geometry): Array[Byte] = {
    val serializedGeom = wkbWriter.write(geom)
    GeomSerdeUtil.intToByteArray(serializedGeom.length) ++ serializedGeom
  }
}

object GeomSerdeUtil {

  def intToByteArray(value: Int): Array[Byte] = {
    val bb = java.nio.ByteBuffer.allocate(4)
    bb.putInt(value)

    bb.array()
  }

  def getSerializationBytes(userSerializerType: SerializerType): Array[Byte] = {
    userSerializerType match {
      case SerializerType.SHAPE => intToByteArray(SerializerType.SHAPE.getId)
      case SerializerType.WKB => intToByteArray(SerializerType.WKB.getId)
    }
  }
  def getSerializer(userSerializerType: SerializerType): PythonGeometrySerde =
    userSerializerType match {
      case SerializerType.SHAPE => new PythonShapeSerde()
      case SerializerType.WKB => new PythonWkbSerde()
    }
}
