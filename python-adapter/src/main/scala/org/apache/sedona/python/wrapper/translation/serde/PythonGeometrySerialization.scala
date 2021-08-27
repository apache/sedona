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

import org.apache.sedona.core.enums.SerializerType
import org.apache.spark.sql.sedona_sql.userSerializerType
import org.locationtech.jts.geom.Geometry

object PythonGeometrySerialization {
  def serialize(geometry: Geometry): Array[Byte] = {
    userSerializerType match {
      case SerializerType.SHAPE => ShapePythonGeometrySerde.serialize(geometry)
      case SerializerType.WKB => WkbPythonGeometrySerde.serialize(geometry)
      case _ => ShapePythonGeometrySerde.serialize(geometry)
    }
  }

  def deserialize(array: Array[Byte]): Geometry = {
    userSerializerType match {
      case SerializerType.SHAPE => ShapePythonGeometrySerde.deserialize(array)
      case SerializerType.WKB => WkbPythonGeometrySerde.deserialize(array)
      case _ => ShapePythonGeometrySerde.deserialize(array)
    }
  }
}
