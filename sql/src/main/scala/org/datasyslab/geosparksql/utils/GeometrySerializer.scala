/*
 * FILE: GeometrySerializer.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geosparksql.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.util.ArrayData
import org.datasyslab.geospark.geometryObjects.GeometrySerde

// This is a wrapper of GeoSpark core kryo serializer
object GeometrySerializer {

  def serialize(geometry: Geometry): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val geometrySerde = new GeometrySerde()
    val output = new Output(out)
    geometrySerde.write(kryo, output, geometry)
    output.close()
    return out.toByteArray
  }

  def deserialize(values: ArrayData): Geometry = {
    val in = new ByteArrayInputStream(values.toByteArray())
    val kryo = new Kryo()
    val geometrySerde = new GeometrySerde()
    val input = new Input(in)
    val geometry = geometrySerde.read(kryo, input, classOf[Geometry])
    input.close()
    return geometry.asInstanceOf[Geometry]
  }
}
