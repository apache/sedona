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
package org.apache.sedona.sql.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.locationtech.jts.index.SpatialIndex

// This is a wrapper of Sedona core kryo serializer
object IndexSerializer {
  def serialize(index: SpatialIndex): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val output = new Output(out)
    kryo.writeObject(output, index)
    output.close()
    return out.toByteArray
  }

  def deserialize(values: ArrayData): SpatialIndex = {
    val in = new ByteArrayInputStream(values.toByteArray())
    val kryo = new Kryo()
    val input = new Input(in)
    val spatialIndex = kryo.readObject(input, classOf[SpatialIndex])
    input.close()
    return spatialIndex.asInstanceOf[SpatialIndex]
  }

}
