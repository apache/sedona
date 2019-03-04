/*
 * FILE: PixelUDT.scala
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
package org.apache.spark.sql.geosparkviz.UDT

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, UserDefinedType}
import org.datasyslab.geosparkviz.core.Serde.PixelSerializer
import org.datasyslab.geosparkviz.utils.Pixel

private[sql] class PixelUDT extends UserDefinedType[Pixel]{
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)

  override def serialize(obj: Pixel): GenericArrayData =
  {
    val out = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val pixelSerializer = new PixelSerializer()
    val output = new Output(out)
    pixelSerializer.write(kryo,output,obj)
    output.close()
    new GenericArrayData(out.toByteArray)
  }

  override def deserialize(datum: Any): Pixel =
  {
    datum match
    {
      case values: ArrayData => {
        val in = new ByteArrayInputStream(values.toByteArray())
        val kryo = new Kryo()
        val pixelSerializer = new PixelSerializer()
        val input = new Input(in)
        val pixel = pixelSerializer.read(kryo, input,classOf[Pixel])
        input.close()
        return pixel
      }
    }
  }

  override def userClass: Class[Pixel] = classOf[Pixel]
}
