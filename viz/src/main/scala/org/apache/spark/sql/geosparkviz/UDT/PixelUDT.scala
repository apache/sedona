/**
  * FILE: PixelUDT
  * PATH: org.apache.spark.sql.babylon.UDT.PixelUDT
  * Copyright (c) GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
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
