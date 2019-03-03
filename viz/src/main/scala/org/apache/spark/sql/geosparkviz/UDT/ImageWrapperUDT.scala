/**
  * FILE: ImageWrapperUDT
  * PATH: org.apache.spark.sql.geosparksql.UDT.ImageWrapperUDT
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

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, UserDefinedType}
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper
import org.datasyslab.geosparkviz.core.Serde.ImageWrapperSerializer

private[sql] class ImageWrapperUDT extends UserDefinedType[ImageSerializableWrapper]{
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)

  override def serialize(obj: ImageSerializableWrapper): GenericArrayData =
  {

    val serializer = new ImageWrapperSerializer
    new GenericArrayData(serializer.writeImage(obj))
  }

  override def deserialize(datum: Any): ImageSerializableWrapper =
  {
    datum match
    {
      case values: ArrayData => {
        val serializer = new ImageWrapperSerializer
        serializer.readImage(values.toByteArray())
      }
    }
  }

  override def userClass: Class[ImageSerializableWrapper] = classOf[ImageSerializableWrapper]
}
