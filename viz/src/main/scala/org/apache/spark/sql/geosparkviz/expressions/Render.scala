/**
  * FILE: Render
  * Copyright (c) 2015 - 2019 GeoSpark Development Team
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
package org.apache.spark.sql.geosparkviz.expressions

import java.awt.image.BufferedImage

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.geosparkviz.UDT.{ImageWrapperUDT, PixelUDT}
import org.apache.spark.sql.types._
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper
import org.datasyslab.geosparkviz.extension.coloringRule.GenericColoringRule
import org.datasyslab.geosparkviz.utils.Pixel

case class ST_Render_v2() extends UserDefinedAggregateFunction with Logging{
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = new StructType()
    .add("Pixel", new PixelUDT).add("Weight", DoubleType).add("Max", DoubleType)
  override def bufferSchema: StructType = new StructType()
    .add("WeightArray", ArrayType(DoubleType, containsNull = true))
    .add("ResolutionX", IntegerType)
    .add("ResolutionY", IntegerType)
  override def toString: String = s" **${ST_Render_v2.getClass.getName}**  "
  override def dataType: DataType = new ImageWrapperUDT

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
  {
    // No need to initialize
    var array = new Array[Int](1)
    array(0)=999
    buffer(0)=array
    buffer(1)=0
    buffer(2)=0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
  {
    var colorArray = buffer.getAs[Seq[Double]](0)
    val inputPixel = input.getAs[Pixel](0)
    val reversedY = inputPixel.getResolutionY - inputPixel.getY -1
    var weight = input.getDouble(1)
    val max = input.getDouble(2)
    var currentColorArray: Array[Double] = null
    if(colorArray.length==1)
    {
      // We got an empty image array which just left the initialize function
      currentColorArray = new Array[Double](inputPixel.getResolutionX*inputPixel.getResolutionY)
    }
    else
    {
      currentColorArray = colorArray.toArray
    }

    weight = (weight - 0) * 255 / (max - 0)
    if(inputPixel.getX<0 || inputPixel.getX>=inputPixel.getResolutionX || inputPixel.getY<0 || inputPixel.getY>=inputPixel.getResolutionY )
    {
      log.warn(s"$inputPixel")
    }
    currentColorArray(inputPixel.getX+reversedY*inputPixel.getResolutionX) = weight//GenericColoringRule.EncodeToRGB(weight)
    //var image = new BufferedImage(inputPixel.getResolutionX, inputPixel.getResolutionY)
    //image.setData().setRGB(inputPixel.getX, inputPixel.getY, GenericColoringRule.EncodeToRGB(weight))
    buffer(0) = currentColorArray
    buffer(1) = inputPixel.getResolutionX
    buffer(2) = inputPixel.getResolutionY
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
  {
    val leftColorArray = buffer1.getAs[Seq[Double]](0)
    val rightColorArray = buffer2.getAs[Seq[Double]](0)
    if (leftColorArray.length==1)
    {
      buffer1(0) = buffer2(0)
      buffer1(1) = buffer2(1)
      buffer1(2) = buffer2(2)
      return
    }
    else if (rightColorArray.length==1)
    {
      return
    }
    val w = buffer1.getAs[Int](1) // This can be rightColorArray. The left and right are expected to have the same resolutions
    val h = buffer1.getAs[Int](2)
    var combinedColorArray = new Array[Double](w*h)
    for (i <- 0 to (w*h-1))
    {
      // We expect that for each i, only one of leftColorArray and RightColorArray has non-zero value.
      combinedColorArray(i) = leftColorArray(i)+rightColorArray(i)
    }
    buffer1(0) = combinedColorArray
    buffer1(1) = w
    buffer1(2) = h
  }

  override def evaluate(buffer: Row): Any =
  {
    val colorArray = buffer.getAs[Seq[Double]](0)
    val w = buffer.getAs[Int](1)
    val h = buffer.getAs[Int](2)
    var bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
    for (j <- 0 to h-1)
    {
      for(i <- 0 to w-1)
      {
        bufferedImage.setRGB(i, j, GenericColoringRule.EncodeToRGB(colorArray(i+j*w)))
      }
    }
    return new ImageSerializableWrapper(bufferedImage)
  }
}

case class ST_Render() extends UserDefinedAggregateFunction with Logging{
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = new StructType()
    .add("Pixel", new PixelUDT).add("Weight", DoubleType).add("Max", DoubleType)
  override def bufferSchema: StructType = new StructType()
    .add("WeightArray", new ImageWrapperUDT)
//    .add("ResolutionX", IntegerType)
//    .add("ResolutionY", IntegerType)
  override def toString: String = s" **${ST_Render.getClass.getName}**  "
  override def dataType: DataType = new ImageWrapperUDT

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
  {
    // No need to initialize
//    var array = new Array[Int](1)
//    array(0)=999
//    buffer(0)=array
//    buffer(1)=0
//    buffer(2)=0
    var image = new BufferedImage(1,1,BufferedImage.TYPE_INT_ARGB)
    buffer(0) = new ImageSerializableWrapper(image)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
  {
    var image = buffer.getAs[ImageSerializableWrapper](0).getImage
    val inputPixel = input.getAs[Pixel](0)
    if (image.getWidth == 1){
      // This is a new image array
      image = new BufferedImage(inputPixel.getResolutionX,inputPixel.getResolutionY,BufferedImage.TYPE_INT_ARGB)
    }

    val reversedY = inputPixel.getResolutionY - inputPixel.getY -1
    var weight = input.getDouble(1)
    val max = input.getDouble(2)

    weight = (weight - 0) * 255 / (max - 0)
    if(inputPixel.getX<0 || inputPixel.getX>=inputPixel.getResolutionX || inputPixel.getY<0 || inputPixel.getY>=inputPixel.getResolutionY )
    {
      log.warn(s"$inputPixel")
    }
    image.setRGB(inputPixel.getX, reversedY, GenericColoringRule.EncodeToRGB(weight))
    buffer(0) = new ImageSerializableWrapper(image)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
  {
    val leftImage = buffer1.getAs[ImageSerializableWrapper](0)
    val rightImage = buffer2.getAs[ImageSerializableWrapper](0)
    val w = leftImage.getImage.getWidth
    val h = rightImage.getImage.getHeight
    var combinedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
    var graphics = combinedImage.getGraphics
    graphics.drawImage(leftImage.getImage, 0, 0, null)
    graphics.drawImage(rightImage.getImage, 0, 0, null)
    buffer1(0) = new ImageSerializableWrapper(combinedImage)
  }

  override def evaluate(buffer: Row): Any =
  {
    buffer.getAs[ImageSerializableWrapper](0)
  }
}