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
package org.apache.spark.sql.sedona_viz.expressions

import java.awt.image.BufferedImage // scalastyle:ignore illegal.imports

import org.apache.sedona.viz.core.ImageSerializableWrapper
import org.apache.sedona.viz.utils.Pixel
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.sedona_viz.UDT.{ImageWrapperUDT, PixelUDT}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructType}

case class ST_Render() extends UserDefinedAggregateFunction with Logging {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = new StructType()
    .add("Pixel", new PixelUDT).add("Color", IntegerType).add("Zoom Level", IntegerType, nullable = true)

  override def bufferSchema: StructType = new StructType()
    .add("ColorArray", ArrayType(IntegerType, containsNull = false))
    .add("XArray", ArrayType(IntegerType, containsNull = false))
    .add("YArray", ArrayType(IntegerType, containsNull = false))
    .add("ResolutionX", IntegerType)
    .add("ResolutionY", IntegerType)

  override def toString: String = s" **${ST_Render.getClass.getName}**  "

  override def dataType: DataType = new ImageWrapperUDT

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // No need to initialize
    var array = new Array[Int](0)
    buffer(0) = array
    buffer(1) = array
    buffer(2) = array
    buffer(3) = 0
    buffer(4) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var colorArray = buffer.getAs[Seq[Int]](0)
    var xArray = buffer.getAs[Seq[Int]](1)
    var yArray = buffer.getAs[Seq[Int]](2)
    val inputPixel = input.getAs[Pixel](0)
    var color = input.getInt(1)
    var numPartOnAxis = 1.0
    // If the user gives a zoom level, then we cut the images to tiles
    if (input.length == 3) numPartOnAxis = Math.pow(2, input.getInt(2))
    val reversedY = inputPixel.getResolutionY - inputPixel.getY - 1
    val truncatedResX = inputPixel.getResolutionX / numPartOnAxis
    val truncatedResY = inputPixel.getResolutionY / numPartOnAxis
    colorArray = colorArray :+ color
    xArray = xArray :+ inputPixel.getX.intValue() % truncatedResX.intValue()
    yArray = yArray :+ reversedY.intValue() % truncatedResY.intValue()

    assert(truncatedResX>0)
    assert(truncatedResY>0)
    buffer(0) = colorArray
    buffer(1) = xArray
    buffer(2) = yArray
    buffer(3) = truncatedResX.intValue()
    buffer(4) = truncatedResY.intValue()
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftColorArray = buffer1.getAs[Seq[Int]](0)
    var leftXArray = buffer1.getAs[Seq[Int]](1)
    var leftYArray = buffer1.getAs[Seq[Int]](2)
    var leftResX = buffer1.getInt(3)
    var leftResY = buffer1.getInt(4)
    val rightColorArray = buffer2.getAs[Seq[Int]](0)
    var rightXArray = buffer2.getAs[Seq[Int]](1)
    var rightYArray = buffer2.getAs[Seq[Int]](2)
    var rightResX = buffer2.getInt(3)
    var rightResY = buffer2.getInt(4)
    buffer1(0) = leftColorArray ++ rightColorArray
    buffer1(1) = leftXArray ++ rightXArray
    buffer1(2) = leftYArray ++ rightYArray
    assert(Math.max(leftResX, rightResX) > 0)
    assert(Math.max(leftResY, rightResY) > 0)
    buffer1(3) = Math.max(leftResX, rightResX)
    buffer1(4) = Math.max(leftResY, rightResY)
  }

  override def evaluate(buffer: Row): Any = {
    val colorArray = buffer.getAs[Seq[Int]](0)
    val xArray = buffer.getAs[Seq[Int]](1)
    val yArray = buffer.getAs[Seq[Int]](2)
    val w = buffer.getAs[Int](3)
    val h = buffer.getAs[Int](4)
    var bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
    assert(xArray.length == yArray.length)
    assert(xArray.length == colorArray.length)
    for (i <- xArray.indices) {
      bufferedImage.setRGB(xArray(i), yArray(i), colorArray(i))
    }
    new ImageSerializableWrapper(bufferedImage)
  }
}
