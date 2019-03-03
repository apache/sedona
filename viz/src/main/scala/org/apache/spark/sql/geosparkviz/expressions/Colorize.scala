/**
  * FILE: Colorize
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String
import org.beryx.awt.color.ColorFactory
import org.datasyslab.geosparkviz.extension.coloringRule.GenericColoringRule


case class ST_Colorize(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging{
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length <= 3)
    if (inputExpressions.length==3) {
      // This means the user wants to apply the same color to everywhere
      // Fetch the color from the third input string
      // supported color can be found at: https://github.com/beryx/awt-color-factory#example-usage
      var color = ColorFactory.valueOf(inputExpressions(2).eval(input).asInstanceOf[UTF8String].toString)
      return color.getRGB
    }
    var weight = 0.0
    try {
      weight = inputExpressions(0).eval(input).asInstanceOf[Double]
    }
    catch {
      case e:java.lang.ClassCastException => weight = inputExpressions(0).eval(input).asInstanceOf[Long]
    }
    var max = 0.0
    try {
      max = inputExpressions(1).eval(input).asInstanceOf[Double]
    }
    catch {
      case e:java.lang.ClassCastException => max = inputExpressions(1).eval(input).asInstanceOf[Long]
    }
    val normalizedWeight:Double = weight * 255.0 / max
    GenericColoringRule.EncodeToRGB(weight)
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions
}