/*
 * FILE: Predicates.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
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
 *
 */
package org.apache.spark.sql.geosparksql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.BooleanType
import org.datasyslab.geosparksql.utils.GeometrySerializer

/**
  * Test if leftGeometry full contains rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Contains(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def toString: String = s" **${ST_Contains.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = GeometrySerializer.deserialize(leftArray)

    val rightGeometry = GeometrySerializer.deserialize(rightArray)

    return leftGeometry.covers(rightGeometry)
  }

  override def dataType = BooleanType
}

/**
  * Test if leftGeometry full intersects rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Intersects(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Intersects.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = GeometrySerializer.deserialize(leftArray)

    val rightGeometry = GeometrySerializer.deserialize(rightArray)

    return leftGeometry.intersects(rightGeometry)
  }

  override def dataType = BooleanType
}

/**
  * Test if leftGeometry is full within rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Within(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Intersects.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = GeometrySerializer.deserialize(leftArray)

    val rightGeometry = GeometrySerializer.deserialize(rightArray)

    return leftGeometry.coveredBy(rightGeometry)
  }

  override def dataType = BooleanType
}