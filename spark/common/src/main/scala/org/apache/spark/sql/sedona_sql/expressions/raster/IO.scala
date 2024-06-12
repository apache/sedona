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
package org.apache.spark.sql.sedona_sql.expressions.raster

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.expressions.UserDataGeneratator
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.util.Base64
import javax.imageio.ImageIO

case class RS_Array(inputExpressions: Seq[Expression])
    extends Expression
    with ImplicitCastInputTypes
    with CodegenFallback
    with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val len = inputExpressions(0).eval(inputRow).asInstanceOf[Int]
    val num = inputExpressions(1).eval(inputRow).asInstanceOf[Double]
    val result = createarray(len, num)
    new GenericArrayData(result)
  }

  // Generate an empty band for the given spectral band in ageotiff image
  private def createarray(len: Int, num: Double): Array[Double] = {

    val result = new Array[Double](len)
    for (i <- 0 until len) {
      result(i) = num
    }
    result
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, DoubleType)

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
