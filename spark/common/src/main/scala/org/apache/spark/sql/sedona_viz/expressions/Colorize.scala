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

import org.apache.sedona.viz.`extension`.coloringRule.GenericColoringRule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String
import org.beryx.awt.color.ColorFactory


case class ST_Colorize(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging {
  assert(inputExpressions.length <= 3)
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    if (inputExpressions.length == 3) {
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
      case e: java.lang.ClassCastException => weight = inputExpressions(0).eval(input).asInstanceOf[Long]
    }
    var max = 0.0
    try {
      max = inputExpressions(1).eval(input).asInstanceOf[Double]
    }
    catch {
      case e: java.lang.ClassCastException => max = inputExpressions(1).eval(input).asInstanceOf[Long]
    }
    val normalizedWeight: Double = weight * 255.0 / max
    GenericColoringRule.EncodeToRGB(normalizedWeight)
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
