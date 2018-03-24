/*
 * FILE: RangeJoinExec.scala
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
package org.apache.spark.sql.geosparksql.strategy.join

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

/**
  * ST_Contains(left, right) - left contains right
  * or
  * ST_Intersects(left, right) - left and right intersect
  *
  * @param left       left side of the join
  * @param right      right side of the join
  * @param leftShape  expression for the first argument of ST_Contains or ST_Intersects
  * @param rightShape expression for the second argument of ST_Contains or ST_Intersects
  * @param intersects boolean indicating whether spatial relationship is 'intersects' (true)
  *                   or 'contains' (false)
  */
case class RangeJoinExec(left: SparkPlan,
                         right: SparkPlan,
                         leftShape: Expression,
                         rightShape: Expression,
                         intersects: Boolean,
                         extraCondition: Option[Expression] = None)
  extends BinaryExecNode
    with TraitJoinQueryExec
    with Logging {}
