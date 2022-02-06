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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._

abstract class UnaryGeometryExpression extends Expression {
  def inputExpressions: Seq[Expression]

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    (geometry) match {
      case (geometry: Geometry) => nullSafeEval(geometry)
      case _ => null
    }
  }

  protected def nullSafeEval(geometry: Geometry): Any
}

abstract class BinaryGeometryExpression extends Expression {
  def inputExpressions: Seq[Expression]

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val leftGeometry = inputExpressions(0).toGeometry(input)
    val rightGeometry = inputExpressions(1).toGeometry(input)
    (leftGeometry, rightGeometry) match {
      case (leftGeometry: Geometry, rightGeometry: Geometry) => nullSafeEval(leftGeometry, rightGeometry)
      case _ => null
    }
  }

  protected def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any
}