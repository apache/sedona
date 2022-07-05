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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

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

// This is a compile time type shield for the types we are able to infer. Anything
// other than these types will cause a compilation error. This is the Scala
// 2 way of making a union type.
sealed class InferrableType[T: TypeTag]
object InferrableType {
  implicit val geometryInstance: InferrableType[Geometry] =
    new InferrableType[Geometry] {}
  implicit val doubleInstance: InferrableType[Double] =
    new InferrableType[Double] {}
}

object InferredTypes {
  def buildExtractor[T: TypeTag](expr: Expression): InternalRow => T = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      input: InternalRow => expr.toGeometry(input).asInstanceOf[T]
    } else {
      input: InternalRow => expr.eval(input).asInstanceOf[T]
    }
  }

  def inferSparkType[T: TypeTag]: DataType = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      GeometryUDT
    } else {
      DoubleType
    }
  }
}

abstract class InferredUnaryExpression[A1: InferrableType, R: InferrableType]
    (f: (A1) => R)
    (implicit val a1Tag: TypeTag[A1], implicit val rTag: TypeTag[R])
    extends Expression with ImplicitCastInputTypes with CodegenFallback with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]
  assert(inputExpressions.length == 1)

  override def children: Seq[Expression] = inputExpressions

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extract = buildExtractor[A1](inputExpressions(0))

  override def eval(input: InternalRow): Any = {
    val value = extract(input)
    if (value != null) {
      f(value)
    } else {
      null
    }
  }
}

abstract class InferredBinaryExpression[A1: InferrableType, A2: InferrableType, R: InferrableType]
    (f: (A1, A2) => R)
    (implicit val a1Tag: TypeTag[A1], implicit val a2Tag: TypeTag[A2], implicit val rTag: TypeTag[R])
    extends Expression with ImplicitCastInputTypes with CodegenFallback with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]
  assert(inputExpressions.length == 2)

  override def children: Seq[Expression] = inputExpressions

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extractLeft = buildExtractor[A1](inputExpressions(0))
  lazy val extractRight = buildExtractor[A2](inputExpressions(1))

  override def eval(input: InternalRow): Any = {
    val left = extractLeft(input)
    val right = extractRight(input)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null
    }
  }
}
