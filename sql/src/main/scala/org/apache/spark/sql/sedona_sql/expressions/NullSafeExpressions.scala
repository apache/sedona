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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.reflect.runtime.universe._

/**
  * Make expression foldable by constant folding optimizer. If all children
  * expressions are foldable, then the expression itself is foldable.
  */
trait FoldableExpression extends Expression {
  override def foldable: Boolean = children.forall(_.foldable)
}

abstract class UnaryGeometryExpression extends Expression with SerdeAware with ExpectsInputTypes {
  def inputExpressions: Seq[Expression]

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT)

  override def eval(input: InternalRow): Any = {
    val result = evalWithoutSerialization(input)
    serializeResult(result)
  }

  override def evalWithoutSerialization(input: InternalRow): Any ={
    val inputExpression = inputExpressions.head
    val geometry = inputExpression match {
      case expr: SerdeAware => expr.evalWithoutSerialization(input)
      case expr: Any => expr.toGeometry(input)
    }

    (geometry) match {
      case (geometry: Geometry) => nullSafeEval(geometry)
      case _ => null
    }
  }

  protected def serializeResult(result: Any): Any = {
    result match {
      case geometry: Geometry => geometry.toGenericArrayData
      case _ => result
    }
  }

  protected def nullSafeEval(geometry: Geometry): Any


}

abstract class BinaryGeometryExpression extends Expression with SerdeAware with ExpectsInputTypes {
  def inputExpressions: Seq[Expression]

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT, GeometryUDT)

  override def eval(input: InternalRow): Any = {
    val result = evalWithoutSerialization(input)
    serializeResult(result)
  }

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val leftExpression = inputExpressions(0)
    val leftGeometry = leftExpression match {
      case expr: SerdeAware => expr.evalWithoutSerialization(input)
      case _ => leftExpression.toGeometry(input)
    }

    val rightExpression = inputExpressions(1)
    val rightGeometry = rightExpression match {
      case expr: SerdeAware => expr.evalWithoutSerialization(input)
      case _ => rightExpression.toGeometry(input)
    }

    (leftGeometry, rightGeometry) match {
      case (leftGeometry: Geometry, rightGeometry: Geometry) => nullSafeEval(leftGeometry, rightGeometry)
      case _ => null
    }
  }

  protected def serializeResult(result: Any): Any = {
    result match {
      case geometry: Geometry => geometry.toGenericArrayData
      case _ => result
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
  implicit val geometryArrayInstance: InferrableType[Array[Geometry]] =
    new InferrableType[Array[Geometry]] {}
  implicit val javaDoubleInstance: InferrableType[java.lang.Double] =
    new InferrableType[java.lang.Double] {}
  implicit val javaIntegerInstance: InferrableType[java.lang.Integer] =
    new InferrableType[java.lang.Integer] {}
  implicit val doubleInstance: InferrableType[Double] =
    new InferrableType[Double] {}
  implicit val booleanInstance: InferrableType[Boolean] =
    new InferrableType[Boolean] {}
  implicit val intInstance: InferrableType[Int] =
    new InferrableType[Int] {}
  implicit val stringInstance: InferrableType[String] =
    new InferrableType[String] {}
  implicit val binaryInstance: InferrableType[Array[Byte]] =
    new InferrableType[Array[Byte]] {}
  implicit val longArrayInstance: InferrableType[Array[java.lang.Long]] =
    new InferrableType[Array[java.lang.Long]] {}
}

object InferredTypes {
  def buildExtractor[T: TypeTag](expr: Expression): InternalRow => T = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      input: InternalRow => expr.toGeometry(input).asInstanceOf[T]
    } else if (typeOf[T] =:= typeOf[Array[Geometry]]) {
      input: InternalRow => expr.toGeometryArray(input).asInstanceOf[T]
    } else if (typeOf[T] =:= typeOf[String]) {
      input: InternalRow => expr.asString(input).asInstanceOf[T]
    } else {
      input: InternalRow => expr.eval(input).asInstanceOf[T]
    }
  }

  def buildSerializer[T: TypeTag]: T => Any = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      output: T => if (output != null) {
        output.asInstanceOf[Geometry].toGenericArrayData
      } else {
        null
      }
    } else if (typeOf[T] =:= typeOf[String]) {
      output: T => if (output != null) {
        UTF8String.fromString(output.asInstanceOf[String])
      } else {
        null
      }
    } else if (typeOf[T] =:= typeOf[Array[java.lang.Long]]) {
      output: T =>
        if (output != null) {
          ArrayData.toArrayData(output)
        } else {
          null
        }
    } else if (typeOf[T] =:= typeOf[Array[Geometry]]) {
      output: T =>
        if (output != null) {
          ArrayData.toArrayData(output.asInstanceOf[Array[Geometry]].map(_.toGenericArrayData))
        } else {
          null
        }
    } else {
      output: T => output
    }
  }

  def inferSparkType[T: TypeTag]: DataType = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      GeometryUDT
    } else if (typeOf[T] =:= typeOf[Array[Geometry]]) {
      DataTypes.createArrayType(GeometryUDT)
    } else if (typeOf[T] =:= typeOf[java.lang.Double]) {
      DoubleType
    } else if (typeOf[T] =:= typeOf[java.lang.Integer]) {
      IntegerType
    } else if (typeOf[T] =:= typeOf[Double]) {
      DoubleType
    } else if (typeOf[T] =:= typeOf[Int]) {
      IntegerType
    } else if (typeOf[T] =:= typeOf[String]) {
      StringType
    } else if (typeOf[T] =:= typeOf[Array[Byte]]) {
      BinaryType
    } else if (typeOf[T] =:= typeOf[Array[java.lang.Long]]) {
      DataTypes.createArrayType(LongType)
    } else {
      BooleanType
    }
  }
}

/**
  * The implicit TypeTag's tell Scala to maintain generic type info at runtime. Normally type
  * erasure would remove any knowledge of what the passed in generic type is.
  */
abstract class InferredUnaryExpression[A1: InferrableType, R: InferrableType]
    (f: (A1) => R)
    (implicit val a1Tag: TypeTag[A1], implicit val rTag: TypeTag[R])
    extends Expression with ImplicitCastInputTypes with SerdeAware with CodegenFallback with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extract = buildExtractor[A1](inputExpressions(0))

  lazy val serialize = buildSerializer[R]

  override def eval(input: InternalRow): Any = serialize(evalWithoutSerialization(input).asInstanceOf[R])

  override def evalWithoutSerialization(input: InternalRow): Any = {
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
    extends Expression with ImplicitCastInputTypes with SerdeAware with CodegenFallback with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2])

  override def nullable: Boolean = true

  def allowRightNull: Boolean = false

  override def dataType = inferSparkType[R]

  lazy val extractLeft = buildExtractor[A1](inputExpressions(0))
  lazy val extractRight = buildExtractor[A2](inputExpressions(1))

  lazy val serialize = buildSerializer[R]

  override def eval(input: InternalRow): Any = serialize(evalWithoutSerialization(input).asInstanceOf[R])

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val left = extractLeft(input)
    val right = extractRight(input)
    if (left != null && (right != null || allowRightNull)) {
        f(left, right)
    } else {
      null
    }
  }
}

abstract class InferredTernaryExpression[A1: InferrableType, A2: InferrableType, A3: InferrableType, R: InferrableType]
(f: (A1, A2, A3) => R)
(implicit val a1Tag: TypeTag[A1], implicit val a2Tag: TypeTag[A2], implicit val a3Tag: TypeTag[A3], implicit val rTag: TypeTag[R])
  extends Expression with ImplicitCastInputTypes with SerdeAware with CodegenFallback with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2], inferSparkType[A3])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extractFirst = buildExtractor[A1](inputExpressions(0))
  lazy val extractSecond = buildExtractor[A2](inputExpressions(1))
  lazy val extractThird = buildExtractor[A3](inputExpressions(2))

  lazy val serialize = buildSerializer[R]

  override def eval(input: InternalRow): Any = serialize(evalWithoutSerialization(input).asInstanceOf[R])

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val first = extractFirst(input)
    val second = extractSecond(input)
    val third = extractThird(input)
    if (first != null && second != null && third != null) {
      f(first, second, third)
    } else {
      null
    }
  }
}

abstract class InferredQuarternaryExpression[A1: InferrableType, A2: InferrableType, A3: InferrableType, A4: InferrableType, R: InferrableType]
(f: (A1, A2, A3, A4) => R)
(implicit val a1Tag: TypeTag[A1], implicit val a2Tag: TypeTag[A2], implicit val a3Tag: TypeTag[A3], implicit val a4Tag: TypeTag[A4], implicit val rTag: TypeTag[R])
  extends Expression with ImplicitCastInputTypes with CodegenFallback with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2], inferSparkType[A3], inferSparkType[A4])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extractFirst = buildExtractor[A1](inputExpressions(0))
  lazy val extractSecond = buildExtractor[A2](inputExpressions(1))
  lazy val extractThird = buildExtractor[A3](inputExpressions(2))
  lazy val extractForth = buildExtractor[A4](inputExpressions(3))

  lazy val serialize = buildSerializer[R]

  override def eval(input: InternalRow): Any = {
    val first = extractFirst(input)
    val second = extractSecond(input)
    val third = extractThird(input)
    val forth = extractForth(input)
    if (first != null && second != null && third != null && forth != null) {
      serialize(f(first, second, third, forth))
    } else {
      null
    }
  }
}
