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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, DataType, DataTypes, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

/**
 * Custom exception to include the input row and the original exception message.
 */
class InferredExpressionException(message: String, cause: Throwable)
    extends Exception(s"$message, cause: " + cause.getMessage, cause)

/**
 * This is the base class for wrapping Java/Scala functions as a catalyst expression in Spark SQL.
 * @param fSeq
 *   The functions to be wrapped. Subclasses can simply pass a function to this constructor, and
 *   the function will be converted to [[InferrableFunction]] by [[InferrableFunctionConverter]]
 *   automatically.
 */
abstract class InferredExpression(fSeq: InferrableFunction*)
    extends Expression
    with ImplicitCastInputTypes
    with SerdeAware
    with CodegenFallback
    with FoldableExpression
    with Serializable {

  def inputExpressions: Seq[Expression]

  lazy val f: InferrableFunction = fSeq match {
    // If there is only one function, simply use it and let org.apache.sedona.sql.UDF.Catalog handle default arguments.
    case Seq(f) => f
    // If there are multiple overloaded functions, find the one with the same number of arguments as the input
    // expressions. Please note that the Catalog won't be able to handle default arguments in this case. We'll
    // move default argument handling from Catalog to this class in the future.
    case _ =>
      fSeq.find(f => f.sparkInputTypes.size == inputExpressions.size) match {
        case Some(f) => f
        case None =>
          throw new IllegalArgumentException(
            s"No overloaded function ${getClass.getName} has ${inputExpressions.size} arguments")
      }
  }

  override def children: Seq[Expression] = inputExpressions
  override def toString: String = s" **${getClass.getName}**  "
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = f.sparkInputTypes
  override def dataType: DataType = f.sparkReturnType

  private lazy val argExtractors: Array[InternalRow => Any] = buildExtractors(inputExpressions)
  private lazy val evaluator: InternalRow => Any = f.evaluatorBuilder(argExtractors)

  // Remember input args to generate error messages when exceptions occur. The input arguments are
  // helpful for troubleshooting the cause of errors.
  private val inputArgs: ArrayBuffer[AnyRef] = ArrayBuffer.empty[AnyRef]

  private def buildExtractors(expressions: Seq[Expression]): Array[InternalRow => Any] = {
    f.argExtractorBuilders
      .zipAll(expressions, null, null)
      .flatMap {
        case (null, _) => None
        case (builder, expr) =>
          val extractor = builder(expr)
          Some((input: InternalRow) => {
            val arg = extractor(input)
            inputArgs += arg.asInstanceOf[AnyRef]
            arg
          })
      }
      .toArray
  }

  override def eval(input: InternalRow): Any = {
    try {
      f.serializer(evaluator(input))
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          inputArgs.toSeq,
          e)
    } finally {
      inputArgs.clear()
    }
  }

  override def evalWithoutSerialization(input: InternalRow): Any = {
    try {
      evaluator(input)
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          inputArgs.toSeq,
          e)
    } finally {
      inputArgs.clear()
    }
  }
}

object InferredExpression {
  def throwExpressionInferenceException(
      name: String,
      inputArgs: Seq[Any],
      e: Exception): Nothing = {
    if (e.isInstanceOf[InferredExpressionException]) {
      throw e
    } else {
      val inputsAsStrings = inputArgs.map { arg =>
        val argStr = if (arg != null) arg.toString else "null"
        StringUtils.abbreviate(argStr, 5000)
      }
      val inputsString = inputsAsStrings.mkString(", ")
      throw new InferredExpressionException(
        s"Exception occurred while evaluating expression $name - inputs: [$inputsString]",
        e)
    }
  }
}

// This is a compile time type shield for the types we are able to infer. Anything
// other than these types will cause a compilation error. This is the Scala
// 2 way of making a union type.
class InferrableType[T: TypeTag]
object InferrableType {
  implicit val geometryInstance: InferrableType[Geometry] =
    new InferrableType[Geometry] {}
  implicit val geometryArrayInstance: InferrableType[Array[Geometry]] =
    new InferrableType[Array[Geometry]] {}
  implicit val javaDoubleInstance: InferrableType[java.lang.Double] =
    new InferrableType[java.lang.Double] {}
  implicit val javaIntegerInstance: InferrableType[java.lang.Integer] =
    new InferrableType[java.lang.Integer] {}
  implicit val javaLongInstance: InferrableType[java.lang.Long] =
    new InferrableType[java.lang.Long] {}
  implicit val doubleInstance: InferrableType[Double] =
    new InferrableType[Double] {}
  implicit val booleanInstance: InferrableType[Boolean] =
    new InferrableType[Boolean] {}
  implicit val booleanOptInstance: InferrableType[Option[Boolean]] =
    new InferrableType[Option[Boolean]] {}
  implicit val intInstance: InferrableType[Int] =
    new InferrableType[Int] {}
  implicit val longInstance: InferrableType[Long] =
    new InferrableType[Long] {}
  implicit val stringInstance: InferrableType[String] =
    new InferrableType[String] {}
  implicit val binaryInstance: InferrableType[Array[Byte]] =
    new InferrableType[Array[Byte]] {}
  implicit val intArrayInstance: InferrableType[Array[Int]] =
    new InferrableType[Array[Int]] {}
  implicit val javaIntArrayInstance: InferrableType[Array[java.lang.Integer]] =
    new InferrableType[Array[java.lang.Integer]]
  implicit val longArrayInstance: InferrableType[Array[Long]] =
    new InferrableType[Array[Long]] {}
  implicit val javaLongArrayInstance: InferrableType[Array[java.lang.Long]] =
    new InferrableType[Array[java.lang.Long]] {}
  implicit val doubleArrayInstance: InferrableType[Array[Double]] =
    new InferrableType[Array[Double]] {}
  implicit val javaDoubleListInstance: InferrableType[java.util.List[java.lang.Double]] =
    new InferrableType[java.util.List[java.lang.Double]] {}
  implicit val javaGeomListInstance: InferrableType[java.util.List[Geometry]] =
    new InferrableType[java.util.List[Geometry]] {}
}

object InferredTypes {
  def buildArgumentExtractor(t: Type): Expression => InternalRow => Any = {
    if (t =:= typeOf[Geometry]) { expr => input =>
      expr.toGeometry(input)
    } else if (t =:= typeOf[Array[Geometry]]) { expr => input =>
      expr.toGeometryArray(input)
    } else if (InferredRasterExpression.isRasterType(t)) {
      InferredRasterExpression.rasterExtractor
    } else if (t =:= typeOf[Array[Double]]) { expr => input =>
      expr.eval(input).asInstanceOf[ArrayData].toDoubleArray()
    } else if (t =:= typeOf[String]) { expr => input =>
      expr.asString(input)
    } else if (t =:= typeOf[Array[Long]]) { expr => input =>
      expr.eval(input).asInstanceOf[ArrayData].toLongArray()
    } else if (t =:= typeOf[Array[Int]]) { expr => input =>
      expr.eval(input).asInstanceOf[ArrayData] match {
        case null => null
        case arrayData: ArrayData => arrayData.toIntArray()
      }
    } else if (t =:= typeOf[java.util.List[Geometry]]) { expr => input =>
      expr.toGeometryList(input)
    } else if (t =:= typeOf[java.util.List[java.lang.Double]]) { expr => input =>
      expr.toDoubleList(input)
    } else { expr => input =>
      expr.eval(input)
    }
  }
  def buildSerializer(t: Type): Any => Any = {
    if (t =:= typeOf[Geometry]) { output =>
      if (output != null) {
        output.asInstanceOf[Geometry].toGenericArrayData
      } else {
        null
      }
    } else if (InferredRasterExpression.isRasterType(t)) {
      InferredRasterExpression.rasterSerializer
    } else if (t =:= typeOf[String]) { output =>
      if (output != null) {
        UTF8String.fromString(output.asInstanceOf[String])
      } else {
        null
      }
    } else if (t =:= typeOf[Array[java.lang.Long]] || t =:= typeOf[Array[Long]] ||
      t =:= typeOf[Array[Double]]) { output =>
      if (output != null) {
        ArrayData.toArrayData(output)
      } else {
        null
      }
    } else if (t =:= typeOf[java.util.List[java.lang.Double]]) { output =>
      if (output != null) {
        ArrayData.toArrayData(
          output.asInstanceOf[java.util.List[java.lang.Double]].map(elem => elem))
      } else {
        null
      }
    } else if (t =:= typeOf[Array[Geometry]] || t =:= typeOf[java.util.List[Geometry]]) {
      output =>
        if (output != null) {
          ArrayData.toArrayData(output.asInstanceOf[Array[Geometry]].map(_.toGenericArrayData))
        } else {
          null
        }
    } else if (InferredRasterExpression.isRasterArrayType(t)) {
      InferredRasterExpression.rasterArraySerializer
    } else if (t =:= typeOf[Option[Boolean]]) { output =>
      if (output != null) {
        output.asInstanceOf[Option[Boolean]].orNull
      } else {
        null
      }
    } else { output =>
      output
    }
  }

  def inferSparkType(t: Type): DataType = {
    if (t =:= typeOf[Geometry]) {
      GeometryUDT
    } else if (t =:= typeOf[Array[Geometry]] || t =:= typeOf[java.util.List[Geometry]]) {
      DataTypes.createArrayType(GeometryUDT)
    } else if (InferredRasterExpression.isRasterType(t)) {
      InferredRasterExpression.rasterUDT
    } else if (InferredRasterExpression.isRasterArrayType(t)) {
      InferredRasterExpression.rasterUDTArray
    } else if (t =:= typeOf[java.lang.Double]) {
      DoubleType
    } else if (t =:= typeOf[java.lang.Integer]) {
      IntegerType
    } else if (t =:= typeOf[Double]) {
      DoubleType
    } else if (t =:= typeOf[Int]) {
      IntegerType
    } else if (t =:= typeOf[Long] || t =:= typeOf[java.lang.Long]) {
      LongType
    } else if (t =:= typeOf[String]) {
      StringType
    } else if (t =:= typeOf[Array[Byte]]) {
      BinaryType
    } else if (t =:= typeOf[Array[Int]] || t =:= typeOf[Array[java.lang.Integer]]) {
      DataTypes.createArrayType(IntegerType)
    } else if (t =:= typeOf[Array[Long]] || t =:= typeOf[Array[java.lang.Long]]) {
      DataTypes.createArrayType(LongType)
    } else if (t =:= typeOf[Array[Double]] || t =:= typeOf[java.util.List[java.lang.Double]]) {
      DataTypes.createArrayType(DoubleType)
    } else if (t =:= typeOf[Option[Boolean]]) {
      BooleanType
    } else if (t =:= typeOf[Boolean]) {
      BooleanType
    } else {
      throw new IllegalArgumentException(s"Cannot infer spark type for $t")
    }
  }
}

case class InferrableFunction(
    sparkInputTypes: Seq[AbstractDataType],
    sparkReturnType: DataType,
    serializer: Any => Any,
    argExtractorBuilders: Seq[Expression => InternalRow => Any],
    evaluatorBuilder: Array[InternalRow => Any] => InternalRow => Any)

object InferrableFunction {

  /**
   * Infer input types and return type from a type tag, and construct builder for argument
   * extractors.
   * @param typeTag
   *   Type tag of the function.
   * @param evaluatorBuilder
   *   Builder for the evaluator.
   * @return
   *   InferrableFunction.
   */
  def apply(
      typeTag: TypeTag[_],
      evaluatorBuilder: Array[InternalRow => Any] => InternalRow => Any): InferrableFunction = {
    val argTypes = typeTag.tpe.typeArgs.init
    val returnType = typeTag.tpe.typeArgs.last
    val sparkInputTypes: Seq[AbstractDataType] = argTypes.map(InferredTypes.inferSparkType)
    val sparkReturnType: DataType = InferredTypes.inferSparkType(returnType)
    val serializer = InferredTypes.buildSerializer(returnType)
    val argExtractorBuilders = argTypes.map(InferredTypes.buildArgumentExtractor)
    InferrableFunction(
      sparkInputTypes,
      sparkReturnType,
      serializer,
      argExtractorBuilders,
      evaluatorBuilder)
  }

  /**
   * A variant of binary inferred expression which allows the second argument to be null.
   * @param f
   *   Function to be wrapped as a catalyst expression.
   * @param typeTag
   *   Type tag of the function.
   * @tparam R
   *   Return type of the function.
   * @tparam A1
   *   Type of the first argument.
   * @tparam A2
   *   Type of the second argument.
   * @return
   *   InferrableFunction.
   */
  def allowRightNull[R, A1, A2](f: (A1, A2) => R)(implicit
      typeTag: TypeTag[(A1, A2) => R]): InferrableFunction = {
    apply(
      typeTag,
      extractors => {
        val func = f.asInstanceOf[(Any, Any) => Any]
        val extractor1 = extractors(0)
        val extractor2 = extractors(1)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          if (arg1 != null) {
            func(arg1, arg2)
          } else {
            null
          }
        }
      })
  }

}
