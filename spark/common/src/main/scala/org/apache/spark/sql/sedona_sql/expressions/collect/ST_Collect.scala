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
package org.apache.spark.sql.sedona_sql.expressions.collect

import org.apache.sedona.common.Functions
import org.apache.sedona.common.S2Geography.Geography
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.expressions.{InferredExpression, SerdeAware}
import org.apache.spark.sql.types.{ArrayType, _}
import org.locationtech.jts.geom.Geometry

private[apache] case class ST_Collect(inputExpressions: Seq[Expression])
    extends Expression
    with SerdeAware
    with CodegenFallback {
  assert(inputExpressions.length >= 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val result = evalWithoutSerialization(input)
    if (result == null) {
      null
    } else if (returnsGeography) {
      result.asInstanceOf[Geography].toGenericArrayData
    } else {
      result.asInstanceOf[Geometry].toGenericArrayData
    }
  }

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val firstElement = inputExpressions.head

    if (returnsGeography) {
      val geographies = firstElement.dataType match {
        case ArrayType(_, _) =>
          Option(firstElement.eval(input).asInstanceOf[ArrayData])
            .map(data =>
              (0 until data.numElements())
                .filterNot(data.isNullAt)
                .map(element => data.getBinary(element).toGeography))
            .getOrElse(Seq.empty)
        case _ =>
          inputExpressions.map(_.toGeography(input)).filter(_ != null)
      }
      try {
        org.apache.sedona.common.geography.Functions.createMultiGeography(geographies.toArray)
      } catch {
        case e: Exception =>
          InferredExpression.throwExpressionInferenceException(
            getClass.getSimpleName,
            Seq(geographies),
            e)
      }
    } else {
      val geometries = firstElement.dataType match {
        case ArrayType(_, _) =>
          Option(firstElement.eval(input).asInstanceOf[ArrayData])
            .map(data =>
              (0 until data.numElements())
                .filterNot(data.isNullAt)
                .map(element => data.getBinary(element).toGeometry))
            .getOrElse(Seq.empty)
        case _ =>
          inputExpressions.map(_.toGeometry(input)).filter(_ != null)
      }
      try {
        Functions.createMultiGeometry(geometries.toArray)
      } catch {
        case e: Exception =>
          InferredExpression.throwExpressionInferenceException(
            getClass.getSimpleName,
            Seq(geometries),
            e)
      }
    }
  }

  private def elementType(dataType: DataType): DataType = dataType match {
    case ArrayType(element, _) => element
    case other => other
  }

  private def isGeometry(dataType: DataType): Boolean =
    dataType.isInstanceOf[GeometryUDT]

  private def isGeography(dataType: DataType): Boolean =
    dataType.isInstanceOf[GeographyUDT]

  private def returnsGeography: Boolean =
    inputExpressions.exists(expression => isGeography(elementType(expression.dataType)))

  override def checkInputDataTypes(): TypeCheckResult = {
    val hasArray = inputExpressions.exists(_.dataType.isInstanceOf[ArrayType])
    if (hasArray && inputExpressions.length != 1) {
      return TypeCheckFailure("ST_Collect accepts either one array or one or more scalar values")
    }

    val unsupported = inputExpressions.map(_.dataType).filterNot { dataType =>
      val valueType = elementType(dataType)
      valueType == NullType || isGeometry(valueType) || isGeography(valueType)
    }
    if (unsupported.nonEmpty) {
      return TypeCheckFailure(
        s"ST_Collect expects Geometry or Geography values, but found ${unsupported.mkString(", ")}")
    }

    val hasGeometry =
      inputExpressions.exists(expression => isGeometry(elementType(expression.dataType)))
    val hasGeography =
      inputExpressions.exists(expression => isGeography(elementType(expression.dataType)))
    if (hasGeometry && hasGeography) {
      TypeCheckFailure("ST_Collect does not accept mixed Geometry and Geography inputs")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = {
    if (returnsGeography) GeographyUDT() else GeometryUDT()
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

}
