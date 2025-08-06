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

import org.apache.sedona.common.GeogConstructors
import org.apache.sedona.common.S2Geography.S2Geography.GeographyKind
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.common.geometryObjects.Geography
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeographyEnhancer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Return a Geography from a WKB string
 *
 * @param inputExpressions
 *   This function takes 1 parameter which is the utf-8 encoded geography wkb string or the binary
 *   wkb array.
 */
case class ST_S2GeogFromWKB(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with UserDataGenerator {
  // This is an expression which takes one input expressions
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val arg = inputExpressions.head.eval(inputRow)
    try {
      arg match {
        case geogString: UTF8String =>
          // Parse UTF-8 encoded wkb string
          GeogConstructors
            .geogFromText(geogString.toString, FileDataSplitter.WKB)
            .toGenericArrayData
        case wkb: Array[Byte] =>
          // convert raw wkb byte array to geogetry
          GeogConstructors.geogFromWKB(wkb).toGenericArrayData
        case null => null
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(getClass.getSimpleName, Seq(arg), e)
    }
  }

  override def dataType: DataType = GeographyUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_S2GeogFromEWKB(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with UserDataGenerator {
  // This is an expression which takes one input expressions
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val arg = inputExpressions.head.eval(inputRow)
    try {
      arg match {
        case geogString: UTF8String =>
          // Parse UTF-8 encoded wkb string
          GeogConstructors
            .geogFromText(geogString.toString, FileDataSplitter.WKB)
            .toGenericArrayData
        case wkb: Array[Byte] =>
          GeogConstructors.geogFromWKB(wkb).toGenericArrayData
        case null => null
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(getClass.getSimpleName, Seq(arg), e)
    }
  }

  override def dataType: DataType = GeographyUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_S2LineFromWKB(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with UserDataGenerator {

  // Validate the number of input expressions (1 or 2)
  assert(inputExpressions.length >= 1 && inputExpressions.length <= 2)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val wkb = inputExpressions.head.eval(inputRow)
    val srid =
      if (inputExpressions.length > 1) inputExpressions(1).eval(inputRow).asInstanceOf[Int]
      else -1
    try {
      wkb match {
        case geogString: UTF8String =>
          // Parse UTF-8 encoded WKB string
          val geog = GeogConstructors.lineStringFromText(geogString.toString, "wkb")
          val kind = GeographyKind.fromKind(geog.getKind)
          if (kind == GeographyKind.POLYLINE || kind == GeographyKind.SINGLEPOLYLINE) {
            geog.toGenericArrayData
          } else {
            null
          }
        case wkbArray: Array[Byte] =>
          GeogConstructors.lineFromWKB(wkbArray, srid).toGenericArrayData

        case _ => null
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(wkb, srid),
          e)
    }
  }

  override def dataType: DataType = GeographyUDT

  override def inputTypes: Seq[AbstractDataType] =
    if (inputExpressions.length == 1) Seq(TypeCollection(StringType, BinaryType))
    else Seq(TypeCollection(StringType, BinaryType), IntegerType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ST_S2LineFromWKB = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_S2LinestringFromWKB(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with UserDataGenerator {

  // Validate the number of input expressions (1 or 2)
  assert(inputExpressions.length >= 1 && inputExpressions.length <= 2)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val wkb = inputExpressions.head.eval(inputRow)
    val srid =
      if (inputExpressions.length > 1) inputExpressions(1).eval(inputRow).asInstanceOf[Int]
      else -1

    try {
      wkb match {
        case geogString: UTF8String =>
          // Parse UTF-8 encoded WKB string
          val geog = GeogConstructors.lineStringFromText(geogString.toString, "wkb")
          val kind = GeographyKind.fromKind(geog.getKind)
          if (kind == GeographyKind.POLYLINE || kind == GeographyKind.SINGLEPOLYLINE) {
            geog.toGenericArrayData
          } else {
            null
          }

        case wkbArray: Array[Byte] =>
          GeogConstructors.lineFromWKB(wkbArray, srid).toGenericArrayData

        case _ => null
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(wkb, srid),
          e)
    }
  }

  override def dataType: DataType = GeographyUDT

  override def inputTypes: Seq[AbstractDataType] =
    if (inputExpressions.length == 1) Seq(TypeCollection(StringType, BinaryType))
    else Seq(TypeCollection(StringType, BinaryType), IntegerType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_S2PointFromWKB(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with UserDataGenerator {

  // Validate the number of input expressions (1 or 2)
  assert(inputExpressions.length >= 1 && inputExpressions.length <= 2)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val wkb = inputExpressions.head.eval(inputRow)
    val srid =
      if (inputExpressions.length > 1) inputExpressions(1).eval(inputRow).asInstanceOf[Int]
      else -1

    try {
      wkb match {
        case geogString: UTF8String =>
          // Parse UTF-8 encoded WKB string
          val geog: Geography = GeogConstructors.pointFromText(geogString.toString, "wkb")
          val kind = GeographyKind.fromKind(geog.getKind)
          if (kind == GeographyKind.POINT || kind == GeographyKind.SINGLEPOINT) {
            geog.toGenericArrayData
          } else {
            null
          }
        case wkbArray: Array[Byte] =>
          val geog: Geography = GeogConstructors.pointFromWKB(wkbArray)
          geog.toGenericArrayData
        case _ => null
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(wkb, srid),
          e)
    }
  }

  override def dataType: DataType = GeographyUDT

  override def inputTypes: Seq[AbstractDataType] =
    if (inputExpressions.length == 1) Seq(TypeCollection(StringType, BinaryType))
    else Seq(TypeCollection(StringType, BinaryType), IntegerType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ST_S2PointFromWKB = {
    copy(inputExpressions = newChildren)
  }
}
