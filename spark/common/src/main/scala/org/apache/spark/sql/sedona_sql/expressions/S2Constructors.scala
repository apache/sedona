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

import org.apache.sedona.common.S2Geography.S2Geography
import org.apache.sedona.common.S2Constructors
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT}
import org.apache.spark.sql.sedona_sql.expressions.implicits.{S2GeographyEnhancer}
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Return a point from a string. The string must be plain string and each coordinate must be
 * separated by a delimiter.
 *
 * @param inputExpressions
 *   This function takes 2 parameters. The first parameter is the input geometry string, the
 *   second parameter is the delimiter. String format should be similar to CSV/TSV
 */
case class ST_S2PointFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.pointFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a polygon from a string. The string must be plain string and each coordinate must be
 * separated by a delimiter.
 *
 * @param inputExpressions
 */
case class ST_S2PolygonFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.polygonFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a line from a string. The string must be plain string and each coordinate must be
 * separated by a delimiter.
 *
 * @param inputExpressions
 */
case class ST_S2LineFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.lineFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a linestring from a string. The string must be plain string and each coordinate must be
 * separated by a delimiter.
 *
 * @param inputExpressions
 */
case class ST_S2LinestringFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.lineStringFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_S2GeogFromWKT(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.geogFromWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a OGC Extended WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string. The string format must be OGC Extended Well-Known text
 *   (EWKT) representation.
 */
case class ST_S2GeogFromEWKT(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.geogFromEWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKT string. Alias to ST_GeomFromWKT
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_S2GeometryFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.geogFromWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_S2GeogFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.geogFromWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKB string
 *
 * @param inputExpressions
 *   This function takes 1 parameter which is the utf-8 encoded geometry wkb string or the binary
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
        case geomString: UTF8String =>
          // Parse UTF-8 encoded wkb string
          S2Constructors
            .geogFromText(geomString.toString, FileDataSplitter.WKB)
            .toGenericArrayData
        case wkb: Array[Byte] =>
          // convert raw wkb byte array to geometry
          S2Constructors.geogFromWKB(wkb).toGenericArrayData
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
        case geomString: UTF8String =>
          // Parse UTF-8 encoded wkb string
          S2Constructors
            .geogFromText(geomString.toString, FileDataSplitter.WKB)
            .toGenericArrayData
        case wkb: Array[Byte] =>
          // convert raw wkb byte array to geometry
          S2Constructors.geogFromWKB(wkb).toGenericArrayData
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
        case geomString: UTF8String =>
          // Parse UTF-8 encoded WKB string
          val geom = S2Constructors.lineStringFromText(geomString.toString, "wkb")
          val kind = S2Geography.GeographyKind.fromKind(geom.getKind)
          if (kind == (S2Geography.GeographyKind.POLYLINE) || kind == S2Geography.GeographyKind.SINGLEPOLYLINE) {
            // (if (srid != -1) Functions.setSRID(geom, srid) else geom).toGenericArrayData
            geom.toGenericArrayData
          } else {
            null
          }

        case wkbArray: Array[Byte] =>
          // Convert raw WKB byte array to geometry
          S2Constructors.lineFromWKB(wkbArray, srid).toGenericArrayData

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
        case geomString: UTF8String =>
          // Parse UTF-8 encoded WKB string
          val geom = S2Constructors.lineStringFromText(geomString.toString, "wkb")
          val kind = S2Geography.GeographyKind.fromKind(geom.getKind)
          if (kind == S2Geography.GeographyKind.POLYLINE || kind == S2Geography.GeographyKind.SINGLEPOLYLINE) {
            // (if (srid != -1) Functions.setSRID(geom, srid) else geom).toGenericArrayData
            geom.toGenericArrayData
          } else {
            null
          }

        case wkbArray: Array[Byte] =>
          // Convert raw WKB byte array to geometry
          S2Constructors.lineFromWKB(wkbArray, srid).toGenericArrayData

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
        case geomString: UTF8String =>
          // Parse UTF-8 encoded WKB string
          val geom = S2Constructors.pointFromText(geomString.toString, "wkb")
          val kind = S2Geography.GeographyKind.fromKind(geom.getKind)
          if (kind == S2Geography.GeographyKind.POINT || kind == S2Geography.GeographyKind.SINGLEPOINT) {
            // (if (srid != -1) Functions.setSRID(geom, srid) else geom).toGenericArrayData
            geom.toGenericArrayData
          } else {
            null
          }
        case wkbArray: Array[Byte] =>
          // Convert raw WKB byte array to geometry
          S2Constructors.pointFromWKB(wkbArray, srid).toGenericArrayData
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

/**
 * Return a polygon given minX,minY,maxX,maxY and optional SRID
 *
 * @param inputExpressions
 */
//case class ST_S2MakeEnvelope(inputExpressions: Seq[Expression])
//    extends InferredExpression(
//      inferrableFunction5(S2Constructors.makeEnvelope),
//      inferrableFunction4(S2Constructors.makeEnvelope)) {
//
//  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
//    copy(inputExpressions = newChildren)
//  }
//}

/**
 * Return a polygon given minX,minY,maxX,maxY
 *
 * @param inputExpressions
 */
//case class ST_S2PolygonFromEnvelope(inputExpressions: Seq[Expression])
//    extends InferredExpression(S2Constructors.polygonFromEnvelope _) {
//
//  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
//    copy(inputExpressions = newChildren)
//  }
//}

trait S2UserDataGenerator {
  def generateUserData(
      minInputLength: Integer,
      inputExpressions: Seq[Expression],
      inputRow: InternalRow): String = {
    var userData =
      inputExpressions(minInputLength).eval(inputRow).asInstanceOf[UTF8String].toString

    for (i <- minInputLength + 1 until inputExpressions.length) {
      userData =
        userData + "\t" + inputExpressions(i).eval(inputRow).asInstanceOf[UTF8String].toString
    }
    userData
  }
}

/**
 * Return a Geometry from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_S2MPolyFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.mPolyFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_S2MLineFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.mLineFromText _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_S2MPointFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.mPointFromText _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_S2GeogCollFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(S2Constructors.geogCollFromText _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
