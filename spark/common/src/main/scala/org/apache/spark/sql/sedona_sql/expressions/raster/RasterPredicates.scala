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

import org.apache.sedona.common.raster.RasterPredicates
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.FoldableExpression
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}
import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.jts.geom.Geometry

abstract class RS_Predicate
    extends Expression
    with FoldableExpression
    with ExpectsInputTypes
    with NullIntolerant {
  def inputExpressions: Seq[Expression]

  override def toString: String = s" **${this.getClass.getName}**  "

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[AbstractDataType] = if (inputExpressions.length != 2) {
    throw new IllegalArgumentException(
      s"Expected exactly 2 inputs, but got ${inputExpressions.length}")
  } else {
    val leftType = inputExpressions.head.dataType
    val rightType = inputExpressions(1).dataType
    (leftType, rightType) match {
      case (_: RasterUDT, _: GeometryUDT) => Seq(RasterUDT, GeometryUDT)
      case (_: GeometryUDT, _: RasterUDT) => Seq(GeometryUDT, RasterUDT)
      case (_: RasterUDT, _: RasterUDT) => Seq(RasterUDT, RasterUDT)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported input types: $leftType, $rightType")
    }
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  lazy val evaluator: (Array[Byte], Array[Byte]) => Boolean = inputTypes match {
    case Seq(RasterUDT, GeometryUDT) =>
      (leftArray: Array[Byte], rightArray: Array[Byte]) =>
        val leftRaster = RasterSerializer.deserialize(leftArray)
        val rightGeometry = GeometrySerializer.deserialize(rightArray)
        evalRasterGeom(leftRaster, rightGeometry)
    case Seq(GeometryUDT, RasterUDT) =>
      (leftArray: Array[Byte], rightArray: Array[Byte]) =>
        val leftGeometry = GeometrySerializer.deserialize(leftArray)
        val rightRaster = RasterSerializer.deserialize(rightArray)
        evalGeomRaster(leftGeometry, rightRaster)
    case Seq(RasterUDT, RasterUDT) =>
      (leftArray: Array[Byte], rightArray: Array[Byte]) =>
        val leftRaster = RasterSerializer.deserialize(leftArray)
        val rightRaster = RasterSerializer.deserialize(rightArray)
        evalRasters(leftRaster, rightRaster)
    case _ => throw new IllegalArgumentException(s"Unsupported input types: $inputTypes")
  }

  override final def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions.head.eval(inputRow).asInstanceOf[Array[Byte]]
    if (leftArray == null) {
      null
    } else {
      val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
      if (rightArray == null) {
        null
      } else {
        evaluator(leftArray, rightArray)
      }
    }
  }

  def evalRasterGeom(leftRaster: GridCoverage2D, rightGeometry: Geometry): Boolean

  def evalGeomRaster(leftGeometry: Geometry, rightRaster: GridCoverage2D): Boolean

  def evalRasters(leftRaster: GridCoverage2D, rightRaster: GridCoverage2D): Boolean
}

case class RS_Intersects(inputExpressions: Seq[Expression])
    extends RS_Predicate
    with CodegenFallback {

  override def evalRasterGeom(leftRaster: GridCoverage2D, rightGeometry: Geometry): Boolean = {
    RasterPredicates.rsIntersects(leftRaster, rightGeometry)
  }

  override def evalGeomRaster(leftGeometry: Geometry, rightRaster: GridCoverage2D): Boolean =
    RasterPredicates.rsIntersects(rightRaster, leftGeometry)

  override def evalRasters(leftRaster: GridCoverage2D, rightRaster: GridCoverage2D): Boolean =
    RasterPredicates.rsIntersects(leftRaster, rightRaster)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Contains(inputExpressions: Seq[Expression])
    extends RS_Predicate
    with CodegenFallback {

  override def evalRasterGeom(leftRaster: GridCoverage2D, rightGeometry: Geometry): Boolean = {
    RasterPredicates.rsContains(leftRaster, rightGeometry)
  }

  override def evalGeomRaster(leftGeometry: Geometry, rightRaster: GridCoverage2D): Boolean =
    RasterPredicates.rsWithin(rightRaster, leftGeometry)

  override def evalRasters(leftRaster: GridCoverage2D, rightRaster: GridCoverage2D): Boolean =
    RasterPredicates.rsContains(leftRaster, rightRaster)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Within(inputExpressions: Seq[Expression])
    extends RS_Predicate
    with CodegenFallback {

  override def evalRasterGeom(leftRaster: GridCoverage2D, rightGeometry: Geometry): Boolean = {
    RasterPredicates.rsWithin(leftRaster, rightGeometry)
  }

  override def evalGeomRaster(leftGeometry: Geometry, rightRaster: GridCoverage2D): Boolean =
    RasterPredicates.rsContains(rightRaster, leftGeometry)

  override def evalRasters(leftRaster: GridCoverage2D, rightRaster: GridCoverage2D): Boolean =
    RasterPredicates.rsContains(rightRaster, leftRaster)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
