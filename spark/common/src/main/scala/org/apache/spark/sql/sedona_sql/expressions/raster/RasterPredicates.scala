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
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.{FoldableExpression, NullIntolerantShim}
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, DoubleType}
import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.jts.geom.Geometry

abstract class RS_Predicate
    extends Expression
    with FoldableExpression
    with ExpectsInputTypes
    with NullIntolerantShim {
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
      case (_: RasterUDT, _: GeometryUDT) => Seq(RasterUDT(), GeometryUDT())
      case (_: GeometryUDT, _: RasterUDT) => Seq(GeometryUDT(), RasterUDT())
      case (_: RasterUDT, _: RasterUDT) => Seq(RasterUDT(), RasterUDT())
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

private[apache] case class RS_Intersects(inputExpressions: Seq[Expression])
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

private[apache] case class RS_Contains(inputExpressions: Seq[Expression])
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

private[apache] case class RS_Within(inputExpressions: Seq[Expression])
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

/**
 * Distance predicate for rasters: `RS_DWithin(left, right, distance)`. `left` and `right` can
 * each be a raster or a geometry (at least one must be a raster). Returns true when the minimum
 * geodesic distance between the two shapes is `<= distance` meters — `RasterPredicates.rsDWithin`
 * delegates to the Geography `dWithin`, which uses S2's `ClosestEdgeQuery`. Overlapping or
 * touching shapes therefore yield distance `0` (consistent with [[RS_Intersects]]), and
 * `distance` is always interpreted as meters regardless of the input CRS. This expression is
 * recognised by [[org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector]] as a
 * distance-join key, so a coarse R-tree filter over WGS84 envelopes expanded by `distance` meters
 * via the Haversine polar-radius approximation (the same expansion `ST_DistanceSphere` uses)
 * prunes candidates before the per-row check.
 */
private[apache] case class RS_DWithin(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with ImplicitCastInputTypes
    with NullIntolerantShim
    with CodegenFallback {

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = children.exists(_.nullable)

  override def dataType: DataType = BooleanType

  override def toString: String = s" **${getClass.getName}**  "

  override def inputTypes: Seq[AbstractDataType] = {
    if (inputExpressions.length != 3) {
      throw new IllegalArgumentException(
        s"RS_DWithin requires exactly 3 inputs, but got ${inputExpressions.length}")
    }
    val leftType = inputExpressions.head.dataType
    val rightType = inputExpressions(1).dataType
    (leftType, rightType) match {
      case (_: RasterUDT, _: GeometryUDT) => Seq(RasterUDT(), GeometryUDT(), DoubleType)
      case (_: GeometryUDT, _: RasterUDT) => Seq(GeometryUDT(), RasterUDT(), DoubleType)
      case (_: RasterUDT, _: RasterUDT) => Seq(RasterUDT(), RasterUDT(), DoubleType)
      case _ =>
        throw new IllegalArgumentException(
          s"RS_DWithin requires at least one raster input; got: $leftType, $rightType")
    }
  }

  override def eval(inputRow: InternalRow): Any = {
    val leftValue = inputExpressions.head.eval(inputRow)
    if (leftValue == null) return null
    val rightValue = inputExpressions(1).eval(inputRow)
    if (rightValue == null) return null
    val distanceValue = inputExpressions(2).eval(inputRow)
    if (distanceValue == null) return null

    val leftBytes = leftValue.asInstanceOf[Array[Byte]]
    val rightBytes = rightValue.asInstanceOf[Array[Byte]]
    val distance = distanceValue.asInstanceOf[Double]
    val leftType = inputExpressions.head.dataType
    val rightType = inputExpressions(1).dataType
    (leftType, rightType) match {
      case (_: RasterUDT, _: GeometryUDT) =>
        RasterPredicates.rsDWithin(
          RasterSerializer.deserialize(leftBytes),
          GeometrySerializer.deserialize(rightBytes),
          distance)
      case (_: GeometryUDT, _: RasterUDT) =>
        RasterPredicates.rsDWithin(
          RasterSerializer.deserialize(rightBytes),
          GeometrySerializer.deserialize(leftBytes),
          distance)
      case (_: RasterUDT, _: RasterUDT) =>
        RasterPredicates.rsDWithin(
          RasterSerializer.deserialize(leftBytes),
          RasterSerializer.deserialize(rightBytes),
          distance)
      case _ =>
        throw new IllegalArgumentException(
          s"RS_DWithin requires at least one raster input; got: $leftType, $rightType")
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
