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

import org.apache.sedona.common.raster.{RasterConstructors, RasterConstructorsForTesting}
import org.apache.sedona.sql.utils.RasterSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Generator, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.{RasterEnhancer, RasterInputExpressionEnhancer}
import org.apache.spark.sql.types.{ArrayType, BooleanType, Decimal, IntegerType, NullType, StructType}

case class RS_FromArcInfoAsciiGrid(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterConstructors.fromArcInfoAsciiGrid _) {
  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_AsRaster(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction5(RasterConstructors.asRaster),
      inferrableFunction3(RasterConstructors.asRaster),
      inferrableFunction4(RasterConstructors.asRaster),
      inferrableFunction6(RasterConstructors.asRaster)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_FromGeoTiff(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterConstructors.fromGeoTiff _) {

  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_MakeEmptyRaster(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction6(RasterConstructors.makeEmptyRaster),
      inferrableFunction7(RasterConstructors.makeEmptyRaster),
      inferrableFunction10(RasterConstructors.makeEmptyRaster),
      inferrableFunction11(RasterConstructors.makeEmptyRaster)) {

  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_MakeRaster(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction3(RasterConstructors.makeNonEmptyRaster)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_MakeRasterForTesting(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterConstructorsForTesting.makeRasterForTesting _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Tile(inputExpressions: Seq[Expression])
    extends InferredExpression(
      nullTolerantInferrableFunction3(RasterConstructors.rsTile),
      nullTolerantInferrableFunction4(RasterConstructors.rsTile),
      nullTolerantInferrableFunction5(RasterConstructors.rsTile),
      nullTolerantInferrableFunction6(RasterConstructors.rsTile)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_TileExplode(children: Seq[Expression]) extends Generator with CodegenFallback {
  private val arguments = RS_TileExplode.arguments(children)

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val raster = arguments.rasterExpr.toRaster(input)
    try {
      val bandIndices = arguments.bandIndicesExpr.eval(input).asInstanceOf[ArrayData] match {
        case null => null
        case value: Any => value.toIntArray
      }
      val tileWidth = arguments.tileWidthExpr.eval(input).asInstanceOf[Int]
      val tileHeight = arguments.tileHeightExpr.eval(input).asInstanceOf[Int]
      val padWithNoDataValue = arguments.padWithNoDataExpr.eval(input).asInstanceOf[Boolean]
      val noDataValue = arguments.noDataValExpr.eval(input) match {
        case null => Double.NaN
        case value: Integer => value.toDouble
        case value: Decimal => value.toDouble
        case value: Float => value.toDouble
        case value: Double => value
        case value: Any =>
          throw new IllegalArgumentException(
            "Unsupported class for noDataValue: " + value.getClass)
      }
      val tiles = RasterConstructors.generateTiles(
        raster,
        bandIndices,
        tileWidth,
        tileHeight,
        padWithNoDataValue,
        noDataValue)
      tiles.map { tile =>
        val gridCoverage2D = tile.getCoverage
        val row = InternalRow(tile.getTileX, tile.getTileY, gridCoverage2D.serialize)
        gridCoverage2D.dispose(true)
        row
      }
    } finally {
      raster.dispose(true)
    }
  }

  override def elementSchema: StructType = {
    new StructType()
      .add("x", IntegerType, nullable = false)
      .add("y", IntegerType, nullable = false)
      .add("tile", RasterUDT, nullable = false)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(children = newChildren)
  }
}

object RS_TileExplode {
  case class Arguments(
      rasterExpr: Expression,
      bandIndicesExpr: Expression,
      tileWidthExpr: Expression,
      tileHeightExpr: Expression,
      padWithNoDataExpr: Expression,
      noDataValExpr: Expression)

  def arguments(inputExpressions: Seq[Expression]): Arguments = {
    // RS_Tile/RS_TileExplode has 3 forms:
    // 1: raster rast, int[] nband, integer width, integer height, boolean padwithnodata=FALSE, double nodataval=NULL
    // 2: raster rast, integer nband, integer width, integer height, boolean padwithnodata=FALSE, double nodataval=NULL
    // 3: raster rast, integer width, integer height, boolean padwithnodata=FALSE, double nodataval=NULL
    // This method detects which form is used and returns the arguments in the form of Arguments case class.
    if (inputExpressions.length < 3) {
      throw new IllegalArgumentException("RS_Tile requires at least 3 arguments.")
    }
    val rasterExpr = inputExpressions.head
    val arg1Type = inputExpressions(1).dataType
    if (arg1Type.isInstanceOf[ArrayType] || arg1Type == NullType) {
      // raster rast, int[] nband, integer width, integer height, boolean padwithnodata=FALSE, double nodataval=NULL
      Arguments(
        rasterExpr,
        inputExpressions(1),
        inputExpressions(2),
        inputExpressions(3),
        if (inputExpressions.length > 4) inputExpressions(4) else Literal(false),
        if (inputExpressions.length > 5) inputExpressions(5) else Literal(null))
    } else if (inputExpressions.length < 4 || inputExpressions(3).dataType == BooleanType) {
      // raster rast, integer width, integer height, boolean padwithnodata=FALSE, double nodataval=NULL
      Arguments(
        rasterExpr,
        Literal(null),
        inputExpressions(1),
        inputExpressions(2),
        if (inputExpressions.length > 3) inputExpressions(3) else Literal(false),
        if (inputExpressions.length > 4) inputExpressions(4) else Literal(null))
    } else {
      // raster rast, integer nband, integer width, integer height, boolean padwithnodata=FALSE, double nodataval=NULL
      Arguments(
        rasterExpr,
        CreateArray(Seq(inputExpressions(1))),
        inputExpressions(2),
        inputExpressions(3),
        if (inputExpressions.length > 4) inputExpressions(4) else Literal(false),
        if (inputExpressions.length > 5) inputExpressions(5) else Literal(null))
    }
  }
}

case class RS_FromNetCDF(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterConstructors.fromNetCDF),
      inferrableFunction4(RasterConstructors.fromNetCDF)) {

  override def foldable: Boolean = false
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_NetCDFInfo(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterConstructors.getRecordInfo _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
