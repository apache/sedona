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

import org.apache.sedona.common.raster.{PixelFunctions}
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, DoubleType, IntegerType, StructType}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

case class RS_Value(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(PixelFunctions.value),
      inferrableFunction3(PixelFunctions.value),
      inferrableFunction4(PixelFunctions.value)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_PixelAsPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(PixelFunctions.getPixelAsPoint _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_PixelAsPoints(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(
    new StructType()
      .add("geom", GeometryUDT)
      .add("value", DoubleType)
      .add("x", IntegerType)
      .add("y", IntegerType))

  override def eval(input: InternalRow): Any = {
    val rasterGeom = inputExpressions(0).toRaster(input)
    val band = inputExpressions(1).eval(input).asInstanceOf[Int]

    if (rasterGeom == null) {
      null
    } else {
      val pixelRecords = PixelFunctions.getPixelAsPoints(rasterGeom, band)
      val rows = pixelRecords.map { pixelRecord =>
        val serializedGeom = GeometrySerializer.serialize(pixelRecord.geom)
        val rowArray =
          Array[Any](serializedGeom, pixelRecord.value, pixelRecord.colX, pixelRecord.rowY)
        InternalRow.fromSeq(rowArray)
      }
      new GenericArrayData(rows.toArray)
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): RS_PixelAsPoints = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, IntegerType)
}

case class RS_PixelAsPolygon(inputExpressions: Seq[Expression])
    extends InferredExpression(PixelFunctions.getPixelAsPolygon _) {
  protected def withNewChildrenInternal(newChilren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChilren)
  }
}

case class RS_PixelAsPolygons(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(
    new StructType()
      .add("geom", GeometryUDT)
      .add("value", DoubleType)
      .add("x", IntegerType)
      .add("y", IntegerType))

  override def eval(input: InternalRow): Any = {
    val rasterGeom = inputExpressions(0).toRaster(input)
    val band = inputExpressions(1).eval(input).asInstanceOf[Int]

    if (rasterGeom == null) {
      null
    } else {
      val pixelRecords = PixelFunctions.getPixelAsPolygons(rasterGeom, band)
      val rows = pixelRecords.map { pixelRecord =>
        val serializedGeom = GeometrySerializer.serialize(pixelRecord.geom)
        val rowArray =
          Array[Any](serializedGeom, pixelRecord.value, pixelRecord.colX, pixelRecord.rowY)
        InternalRow.fromSeq(rowArray)
      }
      new GenericArrayData(rows.toArray)
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RS_PixelAsPolygons = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, IntegerType)
}

case class RS_PixelAsCentroid(inputExpressions: Seq[Expression])
    extends InferredExpression(PixelFunctions.getPixelAsCentroid _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_PixelAsCentroids(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(
    new StructType()
      .add("geom", GeometryUDT)
      .add("value", DoubleType)
      .add("x", IntegerType)
      .add("y", IntegerType))

  override def eval(input: InternalRow): Any = {
    val rasterGeom = inputExpressions(0).toRaster(input)
    val band = inputExpressions(1).eval(input).asInstanceOf[Int]

    if (rasterGeom == null) {
      null
    } else {
      val pixelRecords = PixelFunctions.getPixelAsCentroids(rasterGeom, band)
      val rows = pixelRecords.map { pixelRecord =>
        val serializedGeom = GeometrySerializer.serialize(pixelRecord.geom)
        val rowArray =
          Array[Any](serializedGeom, pixelRecord.value, pixelRecord.colX, pixelRecord.rowY)
        InternalRow.fromSeq(rowArray)
      }
      new GenericArrayData(rows.toArray)
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RS_PixelAsCentroids = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, IntegerType)
}

case class RS_Values(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(PixelFunctions.values),
      inferrableFunction3(PixelFunctions.values),
      inferrableFunction4(PixelFunctions.values)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
