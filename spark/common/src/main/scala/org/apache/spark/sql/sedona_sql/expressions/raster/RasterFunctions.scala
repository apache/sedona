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

import org.apache.sedona.common.raster.{RasterAccessors, RasterBandAccessors}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.implicits.InputExpressionEnhancer
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, DoubleType, IntegerType, StructField, StructType}

case class RS_Metadata(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = StructType(
    Seq(
      StructField("upperLeftX", DoubleType, nullable = false),
      StructField("upperLeftY", DoubleType, nullable = false),
      StructField("gridWidth", DoubleType, nullable = false),
      StructField("gridHeight", DoubleType, nullable = false),
      StructField("scaleX", DoubleType, nullable = false),
      StructField("scaleY", DoubleType, nullable = false),
      StructField("skewX", DoubleType, nullable = false),
      StructField("skewY", DoubleType, nullable = false),
      StructField("srid", DoubleType, nullable = false),
      StructField("numSampleDimensions", DoubleType, nullable = false)))

  override def eval(input: InternalRow): Any = {
    // Evaluate the input expressions
    val rasterGeom = inputExpressions(0).toRaster(input)

    // Check if the raster geometry is null
    if (rasterGeom == null) {
      null
    } else {
      // Get the metadata using the Java method
      val metaData = RasterAccessors.metadata(rasterGeom)

      // Create an InternalRow with the metadata
      InternalRow.fromSeq(metaData.map(_.asInstanceOf[Any]))
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): RS_Metadata = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT)
}

case class RS_SummaryStatsAll(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = StructType(
    Seq(
      StructField("count", DoubleType, nullable = false),
      StructField("sum", DoubleType, nullable = false),
      StructField("mean", DoubleType, nullable = false),
      StructField("stddev", DoubleType, nullable = false),
      StructField("min", DoubleType, nullable = false),
      StructField("max", DoubleType, nullable = false)))

  override def eval(input: InternalRow): Any = {
    // Evaluate the input expressions
    val rasterGeom = inputExpressions(0).toRaster(input)
    val band = if (inputExpressions.length >= 2) {
      inputExpressions(1).eval(input).asInstanceOf[Int]
    } else {
      1
    }
    val noData = if (inputExpressions.length >= 3) {
      inputExpressions(2).eval(input).asInstanceOf[Boolean]
    } else {
      true
    }

    // Check if the raster geometry is null
    if (rasterGeom == null) {
      null
    } else {
      val summaryStatsAll = RasterBandAccessors.getSummaryStatsAll(rasterGeom, band, noData)
      // Create an InternalRow with the summaryStatsAll
      InternalRow.fromSeq(summaryStatsAll.map(_.asInstanceOf[Any]))
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RS_SummaryStatsAll = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    if (inputExpressions.length == 1) {
      Seq(RasterUDT)
    } else if (inputExpressions.length == 2) {
      Seq(RasterUDT, IntegerType)
    } else if (inputExpressions.length == 3) {
      Seq(RasterUDT, IntegerType, BooleanType)
    } else {
      Seq(RasterUDT)
    }
  }
}

case class RS_ZonalStatsAll(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = StructType(
    Seq(
      StructField("count", DoubleType, nullable = false),
      StructField("sum", DoubleType, nullable = false),
      StructField("mean", DoubleType, nullable = false),
      StructField("median", DoubleType, nullable = false),
      StructField("mode", DoubleType, nullable = false),
      StructField("stddev", DoubleType, nullable = false),
      StructField("variance", DoubleType, nullable = false),
      StructField("min", DoubleType, nullable = false),
      StructField("max", DoubleType, nullable = false)))

  override def eval(input: InternalRow): Any = {
    // Evaluate the input expressions
    val rasterGeom = inputExpressions(0).toRaster(input)
    val roi = if (inputExpressions.length >= 2) {
      inputExpressions(1).toGeometry(input)
    } else {
      null
    }
    val band = if (inputExpressions.length >= 3) {
      inputExpressions(2).eval(input).asInstanceOf[Int]
    } else {
      1
    }
    val noData = if (inputExpressions.length >= 4) {
      inputExpressions(3).eval(input).asInstanceOf[Boolean]
    } else {
      true
    }
    val lenient = if (inputExpressions.length >= 5) {
      inputExpressions(4).eval(input).asInstanceOf[Boolean]
    } else {
      true
    }

    // Check if the raster geometry is null
    if (rasterGeom == null) {
      null
    } else {
      val zonalStatsAll =
        RasterBandAccessors.getZonalStatsAll(rasterGeom, roi, band, noData, lenient)
      // Create an InternalRow with the zonalStatsAll
      if (zonalStatsAll == null) {
        return null
      }
      InternalRow.fromSeq(zonalStatsAll.map(_.asInstanceOf[Any]))
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): RS_ZonalStatsAll = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    if (inputExpressions.length == 2) {
      Seq(RasterUDT, GeometryUDT)
    } else if (inputExpressions.length == 3) {
      Seq(RasterUDT, GeometryUDT, IntegerType)
    } else if (inputExpressions.length == 4) {
      Seq(RasterUDT, GeometryUDT, IntegerType, BooleanType)
    } else if (inputExpressions.length >= 5) {
      Seq(RasterUDT, GeometryUDT, IntegerType, BooleanType)
    } else {
      Seq(RasterUDT, GeometryUDT)
    }
  }
}
