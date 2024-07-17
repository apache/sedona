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

import org.apache.sedona.common.raster.RasterAccessors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, IntegerType, StructField, StructType}
import org.geotools.coverage.grid.GridCoverage2D

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
