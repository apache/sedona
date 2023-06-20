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

import org.apache.sedona.common.geometrySerde.GeometrySerializer
import org.apache.sedona.common.raster.PixelFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.implicits.InputExpressionEnhancer
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, DoubleType, IntegerType}

case class RS_Value(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions.head.toRaster(input)
    val geom = inputExpressions(1).toGeometry(input)
    val band = inputExpressions(2).eval(input).asInstanceOf[Int]
    if (raster == null || geom == null) {
      null
    } else {
      PixelFunctions.value(raster, geom, band)
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, GeometryUDT, IntegerType)
}

case class RS_Values(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(DoubleType)

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    val serializedGeometries = inputExpressions(1).eval(input).asInstanceOf[ArrayData]
    val band = inputExpressions(2).eval(input).asInstanceOf[Int]
    if (raster == null || serializedGeometries == null) {
      null
    } else {
      val geometries = (0 until serializedGeometries.numElements()).map {
        i => Option(serializedGeometries.getBinary(i)).map(GeometrySerializer.deserialize).orNull
      }
      new GenericArrayData(PixelFunctions.values(raster, java.util.Arrays.asList(geometries:_*), band).toArray)
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, ArrayType(GeometryUDT), IntegerType)
}