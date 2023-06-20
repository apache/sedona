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
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, DoubleType, IntegerType}

case class RS_Envelope(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) {
      null
    } else {
      RasterAccessors.envelope(raster).toGenericArrayData
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT)
}

case class RS_NumBands(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) {
      null
    } else {
      RasterAccessors.numBands(raster)
    }
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT)
}

case class RS_SRID(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) {
      null
    } else {
      RasterAccessors.srid(raster)
    }
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT)
}

case class RS_Metadata(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) {
      null
    } else {
      new GenericArrayData(RasterAccessors.metadata(raster))
    }
  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT)
}