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

import org.apache.sedona.common.raster.Outputs
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// Expected Types (RasterUDT, StringType, IntegerType) or (RasterUDT, StringType, DecimalType)
case class RS_AsGeoTiff(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) return null
    // If there are more than one input expressions, the additional ones are used as parameters
    if (inputExpressions.length > 1) {
      Outputs.asGeoTiff(raster, inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString, inputExpressions(2).eval(input).toString.toFloat)
    }
    else {
      Outputs.asGeoTiff(raster, null, -1)
    }
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_AsArcGrid(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) return null
    // If there are more than one input expressions, the additional ones are used as parameters
    if (inputExpressions.length > 1) {
      Outputs.asArcGrid(raster, inputExpressions(1).eval(input).asInstanceOf[Int])
    }
    else {
      Outputs.asArcGrid(raster, -1)
    }
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}