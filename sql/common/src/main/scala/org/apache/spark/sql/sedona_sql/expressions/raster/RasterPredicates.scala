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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.implicits.InputExpressionEnhancer
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}

case class RS_Intersects(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions.head.toRaster(input)
    val geom = inputExpressions(1).toGeometry(input)
    if (raster == null || geom == null) {
      null
    } else {
      RasterPredicates.rsIntersects(raster, geom)
    }
  }

  override def nullable: Boolean = true
  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, GeometryUDT)
  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
