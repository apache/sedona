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

import org.apache.sedona.common.raster.{RasterEditors, Serde}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.SerdeAware
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType}
import org.geotools.coverage.grid.GridCoverage2D

case class RS_SetSRID(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes with SerdeAware {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    Option(evalWithoutSerialization(input)).map(Serde.serialize).orNull
  }

  override def evalWithoutSerialization(input: InternalRow): GridCoverage2D = {
    val raster = inputExpressions(0).toRaster(input)
    val srid = inputExpressions(1).eval(input).asInstanceOf[Int]
    if (raster == null) {
      null
    } else {
      RasterEditors.setSrid(raster, srid)
    }
  }

  override def dataType: DataType = RasterUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, IntegerType)
}