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

import org.apache.sedona.common.raster.Constructors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits._


case class RS_FromArcInfoAsciiGrid(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val bytes = inputExpressions(0).eval(input).asInstanceOf[Array[Byte]]
    if (bytes == null) {
      null
    } else {
      Constructors.fromArcInfoAsciiGrid(bytes).serialize
    }
  }

  override def dataType: DataType = RasterUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
}

case class RS_FromGeoTiff(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val bytes = inputExpressions(0).eval(input).asInstanceOf[Array[Byte]]
    if (bytes == null) {
      null
    } else {
      Constructors.fromGeoTiff(bytes).serialize
    }
  }

  override def dataType: DataType = RasterUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
}
