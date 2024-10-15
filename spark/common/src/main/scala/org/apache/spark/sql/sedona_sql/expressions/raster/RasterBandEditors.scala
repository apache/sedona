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

import org.apache.sedona.common.raster.RasterBandEditors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_SetBandNoDataValue(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction4(RasterBandEditors.setBandNoDataValue),
      inferableFunction3(RasterBandEditors.setBandNoDataValue),
      inferableFunction2(RasterBandEditors.setBandNoDataValue)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_AddBand(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction4(RasterBandEditors.addBand),
      inferableFunction3(RasterBandEditors.addBand),
      inferableFunction2(RasterBandEditors.addBand)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Union(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction2(RasterBandEditors.rasterUnion),
      inferableFunction3(RasterBandEditors.rasterUnion),
      inferableFunction4(RasterBandEditors.rasterUnion),
      inferableFunction5(RasterBandEditors.rasterUnion),
      inferableFunction6(RasterBandEditors.rasterUnion),
      inferableFunction7(RasterBandEditors.rasterUnion)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Clip(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction6(RasterBandEditors.clip),
      inferableFunction5(RasterBandEditors.clip),
      inferableFunction4(RasterBandEditors.clip),
      inferableFunction3(RasterBandEditors.clip)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
