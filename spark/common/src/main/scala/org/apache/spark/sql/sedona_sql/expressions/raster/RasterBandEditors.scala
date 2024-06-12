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
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_SetBandNoDataValue(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction4(RasterBandEditors.setBandNoDataValue),
      inferrableFunction3(RasterBandEditors.setBandNoDataValue),
      inferrableFunction2(RasterBandEditors.setBandNoDataValue)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_AddBand(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction4(RasterBandEditors.addBand),
      inferrableFunction3(RasterBandEditors.addBand),
      inferrableFunction2(RasterBandEditors.addBand)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Union(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterBandEditors.rasterUnion),
      inferrableFunction3(RasterBandEditors.rasterUnion),
      inferrableFunction4(RasterBandEditors.rasterUnion),
      inferrableFunction5(RasterBandEditors.rasterUnion),
      inferrableFunction6(RasterBandEditors.rasterUnion),
      inferrableFunction7(RasterBandEditors.rasterUnion)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Clip(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction5(RasterBandEditors.clip),
      inferrableFunction4(RasterBandEditors.clip),
      inferrableFunction3(RasterBandEditors.clip)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
