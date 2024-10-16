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

import org.apache.sedona.common.raster.RasterEditors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_SetSRID(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.setSrid _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SetGeoReference(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction2(RasterEditors.setGeoReference),
      inferableFunction3(RasterEditors.setGeoReference),
      inferableFunction7(RasterEditors.setGeoReference)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SetPixelType(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.setPixelType _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Resample(inputExpressions: Seq[Expression])
    extends InferredExpression(
      nullTolerantInferableFunction4(RasterEditors.resample),
      nullTolerantInferableFunction5(RasterEditors.resample),
      nullTolerantInferableFunction7(RasterEditors.resample)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_NormalizeAll(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction1(RasterEditors.normalizeAll),
      inferableFunction3(RasterEditors.normalizeAll),
      inferableFunction4(RasterEditors.normalizeAll),
      inferableFunction5(RasterEditors.normalizeAll),
      inferableFunction6(RasterEditors.normalizeAll),
      inferableFunction7(RasterEditors.normalizeAll)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_ReprojectMatch(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.reprojectMatch _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Interpolate(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferableFunction1(RasterEditors.interpolate),
      inferableFunction2(RasterEditors.interpolate),
      inferableFunction3(RasterEditors.interpolate),
      inferableFunction4(RasterEditors.interpolate),
      inferableFunction5(RasterEditors.interpolate),
      inferableFunction6(RasterEditors.interpolate)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
