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
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_SetSRID(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterEditors.setSrid _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SetGeoReference(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterEditors.setGeoReference),
      inferrableFunction3(RasterEditors.setGeoReference),
      inferrableFunction7(RasterEditors.setGeoReference)) {
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
      nullTolerantInferrableFunction4(RasterEditors.resample),
      nullTolerantInferrableFunction5(RasterEditors.resample),
      nullTolerantInferrableFunction7(RasterEditors.resample)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_NormalizeAll(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterEditors.normalizeAll),
      inferrableFunction3(RasterEditors.normalizeAll),
      inferrableFunction4(RasterEditors.normalizeAll),
      inferrableFunction5(RasterEditors.normalizeAll),
      inferrableFunction6(RasterEditors.normalizeAll),
      inferrableFunction7(RasterEditors.normalizeAll)) {
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
      inferrableFunction1(RasterEditors.interpolate),
      inferrableFunction2(RasterEditors.interpolate),
      inferrableFunction3(RasterEditors.interpolate),
      inferrableFunction4(RasterEditors.interpolate),
      inferrableFunction5(RasterEditors.interpolate),
      inferrableFunction6(RasterEditors.interpolate)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
