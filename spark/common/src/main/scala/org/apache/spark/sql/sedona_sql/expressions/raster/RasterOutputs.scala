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

import org.apache.sedona.common.raster.RasterOutputs
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

private[apache] case class RS_AsGeoTiff(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterOutputs.asGeoTiff),
      inferrableFunction1(RasterOutputs.asGeoTiff)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsArcGrid(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterOutputs.asArcGrid),
      inferrableFunction1(RasterOutputs.asArcGrid)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsPNG(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterOutputs.asPNG),
      inferrableFunction2(RasterOutputs.asPNG)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsBase64(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterOutputs.asBase64),
      inferrableFunction2(RasterOutputs.asBase64)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsMatrix(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterOutputs.asMatrix),
      inferrableFunction2(RasterOutputs.asMatrix),
      inferrableFunction1(RasterOutputs.asMatrix)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsImage(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterOutputs.createHTMLString),
      inferrableFunction1(RasterOutputs.createHTMLString)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsCOG(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction6(RasterOutputs.asCOG),
      inferrableFunction5(RasterOutputs.asCOG),
      inferrableFunction4(RasterOutputs.asCOG),
      inferrableFunction3(RasterOutputs.asCOG),
      inferrableFunction2(RasterOutputs.asCOG),
      inferrableFunction1(RasterOutputs.asCOG)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
