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

import org.apache.sedona.common.raster.PixelFunctions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_Value(inputExpressions: Seq[Expression]) extends InferredExpression(
  inferrableFunction3(PixelFunctions.value), inferrableFunction4(PixelFunctions.value)
) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_PixelAsPoint(inputExpressions: Seq[Expression]) extends InferredExpression(PixelFunctions.getPixelAsPoint _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_PixelAsPolygon(inputExpressions: Seq[Expression]) extends InferredExpression(PixelFunctions.getPixelAsPolygon _) {
  protected def withNewChildrenInternal(newChilren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChilren)
  }
}

case class RS_PixelAsCentroid(inputExpressions: Seq[Expression]) extends InferredExpression(PixelFunctions.getPixelAsCentroid _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Values(inputExpressions: Seq[Expression]) extends InferredExpression(
  inferrableFunction3(PixelFunctions.values), inferrableFunction4(PixelFunctions.values)
) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
