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

import org.apache.sedona.common.raster.RasterBandAccessors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_BandNoDataValue(inputExpressions: Seq[Expression]) extends InferredExpression(inferrableFunction2(RasterBandAccessors.getBandNoDataValue), inferrableFunction1(RasterBandAccessors.getBandNoDataValue)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Count(inputExpressions: Seq[Expression]) extends InferredExpression(
  inferrableFunction2(RasterBandAccessors.getCount), inferrableFunction1(RasterBandAccessors.getCount),
    inferrableFunction3(RasterBandAccessors.getCount)) {
    protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
      copy(inputExpressions = newChildren)
    }
  }
case class RS_SummaryStats(inputExpressions: Seq[Expression]) extends InferredExpression(
  inferrableFunction1(RasterBandAccessors.getSummaryStats), inferrableFunction2(RasterBandAccessors.getSummaryStats),
  inferrableFunction3(RasterBandAccessors.getSummaryStats)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Band(inputExpressions: Seq[Expression]) extends InferredExpression(
  inferrableFunction3(RasterBandAccessors.getBand), inferrableFunction2(RasterBandAccessors.getBand),
  inferrableFunction1(RasterBandAccessors.getBand)
) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_BandPixelType(inputExpressions: Seq[Expression]) extends InferredExpression(inferrableFunction2(RasterBandAccessors.getBandType), inferrableFunction1(RasterBandAccessors.getBandType)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

