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

import org.apache.sedona.common.raster.RasterConstructors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_FromArcInfoAsciiGrid(inputExpressions: Seq[Expression])
  extends InferredExpression(RasterConstructors.fromArcInfoAsciiGrid _) {
  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_AsRaster(inputExpressions: Seq[Expression]) extends InferredExpression(
  inferrableFunction5(RasterConstructors.asRaster), inferrableFunction3(RasterConstructors.asRaster),
  inferrableFunction4(RasterConstructors.asRaster)
) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_FromGeoTiff(inputExpressions: Seq[Expression])
  extends InferredExpression(RasterConstructors.fromGeoTiff _) {

  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_MakeEmptyRaster(inputExpressions: Seq[Expression])
  extends InferredExpression(
    inferrableFunction6(RasterConstructors.makeEmptyRaster), inferrableFunction7(RasterConstructors.makeEmptyRaster),
    inferrableFunction10(RasterConstructors.makeEmptyRaster), inferrableFunction11(RasterConstructors.makeEmptyRaster)) {

  override def foldable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class RS_FromNetCDF(inputExpressions: Seq[Expression])
  extends InferredExpression(
    inferrableFunction2(RasterConstructors.fromNetCDF), inferrableFunction4(RasterConstructors.fromNetCDF)) {

  override def foldable: Boolean = false
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_NetCDFInfo(inputExpressions: Seq[Expression])
  extends InferredExpression(RasterConstructors.getRecordInfo _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

