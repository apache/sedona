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

