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

import org.apache.sedona.common.raster.RasterAccessors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_NumBands(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.numBands _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SRID(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.srid _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Width(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getWidth _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_UpperLeftX(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getUpperLeftX _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_UpperLeftY(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getUpperLeftY _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Height(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getHeight _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_ScaleX(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getScaleX _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_ScaleY(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getScaleY _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_GeoReference(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterAccessors.getGeoReference),
      inferrableFunction1(RasterAccessors.getGeoReference)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Rotation(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getRotation _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SkewX(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getSkewX _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_SkewY(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getSkewY _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_RasterToWorldCoordX(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getWorldCoordX _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_RasterToWorldCoordY(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getWorldCoordY _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_RasterToWorldCoord(inputExpressions: Seq[Expression])
    extends InferredExpression(RasterAccessors.getWorldCoord _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_WorldToRasterCoord(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterAccessors.getGridCoord),
      inferrableFunction2(RasterAccessors.getGridCoord)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_WorldToRasterCoordX(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterAccessors.getGridCoordX),
      inferrableFunction2(RasterAccessors.getGridCoordX)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_WorldToRasterCoordY(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterAccessors.getGridCoordY),
      inferrableFunction2(RasterAccessors.getGridCoordY)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
