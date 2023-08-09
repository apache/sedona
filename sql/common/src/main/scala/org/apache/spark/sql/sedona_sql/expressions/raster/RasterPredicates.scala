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

import org.apache.sedona.common.raster.RasterPredicates
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

case class RS_Intersects(inputExpressions: Seq[Expression]) extends InferredExpression(RasterPredicates.rsIntersects _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Within(inputExpressions: Seq[Expression]) extends InferredExpression(RasterPredicates.rsWithin _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Contains(inputExpressions: Seq[Expression]) extends InferredExpression(RasterPredicates.rsContains _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
