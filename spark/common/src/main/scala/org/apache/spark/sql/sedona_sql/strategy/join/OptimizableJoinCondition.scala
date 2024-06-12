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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sedona_sql.expressions._
import org.apache.spark.sql.sedona_sql.expressions.raster.RS_Predicate
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils

case class OptimizableJoinCondition(left: LogicalPlan, right: LogicalPlan) {

  /**
   * An extractor that matches expressions that are optimizable join conditions. Join queries with
   * optimizable join conditions will be executed as a spatial join (RangeJoin or DistanceJoin).
   * @param expression
   *   the join condition
   * @return
   *   an optional tuple containing the spatial predicate and the other predicates
   */
  def unapply(expression: Expression): Option[(Expression, Option[Expression])] = {
    val predicates = ExpressionUtils.splitConjunctivePredicates(expression)
    val (maybeSpatialPredicate, otherPredicates) = extractFirstOptimizablePredicate(predicates)
    maybeSpatialPredicate match {
      case Some(spatialPredicate) =>
        val other = otherPredicates.reduceOption((l, r) => And(l, r))
        Some(spatialPredicate, other)
      case None => None
    }
  }

  private def extractFirstOptimizablePredicate(
      expressions: Seq[Expression]): (Option[Expression], Seq[Expression]) = {
    expressions match {
      case Nil => (None, Nil)
      case head :: tail =>
        if (isOptimizablePredicate(head)) {
          (Some(head), tail)
        } else {
          val (spatialPredicate, otherPredicates) = extractFirstOptimizablePredicate(tail)
          (spatialPredicate, head +: otherPredicates)
        }
    }
  }

  private def isOptimizablePredicate(expression: Expression): Boolean = {
    expression match {
      case _: ST_Intersects | _: ST_Contains | _: ST_Covers | _: ST_Within | _: ST_CoveredBy |
          _: ST_Overlaps | _: ST_Touches | _: ST_Equals | _: ST_Crosses | _: RS_Predicate =>
        val leftShape = expression.children.head
        val rightShape = expression.children(1)
        ExpressionUtils.matchExpressionsToPlans(leftShape, rightShape, left, right).isDefined

      case ST_DWithin(Seq(leftShape, rightShape, distance)) =>
        isDistanceJoinOptimizable(leftShape, rightShape, distance)
      case ST_DWithin(Seq(leftShape, rightShape, distance, useSpheroid)) =>
        useSpheroid
          .isInstanceOf[Literal] && isDistanceJoinOptimizable(leftShape, rightShape, distance)

      case _: LessThan | _: LessThanOrEqual =>
        val (smaller, larger) = (expression.children.head, expression.children(1))
        smaller match {
          case _: ST_Distance | _: ST_DistanceSphere | _: ST_DistanceSpheroid |
              _: ST_FrechetDistance =>
            val leftShape = smaller.children.head
            val rightShape = smaller.children(1)
            isDistanceJoinOptimizable(leftShape, rightShape, larger)

          case ST_HausdorffDistance(Seq(leftShape, rightShape)) =>
            isDistanceJoinOptimizable(leftShape, rightShape, larger)
          case ST_HausdorffDistance(Seq(leftShape, rightShape, densityFrac)) =>
            isDistanceJoinOptimizable(leftShape, rightShape, larger)

          case _ => false
        }

      case _ => false
    }
  }

  private def isDistanceJoinOptimizable(
      leftShape: Expression,
      rightShape: Expression,
      distance: Expression): Boolean = {
    ExpressionUtils.matchExpressionsToPlans(leftShape, rightShape, left, right).isDefined &&
    ExpressionUtils.matchDistanceExpressionToJoinSide(distance, left, right).isDefined
  }
}
