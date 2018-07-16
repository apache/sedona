/*
 * FILE: JoinQueryDetector.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.apache.spark.sql.geosparksql.strategy.join

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.geosparksql.expressions.{ST_Contains, ST_Distance, ST_Intersects, ST_Within}
import org.datasyslab.geospark.spatialOperator.JoinQuery

/**
  * Plans `RangeJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
  * and ST_Intersects(a, b).
  *
  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
  */
object JoinQueryDetector extends Strategy {

  /**
    * Returns true if specified expression has at least one reference and all its references
    * map to the output of the specified plan.
    */
  private def matches(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.exists(plan.outputSet.contains(_)) &&
      expr.references.forall(plan.outputSet.contains(_))

  private def matchExpressionsToPlans(exprA: Expression,
                                      exprB: Expression,
                                      planA: LogicalPlan,
                                      planB: LogicalPlan): Option[(LogicalPlan, LogicalPlan)] =
    if (matches(exprA, planA) && matches(exprB, planB)) {
      Some((planA, planB))
    } else if (matches(exprA, planB) && matches(exprB, planA)) {
      Some((planB, planA))
    } else {
      None
    }


  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // ST_Contains(a, b) - a contains b
    case Join(left, right, joinType, Some(ST_Contains(Seq(leftShape, rightShape)))) =>
      planSpatialJoin(left, right, joinType, Seq(leftShape, rightShape),
        intersects = false, swapLeftRight = false)

    // ST_Intersects(a, b) - a intersects b
    case Join(left, right, joinType, Some(ST_Intersects(Seq(leftShape, rightShape)))) =>
      planSpatialJoin(left, right, joinType, Seq(leftShape, rightShape),
        intersects = true, swapLeftRight = false)

    // ST_WITHIN(a, b) - a is within b
    case Join(left, right, joinType, Some(ST_Within(Seq(leftShape, rightShape)))) =>
      planSpatialJoin(left, right, joinType, Seq(leftShape, rightShape),
        intersects = false, swapLeftRight = true)

    // ST_Distance(a, b) <= radius consider boundary intersection
    case Join(left, right, joinType, Some(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), radius))) =>
      planDistanceJoin(left, right, joinType, Seq(leftShape, rightShape), radius,
        intersects = true, swapLeftRight = false)

    // ST_Distance(a, b) < radius don't consider boundary intersection
    case Join(left, right, joinType, Some(LessThan(ST_Distance(Seq(leftShape, rightShape)), radius))) =>
      planDistanceJoin(left, right, joinType, Seq(leftShape, rightShape), radius,
        intersects = false, swapLeftRight = false)

    case _ =>
      Nil
  }

  private def getSupportedJoinType(joinType: JoinType): Option[JoinQuery.JoinType] = {
    joinType match {
      case Inner => Some(JoinQuery.JoinType.INNER)
      case LeftOuter => Some(JoinQuery.JoinType.LEFT_OUTER)
      case RightOuter => Some(JoinQuery.JoinType.RIGHT_OUTER)
      case _ => None
    }
  }

  private def planSpatialJoin(left: LogicalPlan,
                              right: LogicalPlan,
                              joinType: JoinType,
                              children: Seq[Expression],
                              intersects: Boolean,
                              swapLeftRight: Boolean,
                              extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    val relationship = if (intersects) "ST_Intersects" else "ST_Contains"

    getSupportedJoinType(joinType) match {
      case Some(queryJoinType) => matchExpressionsToPlans(a, b, left, right) match {
        case Some((planA, planB)) =>
          logInfo(s"Planning spatial join for $relationship relationship")
          RangeJoinExec(planLater(planA), planLater(planB), a, b, queryJoinType, intersects,
            swapLeftRight, extraCondition) :: Nil
        case None =>
          logInfo(
            s"Spatial join for $relationship with arguments not aligned " +
              "with join relations is not supported")
          Nil
      }
      case _ =>
        logWarning(
          s"Spatial join of type $joinType is not supported")
        Nil
    }
  }

  private def planDistanceJoin(left: LogicalPlan,
                               right: LogicalPlan,
                               joinType: JoinType,
                               children: Seq[Expression],
                               radius: Expression,
                               intersects: Boolean,
                               swapLeftRight: Boolean,
                               extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    val relationship = if (intersects) "ST_Distance <=" else "ST_Distance <"

    getSupportedJoinType(joinType) match {
      case Some(queryJoinType) =>
        matchExpressionsToPlans(a, b, left, right) match {
          case Some((planA, planB)) =>
            if (radius.references.isEmpty || matches(radius, planA)) {
              logInfo(s"Planning spatial distance join for $relationship relationship")
              DistanceJoinExec(planLater(planA), planLater(planB), a, b, queryJoinType, radius, intersects,
                swapLeftRight, extraCondition) :: Nil
            } else if (matches(radius, planB)) {
              logInfo(s"Planning spatial distance join for $relationship relationship")
              DistanceJoinExec(planLater(planB), planLater(planA), b, a, queryJoinType, radius, intersects,
                swapLeftRight, extraCondition) :: Nil
            } else {
              logInfo(
                "Spatial distance join for ST_Distance with non-scalar radius " +
                  "that is not a computation over just one side of the join is not supported")
              Nil
            }
          case None =>
            logInfo(
              "Spatial distance join for ST_Distance with arguments not " +
                "aligned with join relations is not supported")
            Nil
        }
      case _ =>
        logWarning(
          s"Spatial join of type $joinType is not supported")
        Nil
    }
  }
}
