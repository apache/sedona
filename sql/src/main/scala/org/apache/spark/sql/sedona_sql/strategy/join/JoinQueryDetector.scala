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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.expressions._

/**
  * Plans `RangeJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
  * and ST_Intersects(a, b).
  *
  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
  */
object JoinQueryDetector extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // ST_Contains(a, b) - a contains b
case Join(left, right, Inner, Some(ST_Contains(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Contains(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(left, right, Seq(leftShape, rightShape), false)

    // ST_Intersects(a, b) - a intersects b
case Join(left, right, Inner, Some(ST_Intersects(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Intersects(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(left, right, Seq(leftShape, rightShape), true)

    // ST_WITHIN(a, b) - a is within b
case Join(left, right, Inner, Some(ST_Within(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Within(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(right, left, Seq(rightShape, leftShape), false)

    // ST_Overlaps(a, b) - a overlaps b
case Join(left, right, Inner, Some(ST_Overlaps(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Overlaps(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(right, left, Seq(rightShape, leftShape), false)

    // ST_Touches(a, b) - a touches b
case Join(left, right, Inner, Some(ST_Touches(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Touches(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(left, right, Seq(leftShape, rightShape), true)

    // ST_Distance(a, b) <= radius consider boundary intersection
case Join(left, right, Inner, Some(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), radius)), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), radius))) => // SPARK2 anchor
      planDistanceJoin(left, right, Seq(leftShape, rightShape), radius, true)

    // ST_Distance(a, b) < radius don't consider boundary intersection
case Join(left, right, Inner, Some(LessThan(ST_Distance(Seq(leftShape, rightShape)), radius)), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(LessThan(ST_Distance(Seq(leftShape, rightShape)), radius))) => // SPARK2 anchor
      planDistanceJoin(left, right, Seq(leftShape, rightShape), radius, false)

    // ST_Equals(a, b) - a is equal to b
case Join(left, right, Inner, Some(ST_Equals(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Equals(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(left, right, Seq(leftShape, rightShape), false)

    // ST_Crosses(a, b) - a crosses b
case Join(left, right, Inner, Some(ST_Crosses(Seq(leftShape, rightShape))), _) => // SPARK3 anchor
//case Join(left, right, Inner, Some(ST_Crosses(Seq(leftShape, rightShape)))) => // SPARK2 anchor
      planSpatialJoin(right, left, Seq(rightShape, leftShape), false)

    case _ =>
      Nil
  }

  /**
    * Returns true if specified expression has at least one reference and all its references
    * map to the output of the specified plan.
    */
  private def matches(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.find(plan.outputSet.contains(_)).isDefined &&
      expr.references.find(!plan.outputSet.contains(_)).isEmpty

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

  private def planSpatialJoin(left: LogicalPlan,
                              right: LogicalPlan,
                              children: Seq[Expression],
                              intersects: Boolean,
                              extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    val relationship = if (intersects) "ST_Intersects" else "ST_Contains";

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((planA, planB)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        RangeJoinExec(planLater(planA), planLater(planB), a, b, intersects, extraCondition) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }

  private def planDistanceJoin(left: LogicalPlan,
                               right: LogicalPlan,
                               children: Seq[Expression],
                               radius: Expression,
                               intersects: Boolean,
                               extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    val relationship = if (intersects) "ST_Distance <=" else "ST_Distance <";

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((planA, planB)) =>
        if (radius.references.isEmpty || matches(radius, planA)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planA), planLater(planB), a, b, radius, intersects, extraCondition) :: Nil
        } else if (matches(radius, planB)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planB), planLater(planA), b, a, radius, intersects, extraCondition) :: Nil
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
  }
}
