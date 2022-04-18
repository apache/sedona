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

import org.apache.sedona.core.enums.IndexType
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.expressions._



case class JoinQueryDetection(
  left: LogicalPlan,
  right: LogicalPlan,
  leftShape: Expression,
  rightShape: Expression,
  intersects: Boolean,
  extraCondition: Option[Expression] = None,
  radius: Option[Expression] = None
)

/**
  * Plans `RangeJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
  * and ST_Intersects(a, b).
  *
  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
  * 
  * Plans `BroadcastIndexJoinExec for inner joins on spatial relationships with a broadcast hint.
  */
class JoinQueryDetector(sparkSession: SparkSession) extends Strategy {

  private def getJoinDetection(
    left: LogicalPlan,
    right: LogicalPlan,
    predicate: ST_Predicate,
    extraCondition: Option[Expression] = None): Option[JoinQueryDetection] = {
      predicate match {
        case ST_Contains(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, false, extraCondition))
        case ST_Intersects(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, true, extraCondition))
        case ST_Within(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(right, left, rightShape, leftShape, false, extraCondition))
        case ST_Overlaps(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(right, left, rightShape, leftShape, false, extraCondition))
        case ST_Touches(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, true, extraCondition))
        case ST_Equals(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, false, extraCondition))
        case ST_Crosses(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(right, left, rightShape, leftShape, false, extraCondition))
        case _ => None
      }
    }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Join(left, right, Inner, condition, JoinHint(leftHint, rightHint)) => { // SPARK3 anchor
//    case Join(left, right, Inner, condition) => { // SPARK2 anchor
      val broadcastLeft = leftHint.exists(_.strategy.contains(BROADCAST)) // SPARK3 anchor
      val broadcastRight = rightHint.exists(_.strategy.contains(BROADCAST)) // SPARK3 anchor
//      val broadcastLeft = left.isInstanceOf[ResolvedHint] && left.asInstanceOf[ResolvedHint].hints.broadcast  // SPARK2 anchor
//      val broadcastRight = right.isInstanceOf[ResolvedHint] && right.asInstanceOf[ResolvedHint].hints.broadcast // SPARK2 anchor

      val queryDetection: Option[JoinQueryDetection] = condition match {
        case Some(predicate: ST_Predicate) =>
          getJoinDetection(left, right, predicate)
        case Some(And(predicate: ST_Predicate, extraCondition)) =>
          getJoinDetection(left, right, predicate, Some(extraCondition))
        case Some(And(extraCondition, predicate: ST_Predicate)) =>
          getJoinDetection(left, right, predicate, Some(extraCondition))
        case Some(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), radius)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, true, None, Some(radius)))
        case Some(And(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), radius), extraCondition)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, true, Some(extraCondition), Some(radius)))
        case Some(And(extraCondition, LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), radius))) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, true, Some(extraCondition), Some(radius)))
        case Some(LessThan(ST_Distance(Seq(leftShape, rightShape)), radius)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, false, None, Some(radius)))
        case Some(And(LessThan(ST_Distance(Seq(leftShape, rightShape)), radius), extraCondition)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, false, Some(extraCondition), Some(radius)))
        case Some(And(extraCondition, LessThan(ST_Distance(Seq(leftShape, rightShape)), radius))) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, false, Some(extraCondition), Some(radius)))
        case _ =>
          None
      }

      val sedonaConf = new SedonaConf(sparkSession.sparkContext.conf)

      if ((broadcastLeft || broadcastRight) && sedonaConf.getUseIndex) {
        queryDetection match {
          case Some(JoinQueryDetection(left, right, leftShape, rightShape, intersects, extraCondition, radius)) =>
            planBroadcastJoin(left, right, Seq(leftShape, rightShape), intersects, sedonaConf.getIndexType, broadcastLeft, extraCondition, radius)
          case _ =>
            Nil
        }
      } else {
        queryDetection match {
          case Some(JoinQueryDetection(left, right, leftShape, rightShape, intersects, extraCondition, None)) =>
            planSpatialJoin(left, right, Seq(leftShape, rightShape), intersects, extraCondition)
          case Some(JoinQueryDetection(left, right, leftShape, rightShape, intersects, extraCondition, Some(radius))) =>
            planDistanceJoin(left, right, Seq(leftShape, rightShape), radius, intersects, extraCondition)
          case None => 
            Nil
        }
      }
    }
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
                                      planB: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Boolean)] =
    if (matches(exprA, planA) && matches(exprB, planB)) {
      Some((planA, planB, false))
    } else if (matches(exprA, planB) && matches(exprB, planA)) {
      Some((planB, planA, true))
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
      case Some((planA, planB, _)) =>
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
      case Some((planA, planB, _)) =>
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

  private def planBroadcastJoin(left: LogicalPlan,
                                right: LogicalPlan,
                                children: Seq[Expression],
                                intersects: Boolean,
                                indexType: IndexType,
                                broadcastLeft: Boolean,
                                extraCondition: Option[Expression],
                                radius: Option[Expression]): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    val relationship = radius match {
      case Some(_) if intersects => "ST_Distance <="
      case Some(_) if !intersects => "ST_Distance <"
      case None if intersects => "ST_Intersects"
      case None if !intersects => "ST_Contains"
    }

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((_, _, swapped)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        val broadcastSide = if (broadcastLeft) LeftSide else RightSide
        val (leftPlan, rightPlan, streamShape, windowSide) = (broadcastSide, swapped) match {
          case (LeftSide, false) => // Broadcast the left side, windows on the left
            (SpatialIndexExec(planLater(left), a, indexType, radius), planLater(right), b, LeftSide)
          case (LeftSide, true) => // Broadcast the left side, objects on the left
            (SpatialIndexExec(planLater(left), b, indexType), planLater(right), a, RightSide)
          case (RightSide, false) => // Broadcast the right side, windows on the left
            (planLater(left), SpatialIndexExec(planLater(right), b, indexType), a, LeftSide)
          case (RightSide, true) => // Broadcast the right side, objects on the left
            (planLater(left), SpatialIndexExec(planLater(right), a, indexType, radius), b, RightSide)
        }
        BroadcastIndexJoinExec(leftPlan, rightPlan, streamShape, broadcastSide, windowSide, intersects, extraCondition, radius) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }
}
