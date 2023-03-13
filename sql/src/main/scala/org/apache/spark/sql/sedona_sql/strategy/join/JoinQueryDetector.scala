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

import org.apache.sedona.core.enums.{IndexType, SpatialJoinOptimizationMode}
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, EqualTo, Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, NaturalJoin, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.expressions._
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates


case class JoinQueryDetection(
  left: LogicalPlan,
  right: LogicalPlan,
  leftShape: Expression,
  rightShape: Expression,
  spatialPredicate: SpatialPredicate,
  extraCondition: Option[Expression] = None,
  distance: Option[Expression] = None
)

/**
  * Plans `RangeJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
  * and ST_Intersects(a, b).
  *
  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
  *
  * Plans `BroadcastIndexJoinExec` for inner joins on spatial relationships with a broadcast hint.
  */
class JoinQueryDetector(sparkSession: SparkSession) extends Strategy {

  private def getJoinDetection(
    left: LogicalPlan,
    right: LogicalPlan,
    predicate: ST_Predicate,
    extraCondition: Option[Expression] = None): Option[JoinQueryDetection] = {
      predicate match {
        case ST_Contains(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.CONTAINS, extraCondition))
        case ST_Intersects(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, extraCondition))
        case ST_Within(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.WITHIN, extraCondition))
        case ST_Covers(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.COVERS, extraCondition))
        case ST_CoveredBy(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.COVERED_BY, extraCondition))
        case ST_Overlaps(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.OVERLAPS, extraCondition))
        case ST_Touches(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.TOUCHES, extraCondition))
        case ST_Equals(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.EQUALS, extraCondition))
        case ST_Crosses(Seq(leftShape, rightShape)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.CROSSES, extraCondition))
        case _ => None
      }
    }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Join(left, right, joinType, condition, JoinHint(leftHint, rightHint)) if optimizationEnabled(left, right, condition) => {
      var broadcastLeft = leftHint.exists(_.strategy.contains(BROADCAST))
      var broadcastRight = rightHint.exists(_.strategy.contains(BROADCAST))

      /*
      If either side is small we can automatically broadcast just like Spark does.
      This only applies to inner joins as there are no optimized fallback plan for other join types.
      It's better that users are explicit about broadcasting for other join types than seeing wildly different behavior
      depending on data size.
       */
      if (!broadcastLeft && !broadcastRight && joinType == Inner) {
        val canAutoBroadCastLeft = canAutoBroadcastBySize(left)
        val canAutoBroadCastRight = canAutoBroadcastBySize(right)
        if (canAutoBroadCastLeft && canAutoBroadCastRight) {
          // Both sides can be broadcasted. Choose the smallest side.
          broadcastLeft = left.stats.sizeInBytes <= right.stats.sizeInBytes
          broadcastRight = !broadcastLeft
        } else {
          broadcastLeft = canAutoBroadCastLeft
          broadcastRight = canAutoBroadCastRight
        }
      }

      val queryDetection: Option[JoinQueryDetection] = condition match {
        case Some(predicate: ST_Predicate) =>
          getJoinDetection(left, right, predicate)
        case Some(And(predicate: ST_Predicate, extraCondition)) =>
          getJoinDetection(left, right, predicate, Some(extraCondition))
        case Some(And(extraCondition, predicate: ST_Predicate)) =>
          getJoinDetection(left, right, predicate, Some(extraCondition))

        // For distance joins we execute the actual predicate (condition) and not only extraConditions.
        case Some(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), distance)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, condition, Some(distance)))
        case Some(And(LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), distance), _)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, condition, Some(distance)))
        case Some(And(_, LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), distance))) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, condition, Some(distance)))
        case Some(LessThan(ST_Distance(Seq(leftShape, rightShape)), distance)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, condition, Some(distance)))
        case Some(And(LessThan(ST_Distance(Seq(leftShape, rightShape)), distance), _)) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, condition, Some(distance)))
        case Some(And(_, LessThan(ST_Distance(Seq(leftShape, rightShape)), distance))) =>
          Some(JoinQueryDetection(left, right, leftShape, rightShape, SpatialPredicate.INTERSECTS, condition, Some(distance)))
        case _ =>
          None
      }

      val sedonaConf = new SedonaConf(sparkSession.conf)

      if ((broadcastLeft || broadcastRight) && sedonaConf.getUseIndex) {
        queryDetection match {
          case Some(JoinQueryDetection(left, right, leftShape, rightShape, spatialPredicate, extraCondition, distance)) =>
            planBroadcastJoin(
              left, right, Seq(leftShape, rightShape), joinType,
              spatialPredicate, sedonaConf.getIndexType,
              broadcastLeft, broadcastRight, extraCondition, distance)
          case _ =>
            Nil
        }
      } else {
        queryDetection match {
          case Some(JoinQueryDetection(left, right, leftShape, rightShape, spatialPredicate, extraCondition, None)) =>
            planSpatialJoin(left, right, Seq(leftShape, rightShape), joinType, spatialPredicate, extraCondition)
          case Some(JoinQueryDetection(left, right, leftShape, rightShape, spatialPredicate, extraCondition, Some(distance))) =>
            planDistanceJoin(left, right, Seq(leftShape, rightShape), joinType, distance, spatialPredicate, extraCondition)
          case None =>
            Nil
        }
      }
    }
    case _ =>
      Nil
  }

  private def optimizationEnabled(left: LogicalPlan, right: LogicalPlan, condition: Option[Expression]): Boolean = {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    sedonaConf.getSpatialJoinOptimizationMode match {
      case SpatialJoinOptimizationMode.NONE => false
      case SpatialJoinOptimizationMode.ALL => true
      case SpatialJoinOptimizationMode.NONEQUI => !isEquiJoin(left, right, condition)
      case mode => throw new IllegalArgumentException(s"Unknown spatial join optimization mode: $mode")
    }
  }

  private def canAutoBroadcastBySize(plan: LogicalPlan) =
    plan.stats.sizeInBytes != 0 && plan.stats.sizeInBytes <= SedonaConf.fromActiveSession.getAutoBroadcastJoinThreshold

  /**
    * Returns true if specified expression has at least one reference and all its references
    * map to the output of the specified plan.
    */
  private def matches(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.nonEmpty && expr.references.subsetOf(plan.outputSet)

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

  private def matchDistanceExpressionToJoinSide(distance: Expression, left: LogicalPlan, right: LogicalPlan): Option[JoinSide] = {
    if (distance.references.isEmpty || matches(distance, left)) {
      Some(LeftSide)
    } else if (matches(distance, right)) {
      Some(RightSide)
    } else {
      None
    }
  }

  private def planSpatialJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    children: Seq[Expression],
    joinType: JoinType,
    spatialPredicate: SpatialPredicate,
    extraCondition: Option[Expression] = None): Seq[SparkPlan] = {

    if (joinType != Inner) {
      return Nil
    }

    val a = children.head
    val b = children.tail.head

    val relationship = s"ST_$spatialPredicate"

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((_, _, false)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        RangeJoinExec(planLater(left), planLater(right), a, b, spatialPredicate, extraCondition) :: Nil
      case Some((_, _, true)) =>
        logInfo(s"Planning spatial join for $relationship relationship with swapped left and right shapes")
        val invSpatialPredicate = SpatialPredicate.inverse(spatialPredicate)
        RangeJoinExec(planLater(left), planLater(right), b, a, invSpatialPredicate, extraCondition) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }

  private def planDistanceJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    children: Seq[Expression],
    joinType: JoinType,
    distance: Expression,
    spatialPredicate: SpatialPredicate,
    extraCondition: Option[Expression] = None): Seq[SparkPlan] = {

    if (joinType != Inner) {
      return Nil
    }

    val a = children.head
    val b = children.tail.head

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((_, _, swappedLeftAndRight)) =>
        val (leftShape, rightShape) = if (swappedLeftAndRight) (b, a) else (a, b)
        matchDistanceExpressionToJoinSide(distance, left, right) match {
          case Some(LeftSide) =>
            logInfo("Planning spatial distance join, distance bound to left relation")
            DistanceJoinExec(planLater(left), planLater(right), leftShape, rightShape, distance, distanceBoundToLeft = true,
              spatialPredicate, extraCondition) :: Nil
          case Some(RightSide) =>
            logInfo("Planning spatial distance join, distance bound to right relation")
            DistanceJoinExec(planLater(left), planLater(right), leftShape, rightShape, distance, distanceBoundToLeft = false,
              spatialPredicate, extraCondition) :: Nil
          case _ =>
            logInfo(
              "Spatial distance join for ST_Distance with non-scalar distance " +
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

  private def planBroadcastJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    children: Seq[Expression],
    joinType: JoinType,
    spatialPredicate: SpatialPredicate,
    indexType: IndexType,
    broadcastLeft: Boolean,
    broadcastRight: Boolean,
    extraCondition: Option[Expression],
    distance: Option[Expression]): Seq[SparkPlan] = {

    val broadcastSide = joinType match {
      case Inner if broadcastLeft => Some(LeftSide)
      case Inner if broadcastRight => Some(RightSide)
      case LeftSemi if broadcastRight => Some(RightSide)
      case LeftAnti if broadcastRight => Some(RightSide)
      case LeftOuter if broadcastRight => Some(RightSide)
      case RightOuter if broadcastLeft => Some(LeftSide)
      case _ => None
    }

    if (broadcastSide.isEmpty) {
      return Nil
    }

    val a = children.head
    val b = children.tail.head

    val relationship = (distance, spatialPredicate) match {
      case (Some(_), SpatialPredicate.INTERSECTS) => "ST_Distance <="
      case (Some(_), _) => "ST_Distance <"
      case (None, _) => s"ST_$spatialPredicate"
    }

    val (distanceOnIndexSide, distanceOnStreamSide) = distance.map { distanceExpr =>
      matchDistanceExpressionToJoinSide(distanceExpr, left, right) match {
        case Some(side) =>
          if (broadcastSide.get == side) (Some(distanceExpr), None)
          else if (distanceExpr.references.isEmpty) (Some(distanceExpr), None)
          else (None, Some(distanceExpr))
        case _ => throw new IllegalArgumentException("Distance expression must be bound to one side of the join")
      }
    }.getOrElse((None, None))

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((_, _, swapped)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        val (leftPlan, rightPlan, streamShape, windowSide) = (broadcastSide.get, swapped) match {
          case (LeftSide, false) => // Broadcast the left side, windows on the left
            (SpatialIndexExec(planLater(left), a, indexType, distanceOnIndexSide), planLater(right), b, LeftSide)
          case (LeftSide, true) => // Broadcast the left side, objects on the left
            (SpatialIndexExec(planLater(left), b, indexType, distanceOnIndexSide), planLater(right), a, RightSide)
          case (RightSide, false) => // Broadcast the right side, windows on the left
            (planLater(left), SpatialIndexExec(planLater(right), b, indexType, distanceOnIndexSide), a, LeftSide)
          case (RightSide, true) => // Broadcast the right side, objects on the left
            (planLater(left), SpatialIndexExec(planLater(right), a, indexType, distanceOnIndexSide), b, RightSide)
        }
        BroadcastIndexJoinExec(leftPlan, rightPlan, streamShape, broadcastSide.get, windowSide, joinType,
          spatialPredicate, extraCondition, distanceOnStreamSide) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }

  /**
   * Check if the given condition is an equi-join between the given plans. This method basically replicates
   * the logic of [[org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys.unapply]] but it does not
   * populate the join keys.
   *
   * @param left left side of the join
   * @param right right side of the join
   * @param condition join condition
   * @return true if the condition is an equi-join between the given plans
   */
  private def isEquiJoin(left: LogicalPlan, right: LogicalPlan, condition: Option[Expression]): Boolean = {
    val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
    predicates.exists {
      case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => false
      case EqualTo(l, r) if matches(l, left) && matches(r, right) => true
      case EqualTo(l, r) if matches(l, right) && matches(r, left) => true
      case EqualNullSafe(l, r) if matches(l, left) && matches(r, right) => true
      case EqualNullSafe(l, r) if matches(l, right) && matches(r, left) => true
      case _ => false
    }
  }
}
