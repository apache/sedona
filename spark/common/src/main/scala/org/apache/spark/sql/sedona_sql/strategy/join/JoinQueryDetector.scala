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
import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, EqualTo, Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.{ST_KNN, _}
import org.apache.spark.sql.sedona_sql.expressions.raster._
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.{matchDistanceExpressionToJoinSide, matchExpressionsToPlans, matches}

case class JoinQueryDetection(
    left: LogicalPlan,
    right: LogicalPlan,
    leftShape: Expression,
    rightShape: Expression,
    spatialPredicate: SpatialPredicate,
    isGeography: Boolean,
    extraCondition: Option[Expression] = None,
    distance: Option[Expression] = None)

/**
 * Plans `RangeJoinExec` for inner joins on spatial relationships ST_Contains(a, b) and
 * ST_Intersects(a, b).
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
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.CONTAINS,
            false,
            extraCondition))
      case ST_Intersects(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.INTERSECTS,
            false,
            extraCondition))
      case ST_Within(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.WITHIN,
            false,
            extraCondition))
      case ST_Covers(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.COVERS,
            false,
            extraCondition))
      case ST_CoveredBy(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.COVERED_BY,
            false,
            extraCondition))
      case ST_Overlaps(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.OVERLAPS,
            false,
            extraCondition))
      case ST_Touches(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.TOUCHES,
            false,
            extraCondition))
      case ST_Equals(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.EQUALS,
            false,
            extraCondition))
      case ST_Crosses(Seq(leftShape, rightShape)) =>
        Some(
          JoinQueryDetection(
            left,
            right,
            leftShape,
            rightShape,
            SpatialPredicate.CROSSES,
            false,
            extraCondition))
      case _ => None
    }
  }

  private def getRasterJoinDetection(
      left: LogicalPlan,
      right: LogicalPlan,
      predicate: RS_Predicate,
      extraCondition: Option[Expression] = None): Option[JoinQueryDetection] = {
    // The joined shapes are coarse-grained envelopes of raster or geometry. We can only test for intersections in
    // the spatial join no matter what the actual RS predicate is. The actual raster predicate is in `condition`
    // and will be used for refining the join result.
    val leftShape = predicate.children.head
    val rightShape = predicate.children(1)
    val condition = extraCondition.map(And(_, predicate)).getOrElse(predicate)
    Some(
      JoinQueryDetection(
        left,
        right,
        leftShape,
        rightShape,
        SpatialPredicate.INTERSECTS,
        false,
        Some(condition)))
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Join(left, right, joinType, condition, JoinHint(leftHint, rightHint))
        if optimizationEnabled(left, right, condition) =>
      var broadcastLeft = leftHint.exists(_.strategy.contains(BROADCAST))
      var broadcastRight = rightHint.exists(_.strategy.contains(BROADCAST))

      /*
       * If either side is small we can automatically broadcast just like Spark does.
       * This only applies to inner joins as there are no optimized fallback plan for other join types.
       * It's better that users are explicit about broadcasting for other join types than seeing wildly different behavior
       * depending on data size.
       */
      if (!broadcastLeft && !broadcastRight && joinType == Inner) {
        val canAutoBroadCastLeft = canAutoBroadcastBySize(left)
        val canAutoBroadCastRight = canAutoBroadcastBySize(right)
        if (canAutoBroadCastLeft && canAutoBroadCastRight) {
          // Both sides can be broadcast. Choose the smallest side.
          broadcastLeft = left.stats.sizeInBytes <= right.stats.sizeInBytes
          broadcastRight = !broadcastLeft
        } else {
          broadcastLeft = canAutoBroadCastLeft
          broadcastRight = canAutoBroadCastRight
        }
      }

      // Check if the filters in the plans are supported
      checkPlanFilters(left)
      checkPlanFilters(right)

      val joinConditionMatcher = OptimizableJoinCondition(left, right)
      val queryDetection: Option[JoinQueryDetection] = condition.flatMap {
        case joinConditionMatcher(predicate, extraCondition) =>
          predicate match {
            case pred: ST_Predicate =>
              getJoinDetection(left, right, pred, extraCondition)
            case pred: RS_Predicate =>
              getRasterJoinDetection(left, right, pred, extraCondition)
            case ST_DWithin(Seq(leftShape, rightShape, distance)) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  isGeography = false,
                  condition,
                  Some(distance)))
            case ST_DWithin(Seq(leftShape, rightShape, distance, useSpheroid)) =>
              val useSpheroidUnwrapped = useSpheroid.eval().asInstanceOf[Boolean]
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  isGeography = useSpheroidUnwrapped,
                  condition,
                  Some(distance)))

            // For distance joins we execute the actual predicate (condition) and not only extraConditions.
            // ST_Distance
            case LessThanOrEqual(ST_Distance(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))
            case LessThan(ST_Distance(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))

            // ST_DistanceSphere
            case LessThanOrEqual(ST_DistanceSphere(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  true,
                  condition,
                  Some(distance)))
            case LessThan(ST_DistanceSphere(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  true,
                  condition,
                  Some(distance)))

            // ST_DistanceSpheroid
            case LessThanOrEqual(ST_DistanceSpheroid(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  true,
                  condition,
                  Some(distance)))
            case LessThan(ST_DistanceSpheroid(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  true,
                  condition,
                  Some(distance)))

            // ST_HausdorffDistance
            case LessThanOrEqual(ST_HausdorffDistance(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))
            case LessThan(ST_HausdorffDistance(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))
            case LessThanOrEqual(
                  ST_HausdorffDistance(Seq(leftShape, rightShape, densityFrac)),
                  distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))
            case LessThan(
                  ST_HausdorffDistance(Seq(leftShape, rightShape, densityFrac)),
                  distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))

            // ST_FrechetDistance
            case LessThanOrEqual(ST_FrechetDistance(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))
            case LessThan(ST_FrechetDistance(Seq(leftShape, rightShape)), distance) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  SpatialPredicate.INTERSECTS,
                  false,
                  condition,
                  Some(distance)))

            // ST_KNN
            case ST_KNN(Seq(leftShape, rightShape, k)) =>
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  spatialPredicate = SpatialPredicate.KNN,
                  isGeography = false,
                  condition,
                  Some(k)))

            case ST_KNN(Seq(leftShape, rightShape, k, useSpheroid)) =>
              val useSpheroidUnwrapped = useSpheroid.eval().asInstanceOf[Boolean]
              Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  spatialPredicate = SpatialPredicate.KNN,
                  isGeography = useSpheroidUnwrapped,
                  condition,
                  Some(k)))

            case _ => None
          }
        case _ => None
      }

      val sedonaConf = new SedonaConf(sparkSession.conf)

      if ((broadcastLeft || broadcastRight) && sedonaConf.getUseIndex) {
        queryDetection match {
          case Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  spatialPredicate,
                  isGeography,
                  extraCondition,
                  distance)) =>
            planBroadcastJoin(
              left,
              right,
              Seq(leftShape, rightShape),
              joinType,
              spatialPredicate,
              sedonaConf.getIndexType,
              broadcastLeft,
              broadcastRight,
              isGeography,
              extraCondition,
              distance)
          case _ =>
            Nil
        }
      } else {
        queryDetection match {
          case Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  spatialPredicate,
                  isGeography,
                  extraCondition,
                  None)) =>
            planSpatialJoin(
              left,
              right,
              Seq(leftShape, rightShape),
              joinType,
              spatialPredicate,
              extraCondition)
          case Some(
                JoinQueryDetection(
                  left,
                  right,
                  leftShape,
                  rightShape,
                  spatialPredicate,
                  isGeography,
                  extraCondition,
                  Some(distance))) =>
            Option(spatialPredicate) match {
              case Some(SpatialPredicate.KNN) =>
                planKNNJoin(
                  left,
                  right,
                  Seq(leftShape, rightShape),
                  joinType,
                  distance,
                  isGeography,
                  condition.get,
                  extraCondition)
              case Some(predicate) =>
                planDistanceJoin(
                  left,
                  right,
                  Seq(leftShape, rightShape),
                  joinType,
                  distance,
                  spatialPredicate,
                  isGeography,
                  extraCondition)
              case None =>
                Nil
            }
          case None =>
            Nil
        }
      }
    case _ =>
      Nil
  }

  private def optimizationEnabled(
      left: LogicalPlan,
      right: LogicalPlan,
      condition: Option[Expression]): Boolean = {
    val sedonaConf = new SedonaConf(sparkSession.conf)
    sedonaConf.getSpatialJoinOptimizationMode match {
      case SpatialJoinOptimizationMode.NONE => false
      case SpatialJoinOptimizationMode.ALL => true
      case SpatialJoinOptimizationMode.NONEQUI => !isEquiJoin(left, right, condition)
      case mode =>
        throw new IllegalArgumentException(s"Unknown spatial join optimization mode: $mode")
    }
  }

  private def canAutoBroadcastBySize(plan: LogicalPlan) =
    plan.stats.sizeInBytes != 0 && plan.stats.sizeInBytes <= SedonaConf.fromActiveSession.getAutoBroadcastJoinThreshold

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

    val isRaster = a.dataType.isInstanceOf[RasterUDT] || b.dataType.isInstanceOf[RasterUDT]
    val relationship = if (isRaster) s"RS_$spatialPredicate" else s"ST_$spatialPredicate"
    matchExpressionsToPlans(a, b, left, right) match {
      case Some((_, _, false)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        RangeJoinExec(
          planLater(left),
          planLater(right),
          a,
          b,
          spatialPredicate,
          extraCondition) :: Nil
      case Some((_, _, true)) =>
        logInfo(
          s"Planning spatial join for $relationship relationship with swapped left and right shapes")
        val invSpatialPredicate = SpatialPredicate.inverse(spatialPredicate)
        RangeJoinExec(
          planLater(left),
          planLater(right),
          b,
          a,
          invSpatialPredicate,
          extraCondition) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }

  private def planKNNJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      children: Seq[Expression],
      joinType: JoinType,
      distance: Expression,
      isGeography: Boolean,
      condition: Expression,
      extraCondition: Option[Expression] = None): Seq[SparkPlan] = {

    if (joinType != Inner) {
      return Nil
    }

    val leftShape = children.head
    val rightShape = children.tail.head

    val querySide = getKNNQuerySide(left, leftShape)
    val objectSidePlan = if (querySide == LeftSide) right else left

    checkObjectPlanFilterPushdown(objectSidePlan)

    logInfo(
      "Planning knn join, left side is for queries and right size is for the object to be searched")
    KNNJoinExec(
      planLater(left),
      planLater(right),
      leftShape,
      rightShape,
      joinType,
      distance,
      spatialPredicate = null,
      isGeography,
      condition,
      extractExtraKNNJoinCondition(condition)) :: Nil
  }

  private def planDistanceJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      children: Seq[Expression],
      joinType: JoinType,
      distance: Expression,
      spatialPredicate: SpatialPredicate,
      isGeography: Boolean,
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
            DistanceJoinExec(
              planLater(left),
              planLater(right),
              leftShape,
              rightShape,
              distance,
              distanceBoundToLeft = true,
              spatialPredicate,
              isGeography,
              extraCondition) :: Nil
          case Some(RightSide) =>
            logInfo("Planning spatial distance join, distance bound to right relation")
            DistanceJoinExec(
              planLater(left),
              planLater(right),
              leftShape,
              rightShape,
              distance,
              distanceBoundToLeft = false,
              spatialPredicate,
              isGeography,
              extraCondition) :: Nil
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

  private def extractExtraKNNJoinCondition(condition: Expression): Option[Expression] = {
    condition match {
      case and: And =>
        // Check both left and right sides for ST_KNN or ST_AKNN
        if (and.left.isInstanceOf[ST_KNN]) {
          Some(and.right)
        } else if (and.right.isInstanceOf[ST_KNN]) {
          Some(and.left)
        } else {
          None
        }
      case _: ST_KNN =>
        None
      case _ =>
        Some(condition)
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
      isGeography: Boolean,
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

    if (spatialPredicate == SpatialPredicate.KNN) {
      {
        val leftShape = children.head
        val rightShape = children.tail.head

        val querySide = getKNNQuerySide(left, leftShape)
        val objectSidePlan = if (querySide == LeftSide) right else left

        checkObjectPlanFilterPushdown(objectSidePlan)

        if (querySide == broadcastSide.get) {
          // broadcast is on query side
          return BroadcastQuerySideKNNJoinExec(
            planLater(left),
            planLater(right),
            leftShape,
            rightShape,
            broadcastSide.get,
            joinType,
            k = distance.get,
            useApproximate = false,
            spatialPredicate,
            isGeography = false,
            condition = null,
            extraCondition = None) :: Nil
        } else {
          // broadcast is on object side
          return BroadcastObjectSideKNNJoinExec(
            planLater(left),
            planLater(right),
            leftShape,
            rightShape,
            broadcastSide.get,
            joinType,
            k = distance.get,
            useApproximate = false,
            spatialPredicate,
            isGeography = false,
            condition = null,
            extraCondition = None) :: Nil
        }
      }
    }

    val a = children.head
    val b = children.tail.head
    val isRasterPredicate =
      a.dataType.isInstanceOf[RasterUDT] || b.dataType.isInstanceOf[RasterUDT]

    val relationship =
      (distance, spatialPredicate, isGeography, extraCondition, isRasterPredicate) match {
        case (Some(_), SpatialPredicate.INTERSECTS, false, Some(ST_DWithin(Seq(_*))), false) =>
          "ST_DWithin"
        case (Some(_), SpatialPredicate.INTERSECTS, false, _, false) => "ST_Distance <="
        case (Some(_), _, false, _, false) => "ST_Distance <"
        case (Some(_), SpatialPredicate.INTERSECTS, true, Some(ST_DWithin(Seq(_*))), false) =>
          "ST_DWithin(useSpheroid = true)"
        case (Some(_), SpatialPredicate.INTERSECTS, true, _, false) =>
          "ST_Distance (Geography) <="
        case (Some(_), _, true, _, false) => "ST_Distance (Geography) <"
        case (None, _, false, _, false) => s"ST_$spatialPredicate"
        case (None, _, false, _, true) => s"RS_$spatialPredicate"
      }
    val (distanceOnIndexSide, distanceOnStreamSide) = distance
      .map { distanceExpr =>
        matchDistanceExpressionToJoinSide(distanceExpr, left, right) match {
          case Some(side) =>
            if (broadcastSide.get == side) (Some(distanceExpr), None)
            else if (distanceExpr.references.isEmpty) (Some(distanceExpr), None)
            else (None, Some(distanceExpr))
          case _ =>
            throw new IllegalArgumentException(
              "Distance expression must be bound to one side of the join")
        }
      }
      .getOrElse((None, None))

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((_, _, swapped)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        val (leftPlan, rightPlan, streamShape, windowSide) = (broadcastSide.get, swapped) match {
          case (LeftSide, false) => // Broadcast the left side, windows on the left
            (
              SpatialIndexExec(
                planLater(left),
                a,
                indexType,
                isRasterPredicate,
                isGeography,
                distanceOnIndexSide),
              planLater(right),
              b,
              LeftSide)
          case (LeftSide, true) => // Broadcast the left side, objects on the left
            (
              SpatialIndexExec(
                planLater(left),
                b,
                indexType,
                isRasterPredicate,
                isGeography,
                distanceOnIndexSide),
              planLater(right),
              a,
              RightSide)
          case (RightSide, false) => // Broadcast the right side, windows on the left
            (
              planLater(left),
              SpatialIndexExec(
                planLater(right),
                b,
                indexType,
                isRasterPredicate,
                isGeography,
                distanceOnIndexSide),
              a,
              LeftSide)
          case (RightSide, true) => // Broadcast the right side, objects on the left
            (
              planLater(left),
              SpatialIndexExec(
                planLater(right),
                a,
                indexType,
                isRasterPredicate,
                isGeography,
                distanceOnIndexSide),
              b,
              RightSide)
        }
        BroadcastIndexJoinExec(
          leftPlan,
          rightPlan,
          streamShape,
          broadcastSide.get,
          windowSide,
          joinType,
          spatialPredicate,
          extraCondition,
          distanceOnStreamSide) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }

  /**
   * Gets the query and object plans based on the left shape.
   *
   * This method checks if the left shape is part of the left or right plan and returns the query
   * and object plans accordingly.
   *
   * @param leftShape
   *   The left shape expression.
   * @return
   *   The join side where the left shape is located.
   */
  private def getKNNQuerySide(left: LogicalPlan, leftShape: Expression) = {
    val isLeftQuerySide =
      left.toString().toLowerCase().contains(leftShape.toString().toLowerCase())
    if (isLeftQuerySide) {
      LeftSide
    } else {
      RightSide
    }
  }

  /**
   * Check if the given condition is an equi-join between the given plans. This method basically
   * replicates the logic of
   * [[org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys.unapply]] but it does not
   * populate the join keys.
   *
   * @param left
   *   left side of the join
   * @param right
   *   right side of the join
   * @param condition
   *   join condition
   * @return
   *   true if the condition is an equi-join between the given plans
   */
  private def isEquiJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      condition: Option[Expression]): Boolean = {
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

  /**
   * Find the first filter expression in the given plan.
   * @param plan
   *   logical plan
   * @return
   *   filter expression if found, None otherwise
   */
  private def findFilterExpression(plan: LogicalPlan): Option[String] = {
    plan match {
      case Filter(condition, _) => Some(condition.getClass.getSimpleName)
      case _ => plan.children.flatMap(findFilterExpression).headOption
    }
  }

  /**
   * Check if the filters in the given plan are supported.
   * @param plan
   *   logical plan
   */
  private def checkPlanFilters(plan: LogicalPlan): Unit = {
    val unsupportedFilters = Map(
      "ST_KNN" -> "ST_KNN filter is not yet supported in the join query")

    val filterInExpression: Option[String] = findFilterExpression(plan)

    filterInExpression match {
      case Some(filter) if unsupportedFilters.contains(filter) =>
        throw new UnsupportedOperationException(unsupportedFilters(filter))
      case _ => // Do nothing
    }
  }

  /**
   * Check if the given logic plan has a filter that can be pushed down to the data source.
   * @param plan
   * @return
   */
  private def containPlanFilterPushdown(plan: LogicalPlan): Boolean = {
    plan match {
      case Filter(condition, child) =>
        // If a Filter node is found, check if it is applied to a scan relation (indicating potential pushdown)
        child match {
          case _: LogicalRelation | _: DataSourceV2ScanRelation =>
            true
          case _ => containPlanFilterPushdown(child)
        }

      // Continue recursively checking for other potential cases
      case Project(_, child) => containPlanFilterPushdown(child)
      case Join(left, right, _, _, _) =>
        containPlanFilterPushdown(left) || containPlanFilterPushdown(right)
      case Aggregate(_, _, child) => containPlanFilterPushdown(child)
      case _: LogicalRelation | _: DataSourceV2ScanRelation => false

      // Default case to check other children
      case other => other.children.exists(containPlanFilterPushdown)
    }
  }

  /**
   * Check if the given plan has a filter that can be pushed down to the object side of the KNN
   * join. Print a warning if a filter pushdown is detected.
   * @param objectSidePlan
   */
  private def checkObjectPlanFilterPushdown(objectSidePlan: LogicalPlan): Unit = {
    if (containPlanFilterPushdown(objectSidePlan)) {
      val warnings = Seq(
        "Warning: One or more filter pushdowns have been detected on the object side of the KNN join. \n" +
          "These filters will be applied to the object side reader before the KNN join is executed. \n" +
          "If you intend to apply the filters after the KNN join, please ensure that you materialize the KNN join results before applying the filters. \n" +
          "For example, you can use the following approach:\n\n" +

          // Scala Example
          "Scala Example:\n" +
          "val knnResult = knnJoinDF.cache()\n" +
          "val filteredResult = knnResult.filter(condition)\n\n" +

          // SQL Example
          "SQL Example:\n" +
          "CREATE OR REPLACE TEMP VIEW knnResult AS\n" +
          "SELECT * FROM (\n" +
          "  -- Your KNN join SQL here\n" +
          ") AS knnView\n" +
          "CACHE TABLE knnResult;\n" +
          "SELECT * FROM knnResult WHERE condition;")
      logWarning(warnings.mkString("\n"))
      println(warnings.mkString("\n"))
    }
  }
}
