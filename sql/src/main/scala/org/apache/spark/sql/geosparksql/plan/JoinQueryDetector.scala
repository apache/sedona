package org.apache.spark.sql.geosparksql.plan

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, Join_Contains, ST_Contains}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType}
//import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}
/**
  * Plans `SpatialJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
  * and ST_Intersects(a, b).
  *
  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
  */
object SpatialJoinSelection extends Strategy {

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


  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // ST_Contains(a, b) - a contains b
    case Join(left, right, Inner, Some(ST_Contains(_, _, children))) =>
      planSpatialJoin(left, right, children, false)

    // ST_Intersects(a, b) - a and b intersect
   // case Join(left, right, Inner, Some(Join_Contains(_, _, children))) =>
   //   planSpatialJoin(left, right, children, true)

    case _ =>
      Nil
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
        SpatialJoinExec(planLater(planA), planLater(planB), a, b, intersects, extraCondition) :: Nil
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
                               extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((planA, planB)) =>
        if (radius.references.isEmpty || matches(radius, planA)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planA), planLater(planB), a, b, radius, extraCondition) :: Nil
        } else if (matches(radius, planB)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planB), planLater(planA), b, a, radius, extraCondition) :: Nil
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
