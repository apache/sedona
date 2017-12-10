package org.apache.spark.sql.geosparksql.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

/**
  *  ST_Contains(left, right) - left contains right
  *  or
  *  ST_Intersects(left, right) - left and right intersect
  *
  * @param left left side of the join
  * @param right right side of the join
  * @param leftShape expression for the first argument of ST_Contains or ST_Intersects
  * @param rightShape expression for the second argument of ST_Contains or ST_Intersects
  * @param intersects boolean indicating whether spatial relationship is 'intersects' (true)
  *                   or 'contains' (false)
  */
case class SpatialJoinExec(left: SparkPlan,
                           right: SparkPlan,
                           leftShape: Expression,
                           rightShape: Expression,
                           intersects: Boolean,
                           extraCondition: Option[Expression] = None)
    extends BinaryExecNode
    with SpatialJoin
    with Logging {}
