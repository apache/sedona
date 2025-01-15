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
package org.apache.spark.sql.sedona_sql.optimization

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sedona_sql.expressions.ST_GeoStatsFunction
import org.apache.spark.sql.sedona_sql.plans.logical.EvalGeoStatsFunction

import scala.collection.mutable

/**
 * Extracts GeoStats functions from operators, rewriting the query plan so that the geo-stats
 * functions can be evaluated alone in its own physical executors.
 */
object ExtractGeoStatsFunctions extends Rule[LogicalPlan] {
  var geoStatsResultCount = 0

  private def collectGeoStatsFunctionsFromExpressions(
      expressions: Seq[Expression]): Seq[ST_GeoStatsFunction] = {
    def collectGeoStatsFunctions(expr: Expression): Seq[ST_GeoStatsFunction] = expr match {
      case expr: ST_GeoStatsFunction => Seq(expr)
      case e => e.children.flatMap(collectGeoStatsFunctions)
    }
    expressions.flatMap(collectGeoStatsFunctions)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // SPARK-26293: A subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as geo-stats functions only needs to be extracted once.
    case s: Subquery if s.correlated => plan
    case _ =>
      plan.transformUp {
        case p: EvalGeoStatsFunction => p
        case plan: LogicalPlan => extract(plan)
      }
  }

  private def canonicalizeDeterministic(u: ST_GeoStatsFunction) = {
    if (u.deterministic) {
      u.canonicalized.asInstanceOf[ST_GeoStatsFunction]
    } else {
      u
    }
  }

  /**
   * Extract all the geo-stats functions from the current operator and evaluate them before the
   * operator.
   */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val geoStatsFuncs = plan match {
      case e: EvalGeoStatsFunction =>
        collectGeoStatsFunctionsFromExpressions(e.function.children)
      case _ =>
        ExpressionSet(collectGeoStatsFunctionsFromExpressions(plan.expressions))
          // ignore the ST_GeoStatsFunction that come from second/third aggregate, which is not used
          .filter(func => func.references.subsetOf(plan.inputSet))
          .filter(func =>
            plan.children.exists(child => func.references.subsetOf(child.outputSet)))
          .toSeq
          .asInstanceOf[Seq[ST_GeoStatsFunction]]
    }

    if (geoStatsFuncs.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      // Transform the first geo-stats function we have found. We'll call extract recursively later
      // to transform the rest.
      val geoStatsFunc = geoStatsFuncs.head

      val attributeMap = mutable.HashMap[ST_GeoStatsFunction, Expression]()
      // Rewrite the child that has the input required for the UDF
      val newChildren = plan.children.map { child =>
        if (geoStatsFunc.references.subsetOf(child.outputSet)) {
          geoStatsResultCount += 1
          val resultAttr =
            AttributeReference(f"geoStatsResult$geoStatsResultCount", geoStatsFunc.dataType)()
          val evaluation = EvalGeoStatsFunction(geoStatsFunc, Seq(resultAttr), child)
          attributeMap += (canonicalizeDeterministic(geoStatsFunc) -> resultAttr)
          extract(evaluation) // handle nested geo-stats functions
        } else {
          child
        }
      }

      // Replace the geo stats function call with the newly created geoStatsResult attribute
      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case p: ST_GeoStatsFunction => attributeMap.getOrElse(canonicalizeDeterministic(p), p)
      }

      // extract remaining geo-stats functions recursively
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        // Trim away the new UDF value if it was only used for filtering or something.
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}
