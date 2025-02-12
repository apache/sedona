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
import org.apache.spark.sql.sedona_sql.expressions.PhysicalFunction
import org.apache.spark.sql.sedona_sql.plans.logical.EvalPhysicalFunction

import scala.collection.mutable

/**
 * Extracts Physical functions from operators, rewriting the query plan so that the functions can
 * be evaluated alone in its own physical executors.
 */
object ExtractPhysicalFunctions extends Rule[LogicalPlan] {
  private var physicalFunctionResultCount = 0

  private def collectPhysicalFunctionsFromExpressions(
      expressions: Seq[Expression]): Seq[PhysicalFunction] = {
    def collectPhysicalFunctions(expr: Expression): Seq[PhysicalFunction] = expr match {
      case expr: PhysicalFunction => Seq(expr)
      case e => e.children.flatMap(collectPhysicalFunctions)
    }
    expressions.flatMap(collectPhysicalFunctions)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // SPARK-26293: A subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as physical functions only needs to be extracted once.
    case s: Subquery if s.correlated => plan
    case _ =>
      plan.transformUp {
        case p: EvalPhysicalFunction => p
        case plan: LogicalPlan => extract(plan)
      }
  }

  private def canonicalizeDeterministic(u: PhysicalFunction) = {
    if (u.deterministic) {
      u.canonicalized.asInstanceOf[PhysicalFunction]
    } else {
      u
    }
  }

  /**
   * Extract all the physical functions from the current operator and evaluate them before the
   * operator.
   */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val physicalFunctions = plan match {
      case e: EvalPhysicalFunction =>
        collectPhysicalFunctionsFromExpressions(e.function.children)
      case _ =>
        ExpressionSet(collectPhysicalFunctionsFromExpressions(plan.expressions))
          // ignore the PhysicalFunction that come from second/third aggregate, which is not used
          .filter(func => func.references.subsetOf(plan.inputSet))
          .filter(func =>
            plan.children.exists(child => func.references.subsetOf(child.outputSet)))
          .toSeq
          .asInstanceOf[Seq[PhysicalFunction]]
    }

    if (physicalFunctions.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      // Transform the first physical function we have found. We'll call extract recursively later
      // to transform the rest.
      val physicalFunction = physicalFunctions.head

      val attributeMap = mutable.HashMap[PhysicalFunction, Expression]()
      // Rewrite the child that has the input required for the UDF
      val newChildren = plan.children.map { child =>
        if (physicalFunction.references.subsetOf(child.outputSet)) {
          physicalFunctionResultCount += 1
          val resultAttr =
            AttributeReference(
              f"physicalFunctionResult$physicalFunctionResultCount",
              physicalFunction.dataType)()
          val evaluation = EvalPhysicalFunction(physicalFunction, Seq(resultAttr), child)
          attributeMap += (canonicalizeDeterministic(physicalFunction) -> resultAttr)
          extract(evaluation) // handle nested functions
        } else {
          child
        }
      }

      // Replace the physical function call with the newly created attribute
      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case p: PhysicalFunction => attributeMap.getOrElse(canonicalizeDeterministic(p), p)
      }

      // extract remaining physical functions recursively
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
