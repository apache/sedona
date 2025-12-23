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
package org.apache.spark.sql.udf

import org.apache.sedona.sql.UDF.PythonEvalType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionSet, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PYTHON_UDF

import scala.collection.mutable

// That rule extracts scalar Python UDFs, currently Apache Spark has
// assert on types which blocks using the vectorized udfs with geometry type
class ExtractSedonaUDFRule extends Rule[LogicalPlan] with Logging {

  private def hasScalarPythonUDF(e: Expression): Boolean = {
    e.exists(PythonUDF.isScalarPythonUDF)
  }

  @scala.annotation.tailrec
  private def canEvaluateInPython(e: PythonUDF): Boolean = {
    e.children match {
      case Seq(u: PythonUDF) => e.evalType == u.evalType && canEvaluateInPython(u)
      case children => !children.exists(hasScalarPythonUDF)
    }
  }

  def isScalarPythonUDF(e: Expression): Boolean = {
    e.isInstanceOf[PythonUDF] && PythonEvalType.evals.contains(e.asInstanceOf[PythonUDF].evalType)
  }

  private def collectEvaluableUDFsFromExpressions(
      expressions: Seq[Expression]): Seq[PythonUDF] = {

    var firstVisitedScalarUDFEvalType: Option[Int] = None

    def canChainUDF(evalType: Int): Boolean = {
      evalType == firstVisitedScalarUDFEvalType.get
    }

    def collectEvaluableUDFs(expr: Expression): Seq[PythonUDF] = expr match {
      case udf: PythonUDF
          if isScalarPythonUDF(udf) && canEvaluateInPython(udf)
            && firstVisitedScalarUDFEvalType.isEmpty =>
        firstVisitedScalarUDFEvalType = Some(udf.evalType)
        Seq(udf)
      case udf: PythonUDF
          if isScalarPythonUDF(udf) && canEvaluateInPython(udf)
            && canChainUDF(udf.evalType) =>
        Seq(udf)
      case e => e.children.flatMap(collectEvaluableUDFs)
    }

    expressions.flatMap(collectEvaluableUDFs)
  }

  private var hasFailedBefore: Boolean = false

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan

    case _ =>
      try {
        plan.transformUpWithPruning(_.containsPattern(PYTHON_UDF)) {
          case p: SedonaArrowEvalPython => p

          case plan: LogicalPlan => extract(plan)
        }
      } catch {
        case e: Throwable =>
          if (!hasFailedBefore) {
            log.warn(
              s"Vectorized UDF feature won't be available due to plan transformation error.")
            log.warn(
              s"Failed to extract Sedona UDFs from plan: ${plan.treeString}\n" +
                s"Exception: ${e.getMessage}",
              e)
            hasFailedBefore = true
          }
          plan
      }
  }

  private def canonicalizeDeterministic(u: PythonUDF) = {
    if (u.deterministic) {
      u.canonicalized.asInstanceOf[PythonUDF]
    } else {
      u
    }
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = ExpressionSet(collectEvaluableUDFsFromExpressions(plan.expressions))
      .filter(udf => udf.references.subsetOf(plan.inputSet))
      .toSeq
      .asInstanceOf[Seq[PythonUDF]]

    udfs match {
      case Seq() => plan
      case _ => resolveUDFs(plan, udfs)
    }
  }

  def resolveUDFs(plan: LogicalPlan, udfs: Seq[PythonUDF]): LogicalPlan = {
    val attributeMap = mutable.HashMap[PythonUDF, Expression]()

    val newChildren = adjustAttributeMap(plan, udfs, attributeMap)

    udfs.map(canonicalizeDeterministic).filterNot(attributeMap.contains).foreach { udf =>
      throw new IllegalStateException(
        s"Invalid PythonUDF $udf, requires attributes from more than one child.")
    }

    val rewritten = plan.withNewChildren(newChildren).transformExpressions { case p: PythonUDF =>
      attributeMap.getOrElse(canonicalizeDeterministic(p), p)
    }

    val newPlan = extract(rewritten)
    if (newPlan.output != plan.output) {
      Project(plan.output, newPlan)
    } else {
      newPlan
    }
  }

  def adjustAttributeMap(
      plan: LogicalPlan,
      udfs: Seq[PythonUDF],
      attributeMap: mutable.HashMap[PythonUDF, Expression]): Seq[LogicalPlan] = {
    plan.children.map { child =>
      val validUdfs = udfs.filter { udf =>
        udf.references.subsetOf(child.outputSet)
      }

      if (validUdfs.nonEmpty) {
        require(
          validUdfs.forall(isScalarPythonUDF),
          "Can only extract scalar vectorized udf or sql batch udf")

        val resultAttrs = validUdfs.zipWithIndex.map { case (u, i) =>
          AttributeReference(s"pythonUDF$i", u.dataType)()
        }

        val evalTypes = validUdfs.map(_.evalType).toSet
        if (evalTypes.size != 1) {
          throw new IllegalStateException(
            "Expected udfs have the same evalType but got different evalTypes: " +
              evalTypes.mkString(","))
        }
        val evalType = evalTypes.head
        if (!PythonEvalType.evals().contains(evalType)) {
          throw new IllegalStateException(s"Unexpected UDF evalType: $evalType")
        }

        val evaluation = SedonaArrowEvalPython(validUdfs, resultAttrs, child, evalType)

        attributeMap ++= validUdfs.map(canonicalizeDeterministic).zip(resultAttrs)
        evaluation
      } else {
        child
      }
    }
  }
}
