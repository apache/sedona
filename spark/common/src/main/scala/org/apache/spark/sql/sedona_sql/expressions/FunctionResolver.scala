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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * A utility object for resolving functions based on input argument types. See
 * [[FunctionResolver.resolveFunction]] for details.
 */
object FunctionResolver {

  /**
   * A utility function for selecting the function to be called from multiple function overloads.
   * The overall rule is:
   *
   *   1. If any of the input argument cannot be coerced to the parameter type, don't select that
   *      function.
   *   1. If there is a perfect match, return it.
   *   1. If there is no perfect match, return the function with the fewest coerced inputs.
   */
  def resolveFunction(
      expressions: Seq[Expression],
      functionOverloads: Seq[InferrableFunction]): InferrableFunction = {

    // If there's only one overload matches the arity of the expression, we'll simply use it.
    // The SQL analyzer will handle the ImplicitCastInputTypes trait and do type checking for us.
    val functionsWithMatchingArity =
      functionOverloads.filter(_.sparkInputTypes.length == expressions.length)
    if (functionsWithMatchingArity.length == 1) {
      return functionsWithMatchingArity.head
    } else if (functionsWithMatchingArity.isEmpty) {
      throw new IllegalArgumentException(
        s"No overloaded function accepts ${expressions.length} arguments")
    }

    val functionWithMatchResults = functionsWithMatchingArity.map { function =>
      val matchResult = matchFunctionToInputTypes(expressions, function)
      (function, matchResult)
    }
    // If there's a perfect match, return it; otherwise, return the function with the fewest
    // coerced inputs.
    val bestMatch = functionWithMatchResults.minBy { case (_, matchResult) =>
      matchResult match {
        case NotMatch => Int.MaxValue
        case PerfectMatch => 0
        case CoercedMatch(coercedInputs) => coercedInputs
      }
    }
    bestMatch match {
      case (_, NotMatch) =>
        val candidateTypesMsg = functionsWithMatchingArity
          .map { function =>
            "  (" + function.sparkInputTypes.mkString(", ") + ")"
          }
          .mkString("\n")
        throw new IllegalArgumentException(
          "Types of arguments does not match with function parameters. " +
            "Candidates are: \n" + candidateTypesMsg)
      case (function, _) =>
        // Make sure that there's no ambiguity in the best match.
        val ambiguousMatches = functionWithMatchResults.filter { case (_, matchResult) =>
          matchResult == bestMatch._2
        }
        if (ambiguousMatches.length == 1) {
          function
        } else {
          // Detected ambiguous matches, throw exception
          val candidateTypesMsg = ambiguousMatches
            .map { case (function, _) =>
              "  (" + function.sparkInputTypes.mkString(", ") + ")"
            }
            .mkString("\n")
          throw new IllegalArgumentException(
            "Ambiguous function call. Candidates are: \n" + candidateTypesMsg)
        }
    }
  }

  private sealed trait MatchResult
  private case object NotMatch extends MatchResult
  private case object PerfectMatch extends MatchResult
  private case class CoercedMatch(coercedInputs: Int) extends MatchResult

  private def matchFunctionToInputTypes(
      expressions: Seq[Expression],
      function: InferrableFunction): MatchResult = {
    if (expressions.length != function.sparkInputTypes.length) {
      NotMatch
    } else {
      val inputMatchResults =
        expressions.zip(function.sparkInputTypes).map { case (expr, parameterType) =>
          val argumentType = expr.dataType
          if (parameterType.acceptsType(argumentType)) {
            PerfectMatch
          } else {
            TypeCoercion.implicitCast(expr, parameterType) match {
              case Some(_) => CoercedMatch(1)
              case None => NotMatch
            }
          }
        }
      if (inputMatchResults.contains(NotMatch)) {
        NotMatch
      } else {
        val numCoercedInputs = inputMatchResults.map {
          case CoercedMatch(coercedInputs) => coercedInputs
          case _ => 0
        }.sum
        if (numCoercedInputs == 0) {
          PerfectMatch
        } else {
          CoercedMatch(numCoercedInputs)
        }
      }
    }
  }
}
