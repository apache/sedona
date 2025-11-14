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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, BooleanType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import scala.util.parsing.combinator._

/**
 * Barrier function to prevent filter pushdown and control predicate evaluation order. Takes a
 * boolean expression string followed by pairs of variable names and their values.
 *
 * Usage: barrier(expression, var_name1, var_value1, var_name2, var_value2, ...) Example:
 * barrier('rating > 4.0 AND stars >= 4', 'rating', r.rating, 'stars', h.stars)
 *
 * Extends CodegenFallback to prevent Catalyst optimizer from pushing this filter through joins.
 * CodegenFallback makes this expression opaque to optimization rules, ensuring it evaluates at
 * runtime in its original position within the query plan.
 */
private[apache] case class Barrier(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback {

  override def nullable: Boolean = false

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  override def eval(input: InternalRow): Any = {
    // Get the expression string
    val exprString = inputExpressions.head.eval(input) match {
      case s: UTF8String => s.toString
      case null => throw new IllegalArgumentException("Barrier expression cannot be null")
      case other =>
        throw new IllegalArgumentException(
          s"Barrier expression must be a string, got: ${other.getClass}")
    }

    // Build variable map from pairs
    val varMap = scala.collection.mutable.Map[String, Any]()
    var i = 1
    while (i < inputExpressions.length) {
      if (i + 1 >= inputExpressions.length) {
        throw new IllegalArgumentException(
          "Barrier function requires pairs of variable names and values")
      }

      val varName = inputExpressions(i).eval(input) match {
        case s: UTF8String => s.toString
        case null => throw new IllegalArgumentException("Variable name cannot be null")
        case other =>
          throw new IllegalArgumentException(
            s"Variable name must be a string, got: ${other.getClass}")
      }

      val varValue = inputExpressions(i + 1).eval(input)
      varMap(varName) = varValue
      i += 2
    }

    // Evaluate the expression with variable substitution
    evaluateBooleanExpression(exprString, varMap.toMap)
  }

  /**
   * Evaluates a boolean expression string with variable substitution. Supports basic comparison
   * operators and logical operators (AND, OR, NOT).
   */
  private def evaluateBooleanExpression(
      expression: String,
      variables: Map[String, Any]): Boolean = {
    val parser = new BooleanExpressionParser(variables)
    parser.parseExpression(expression) match {
      case parser.Success(result, _) => result
      case parser.NoSuccess(msg, _) =>
        throw new IllegalArgumentException(s"Failed to parse barrier expression: $msg")
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Parser for boolean expressions in barrier function. Supports comparison operators: =, !=, <>,
 * <, <=, >, >= Supports logical operators: AND, OR, NOT Supports parentheses for grouping
 */
private class BooleanExpressionParser(variables: Map[String, Any]) extends JavaTokenParsers {

  // Pre-compiled regex patterns for better performance
  private val truePattern = "(?i)true".r
  private val falsePattern = "(?i)false".r
  private val nullPattern = "(?i)null".r
  private val andPattern = "(?i)AND".r
  private val orPattern = "(?i)OR".r
  private val notPattern = "(?i)NOT".r

  def parseExpression(expr: String): ParseResult[Boolean] = parseAll(boolExpr, expr)

  def boolExpr: Parser[Boolean] = orExpr

  def orExpr: Parser[Boolean] = andExpr ~ rep(orPattern ~> andExpr) ^^ { case left ~ rights =>
    rights.foldLeft(left)(_ || _)
  }

  def andExpr: Parser[Boolean] = notExpr ~ rep(andPattern ~> notExpr) ^^ { case left ~ rights =>
    rights.foldLeft(left)(_ && _)
  }

  def notExpr: Parser[Boolean] =
    notPattern ~> notExpr ^^ (!_) |
      primaryExpr

  def primaryExpr: Parser[Boolean] =
    "(" ~> boolExpr <~ ")" |
      attempt(comparison) |
      booleanValue

  def comparison: Parser[Boolean] = value ~ compOp ~ value ^^ { case left ~ op ~ right =>
    compareValues(left, op, right)
  }

  def attempt[T](p: Parser[T]): Parser[T] = Parser { in =>
    p(in) match {
      case s @ Success(_, _) => s
      case _ => Failure("", in)
    }
  }

  def booleanValue: Parser[Boolean] =
    truePattern ^^ (_ => true) |
      falsePattern ^^ (_ => false) |
      ident.filter(id => !id.toUpperCase.matches("AND|OR|NOT")) ^^ { name =>
        variables.get(name) match {
          case Some(b: Boolean) => b
          case Some(other) =>
            throw new IllegalArgumentException(s"Expected boolean value for $name, got: $other")
          case None =>
            throw new IllegalArgumentException(s"Unknown variable: $name")
        }
      }

  def compOp: Parser[String] = ">=" | "<=" | "!=" | "<>" | "=" | ">" | "<"

  def value: Parser[Any] =
    floatingPointNumber ^^ (_.toDouble) |
      wholeNumber ^^ (_.toLong) |
      stringLiteral ^^ (s => s.substring(1, s.length - 1)) | // Remove quotes
      truePattern ^^ (_ => true) |
      falsePattern ^^ (_ => false) |
      nullPattern ^^ (_ => null) |
      ident.filter(id => !id.toUpperCase.matches("AND|OR|NOT")) ^^ (name =>
        variables.getOrElse(name, throw new IllegalArgumentException(s"Unknown variable: $name")))

  private def compareValues(left: Any, op: String, right: Any): Boolean = {
    (left, right) match {
      case (null, null) => op == "=" || op == "<=" || op == ">="
      case (null, _) | (_, null) => op == "!=" || op == "<>"
      case _ =>
        val comparison = compareNonNull(left, right)
        op match {
          case "=" => comparison == 0
          case "!=" | "<>" => comparison != 0
          case "<" => comparison < 0
          case "<=" => comparison <= 0
          case ">" => comparison > 0
          case ">=" => comparison >= 0
        }
    }
  }

  private def compareNonNull(left: Any, right: Any): Int = {
    (left, right) match {
      case (l: Number, r: Number) =>
        val ld = l.doubleValue()
        val rd = r.doubleValue()
        if (ld < rd) -1 else if (ld > rd) 1 else 0
      case (l: String, r: String) => l.compareTo(r)
      case (l: Boolean, r: Boolean) => l.compareTo(r)
      case _ =>
        // Try to compare as strings as a fallback
        left.toString.compareTo(right.toString)
    }
  }
}
