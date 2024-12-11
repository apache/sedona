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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.execution.aggregate.ScalaUDAF

trait DataFrameAPI {

  // The Column class changed in Spark 4, removing any direct usage of Expression
  // Spark 3 Expression based methods:
  val exprMethods =
    try {
      val exprMethod = Column.getClass().getDeclaredMethod("expr")
      val constructor = Column.getClass().getDeclaredConstructor(classOf[Expression])
      Some(exprMethod, constructor)
    } catch {
      case _: NoSuchMethodException => None
    }

  // Spark 4 method using Column.fn
  val fnMethod =
    try {
      Some(Column.getClass().getDeclaredMethod("fn", classOf[String], classOf[Array[Column]]))
    } catch {
      case _: NoSuchMethodException => None
    }

  protected def wrapExpression[E <: Expression: ClassTag](args: Any*): Column = {
    wrapVarArgExpression[E](args)
  }

  protected def wrapVarArgExpression[E <: Expression: ClassTag](arg: Seq[Any]): Column = {
    val runtimeClass = implicitly[ClassTag[E]].runtimeClass
    fnMethod
      .map { fn =>
        val colArgs = arg.map(_ match {
          case c: Column => c
          case s: String => Column(s)
          case x: Any => lit(x)
          case null => lit(null)
        })
        fn.invoke(runtimeClass.getSimpleName(), colArgs: _*).asInstanceOf[Column]
      }
      .getOrElse {
        val (expr, constructor) = exprMethods.get
        val exprArgs = arg.map(_ match {
          case c: Column => expr.invoke(c).asInstanceOf[Expression]
          case s: String => expr.invoke(Column(s)).asInstanceOf[Expression]
          case e: Expression => e
          case x: Any => Literal(x)
          case null => Literal(null)
        })
        val expressionConstructor = runtimeClass.getConstructor(classOf[Seq[Expression]])
        val expressionInstance = expressionConstructor.newInstance(exprArgs).asInstanceOf[E]
        constructor.newInstance(expressionInstance).asInstanceOf[Column]
      }
  }

  protected def wrapAggregator[A <: UserDefinedAggregateFunction: ClassTag](arg: Any*): Column = {
    val runtimeClass = implicitly[ClassTag[A]].runtimeClass
    fnMethod
      .map { fn =>
        val colArgs = arg.map(_ match {
          case c: Column => c
          case s: String => Column(s)
          case x: Any => lit(x)
          case null => lit(null)
        })
        fn.invoke(runtimeClass.getSimpleName(), colArgs: _*).asInstanceOf[Column]
      }
      .getOrElse {
        val (expr, constructor) = exprMethods.get
        val exprArgs = arg.map(_ match {
          case c: Column => expr.invoke(c).asInstanceOf[Expression]
          case s: String => expr.invoke(Column(s)).asInstanceOf[Expression]
          case e: Expression => e
          case x: Any => Literal(x)
          case null => Literal(null)
        })
        val aggregatorConstructor = runtimeClass.getConstructor()
        val aggregatorInstance =
          aggregatorConstructor.newInstance().asInstanceOf[UserDefinedAggregateFunction]
        val scalaAggregator = ScalaUDAF(exprArgs, aggregatorInstance)
        constructor.newInstance(scalaAggregator).asInstanceOf[Column]
      }
  }
}
