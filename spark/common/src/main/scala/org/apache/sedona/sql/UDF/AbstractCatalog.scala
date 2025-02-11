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
package org.apache.sedona.sql.UDF

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.expressions.Aggregator
import org.locationtech.jts.geom.Geometry

import scala.reflect.ClassTag

abstract class AbstractCatalog {

  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)

  val expressions: Seq[FunctionDescription]

  val aggregateExpressions: Seq[Aggregator[Geometry, _, _]]

  protected def function[T <: Expression: ClassTag](defaultArgs: Any*): FunctionDescription = {
    val classTag = implicitly[ClassTag[T]]
    val constructor = classTag.runtimeClass.getConstructor(classOf[Seq[Expression]])
    val functionName = classTag.runtimeClass.getSimpleName
    val functionIdentifier = FunctionIdentifier(functionName)
    val expressionInfo = new ExpressionInfo(
      classTag.runtimeClass.getCanonicalName,
      functionIdentifier.database.orNull,
      functionName)

    def functionBuilder(expressions: Seq[Expression]): T = {
      val expr = constructor.newInstance(expressions).asInstanceOf[T]
      expr match {
        case e: ExpectsInputTypes =>
          val numParameters = e.inputTypes.size
          val numArguments = expressions.size
          if (numParameters == numArguments || numParameters == expr.children.size) expr
          else {
            val numUnspecifiedArgs = numParameters - numArguments
            if (numUnspecifiedArgs > 0) {
              if (numUnspecifiedArgs <= defaultArgs.size) {
                val args =
                  expressions ++ defaultArgs.takeRight(numUnspecifiedArgs).map(Literal(_))
                constructor.newInstance(args).asInstanceOf[T]
              } else {
                throw new IllegalArgumentException(s"function $functionName takes at least " +
                  s"${numParameters - defaultArgs.size} argument(s), $numArguments argument(s) specified")
              }
            } else {
              throw new IllegalArgumentException(
                s"function $functionName takes at most " +
                  s"$numParameters argument(s), $numArguments argument(s) specified")
            }
          }
        case _ => expr
      }
    }

    (functionIdentifier, expressionInfo, functionBuilder)
  }
}
