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
package org.apache.sedona.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.sedona_sql.expressions.{FunctionResolver, InferrableFunction}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

class FunctionResolverSuite extends AnyFunSpec {
  describe("FunctionResolver test") {
    // Helper method to create test functions
    def createTestFunction(inputTypes: Seq[DataType]): InferrableFunction = {
      InferrableFunction(
        sparkInputTypes = inputTypes,
        sparkReturnType = StringType,
        serializer = identity,
        argExtractorBuilders = Seq.empty,
        evaluatorBuilder = _ => _ => "")
    }

    // Helper method to create test expressions
    def createTestExpression(dataType: DataType): Expression = {
      Literal.create(null, dataType)
    }

    it("No function matching input arity") {
      val functions = Seq(
        createTestFunction(Seq(IntegerType)),
        createTestFunction(Seq(IntegerType, StringType)))
      val expressions = Seq(
        createTestExpression(IntegerType),
        createTestExpression(StringType),
        createTestExpression(DoubleType))

      assertThrows[IllegalArgumentException] {
        FunctionResolver.resolveFunction(expressions, functions)
      }
    }

    it("Only one function matches input arity") {
      val functions = Seq(
        createTestFunction(Seq(IntegerType)),
        createTestFunction(Seq(IntegerType, StringType, DoubleType)))
      val expressions = Seq(createTestExpression(IntegerType))

      val result = FunctionResolver.resolveFunction(expressions, functions)
      assert(result.sparkInputTypes == Seq(IntegerType))
    }

    it("Multiple functions match input arity, perfect match") {
      val functions =
        Seq(createTestFunction(Seq(IntegerType)), createTestFunction(Seq(StringType)))
      val expressions = Seq(createTestExpression(IntegerType))

      val result = FunctionResolver.resolveFunction(expressions, functions)
      assert(result.sparkInputTypes == Seq(IntegerType))
    }

    it("Multiple functions match input arity, no perfect match, no ambiguity") {
      val functions = Seq(
        createTestFunction(Seq(LongType, StringType)),
        createTestFunction(Seq(DoubleType, StringType)))
      val expressions = Seq(createTestExpression(LongType), createTestExpression(LongType))

      val result = FunctionResolver.resolveFunction(expressions, functions)
      // Integer can be coerced to Long with less loss of precision than Double
      assert(result.sparkInputTypes == Seq(LongType, StringType))
    }

    it("Multiple functions match input arity, ambiguity") {
      val functions = Seq(
        createTestFunction(Seq(LongType, StringType)),
        createTestFunction(Seq(DoubleType, StringType)))
      val expressions = Seq(createTestExpression(IntegerType), createTestExpression(StringType))

      assertThrows[IllegalArgumentException] {
        FunctionResolver.resolveFunction(expressions, functions)
      }
    }
  }
}
