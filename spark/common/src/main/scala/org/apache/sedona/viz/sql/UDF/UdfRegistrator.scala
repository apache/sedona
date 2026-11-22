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
package org.apache.sedona.viz.sql.UDF

import org.apache.sedona.sql.UDF.SedonaFunctionNames
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.{SQLContext, SparkSession}

object UdfRegistrator {

  private def functionIdentifier(name: String, namespace: String): FunctionIdentifier =
    if (SpatialTypeSupport.usesNativeTypes) {
      FunctionIdentifier(name, Some(namespace), Some("system"))
    } else {
      FunctionIdentifier(name)
    }

  // A scalar is named after its builder, a companion object whose class name ends in "$".
  private def scalarName(builder: FunctionBuilder): String =
    SedonaFunctionNames.resolve(builder.getClass.getSimpleName.dropRight(1))

  private def aggregateName(aggregate: UserDefinedAggregateFunction): String =
    SedonaFunctionNames.resolve(aggregate.getClass.getSimpleName)

  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    SedonaFunctionNames.initialize(sparkSession)
    Catalog.expressions.foreach(f => {
      val identifier = functionIdentifier(scalarName(f), "builtin")
      val expressionInfo = new ExpressionInfo(
        f.getClass.getCanonicalName,
        identifier.database.orNull,
        identifier.funcName)
      sparkSession.sessionState.functionRegistry
        .registerFunction(identifier, expressionInfo, SpatialTypeSupport.adaptFunction(f))
    })
    Catalog.aggregateExpressions.foreach(f => sparkSession.udf.register(aggregateName(f), f))
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    SedonaFunctionNames.initialize(sparkSession)
    Catalog.expressions.foreach(f =>
      sparkSession.sessionState.functionRegistry.dropFunction(
        functionIdentifier(scalarName(f), "builtin")))
    Catalog.aggregateExpressions.foreach(f =>
      sparkSession.sessionState.functionRegistry.dropFunction(
        functionIdentifier(aggregateName(f), "session")))
  }
}
