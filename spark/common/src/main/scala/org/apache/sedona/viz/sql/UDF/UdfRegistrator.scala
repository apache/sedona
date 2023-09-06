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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.{SQLContext, SparkSession}

object UdfRegistrator {

  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    Catalog.expressions.foreach(f => {
      val functionIdentifier = FunctionIdentifier(f.getClass.getSimpleName.dropRight(1))
      val expressionInfo = new ExpressionInfo(
        f.getClass.getCanonicalName,
        functionIdentifier.database.orNull,
        functionIdentifier.funcName)
      sparkSession.sessionState.functionRegistry.registerFunction(
        functionIdentifier,
        expressionInfo,
        f
      )
    })
    Catalog.aggregateExpressions.foreach(f => sparkSession.udf.register(f.getClass.getSimpleName, f))
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    Catalog.expressions.foreach(f => sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier(f.getClass.getSimpleName.dropRight(1))))
    Catalog.aggregateExpressions.foreach(f => sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier(f.getClass.getSimpleName)))
  }
}
