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
package org.apache.spark.sql.sedona_sql.strategy.physical.function

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.sedona_sql.plans.logical.EvalPhysicalFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sedona_sql.expressions.PhysicalFunction

class EvalPhysicalFunctionStrategy(spark: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case EvalPhysicalFunction(function: PhysicalFunction, resultAttrs, child) =>
        EvalPhysicalFunctionExec(function, planLater(child), resultAttrs) :: Nil
      case _ => Nil
    }
  }
}
