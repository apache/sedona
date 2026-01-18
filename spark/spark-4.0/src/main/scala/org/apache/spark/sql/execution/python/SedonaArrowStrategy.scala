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
package org.apache.spark.sql.execution.python

import org.apache.sedona.sql.UDF.PythonEvalType
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.python.SedonaArrowEvalPythonExec
import org.apache.spark.sql.udf.SedonaArrowEvalPython

// We use custom Strategy to avoid Apache Spark assert on types, we
// can consider extending this to support other engines working with
// arrow data
class SedonaArrowStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SedonaArrowEvalPython(udfs, output, child, evalType) =>
      SedonaArrowEvalPythonExec(udfs, output, planLater(child), evalType) :: Nil
    case _ => Nil
  }
}
