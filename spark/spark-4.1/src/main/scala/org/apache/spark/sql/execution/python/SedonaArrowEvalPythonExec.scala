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

import scala.jdk.CollectionConverters._

import org.apache.sedona.sql.UDF.PythonEvalType
import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType

/**
 * A physical plan that evaluates a [[PythonUDF]].
 */
case class SedonaArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
    extends EvalPythonExec
    with PythonSQLMetrics {

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  private[this] val sessionUUID = {
    Option(session).collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override protected def evaluatorFactory: EvalPythonEvaluatorFactory = {
    new SedonaArrowEvalPythonEvaluatorFactory(
      child.output,
      udfs,
      output,
      conf.arrowMaxRecordsPerBatch,
      evalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      ArrowPythonRunner.getPythonRunnerConfMap(conf),
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID,
      conf.pythonUDFProfiler)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

class SedonaArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    output: Seq[Attribute],
    batchSize: Int,
    evalType: Int,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String],
    profiler: Option[String])
    extends EvalPythonEvaluatorFactory(childOutput, udfs, output) {

  override def evaluate(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    val batchIter = Iterator(iter)

    val pyRunner = new ArrowPythonWithNamedArgumentRunner(
      funcs,
      evalType - PythonEvalType.SEDONA_UDF_TYPE_CONSTANT,
      argMetas,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID,
      profiler) with BatchedPythonArrowInput
    val columnarBatchIter = pyRunner.compute(batchIter, context.partitionId(), context)

    columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }
  }
}
