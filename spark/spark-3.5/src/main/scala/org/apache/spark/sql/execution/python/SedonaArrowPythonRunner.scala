package org.apache.spark.sql.execution.python

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.api.python._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
class SedonaArrowPythonRunner(
                         funcs: Seq[ChainedPythonFunctions],
                         evalType: Int,
                         argOffsets: Array[Array[Int]],
                         protected override val schema: StructType,
                         protected override val timeZoneId: String,
                         protected override val largeVarTypes: Boolean,
                         protected override val workerConf: Map[String, String],
                         val pythonMetrics: Map[String, SQLMetric],
                         jobArtifactUUID: Option[String])
  extends SedonaBasePythonRunner[Iterator[InternalRow], ColumnarBatch](
    funcs, evalType, argOffsets, jobArtifactUUID)
    with SedonaBasicPythonArrowInput
    with SedonaBasicPythonArrowOutput {

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(
      funcs.head.funcs.head.pythonExec)

  override val errorOnDuplicatedFieldNames: Boolean = true

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")
}

object SedonaArrowPythonRunner {
  /** Return Map with conf settings to be used in ArrowPythonRunner */
  def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
    val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
    val pandasColsByName = Seq(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
      conf.pandasGroupedMapAssignColumnsByName.toString)
    val arrowSafeTypeCheck = Seq(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key ->
      conf.arrowSafeTypeConversion.toString)
    Map(timeZoneConf ++ pandasColsByName ++ arrowSafeTypeCheck: _*)
  }
}
