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
import org.apache.sedona.sql.UDF.PythonEvalType.{SQL_SCALAR_SEDONA_DB_UDF, SQL_SCALAR_SEDONA_UDF}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.udf.SedonaArrowEvalPython
import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConverters.asScalaIteratorConverter

// We use custom Strategy to avoid Apache Spark assert on types, we
// can consider extending this to support other engines working with
// arrow data
class SedonaArrowStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SedonaArrowEvalPython(udfs, output, child, evalType) =>
      SedonaArrowEvalPythonExec(udfs, output, planLater(child), evalType) :: Nil
    case _ => Nil
  }
}

// It's modification og Apache Spark's ArrowEvalPythonExec, we remove the check on the types to allow geometry types
// here, it's initial version to allow the vectorized udf for Sedona geometry types. We can consider extending this
// to support other engines working with arrow data
case class SedonaArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
    extends EvalPythonExec
    with PythonSQLMetrics {

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val largeVarTypes = conf.arrowUseLargeVarTypes
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    evalType match {
      case SQL_SCALAR_SEDONA_DB_UDF =>
        val columnarBatchIter = new SedonaArrowPythonRunner(
          funcs,
          evalType - PythonEvalType.SEDONA_DB_UDF_TYPE_CONSTANT,
          argOffsets,
          schema,
          sessionLocalTimeZone,
          largeVarTypes,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID).compute(batchIter, context.partitionId(), context)

//        val size = columnarBatchIter.size
//        val iter = columnarBatchIter.foreach { batch =>
//          processBatch(batch)
//        }
//
//        println("sss")
//        val data = columnarBatchIter.flatMap { batch =>
//          batch.rowIterator.asScala
//        }
//
//        val seqData = data.toSeq
//
//        val seqDataSize = seqData.size
//        val seqDataLength = seqData.length
//        println("ssss")

//        columnarBatchIter.flatMap { batch =>
//          batch.rowIterator.asScala
//        }

        val result = columnarBatchIter.flatMap { batch =>
//          val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
//          assert(outputTypes == actualDataTypes, "Invalid schema from pandas_udf: " +
//            s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
          batch.rowIterator.asScala
        }
//
//        try{
//          val first = result.next().toSeq(schema)
//        } catch {
//          case e: Exception => {
//            println("No data returned from Sedona DB UDF")
//          }
//        }
//
//        val first = result.next().toSeq(schema)

        println("ssss")
        return result

      case SQL_SCALAR_SEDONA_UDF =>
        val columnarBatchIter = new ArrowPythonRunner(
          funcs,
          evalType - PythonEvalType.SEDONA_UDF_TYPE_CONSTANT,
          argOffsets,
          schema,
          sessionLocalTimeZone,
          largeVarTypes,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID).compute(batchIter, context.partitionId(), context)

        val iter = columnarBatchIter.flatMap { batch =>
          batch.rowIterator.asScala
        }
//
//        iter.map(
//          row => {
//            processBatch(row)
//          }
//        )
//
//        val seqData = iter.toList
//        println(seqData.head.getClass)

        println("SedonaArrowEvalPythonExec: Executing Sedona DB UDF")
//        iter
        iter
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

