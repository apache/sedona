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
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.udf.SedonaArrowEvalPython
import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT

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
  private val pythonRunnerConf =
    Map[String, String](SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  private def inferCRS(iterator: Iterator[InternalRow], schema: StructType): Seq[(Int, Int)] = {
    // this triggers the iterator
    if (!iterator.hasNext) {
      return Seq.empty
    }

    val row = iterator.next()

    val rowMatched = row match {
      case generic: GenericInternalRow =>
        Some(generic)
      case _ => None
    }

    schema
      .filter { field =>
        field.dataType == GeometryUDT
      }
      .zipWithIndex
      .map { case (_, index) =>
        if (rowMatched.isEmpty || rowMatched.get.values(index) == null) (index, 0)
        else {
          val geom = rowMatched.get.get(index, GeometryUDT).asInstanceOf[Array[Byte]]
          val preambleByte = geom(0) & 0xff
          val hasSrid = (preambleByte & 0x01) != 0

          var srid = 0
          if (hasSrid) {
            val srid2 = (geom(1) & 0xff) << 16
            val srid1 = (geom(2) & 0xff) << 8
            val srid0 = geom(3) & 0xff
            srid = srid2 | srid1 | srid0
          }

          (index, srid)
        }
      }
  }

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    val (probe, full) = iter.duplicate

    val geometryFields = inferCRS(probe, schema)

    val batchIter = if (batchSize > 0) new BatchIterator(full, batchSize) else Iterator(full)

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
          jobArtifactUUID,
          geometryFields).compute(batchIter, context.partitionId(), context)

        val result = columnarBatchIter.flatMap { batch =>
          batch.rowIterator.asScala
        }

        result

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

        iter
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
