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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.InternalRow.copyValue
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection.createObject
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.vectorized.{ColumnarBatchRow, ColumnarRow}
//import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
//import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, CodeGeneratorWithInterpretedFallback, Expression, InterpretedUnsafeProjection, JoinedRow, MutableProjection, Projection, PythonUDF, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.udf.SedonaArrowEvalPython
import org.apache.spark.util.Utils
import org.apache.spark.{ContextAwareIterator, JobArtifactSet, SparkEnv, TaskContext}
import org.locationtech.jts.io.WKTReader
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder
import java.io.File
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

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

/**
 * The factory object for `UnsafeProjection`.
 */
object SedonaUnsafeProjection {

  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): UnsafeProjection = {
    GenerateUnsafeProjection.generate(
      bindReferences(exprs, inputSchema),
      SQLConf.get.subexpressionEliminationEnabled)
//    createObject(bindReferences(exprs, inputSchema))
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

    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val columnarBatchIter = new SedonaArrowPythonRunner(
      funcs,
      evalType - PythonEvalType.SEDONA_UDF_TYPE_CONSTANT,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(!_.exists(_.isInstanceOf[PythonUDF])))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  override def doExecute(): RDD[InternalRow] = {

    val customProjection = new Projection with Serializable {
      def apply(row: InternalRow): InternalRow = {
        row match {
          case joinedRow: JoinedRow =>
            val arrowField = joinedRow.getRight.asInstanceOf[ColumnarBatchRow]
            val left = joinedRow.getLeft

//              resultAttrs.zipWithIndex.map {
//                case (x, y) =>
//                  if (x.dataType.isInstanceOf[GeometryUDT]) {
//                    val wkbReader = new org.locationtech.jts.io.WKBReader()
//                    wkbReader.read(left.getBinary(y))
//
//                    println("ssss")
//                  }
//                  GeometryUDT
//                  left.getByte(y)
//
//                  left.setByte(y, 1.toByte)
//
//                  println(left.getByte(y))
//              }
//
//              println("ssss")
//              arrowField.
            row
            // We need to convert JoinedRow to UnsafeRow
//              val leftUnsafe = left.asInstanceOf[UnsafeRow]
//              val rightUnsafe = right.asInstanceOf[UnsafeRow]
//              val joinedUnsafe = new UnsafeRow(leftUnsafe.numFields + rightUnsafe.numFields)
//              joinedUnsafe.pointTo(
//                leftUnsafe.getBaseObject, leftUnsafe.getBaseOffset,
//                leftUnsafe.getSizeInBytes + rightUnsafe.getSizeInBytes)
//              joinedUnsafe.setLeft(rightUnsafe)
//              joinedUnsafe.setRight(leftUnsafe)
//              joinedUnsafe
//              val wktReader = new WKTReader()
            val resultProj = SedonaUnsafeProjection.create(output, output)
//              val WKBWriter = new org.locationtech.jts.io.WKBWriter()
            resultProj(new JoinedRow(left, arrowField))
          case _ =>
            println(row.getClass)
            throw new UnsupportedOperationException("Unsupported row type")
        }
      }
    }
    val inputRDD = child.execute().map(_.copy())

    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
      val contextAwareIterator = new ContextAwareIterator(context, iter)

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        child.output.length)
      context.addTaskCompletionListener[Unit] { ctx =>
        queue.close()
      }

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }.toArray
      val projection = MutableProjection.create(allInputs.toSeq, child.output)
      projection.initialize(context.partitionId())
      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toArray)

      // Add rows to queue to join later with the result.
      val projectedRowIter = contextAwareIterator.map { inputRow =>
        queue.add(inputRow.asInstanceOf[UnsafeRow])
        projection(inputRow)
      }

      val outputRowIterator = evaluate(pyFuncs, argOffsets, projectedRowIter, schema, context)

      val joined = new JoinedRow

      outputRowIterator.map { outputRow =>
        val joinedRow = joined(queue.remove(), outputRow)

        val projected = customProjection(joinedRow)

        val numFields = projected.numFields
        val startField = numFields - resultAttrs.length
        println(resultAttrs.length)

        val row = new GenericInternalRow(numFields)

        resultAttrs.zipWithIndex.map { case (attr, index) =>
          if (attr.dataType.isInstanceOf[GeometryUDT]) {
            // Convert the geometry type to WKB
            val wkbReader = new org.locationtech.jts.io.WKBReader()
            val wkbWriter = new org.locationtech.jts.io.WKBWriter()
            val geom = wkbReader.read(projected.getBinary(startField + index))

            row.update(startField + index, wkbWriter.write(geom))

            println("ssss")
          }
        }

        println("ssss")
//        3.2838116E-8
        row
      }
    }
  }
}
