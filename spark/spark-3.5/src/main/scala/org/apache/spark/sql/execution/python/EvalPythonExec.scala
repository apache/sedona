package org.apache.spark.sql.execution.python

import org.apache.sedona.common.geometrySerde.GeometrySerde
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.{ContextAwareIterator, SparkEnv, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, JoinedRow, MutableProjection, PythonUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

import java.io.File
import scala.collection.mutable.ArrayBuffer

trait EvalPythonExec extends UnaryExecNode {
  def udfs: Seq[PythonUDF]

  def resultAttrs: Seq[Attribute]

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

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

  protected def evaluate(
                          funcs: Seq[ChainedPythonFunctions],
                          argOffsets: Array[Array[Int]],
                          iter: Iterator[InternalRow],
                          schema: StructType,
                          context: TaskContext): Iterator[InternalRow]

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())

    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
      val contextAwareIterator = new ContextAwareIterator(context, iter)

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), child.output.length)
//      context.addTaskCompletionListener[Unit] { ctx =>
//        queue.close()
//      }

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
        val proj = projection(inputRow)
        proj
      }

      val materializedResult = projectedRowIter.toSeq

      val outputRowIterator = evaluate(
        pyFuncs, argOffsets, materializedResult.toIterator, schema, context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      outputRowIterator.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }
  }
}
