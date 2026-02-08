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

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils
import org.apache.spark.{SparkEnv, TaskContext}

import java.io.DataOutputStream
import java.net.Socket

private[python] trait SedonaPythonArrowInput[IN] extends PythonArrowInput[IN] {
  self: SedonaBasePythonRunner[IN, _] =>
  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        writeUDF(dataOut, funcs, argOffsets)

        // if speedup is not available and we need to use casting
        dataOut.writeBoolean(self.castGeometryToWKB)

        // write
        dataOut.writeInt(self.geometryFields.length)
        // write geometry field indices and their SRIDs
        geometryFields.foreach { case (index, srid) =>
          dataOut.writeInt(index)
          dataOut.writeInt(srid)
        }
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val arrowSchema =
          ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec",
          0,
          Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          writeIteratorToArrowStream(root, writer, dataOut, inputIterator)

          writer.end()
        } {
          root.close()
          allocator.close()
        }
      }
    }
  }
}

private[python] trait SedonaBasicPythonArrowInput
    extends SedonaPythonArrowInput[Iterator[InternalRow]] {
  self: SedonaBasePythonRunner[Iterator[InternalRow], _] =>

  protected def writeIteratorToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[InternalRow]]): Unit = {
    val arrowWriter = ArrowWriter.create(root)
    while (inputIterator.hasNext) {
      val startData = dataOut.size()
      val nextBatch = inputIterator.next()

      while (nextBatch.hasNext) {
        arrowWriter.write(nextBatch.next())
      }

      arrowWriter.finish()
      writer.writeBatch()
      arrowWriter.reset()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
    }
  }
}
