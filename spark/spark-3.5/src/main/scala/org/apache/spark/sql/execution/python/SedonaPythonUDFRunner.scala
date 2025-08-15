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

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark._
import org.apache.spark.api.python._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

/**
 * A helper class to run Python UDFs in Spark.
 */
abstract class SedonaBasePythonUDFRunner(
                                    funcs: Seq[ChainedPythonFunctions],
                                    evalType: Int,
                                    argOffsets: Array[Array[Int]],
                                    pythonMetrics: Map[String, SQLMetric],
                                    jobArtifactUUID: Option[String])
  extends SedonaBasePythonRunner[Array[Byte], Array[Byte]](
    funcs, evalType, argOffsets, jobArtifactUUID) {

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(
      funcs.head.funcs.head.pythonExec)

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  abstract class SedonaPythonUDFWriterThread(
                                        env: SparkEnv,
                                        worker: Socket,
                                        inputIterator: Iterator[Array[Byte]],
                                        partitionIndex: Int,
                                        context: TaskContext)
    extends WriterThread(env, worker, inputIterator, partitionIndex, context) {

    protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
      val startData = dataOut.size()

      PythonRDD.writeIteratorToStream(inputIterator, dataOut)
      dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)

      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
    }
  }

  protected override def newReaderIterator(
                                            stream: DataInputStream,
                                            writerThread: WriterThread,
                                            startTime: Long,
                                            env: SparkEnv,
                                            worker: Socket,
                                            pid: Option[Int],
                                            releasedOrClosed: AtomicBoolean,
                                            context: TaskContext): Iterator[Array[Byte]] = {
    new ReaderIterator(
      stream, writerThread, startTime, env, worker, pid, releasedOrClosed, context) {

      protected override def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              pythonMetrics("pythonDataReceived") += length
              obj
            case 0 => Array.emptyByteArray
            case SpecialLengths.TIMING_DATA =>
              handleTimingData()
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              throw handlePythonException()
            case SpecialLengths.END_OF_DATA_SECTION =>
              handleEndOfDataSection()
              null
          }
        } catch handleException
      }
    }
  }
}

class SedonaPythonUDFRunner(
                       funcs: Seq[ChainedPythonFunctions],
                       evalType: Int,
                       argOffsets: Array[Array[Int]],
                       pythonMetrics: Map[String, SQLMetric],
                       jobArtifactUUID: Option[String])
  extends SedonaBasePythonUDFRunner(funcs, evalType, argOffsets, pythonMetrics, jobArtifactUUID) {

  protected override def newWriterThread(
                                          env: SparkEnv,
                                          worker: Socket,
                                          inputIterator: Iterator[Array[Byte]],
                                          partitionIndex: Int,
                                          context: TaskContext): WriterThread = {
    new SedonaPythonUDFWriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        SedonaPythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

    }
  }
}

object SedonaPythonUDFRunner {

  def writeUDFs(
                 dataOut: DataOutputStream,
                 funcs: Seq[ChainedPythonFunctions],
                 argOffsets: Array[Array[Int]]): Unit = {
    dataOut.writeInt(funcs.length)
    funcs.zip(argOffsets).foreach { case (chained, offsets) =>
      dataOut.writeInt(offsets.length)
      offsets.foreach { offset =>
        dataOut.writeInt(offset)
      }
      dataOut.writeInt(chained.funcs.length)
      chained.funcs.foreach { f =>
        dataOut.writeInt(f.command.length)
        dataOut.write(f.command.toArray)
      }
    }
  }
}
