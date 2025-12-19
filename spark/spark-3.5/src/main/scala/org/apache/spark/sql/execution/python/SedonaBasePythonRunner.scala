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

import java.net._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EXECUTOR_CORES
import org.apache.spark.internal.config.Python._
import org.apache.spark.resource.ResourceProfile.{EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.util._

private object SedonaBasePythonRunner {

  private lazy val faultHandlerLogDir = Utils.createTempDir(namePrefix = "faulthandler")
}

private[spark] abstract class SedonaBasePythonRunner[IN, OUT](
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    jobArtifactUUID: Option[String],
    val geometryFields: Seq[(Int, Int)] = Seq.empty)
    extends BasePythonRunner[IN, OUT](funcs, evalType, argOffsets, jobArtifactUUID)
    with Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  private val conf = SparkEnv.get.conf
  private val faultHandlerEnabled = conf.get(PYTHON_WORKER_FAULTHANLDER_ENABLED)

  private def getWorkerMemoryMb(mem: Option[Long], cores: Int): Option[Long] = {
    mem.map(_ / cores)
  }

  import java.io._

  override def compute(
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): Iterator[OUT] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get

    val execCoresProp = Option(context.getLocalProperty(EXECUTOR_CORES_LOCAL_PROPERTY))
    val memoryMb = Option(context.getLocalProperty(PYSPARK_MEMORY_LOCAL_PROPERTY)).map(_.toLong)

    if (simplifiedTraceback) {
      envVars.put("SPARK_SIMPLIFIED_TRACEBACK", "1")
    }
    // SPARK-30299 this could be wrong with standalone mode when executor
    // cores might not be correct because it defaults to all cores on the box.
    val execCores = execCoresProp.map(_.toInt).getOrElse(conf.get(EXECUTOR_CORES))
    val workerMemoryMb = getWorkerMemoryMb(memoryMb, execCores)
    if (workerMemoryMb.isDefined) {
      envVars.put("PYSPARK_EXECUTOR_MEMORY_MB", workerMemoryMb.get.toString)
    }
    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    if (faultHandlerEnabled) {
      envVars.put("PYTHON_FAULTHANDLER_DIR", SedonaBasePythonRunner.faultHandlerLogDir.toString)
    }

    envVars.put("SPARK_JOB_ARTIFACT_UUID", jobArtifactUUID.getOrElse("default"))

    val (worker: Socket, pid: Option[Int]) = {
      WorkerContext.createPythonWorker(pythonExec, envVars.asScala.toMap)
    }

    val releasedOrClosed = new AtomicBoolean(false)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = newWriterThread(env, worker, inputIterator, partitionIndex, context)

    context.addTaskCompletionListener[Unit] { _ =>
      writerThread.shutdownOnTaskCompletion()
      if (releasedOrClosed.compareAndSet(false, true)) {
        try {
          worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }

    writerThread.start()

    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

    val stdoutIterator = newReaderIterator(
      stream,
      writerThread,
      startTime,
      env,
      worker,
      pid,
      releasedOrClosed,
      context)
    new InterruptibleIterator(context, stdoutIterator)
  }
}
