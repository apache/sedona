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

import org.apache.spark.{SparkException, SparkFiles}
import org.apache.spark.api.python.{PythonUtils, PythonWorkerFactory}
import org.apache.spark.util.Utils

import java.io.{DataInputStream, File}
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.Arrays
import java.io.InputStream
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util.RedirectThread

class SedonaDBWorkerFactory(pythonExec: String, envVars: Map[String, String])
    extends PythonWorkerFactory(pythonExec, envVars) {
  self =>

  private val simpleWorkers = new mutable.WeakHashMap[Socket, Process]()
  private val authHelper = new SocketAuthHelper(SparkEnv.get.conf)

  private val sedonaUDFWorkerModule =
    SparkEnv.get.conf.get("sedona.python.worker.udf.module", "sedona.spark.worker.worker")

  private val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  override def create(): (Socket, Option[Int]) = {
    createSimpleWorker(sedonaUDFWorkerModule)
  }

  private def createSimpleWorker(workerModule: String): (Socket, Option[Int]) = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())

      // Create and start the worker
      val pb = new ProcessBuilder(Arrays.asList(pythonExec, "-m", workerModule))
      val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
      if (jobArtifactUUID != "default") {
        val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
        f.mkdir()
        pb.directory(f)
      }
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
      workerEnv.put("PYTHONPATH", pythonPath)
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      workerEnv.put("PYTHON_WORKER_FACTORY_PORT", serverSocket.getLocalPort.toString)
      workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      if (Utils.preferIPv6) {
        workerEnv.put("SPARK_PREFER_IPV6", "True")
      }
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Wait for it to connect to our socket, and validate the auth secret.
      serverSocket.setSoTimeout(10000)

      try {
        val socket = serverSocket.accept()
        authHelper.authClient(socket)
        // TODO: When we drop JDK 8, we can just use worker.pid()
        val pid = new DataInputStream(socket.getInputStream).readInt()
        if (pid < 0) {
          throw new IllegalStateException("Python failed to launch worker with code " + pid)
        }
        self.synchronized {
          simpleWorkers.put(socket, worker)
        }

        (socket, Some(pid))
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker failed to connect back.", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
  }

  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream): Unit = {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for " + pythonExec).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + pythonExec).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }
}
