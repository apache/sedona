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
import org.apache.spark.api.python.PythonUtils
import org.apache.spark.util.Utils

import java.io.{DataInputStream, DataOutputStream, EOFException, File, InputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.util.Arrays
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.sql.execution.python.SedonaPythonWorkerFactory.PROCESS_WAIT_TIMEOUT_MS
import org.apache.spark.util.RedirectThread

import javax.annotation.concurrent.GuardedBy

class SedonaDBWorkerFactory(pythonExec: String, envVars: Map[String, String]) extends Logging {
  self =>

  private val simpleWorkers = new mutable.WeakHashMap[Socket, Process]()
  private val authHelper = new SocketAuthHelper(SparkEnv.get.conf)
  @GuardedBy("self")
  private var daemon: Process = null
  val daemonHost = InetAddress.getLoopbackAddress()
  @GuardedBy("self")
  private var daemonPort: Int = 0
  @GuardedBy("self")
  private val daemonWorkers = new mutable.WeakHashMap[Socket, Int]()
  @GuardedBy("self")
  private val idleWorkers = new mutable.Queue[Socket]()
  @GuardedBy("self")
  private var lastActivityNs = 0L

  private val useDaemon: Boolean =
    SparkEnv.get.conf.getBoolean("sedona.python.worker.daemon.enabled", false)

  private val sedonaUDFWorkerModule =
    SparkEnv.get.conf.get("sedona.python.worker.udf.module", "sedona.spark.worker.worker")

  private val sedonaDaemonModule =
    SparkEnv.get.conf.get("sedona.python.worker.udf.daemon.module", "sedona.spark.worker.daemon")

  private val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  def create(): (Socket, Option[Int]) = {
    if (useDaemon) {
      self.synchronized {
        if (idleWorkers.nonEmpty) {
          val worker = idleWorkers.dequeue()
          return (worker, daemonWorkers.get(worker))
        }
      }

      createThroughDaemon()
    } else {
      createSimpleWorker(sedonaUDFWorkerModule)
    }
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

  private def createThroughDaemon(): (Socket, Option[Int]) = {

    def createSocket(): (Socket, Option[Int]) = {
      val socket = new Socket(daemonHost, daemonPort)
      val pid = new DataInputStream(socket.getInputStream).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }

      authHelper.authToServer(socket)
      daemonWorkers.put(socket, pid)
      (socket, Some(pid))
    }

    self.synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createSocket()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createSocket()
      }
    }
  }

  private def stopDaemon(): Unit = {
    self.synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
        daemonPort = 0
      } else {
        simpleWorkers.mapValues(_.destroy())
      }
    }
  }

  private def startDaemon(): Unit = {
    self.synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val command = Arrays.asList(pythonExec, "-m", sedonaDaemonModule)
        val pb = new ProcessBuilder(command)
        val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
        if (jobArtifactUUID != "default") {
          val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
          f.mkdir()
          pb.directory(f)
        }
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        if (Utils.preferIPv6) {
          workerEnv.put("SPARK_PREFER_IPV6", "True")
        }
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        try {
          daemonPort = in.readInt()
        } catch {
          case _: EOFException if daemon.isAlive =>
            throw SparkCoreErrors.eofExceptionWhileReadPortNumberError(sedonaDaemonModule)
          case _: EOFException =>
            throw SparkCoreErrors.eofExceptionWhileReadPortNumberError(
              sedonaDaemonModule,
              Some(daemon.exitValue))
        }

        // test that the returned port number is within a valid range.
        // note: this does not cover the case where the port number
        // is arbitrary data but is also coincidentally within range
        if (daemonPort < 1 || daemonPort > 0xffff) {
          val exceptionMessage = f"""
                                    |Bad data in $sedonaDaemonModule's standard output. Invalid port number:
                                    |  $daemonPort (0x$daemonPort%08x)
                                    |Python command to execute the daemon was:
                                    |  ${command.asScala.mkString(" ")}
                                    |Check that you don't have any unexpected modules or libraries in
                                    |your PYTHONPATH:
                                    |  $pythonPath
                                    |Also, check if you have a sitecustomize.py module in your python path,
                                    |or in your python installation, that is printing to standard output"""
          throw new SparkException(exceptionMessage.stripMargin)
        }

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)
      } catch {
        case e: Exception =>
          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
                                  |Error from python worker:
                                  |  $formattedStderr
                                  |PYTHONPATH was:
                                  |  $pythonPath
                                  |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  private def cleanupIdleWorkers(): Unit = {
    while (idleWorkers.nonEmpty) {
      val worker = idleWorkers.dequeue()
      try {
        // the worker will exit after closing the socket
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  def releaseWorker(worker: Socket): Unit = {
    if (useDaemon) {
      self.synchronized {
        lastActivityNs = System.nanoTime()
        idleWorkers.enqueue(worker)
      }
    } else {
      // Cleanup the worker socket. This will also cause the Python worker to exit.
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  def stopWorker(worker: Socket): Unit = {
    self.synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { pid =>
            // tell daemon to kill worker by pid
            val output = new DataOutputStream(daemon.getOutputStream)
            output.writeInt(pid)
            output.flush()
            daemon.getOutputStream.flush()
          }
        }
      } else {
        simpleWorkers.get(worker).foreach(_.destroy())
      }
    }
    worker.close()
  }
}

private object SedonaPythonWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
}
