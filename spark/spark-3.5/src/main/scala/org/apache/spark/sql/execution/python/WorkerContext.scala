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

import java.net.Socket
import scala.collection.mutable

object WorkerContext {

  def createPythonWorker(
      pythonExec: String,
      envVars: Map[String, String]): (java.net.Socket, Option[Int]) = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new SedonaDBWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark] def destroyPythonWorker(
      pythonExec: String,
      envVars: Map[String, String],
      worker: Socket): Unit = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers
        .get(key)
        .foreach(workerFactory => {
          workerFactory.stopWorker(worker)
        })
    }
  }

  private val pythonWorkers =
    mutable.HashMap[(String, Map[String, String]), SedonaDBWorkerFactory]()

}
