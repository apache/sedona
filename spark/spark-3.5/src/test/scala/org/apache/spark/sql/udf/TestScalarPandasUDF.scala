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
package org.apache.spark.sql.udf

import org.apache.sedona.sql.UDF
import org.apache.spark.{SparkEnv, TestUtils}
import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.config.Python.{PYTHON_DAEMON_MODULE, PYTHON_USE_DAEMON, PYTHON_WORKER_MODULE}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.FloatType
import org.apache.spark.util.Utils

import java.io.File
import java.nio.file.{Files, Paths}
import scala.sys.process.Process
import scala.jdk.CollectionConverters._

object ScalarUDF {

  val pythonExec: String = {
    val pythonExec =
      sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3"))
    if (TestUtils.testCommandAvailable(pythonExec)) {
      pythonExec
    } else {
      "python"
    }
  }

  private[spark] lazy val pythonPath = sys.env.getOrElse("PYTHONPATH", "")
  protected lazy val sparkHome: String = {
    sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
  }

  private lazy val py4jPath =
    Paths.get(sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).toAbsolutePath
  private[spark] lazy val pysparkPythonPath = s"$py4jPath"

  private lazy val isPythonAvailable: Boolean = TestUtils.testCommandAvailable(pythonExec)

  lazy val pythonVer: String = if (isPythonAvailable) {
    Process(
      Seq(pythonExec, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"),
      None,
      "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!.trim()
  } else {
    throw new RuntimeException(s"Python executable [$pythonExec] is unavailable.")
  }

  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path)
    finally Utils.deleteRecursively(path)
  }

  val additionalModule = "spark/spark-3.5/src/test/scala/org/apache/spark/sql/udf"

  val geopandasGeometryToNonGeometry: Array[Byte] = {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          f"""
            |from pyspark.sql.types import FloatType
            |from pyspark.serializers import CloudPickleSerializer
            |f = open('$path', 'wb');
            |def apply_geopandas(x):
            |    return x.area
            |f.write(CloudPickleSerializer().dumps((apply_geopandas, FloatType())))
            |""".stripMargin),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPandasFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPandasFunc != null)
    binaryPandasFunc
  }

  val geopandasGeometryToGeometryFunction: Array[Byte] = {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          f"""
             |from sedona.sql.types import GeometryType
             |from pyspark.serializers import CloudPickleSerializer
             |f = open('$path', 'wb');
             |def apply_geopandas(x):
             |    return x.buffer(1)
             |f.write(CloudPickleSerializer().dumps((apply_geopandas, GeometryType())))
             |""".stripMargin),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPandasFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPandasFunc != null)
    binaryPandasFunc
  }

  val geopandasNonGeometryToGeometryFunction: Array[Byte] = {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          f"""
             |from sedona.sql.types import GeometryType
             |from shapely.wkt import loads
             |from pyspark.serializers import CloudPickleSerializer
             |f = open('$path', 'wb');
             |def apply_geopandas(x):
             |    return x.apply(lambda wkt: loads(wkt).buffer(1))
             |f.write(CloudPickleSerializer().dumps((apply_geopandas, GeometryType())))
             |""".stripMargin),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPandasFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPandasFunc != null)
    binaryPandasFunc
  }

  private val workerEnv = new java.util.HashMap[String, String]()
  workerEnv.put("PYTHONPATH", s"$pysparkPythonPath:$pythonPath")
  SparkEnv.get.conf.set(PYTHON_WORKER_MODULE, "sedonaworker.worker")
  SparkEnv.get.conf.set(PYTHON_USE_DAEMON, false)

  val geometryToNonGeometryFunction: UserDefinedPythonFunction = UserDefinedPythonFunction(
    name = "geospatial_udf",
    func = SimplePythonFunction(
      command = geopandasGeometryToNonGeometry,
      envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
      pythonIncludes = List.empty[String].asJava,
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null),
    dataType = FloatType,
    pythonEvalType = UDF.PythonEvalType.SQL_SCALAR_SEDONA_UDF,
    udfDeterministic = true)

  val geometryToGeometryFunction: UserDefinedPythonFunction = UserDefinedPythonFunction(
    name = "geospatial_udf",
    func = SimplePythonFunction(
      command = geopandasGeometryToGeometryFunction,
      envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
      pythonIncludes = List.empty[String].asJava,
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null),
    dataType = GeometryUDT,
    pythonEvalType = UDF.PythonEvalType.SQL_SCALAR_SEDONA_UDF,
    udfDeterministic = true)

  val nonGeometryToGeometryFunction: UserDefinedPythonFunction = UserDefinedPythonFunction(
    name = "geospatial_udf",
    func = SimplePythonFunction(
      command = geopandasNonGeometryToGeometryFunction,
      envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
      pythonIncludes = List.empty[String].asJava,
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null),
    dataType = GeometryUDT,
    pythonEvalType = UDF.PythonEvalType.SQL_SCALAR_SEDONA_UDF,
    udfDeterministic = true)
}
