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
import org.apache.spark.TestUtils
import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
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

  val pandasFunc: Array[Byte] = {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      println(path)
      Process(
        Seq(
          pythonExec,
          "-c",
          f"""
            |from pyspark.sql.types import IntegerType
            |from shapely.geometry import Point
            |from sedona.sql.types import GeometryType
            |from pyspark.serializers import CloudPickleSerializer
            |from sedona.utils import geometry_serde
            |from shapely import box
            |f = open('$path', 'wb');
            |def w(x):
            |    def apply_function(w):
            |        geom, offset = geometry_serde.deserialize(w)
            |        bounds = geom.buffer(1).bounds
            |        x = box(*bounds)
            |        return geometry_serde.serialize(x)
            |    return x.apply(apply_function)
            |f.write(CloudPickleSerializer().dumps((w, GeometryType())))
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

  val geoPandasScalaFunction: UserDefinedPythonFunction = UserDefinedPythonFunction(
    name = "geospatial_udf",
    func = SimplePythonFunction(
      command = pandasFunc,
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
