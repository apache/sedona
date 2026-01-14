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
import org.apache.spark.internal.config.Python.{PYTHON_USE_DAEMON, PYTHON_WORKER_MODULE}
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

  val pythonVer: String = if (isPythonAvailable) {
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

  val vectorizedFunction: Array[Byte] = {
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
             |
             |def apply_function_on_number(x):
             |    return x + 1.0
             |f.write(CloudPickleSerializer().dumps((apply_function_on_number, FloatType())))
             |""".stripMargin),
        None,
        "PYTHONPATH" -> s"$pysparkPythonPath:$pythonPath").!!
      binaryPandasFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryPandasFunc != null)
    binaryPandasFunc
  }

  val sedonaDBGeometryToGeometryFunctionBytes: Array[Byte] = {
    var binaryPandasFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          pythonExec,
          "-c",
          f"""
             |import pyarrow as pa
             |import shapely
             |import geoarrow.pyarrow as ga
             |from sedonadb import udf
             |from sedona.sql.types import GeometryType
             |from pyspark.serializers import CloudPickleSerializer
             |from pyspark.sql.types import DoubleType, IntegerType
             |from sedonadb import udf as sedona_udf_module
             |
             |@sedona_udf_module.arrow_udf(ga.wkb(), [udf.GEOMETRY, udf.NUMERIC])
             |def geometry_udf(geom, distance):
             |    geom_wkb = pa.array(geom.storage.to_array())
             |    distance = pa.array(distance.to_array())
             |    geom = shapely.from_wkb(geom_wkb)
             |    result_shapely = shapely.buffer(geom, distance)
             |
             |    return pa.array(shapely.to_wkb(result_shapely))
             |
             |f = open('$path', 'wb');
             |f.write(CloudPickleSerializer().dumps((lambda: geometry_udf, GeometryType())))
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

  val nonGeometryVectorizedUDF: UserDefinedPythonFunction = UserDefinedPythonFunction(
    name = "vectorized_udf",
    func = SimplePythonFunction(
      command = vectorizedFunction,
      envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
      pythonIncludes = List.empty[String].asJava,
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null),
    dataType = FloatType,
    pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    udfDeterministic = false)

  val sedonaDBGeometryToGeometryFunction: UserDefinedPythonFunction = UserDefinedPythonFunction(
    name = "geospatial_udf",
    func = SimplePythonFunction(
      command = sedonaDBGeometryToGeometryFunctionBytes,
      envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
      pythonIncludes = List.empty[String].asJava,
      pythonExec = pythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null),
    dataType = GeometryUDT,
    pythonEvalType = UDF.PythonEvalType.SQL_SCALAR_SEDONA_DB_UDF,
    udfDeterministic = true)

}
