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

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.SparkEnv
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.udf.ScalarUDF.{geoPandasScalaFunction, nonGeometryVectorizedUDF, sedonaDBGeometryToGeometryFunction}
import org.locationtech.jts.io.WKTReader
import org.scalatest.matchers.should.Matchers

import java.net.{InetAddress, ServerSocket, Socket}

class StrategySuite extends TestBaseScala with Matchers {
  val wktReader = new WKTReader()

  val spark: SparkSession = {
    sparkSession.sparkContext.setLogLevel("ALL")
    sparkSession
  }

  import spark.implicits._

  it("sedona geospatial UDF - geopandas") {
    val df = spark.read
      .format("geoparquet")
      .load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings")
      .withColumn("geom_buffer", geoPandasScalaFunction(col("geometry")) )

    df.printSchema()

    df.show()
//
//    val df = Seq(
//      (1, "value", wktReader.read("POINT(21 52)")),
//      (2, "value1", wktReader.read("POINT(20 50)")),
//      (3, "value2", wktReader.read("POINT(20 49)")),
//      (4, "value3", wktReader.read("POINT(20 48)")),
//      (5, "value4", wktReader.read("POINT(20 47)")))
//      .toDF("id", "value", "geom")
//      .withColumn("geom_buffer", geoPandasScalaFunction(col("geom")))

//    df.count shouldEqual 5
//
//    df.selectExpr("ST_AsText(ST_ReducePrecision(geom_buffer, 2))")
//      .as[String]
//      .collect() should contain theSameElementsAs Seq(
//      "POLYGON ((20 51, 20 53, 22 53, 22 51, 20 51))",
//      "POLYGON ((19 49, 19 51, 21 51, 21 49, 19 49))",
//      "POLYGON ((19 48, 19 50, 21 50, 21 48, 19 48))",
//      "POLYGON ((19 47, 19 49, 21 49, 21 47, 19 47))",
//      "POLYGON ((19 46, 19 48, 21 48, 21 46, 19 46))")
  }

  it("sedona geospatial UDF - sedona db") {
//    val df = Seq(
//      (1, "value", wktReader.read("POINT(21 52)")),
//      (2, "value1", wktReader.read("POINT(20 50)")),
//      (3, "value2", wktReader.read("POINT(20 49)")),
//      (4, "value3", wktReader.read("POINT(20 48)")),
//      (5, "value4", wktReader.read("POINT(20 47)")))
//      .toDF("id", "value", "geometry")

//    df.cache()
//    df.count()
//      .select(
//        sedonaDBGeometryToGeometryFunction(col("geometry")).alias("geom"),
//        nonGeometryVectorizedUDF(col("id")).alias("id_increased"),
//      )

//    spark.read
//      .format("geoparquet")
//      .load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings")
//      .limit(10000)
//      .write.format("geoparquet")
//      .save("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings_2")
    val df = spark.read
      .format("geoparquet")
      .load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings")
      .select("geometry")

//    df.cache()
//    df.count()
//      .limit(100)


//    println(df.count())

//    df.cache()
//
//    df.count()

    val dfVectorized = df
      .withColumn("geometry", expr("ST_SetSRID(geometry, '4326')"))
      .select(
//        col("id"),
//        col("version"),
//        col("bbox"),
        sedonaDBGeometryToGeometryFunction(col("geometry"), lit(100)).alias("geom"),
//        nonGeometryVectorizedUDF(col("id")).alias("id_increased"),
      )

//    dfVectorized.show()
    dfVectorized.selectExpr("ST_X(ST_Centroid(geom)) AS x").selectExpr("sum(x)").show()
//    dfVectorized.selectExpr("ST_X(ST_Centroid(geom)) AS x").selectExpr("sum(x)").show()
//    val processingContext = df.queryExecution.explainString(mode = ExplainMode.fromString("extended"))

//    println(processingContext)
  }

  it("should properly start socket server") {
    val authHelper = new SocketAuthHelper(SparkEnv.get.conf)
    val serverSocket = new ServerSocket(5356, 1, InetAddress.getLoopbackAddress())

//    serverSocket.setSoTimeout(15000)
    println(serverSocket.getLocalPort)
    val socket = serverSocket.accept()

    println("socket accepted")

//
//    val acceptThread = new Thread(() => {
//      println("Waiting for client...")
//      val socket = serverSocket.accept()   // BLOCKS HERE
//      println("Client connected!")
//    }, "accept-thread")
//
//    acceptThread.start()
//
//    println("Main thread continues immediately")

//    val t = new Thread() {
//      override def run(): Unit = {
//        println("starting client")
//        socket = serverSocket.accept()
//        println("client connected")
//
//      }
//    }

//    t.start()
//    val socket = serverSocket.accept()
//    authHelper.authClient(socket)

    println(authHelper.secret)
//    Thread.sleep(10000)
//    authHelper.authClient(socket)

//    var socket: Socket = null

//    new Thread() {
//      socket = serverSocket.accept()
//    }
//
//    val t2 = new Thread() {
//      override def run(): Unit = {
//        println("accepted connection")
//        val socket = serverSocket.accept()
//        val in = socket.getInputStream
//        val buffer = new Array[Byte](1024)
//        println("accepted connection")
//        var bytesRead = in.read(buffer)
//        while (bytesRead != -1) {
//          val received = new String(buffer, 0, bytesRead)
//          println(s"Received: $received")
//          bytesRead = in.read(buffer)
//        }
//        in.close()
//        socket.close()
//      }
//    }
//
//    t2.start()
//
//    val thread2 = new Thread() {
//      override def run(): Unit = {
//        println("starting client")
//        serverSocket.close()
//      }
//    }
//
//    val t = new Thread(() => {
//      println("hello from thread")
//    })
//
//    t.start()
//    t.join()   // <-- this IS valid
//    t2.join()

//    Thread.sleep(30000)

  }
}
