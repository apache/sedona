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

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class testFunctions extends AnyFunSuite with BeforeAndAfterAll {

  var sedona: SparkSession = _

  override def beforeAll(): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    // Main object initialization happens on first access
    // Access resourceFolder to trigger Main's initialization
    println(s"Resource folder: ${Main.resourceFolder}")

    // Create Spark session with driver JVM options for Java module access
    val config = SedonaContext.builder().appName("SedonaSQL-test")
      .master("local[*]")
      .config("spark.driver.extraJavaOptions",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()
    sedona = SedonaContext.create(config)

    SedonaVizRegistrator.registerAll(sedona)
  }

  override def afterAll(): Unit = {
    if (sedona != null) {
      sedona.stop()
    }
  }

  test("SqlExample - testPredicatePushdownAndRangeJonQuery") {
    SqlExample.testPredicatePushdownAndRangeJonQuery(sedona)
  }

  test("SqlExample - testDistanceJoinQuery") {
    SqlExample.testDistanceJoinQuery(sedona)
  }

  test("SqlExample - testAggregateFunction") {
    SqlExample.testAggregateFunction(sedona)
  }

  test("SqlExample - testShapefileConstructor") {
    SqlExample.testShapefileConstructor(sedona)
  }

  test("SqlExample - testRasterIOAndMapAlgebra") {
    SqlExample.testRasterIOAndMapAlgebra(sedona)
  }

  test("RddExample - visualizeSpatialColocation") {
    RddExample.visualizeSpatialColocation(sedona)
  }

  test("RddExample - calculateSpatialColocation") {
    RddExample.calculateSpatialColocation(sedona)
  }

  test("VizExample - buildScatterPlot") {
    VizExample.buildScatterPlot(sedona)
    succeed // Test passes if function completes without exception
  }

  test("VizExample - buildHeatMap") {
    VizExample.buildHeatMap(sedona)
    succeed // Test passes if function completes without exception
  }

  test("VizExample - buildChoroplethMap") {
    VizExample.buildChoroplethMap(sedona)
    succeed // Test passes if function completes without exception
  }

  test("VizExample - parallelFilterRenderNoStitch") {
    VizExample.parallelFilterRenderNoStitch(sedona)
    succeed // Test passes if function completes without exception
  }

  test("VizExample - sqlApiVisualization") {
    VizExample.sqlApiVisualization(sedona)
    succeed // Test passes if function completes without exception
  }
}
