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
package org.apache.sedona.sql

import org.apache.sedona.common.sphere.{Haversine, Spheroid}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, DistanceJoinExec}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.Random

class SphereDistanceJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {
  private val spatialJoinPartitionSideConfKey = "sedona.join.spatitionside"

  private val testData1: Seq[(Int, Double, Geometry)] = generateTestData()
  private val testData2: Seq[(Int, Double, Geometry)] = generateTestData()

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTempViewsForTestData()
  }

  describe("Sedona-SQL Spatial Join Test") {
    val joinConditions = Table(
      "join condition",
      "ST_DistanceSphere(df1.geom, df2.geom) < 2000000",
      "ST_DistanceSphere(df2.geom, df1.geom) < 2000000",
      "ST_DistanceSpheroid(df1.geom, df2.geom) < 2000000",
      "ST_DistanceSpheroid(df2.geom, df1.geom) < 2000000",
      "ST_DistanceSphere(df1.geom, df2.geom) < df1.dist",
      "ST_DistanceSphere(df2.geom, df1.geom) < df1.dist",
      "ST_DistanceSpheroid(df1.geom, df2.geom) < df1.dist",
      "ST_DistanceSpheroid(df2.geom, df1.geom) < df1.dist",
      "ST_DistanceSphere(df1.geom, df2.geom) < df2.dist",
      "ST_DistanceSphere(df2.geom, df1.geom) < df2.dist",
      "ST_DistanceSpheroid(df1.geom, df2.geom) < df2.dist",
      "ST_DistanceSpheroid(df2.geom, df1.geom) < df2.dist")

    forAll(joinConditions) { joinCondition =>
      val expected = buildExpectedResult(joinCondition)
      it(s"sphere distance join ON $joinCondition, with left side as dominant side") {
        withConf(Map(spatialJoinPartitionSideConfKey -> "left")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          verifyResult(expected, result)
        }
      }
      it(s"sphere distance join ON $joinCondition, with right side as dominant side") {
        withConf(Map(spatialJoinPartitionSideConfKey -> "right")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          verifyResult(expected, result)
        }
      }
      it(s"sphere distance ON $joinCondition, broadcast df1") {
        val result = sparkSession.sql(
          s"SELECT /*+ BROADCAST(df1) */ df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
        verifyResult(expected, result)
      }
      it(s"sphere distance ON $joinCondition, broadcast df2") {
        val result = sparkSession.sql(
          s"SELECT /*+ BROADCAST(df2) */ df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
        verifyResult(expected, result)
      }
    }
  }

  describe("Sphere distance join with custom sphere radius") {
    it("do not optimize distance join with custom sphere radius") {
      val df = sparkSession.sql(
        "SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_DistanceSphere(df1.geom, df2.geom, 100) > 10")
      assert(!isUsingOptimizedSpatialJoin(df))
    }
  }

  private def prepareTempViewsForTestData(): Unit = {
    import sparkSession.implicits._
    testData1.toDF("id", "dist", "geom").createOrReplaceTempView("df1")
    testData2.toDF("id", "dist", "geom").createOrReplaceTempView("df2")
  }

  private def buildExpectedResult(joinCondition: String): Seq[(Int, Int)] = {
    val evaluate = joinCondition match {
      case "ST_DistanceSphere(df1.geom, df2.geom) < 2000000" |
          "ST_DistanceSphere(df2.geom, df1.geom) < 2000000" =>
        (g1: Geometry, d1: Double, g2: Geometry, d2: Double) =>
          Haversine.distance(g1, g2) < 2000000
      case "ST_DistanceSphere(df1.geom, df2.geom) < df1.dist" |
          "ST_DistanceSphere(df2.geom, df1.geom) < df1.dist" =>
        (g1: Geometry, d1: Double, g2: Geometry, d2: Double) => Haversine.distance(g1, g2) < d1
      case "ST_DistanceSphere(df1.geom, df2.geom) < df2.dist" |
          "ST_DistanceSphere(df2.geom, df1.geom) < df2.dist" =>
        (g1: Geometry, d1: Double, g2: Geometry, d2: Double) => Haversine.distance(g1, g2) < d2
      case "ST_DistanceSpheroid(df1.geom, df2.geom) < 2000000" |
          "ST_DistanceSpheroid(df2.geom, df1.geom) < 2000000" =>
        (g1: Geometry, d1: Double, g2: Geometry, d2: Double) =>
          Spheroid.distance(g1, g2) < 2000000
      case "ST_DistanceSpheroid(df1.geom, df2.geom) < df1.dist" |
          "ST_DistanceSpheroid(df2.geom, df1.geom) < df1.dist" =>
        (g1: Geometry, d1: Double, g2: Geometry, d2: Double) => Spheroid.distance(g1, g2) < d1
      case "ST_DistanceSpheroid(df1.geom, df2.geom) < df2.dist" |
          "ST_DistanceSpheroid(df2.geom, df1.geom) < df2.dist" =>
        (g1: Geometry, d1: Double, g2: Geometry, d2: Double) => Spheroid.distance(g1, g2) < d2
    }
    testData1.flatMap { case (id1, dist1, geom1) =>
      testData2.flatMap { case (id2, dist2, geom2) =>
        if (evaluate(geom1, dist1, geom2, dist2)) {
          Some((id1, id2))
        } else {
          None
        }
      }
    }
  }

  private def verifyResult(expected: Seq[(Int, Int)], result: DataFrame): Unit = {
    isUsingOptimizedSpatialJoin(result)
    val actual = result.collect().map(row => (row.getInt(0), row.getInt(1))).sorted
    assert(actual.nonEmpty)
    assert(actual === expected)
  }

  private def isUsingOptimizedSpatialJoin(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collect {
      case _: BroadcastIndexJoinExec | _: DistanceJoinExec => true
    }.nonEmpty
  }
}
