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
package org.apache.sedona.stats

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_MakePoint
import org.apache.spark.sql.{DataFrame, Row, functions => f}

import java.io.{ByteArrayOutputStream, PrintStream}

class WeightingTest extends TestBaseScala {

  case class Neighbors(id: Int, neighbor: Seq[Int])

  case class Point(id: Int, x: Double, y: Double)

  var originalAQEValue: String = null

  override def beforeAll: Unit = {
    super.beforeAll()
    originalAQEValue = sparkSession.conf.get("spark.sql.adaptive.enabled")
    sparkSession.conf.set("spark.sql.adaptive.enabled", "false")
  }

  override def afterAll: Unit = {
    super.beforeAll()
    sparkSession.conf.set("spark.sql.adaptive.enabled", originalAQEValue)
  }

  def getData(): DataFrame = {
    sparkSession
      .createDataFrame(
        Seq(
          Point(0, 2.0, 2.0),
          Point(1, 2.0, 3.0),
          Point(2, 3.0, 3.0),
          Point(3, 3.0, 2.0),
          Point(4, 3.0, 1.0),
          Point(5, 2.0, 1.0),
          Point(6, 1.0, 1.0),
          Point(7, 1.0, 2.0),
          Point(8, 1.0, 3.0),
          Point(9, 0.0, 2.0),
          Point(10, 4.0, 2.0)))
      .withColumn("geometry", ST_MakePoint("x", "y"))
      .drop("x", "y")
  }

  def getDupedData(): DataFrame = {
    sparkSession
      .createDataFrame(Seq(Point(0, 1.0, 1.0), Point(1, 1.0, 1.0), Point(2, 2.0, 1.0)))
      .withColumn("geometry", ST_MakePoint("x", "y"))
      .drop("x", "y")
  }

  describe("addDistanceBandColumn") {

    it("returns correct results") {
      // Explode avoids the need to read a nested struct
      // https://issues.apache.org/jira/browse/SPARK-48942
      val actualDf = Weighting
        .addDistanceBandColumn(getData(), 1.0)
        .select(
          f.col("id"),
          f.array_sort(
            f.transform(f.col("weights"), w => w("neighbor")("id")).as("neighbor_ids")))

      hasOptimizationTurnedOn(actualDf)

      val expectedDf = sparkSession.createDataFrame(
        Seq(
          Neighbors(0, Seq(1, 3, 5, 7)),
          Neighbors(1, Seq(0, 2, 8)),
          Neighbors(2, Seq(1, 3)),
          Neighbors(3, Seq(0, 2, 4, 10)),
          Neighbors(4, Seq(3, 5)),
          Neighbors(5, Seq(0, 4, 6)),
          Neighbors(6, Seq(5, 7)),
          Neighbors(7, Seq(0, 6, 8, 9)),
          Neighbors(8, Seq(1, 7)),
          Neighbors(9, Seq(7)),
          Neighbors(10, Seq(3))))

      assertDataFramesEqual(actualDf, expectedDf)
    }

    it("return empty weights array when no neighbors") {
      val actualDf = Weighting.addDistanceBandColumn(getData(), .9)
      hasOptimizationTurnedOn(actualDf)

      assert(actualDf.count() == 11)
      assert(actualDf.filter(f.size(f.col("weights")) > 0).count() == 0)
    }

    it("respect includeZeroDistanceNeighbors flag") {
      val actualDfWithZeroDistanceNeighbors = Weighting
        .addDistanceBandColumn(
          getDupedData(),
          1.1,
          includeZeroDistanceNeighbors = true,
          binary = false)
        .select(
          f.col("id"),
          f.transform(f.col("weights"), w => w("neighbor")("id")).as("neighbor_ids"))

      hasOptimizationTurnedOn(actualDfWithZeroDistanceNeighbors)

      assertDataFramesEqual(
        actualDfWithZeroDistanceNeighbors,
        sparkSession.createDataFrame(
          Seq(Neighbors(0, Seq(2, 1)), Neighbors(1, Seq(2, 0)), Neighbors(2, Seq(1, 0)))))

      val actualDfWithoutZeroDistanceNeighbors = Weighting
        .addDistanceBandColumn(getDupedData(), 1.1)
        .select(
          f.col("id"),
          f.transform(f.col("weights"), w => w("neighbor")("id")).as("neighbor_ids"))

      assertDataFramesEqual(
        actualDfWithoutZeroDistanceNeighbors,
        sparkSession.createDataFrame(
          Seq(Neighbors(0, Seq(2)), Neighbors(1, Seq(2)), Neighbors(2, Seq(1, 0)))))

    }

    it("adds binary weights") {
      val result = Weighting.addDistanceBandColumn(getData(), 2.0, geometry = "geometry")
      val weights = result.select("weights").collect().map(_.getSeq[Row](0))
      hasOptimizationTurnedOn(result)

      assert(weights.forall(_.forall(_.getAs[Double]("value") == 1.0)))

      hasOptimizationTurnedOn(result)
    }

    it("adds non-binary weights when binary is false") {

      val result = Weighting.addDistanceBandColumn(
        getData(),
        2.0,
        binary = false,
        alpha = -.9,
        geometry = "geometry")
      val weights = result.select("weights").collect().map(_.getSeq[Row](0))
      assert(weights.exists(_.exists(_.getAs[Double]("value") != 1.0)))

      hasOptimizationTurnedOn(result)
    }

    it("throws IllegalArgumentException when threshold is negative") {

      assertThrows[IllegalArgumentException] {
        Weighting.addDistanceBandColumn(getData(), -1.0, geometry = "geometry")
      }
    }

    it("throws IllegalArgumentException when alpha is >=0") {

      assertThrows[IllegalArgumentException] {
        Weighting.addDistanceBandColumn(
          getData(),
          2.0,
          binary = false,
          alpha = 1.0,
          geometry = "geometry")
      }
    }

    it("throw IllegalArgumentException with non-existent geometry column") {
      assertThrows[IllegalArgumentException] {
        Weighting.addDistanceBandColumn(getData(), 2.0, geometry = "non_existent")
      }
    }
  }

  private def hasOptimizationTurnedOn(result: DataFrame) = {
    val sparkPlan = captureStdOut(result.explain())

    val distanceJoinOptimization = "DistanceJoin"

    val occurrences =
      sparkPlan.sliding(distanceJoinOptimization.length).count(_ == distanceJoinOptimization)

    assert(occurrences == 1)
  }

  def captureStdOut(block: => Unit): String = {
    val stream = new ByteArrayOutputStream()
    val ps = new PrintStream(stream)

    Console.withOut(ps) {
      block
    }

    ps.flush()
    stream.toString("UTF-8")
  }
}
