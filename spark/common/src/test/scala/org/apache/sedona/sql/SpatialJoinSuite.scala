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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromText
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, DistanceJoinExec, RangeJoinExec}
import org.apache.spark.sql.types.IntegerType
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.prop.TableDrivenPropertyChecks

class SpatialJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val testDataDelimiter = "\t"
  val spatialJoinPartitionSideConfKey = "sedona.join.spatitionside"

  describe("Sedona-SQL Spatial Join Test") {
    val joinConditions = Table("join condition",
      "ST_Contains(df1.geom, df2.geom)",
      "ST_Intersects(df1.geom, df2.geom)",
      "ST_Within(df1.geom, df2.geom)",
      "ST_Covers(df1.geom, df2.geom)",
      "ST_CoveredBy(df1.geom, df2.geom)",
      "ST_Touches(df1.geom, df2.geom)",
      "ST_Crosses(df1.geom, df2.geom)",
      "ST_Overlaps(df1.geom, df2.geom)",
      "ST_Equals(df1.geom, df2.geom)",

      "ST_Contains(df2.geom, df1.geom)",
      "ST_Intersects(df2.geom, df1.geom)",
      "ST_Within(df2.geom, df1.geom)",
      "ST_Covers(df2.geom, df1.geom)",
      "ST_CoveredBy(df2.geom, df1.geom)",
      "ST_Touches(df2.geom, df1.geom)",
      "ST_Crosses(df2.geom, df1.geom)",
      "ST_Overlaps(df2.geom, df1.geom)",
      "ST_Equals(df2.geom, df1.geom)",

      "ST_Distance(df1.geom, df2.geom) < 1.0",
      "ST_Distance(df1.geom, df2.geom) <= 1.0",
      "ST_Distance(df2.geom, df1.geom) < 1.0",
      "ST_Distance(df2.geom, df1.geom) <= 1.0",

      "ST_Distance(df1.geom, df2.geom) < df1.dist",
      "ST_Distance(df1.geom, df2.geom) < df2.dist",
      "ST_Distance(df2.geom, df1.geom) < df1.dist",
      "ST_Distance(df2.geom, df1.geom) < df2.dist"
    )

    var spatialJoinPartitionSide = "left"
    try {
      spatialJoinPartitionSide = sparkSession.sparkContext.getConf.get(spatialJoinPartitionSideConfKey, "left")
      forAll (joinConditions) { joinCondition =>
        it(s"should join two dataframes with $joinCondition") {
          sparkSession.sparkContext.getConf.set(spatialJoinPartitionSideConfKey, "left")
          prepareTempViewsForTestData()
          val result = sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
        it(s"should join two dataframes with $joinCondition, with right side as dominant side") {
          sparkSession.sparkContext.getConf.set(spatialJoinPartitionSideConfKey, "right")
          prepareTempViewsForTestData()
          val result = sparkSession.sql(s"SELECT df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
        it(s"should join two dataframes with $joinCondition, broadcast the left side") {
          prepareTempViewsForTestData()
          val result = sparkSession.sql(s"SELECT /*+ BROADCAST(df1) */ df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
        it(s"should join two dataframes with $joinCondition, broadcast the right side") {
          prepareTempViewsForTestData()
          val result = sparkSession.sql(s"SELECT /*+ BROADCAST(df2) */ df1.id, df2.id FROM df1 JOIN df2 ON $joinCondition")
          val expected = buildExpectedResult(joinCondition)
          verifyResult(expected, result)
        }
      }
    } finally {
      sparkSession.sparkContext.getConf.set(spatialJoinPartitionSideConfKey, spatialJoinPartitionSide)
    }
  }

  describe("Sedona-SQL Spatial Join Test with SELECT *") {
    val joinConditions = Table("join condition",
      "ST_Contains(df1.geom, df2.geom)",
      "ST_Contains(df2.geom, df1.geom)",
      "ST_Distance(df1.geom, df2.geom) < 1.0",
      "ST_Distance(df2.geom, df1.geom) < 1.0"
    )

    forAll (joinConditions) { joinCondition =>
      it(s"should SELECT * in join query with $joinCondition produce correct result") {
        prepareTempViewsForTestData()
        val resultAll = sparkSession.sql(s"SELECT * FROM df1 JOIN df2 ON $joinCondition").collect()
        val result = resultAll.map(row => (row.getInt(0), row.getInt(3))).sorted
        val expected = buildExpectedResult(joinCondition)
        assert(result.nonEmpty)
        assert(result === expected)
      }

      it(s"should SELECT * in join query with $joinCondition produce correct result, broadcast the left side") {
        prepareTempViewsForTestData()
        val resultAll = sparkSession.sql(s"SELECT /*+ BROADCAST(df1) */ * FROM df1 JOIN df2 ON $joinCondition").collect()
        val result = resultAll.map(row => (row.getInt(0), row.getInt(3))).sorted
        val expected = buildExpectedResult(joinCondition)
        assert(result.nonEmpty)
        assert(result === expected)
      }

      it(s"should SELECT * in join query with $joinCondition produce correct result, broadcast the right side") {
        prepareTempViewsForTestData()
        val resultAll = sparkSession.sql(s"SELECT /*+ BROADCAST(df2) */ * FROM df1 JOIN df2 ON $joinCondition").collect()
        val result = resultAll.map(row => (row.getInt(0), row.getInt(3))).sorted
        val expected = buildExpectedResult(joinCondition)
        assert(result.nonEmpty)
        assert(result === expected)
      }
    }
  }

  describe("Spatial join in Sedona SQL should be configurable using sedona.join.optimizationmode") {
    it("Optimize all spatial joins when sedona.join.optimizationmode = all") {
      withOptimizationMode("all") {
        prepareTempViewsForTestData()
        val df = sparkSession.sql("SELECT df1.id, df2.id FROM df1 JOIN df2 ON df1.id = df2.id AND ST_Intersects(df1.geom, df2.geom)")
        assert(isUsingOptimizedSpatialJoin(df))
        val expectedResult = buildExpectedResult("ST_Intersects(df1.geom, df2.geom)")
          .filter { case (id1, id2) => id1 == id2 }
        verifyResult(expectedResult, df)
      }
    }

    it("Only optimize non-equi-joins when sedona.join.optimizationmode = nonequi") {
      withOptimizationMode("nonequi") {
        prepareTempViewsForTestData()
        val df = sparkSession.sql("SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
        assert(isUsingOptimizedSpatialJoin(df))
        val df2 = sparkSession.sql("SELECT df1.id, df2.id FROM df1 JOIN df2 ON df1.id = df2.id AND ST_Intersects(df1.geom, df2.geom)")
        assert(!isUsingOptimizedSpatialJoin(df2))
      }
    }

    it("Won't optimize spatial joins when sedona.join.optimizationmode = none") {
      withOptimizationMode("none") {
        prepareTempViewsForTestData()
        val df = sparkSession.sql("SELECT df1.id, df2.id FROM df1 JOIN df2 ON ST_Intersects(df1.geom, df2.geom)")
        assert(!isUsingOptimizedSpatialJoin(df))
      }
    }
  }

  private def withOptimizationMode(mode: String)(body: => Unit) : Unit = {
    val oldOptimizationMode = sparkSession.conf.get("sedona.join.optimizationmode", "nonequi")
    try {
      sparkSession.conf.set("sedona.join.optimizationmode", mode)
      body
    } finally {
      sparkSession.conf.set("sedona.join.optimizationmode", oldOptimizationMode)
    }
  }

  private def prepareTempViewsForTestData(): (DataFrame, DataFrame) = {
    val df1 = sparkSession.read.format("csv").option("header", "false").option("delimiter", testDataDelimiter)
      .load(spatialJoinLeftInputLocation)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c2")))
      .select("id", "geom")
      .withColumn("dist", expr("ST_Area(geom)"))
    val df2 = sparkSession.read.format("csv").option("header", "false").option("delimiter", testDataDelimiter)
      .load(spatialJoinRightInputLocation)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c2")))
      .select("id", "geom")
      .withColumn("dist", expr("ST_Area(geom)"))
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    (df1, df2)
  }

  private def buildExpectedResult(joinCondition: String): Seq[(Int, Int)] = {
    val left = loadTestData(spatialJoinLeftInputLocation)
    val right = loadTestData(spatialJoinRightInputLocation)
    val udf = joinCondition.split('(')(0)
    val swapped = joinCondition.contains("df2.geom, df1.geom")
    val eval = udf match {
      case "ST_Contains" => (l: Geometry, r: Geometry) => l.contains(r)
      case "ST_CoveredBy" => (l: Geometry, r: Geometry) => l.coveredBy(r)
      case "ST_Covers" => (l: Geometry, r: Geometry) => l.covers(r)
      case "ST_Crosses" => (l: Geometry, r: Geometry) => l.crosses(r)
      case "ST_Equals" => (l: Geometry, r: Geometry) => l.equals(r)
      case "ST_Intersects" => (l: Geometry, r: Geometry) => l.intersects(r)
      case "ST_Overlaps" => (l: Geometry, r: Geometry) => l.overlaps(r)
      case "ST_Touches" => (l: Geometry, r: Geometry) => l.touches(r)
      case "ST_Within" => (l: Geometry, r: Geometry) => l.within(r)
      case "ST_Distance" =>
        if (joinCondition contains "df1.dist")
          (l: Geometry, r: Geometry) => l.distance(r) < (if (!swapped) l.getArea else r.getArea)
        else if (joinCondition contains "df2.dist")
          (l: Geometry, r: Geometry) => l.distance(r) < (if (!swapped) r.getArea else l.getArea)
        else {
          if (joinCondition.contains("<=")) {
            (l: Geometry, r: Geometry) => l.distance(r) <= 1.0
          } else {
            (l: Geometry, r: Geometry) => l.distance(r) < 1.0
          }
        }
    }
    left.flatMap { case (id, geom) =>
      right.filter { case (_, geom2) =>
        if (swapped) eval(geom2, geom) else eval(geom, geom2)
      }.map { case (id2, _) => (id, id2) }
    }.sorted
  }

  private def loadTestData(path: String): Seq[(Int, Geometry)] = {
    val wktReader = new WKTReader()
    val bufferedSource = scala.io.Source.fromFile(path)
    try {
      bufferedSource.getLines().map { line =>
        val Array(id, _, geom) = line.split(testDataDelimiter)
        (id.toInt, wktReader.read(geom))
      }.toList
    } finally {
      bufferedSource.close()
    }
  }

  def verifyResult(expected: Seq[(Int, Int)], result: DataFrame): Unit = {
    isUsingOptimizedSpatialJoin(result)
    val actual = result.collect().map(row => (row.getInt(0), row.getInt(1))).sorted
    assert(actual.nonEmpty)
    assert(actual === expected)
  }

  def isUsingOptimizedSpatialJoin(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collect {
      case _: BroadcastIndexJoinExec |
           _: DistanceJoinExec |
           _: RangeJoinExec => true
    }.nonEmpty
  }
}
