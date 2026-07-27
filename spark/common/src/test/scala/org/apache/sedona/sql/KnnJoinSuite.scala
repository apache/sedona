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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromText
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastObjectSideKNNJoinExec, BroadcastQuerySideKNNJoinExec, KNNJoinExec, TraitKNNJoinQueryExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.scalatest.matchers.must.Matchers.{be, include}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks
import org.apache.spark.sql.functions._

import java.nio.file.Files
import java.util.Random

/**
 * Test suite for KNN spatial join SQLs
 *
 * This test suite validates the KNN spatial join SQLs for Sedona-SQL module. The main purpose of
 * this test suite is to validate the KNN spatial join SQLs but not the validation of the join
 * results.
 *
 * The join results are validated in the Sedona core module with the unit tests.
 */
class KnnJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {

  val testDataDelimiter = "\t"
  val knnPointsLocationQueries: String = resourceFolder + "knn/queries.csv"
  val knnPointsLocationObjects: String = resourceFolder + "knn/objects.csv"
  val knnPointsLocationSkewedObjects: String = resourceFolder + "knn/queries-large-skewed.csv"
  val knnPointsLocationMultipleSkewedObjects: String =
    resourceFolder + "knn/queries-large-skewed-multiple.csv"
  val numPartitions = 4
  val numSkewPartitions = 100

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTempViewsForTestData()
    prepareTempViewsForDifferentPartitionsTestData()
    prepareTempViewsForSkewedTestData()
  }

  describe("KNN spatial join SQLs should be parsed correctly") {
    it("KNN Join with exact algorithms based on euclidean distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 3, true)")

      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = false,
        expressionSize = 4,
        isGeography = true,
        mustInclude = "")
    }

    it("KNN Join based on single point on left side should not be supported") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(ST_MakePoint(100, 100, 1), OBJECTS.GEOM, 3, false)")
        validateQueryPlan(
          df,
          numNeighbors = 3,
          useApproximate = true,
          expressionSize = 4,
          isGeography = false,
          mustInclude = "")
      }
      exception.getMessage should include("ST_KNN filter is not yet supported in the join query")
    }

    it("KNN Join based on single point on right side should not be supported") {
      val exception = intercept[UnsupportedOperationException] {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(OBJECTS.GEOM, ST_MakePoint(100, 100, 1), 3, false)")
        validateQueryPlan(
          df,
          numNeighbors = 3,
          useApproximate = true,
          expressionSize = 4,
          isGeography = false,
          mustInclude = "")
      }
      exception.getMessage should include("ST_KNN filter is not yet supported in the join query")
    }

    it("KNN Join based with complex join conditions using integer columns") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false) AND QUERIES.ID <= 88")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 4,
        isGeography = false,
        mustInclude = "as int) <= 88))")
    }

    it("KNN Join based with complex join conditions using text columns") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false) AND QUERIES.SHAPE = 'point'")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 4,
        isGeography = false,
        mustInclude = "= point))")
    }

    it("KNN Join based with complex join conditions using text columns and using where clause") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 3, false) WHERE QUERIES.SHAPE = 'point'")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 4,
        isGeography = false,
        mustInclude = "= point))")
    }

    it("KNN Join should work with dataframe containing 0 partitions") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, EMPTYTABLE.ID FROM QUERIES JOIN EMPTYTABLE ON ST_KNN(QUERIES.GEOM, EMPTYTABLE.GEOM, 3, false)")
      validateQueryPlan(
        df,
        numNeighbors = 3,
        useApproximate = true,
        expressionSize = 4,
        isGeography = false,
        mustInclude = "")
    }
  }

  describe("KNN spatial join SQLs should be executed correctly") {
    it("KNN Join with exact algorithms based on EUCLIDEAN distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it("KNN Join with exact algorithms based on SPHEROID distance") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, true)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it("KNN Join should support broadcast join hints - left") {
      val df = sparkSession.sql(
        s"SELECT /*+ BROADCAST(QUERIES) */ QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, true)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it("KNN Join should support broadcast join hints - right") {
      val df = sparkSession.sql(
        s"SELECT /*+ BROADCAST(OBJECTS) */ QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, true)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it("KNN Join should support broadcast join hints with different partitions - left") {
      val df = sparkSession.sql(
        s"SELECT /*+ BROADCAST(QUERIES_PAR2) */ QUERIES_PAR2.ID, OBJECTS_PAR4.ID FROM QUERIES_PAR2 JOIN OBJECTS_PAR4 ON ST_KNN(QUERIES_PAR2.GEOM, OBJECTS_PAR4.GEOM, 4, true)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it("KNN Join should support broadcast join hints with different partitions - right") {
      val df = sparkSession.sql(
        s"SELECT /*+ BROADCAST(OBJECTS_PAR4) */ QUERIES_PAR2.ID, OBJECTS_PAR4.ID FROM QUERIES_PAR2 JOIN OBJECTS_PAR4 ON ST_KNN(QUERIES_PAR2.GEOM, OBJECTS_PAR4.GEOM, 4, true)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    for (broadcastSide <- Seq("queries", "objects")) {
      it(s"KNN Join should apply extra conditions when broadcasting $broadcastSide") {
        sparkSession
          .range(0, 2)
          .selectExpr("id", "ST_Point(id, 0.0) AS geom")
          .createOrReplaceTempView("BROADCAST_EXTRA_QUERIES")
        sparkSession
          .range(0, 2)
          .selectExpr("id", "ST_Point(id + 10.0, 0.0) AS geom")
          .createOrReplaceTempView("BROADCAST_EXTRA_OBJECTS")

        val hint = if (broadcastSide == "queries") "BROADCAST(q)" else "BROADCAST(o)"
        val df = sparkSession.sql(s"""SELECT /*+ $hint */ q.id, o.id
             |FROM BROADCAST_EXTRA_QUERIES q
             |JOIN BROADCAST_EXTRA_OBJECTS o
             |ON ST_KNN(q.geom, o.geom, 1, false)
             |AND ST_Distance(q.geom, o.geom) <= 1.0""".stripMargin)

        df.count() should be(0)
        val plan = findKNNJoinExec(df)
        if (broadcastSide == "queries") {
          plan.isInstanceOf[BroadcastQuerySideKNNJoinExec] should be(true)
        } else {
          plan.isInstanceOf[BroadcastObjectSideKNNJoinExec] should be(true)
        }
      }
    }

    it("KNN Join should verify the correct parameter k is passed to the join function") {
      val df = sparkSession
        .range(0, 1)
        .toDF("id")
        .withColumn("geom", expr("ST_Point(id, id)"))
        .repartition(1)
      df.createOrReplaceTempView("df1")
      val exception = intercept[IllegalArgumentException] {
        sparkSession
          .sql(s"SELECT A.ID, B.ID FROM df1 A JOIN df1 B ON ST_KNN(A.GEOM, B.GEOM, 0, false)")
          .collect()
      }
      exception.getMessage should include(
        "The number of neighbors (k) must be equal or greater than 1.")
    }

    it("KNN Join with exact algorithms with additional join conditions on id") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false) AND QUERIES.ID > 1")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(8) // 2 queries (filtered out 1) and 4 neighbors each
      resultAll.mkString should be("[2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it("Should throw KNN predicate is not supported exception") {
      intercept[Exception] {
        sparkSession.sql("SELECT ST_KNN(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0), 1)").show()
      }
    }
  }

  describe("KNN spatial join SQLs should be executed correctly with complex join conditions") {
    it(
      "KNN Join with approximate algorithms based on EUCLIDEAN distance with no additional join conditions") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(3 * 4) // 3 queries and 4 neighbors each
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,3][3,9][3,13][3,19]")
    }

    it(
      "KNN Join with exact algorithms based on EUCLIDEAN distance with additional inequality (<) join conditions") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false) AND QUERIES.ID < OBJECTS.ID")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(10)
      resultAll.mkString should be("[1,3][1,6][1,13][1,16][2,5][2,11][2,15][3,9][3,13][3,19]")
    }

    it(
      "KNN Join with exact algorithms based on EUCLIDEAN distance with additional inequality (!=)  join conditions") {
      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false) AND QUERIES.ID != OBJECTS.ID")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(11)
      resultAll.mkString should be(
        "[1,3][1,6][1,13][1,16][2,1][2,5][2,11][2,15][3,9][3,13][3,19]")
    }

    it(
      "KNN Join with exact algorithms based on EUCLIDEAN distance with additional equality (=) join conditions") {
      withOptimizationMode("all") {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false) AND QUERIES.ID = OBJECTS.ID")
        val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
        resultAll.length should be(1)
        resultAll.mkString should be("[3,3]")
      }
    }

    it("KNN Join should respect export partitioner info - single cluster of points") {
      var tempDir =
        Files
          .createTempDirectory("spatial_partitioner_export")
          .toString // Create temporary directory
      withSpatialPartitionerExport(tempDir) {
        val df = sparkSession.sql(
          s"SELECT QUERIES_SKEWED.ID, OBJECTS_SKEWED.ID FROM QUERIES_SKEWED JOIN OBJECTS_SKEWED ON ST_KNN(QUERIES_SKEWED.GEOM, OBJECTS_SKEWED.GEOM, 4, false)")
        val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
        assert(resultAll.length > 0)
      }
    }

    it("KNN Join should respect export partitioner info - multiple clusters of points") {
      var tempDir =
        Files
          .createTempDirectory("spatial_partitioner_export")
          .toString // Create temporary directory
      withSpatialPartitionerExport(tempDir) {
        val df = sparkSession.sql(
          s"SELECT QUERIES_SKEWED_MULTIPLE.ID, OBJECTS_SKEWED_MULTIPLE.ID FROM QUERIES_SKEWED_MULTIPLE JOIN OBJECTS_SKEWED_MULTIPLE ON ST_KNN(QUERIES_SKEWED_MULTIPLE.GEOM, OBJECTS_SKEWED_MULTIPLE.GEOM, 4, false)")
        val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
        assert(resultAll.length > 0)
      }
    }

    it("AKNN Join should correctly match the join side and swap them if necessary") {
      val df1 =
        sparkSession.range(0, 100).toDF("id1").withColumn("geometry", expr("ST_Point(id1, id1)"))
      val df2 =
        sparkSession
          .range(0, 100)
          .toDF("id2")
          .withColumn("geometry", expr("ST_Point(id2 + 0.1, id2 + 0.1)"))
          .withColumn("id2", expr("concat('str', id2)"))
      df1.createOrReplaceTempView("df1")
      df2.createOrReplaceTempView("df2")

      var dfResult = sparkSession.sql(
        "SELECT df1.id1, df2.id2 FROM df1 JOIN df2 ON ST_KNN(df1.geometry, df2.geometry, 1)")
      println("ST_KNN(df1.geometry, df2.geometry, 1)")
      var resultAll = dfResult.orderBy("id1").take(1)
      resultAll.mkString should be("[0,str0]")

      dfResult = sparkSession.sql(
        "SELECT df1.id1, df2.id2 FROM df1 JOIN df2 ON ST_KNN(df2.geometry, df1.geometry, 1)")
      resultAll = dfResult.orderBy("id1").take(1)
      resultAll.mkString should be("[0,str0]")
    }

    it("KNN Join with exact algorithms based on EUCLIDEAN distance using random data") {
      val queryNumRows = 200
      val objectNumRows = 10000
      val seed = 12345L
      val coordRange = (-50.0, 50.0)
      val (df1, df2) = generateRandomPointsDataFrames(
        sparkSession,
        queryNumRows,
        objectNumRows,
        seed,
        coordRange)

      df1.repartition(numPartitions).createOrReplaceTempView("QUERIES")
      df2.repartition(numPartitions).createOrReplaceTempView("OBJECTS")

      val df = sparkSession.sql(
        s"SELECT QUERIES.ID, OBJECTS.ID FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 4, false)")
      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(queryNumRows * 4)
    }

    it("KNN Join with exact algorithms based on EUCLIDEAN distance using manually generated data") {
      val points1 = Seq(
        (0, "POINT(2.0 2.0)", "0"),
        (1, "POINT(2.0 3.0)", "1"),
        (2, "POINT(3.0 3.0)", "2"),
        (3, "POINT(3.0 2.0)", "3"),
        (4, "POINT(3.0 1.0)", "4"),
        (5, "POINT(2.0 1.0)", "5"),
        (6, "POINT(1.0 1.0)", "6"),
        (7, "POINT(1.0 2.0)", "7"),
        (8, "POINT(1.0 3.0)", "8"),
        (9, "POINT(0.0 2.0)", "9"),
        (10, "POINT(4.0 2.0)", "10"))

      val points2 = Seq(
        (0, "POINT(2.0 2.0)", "0"),
        (1, "POINT(2.0 3.0)", "1"),
        (2, "POINT(3.0 3.0)", "2"),
        (3, "POINT(3.0 2.0)", "3"),
        (4, "POINT(3.0 1.0)", "4"),
        (5, "POINT(2.0 1.0)", "5"),
        (6, "POINT(1.0 1.0)", "6"),
        (7, "POINT(1.0 2.0)", "7"),
        (8, "POINT(1.0 3.0)", "8"),
        (9, "POINT(0.0 2.0)", "9"),
        (10, "POINT(4.0 2.0)", "10"))

      val (df1, df2) = createPointsDataFrames(sparkSession, points1, points2)

      df1.createOrReplaceTempView("QUERIES")
      df2.createOrReplaceTempView("OBJECTS")

      val df = sparkSession.sql(
        s"SELECT QUERIES.ID as qid, OBJECTS.ID as oid FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 5, false)")

      val df_group =
        df.groupBy("qid").agg(collect_list("oid").as("collected_points")).orderBy("qid")

      val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
      resultAll.length should be(points1.size * 5)
    }

    it("KNN Join with exact algorithms with ties exported") {
      val points1 = Seq((0, "POINT(1.0 1.0)", "0"))

      val points2 = Seq(
        (0, "POINT(2.0 2.0)", "0"),
        (1, "POINT(2.0 3.0)", "1"),
        (2, "POINT(3.0 3.0)", "2"),
        (3, "POINT(3.0 2.0)", "3"),
        (4, "POINT(3.0 1.0)", "4"),
        (5, "POINT(2.0 1.0)", "5"),
        (6, "POINT(1.0 1.0)", "6"),
        (7, "POINT(1.0 2.0)", "7"),
        (8, "POINT(1.0 3.0)", "8"),
        (9, "POINT(0.0 2.0)", "9"),
        (10, "POINT(4.0 2.0)", "10"))

      val (df1, df2) = createPointsDataFrames(sparkSession, points1, points2)

      df1.createOrReplaceTempView("QUERIES")
      df2.createOrReplaceTempView("OBJECTS")

      withExportTies(export = true) {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID as qid, OBJECTS.ID as oid FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 2, false)")

        val df_group =
          df.groupBy("qid").agg(collect_list("oid").as("collected_points")).orderBy("qid")

        val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
        print(resultAll.mkString)
        resultAll.length should be(3)
        resultAll.mkString should be("[0,5][0,6][0,7]")
      }

      withExportTies(export = false) {
        val df = sparkSession.sql(
          s"SELECT QUERIES.ID as qid, OBJECTS.ID as oid FROM QUERIES JOIN OBJECTS ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 2, false)")

        val df_group =
          df.groupBy("qid").agg(collect_list("oid").as("collected_points")).orderBy("qid")

        val resultAll = df.collect().sortBy(row => (row.getInt(0), row.getInt(1)))
        print(resultAll.mkString)
        resultAll.length should be(2)
        resultAll.mkString should be("[0,6][0,7]")
      }
    }

    it("KNN Join should support query-local tie controls") {
      val points1 = Seq((0, "POINT(0 0)", "query"))
      val points2 =
        Seq((0, "POINT(-1 0)", "west"), (1, "POINT(1 0)", "east"), (2, "POINT(10 0)", "far"))
      val (df1, df2) = createPointsDataFrames(sparkSession, points1, points2)
      df1.createOrReplaceTempView("QUERIES")
      df2.createOrReplaceTempView("OBJECTS")

      withExportTies(export = false) {
        val df = sparkSession.sql("""SELECT QUERIES.ID AS qid, OBJECTS.ID AS oid
            |FROM QUERIES JOIN OBJECTS
            |ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 1, false, true)""".stripMargin)

        val result = df.collect().sortBy(_.getInt(1))
        result.mkString should be("[0,0][0,1]")
        val plan = findKNNJoinExec(df)
        plan.includeTies should be(Some(true))
        plan.exclusive should be(false)
      }

      withExportTies(export = true) {
        val df = sparkSession.sql("""SELECT QUERIES.ID AS qid, OBJECTS.ID AS oid
            |FROM QUERIES JOIN OBJECTS
            |ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 1, false, false)""".stripMargin)

        val result = df.collect()
        result.length should be(1)
        Set(0, 1).contains(result.head.getInt(1)) should be(true)
        findKNNJoinExec(df).includeTies should be(Some(false))
      }
    }

    it("KNN query-local spheroid ties should preserve duplicate candidate rows") {
      val points1 = Seq((0, "POINT(0 90)", "query"))
      val points2 = Seq(
        (0, "POINT(0 -90)", "antipode-a"),
        (1, "POINT(100 -90)", "antipode-b"),
        (2, "POINT(100 -90)", "antipode-b-duplicate"))
      val (df1, df2) = createPointsDataFrames(sparkSession, points1, points2)
      df1.createOrReplaceTempView("SPHEROID_TIE_QUERIES")
      df2.createOrReplaceTempView("SPHEROID_TIE_OBJECTS")

      val df = sparkSession.sql("""SELECT q.ID AS qid, o.ID AS oid
          |FROM SPHEROID_TIE_QUERIES q JOIN SPHEROID_TIE_OBJECTS o
          |ON ST_KNN(q.GEOM, o.GEOM, 1, true, true)""".stripMargin)

      df.collect().sortBy(_.getInt(1)).mkString should be("[0,0][0,1][0,2]")
    }

    it("KNN Join exclusive mode should promote the nearest non-equal geometry") {
      val queries = sparkSession
        .range(0, 2)
        .selectExpr(
          "cast(id as int) AS id",
          """CASE WHEN id = 0
            |THEN ST_MakeLine(ST_Point(0.0, 0.0), ST_Point(1.0, 0.0))
            |ELSE ST_MakeLine(ST_Point(10.0, 0.0), ST_Point(11.0, 0.0)) END AS geom""".stripMargin)
        .repartition(2)
      val objects = sparkSession
        .range(0, 2)
        .selectExpr(
          "cast(id as int) AS id",
          """CASE WHEN id = 0
            |THEN ST_GeomFromText('LINESTRING(1 0, 0 0)')
            |ELSE ST_GeomFromText('LINESTRING(0 2, 1 2)') END AS geom""".stripMargin)
        .repartition(2)
      queries.createOrReplaceTempView("QUERIES")
      objects.createOrReplaceTempView("OBJECTS")

      val df = sparkSession.sql("""SELECT QUERIES.ID AS qid, OBJECTS.ID AS oid
          |FROM QUERIES JOIN OBJECTS
          |ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 1, false, true, true)""".stripMargin)

      df.where("qid = 0").collect().mkString should be("[0,1]")
      val plan = findKNNJoinExec(df)
      plan.includeTies should be(Some(true))
      plan.exclusive should be(true)
    }

    it("KNN Join exclusive mode should find a distant candidate across partitions") {
      withConf(
        Map(
          "sedona.join.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1")) {
        val queries = sparkSession
          .range(0, 2)
          .selectExpr(
            "cast(id as int) AS id",
            "CASE WHEN id = 0 THEN ST_Point(0.0, 0.0) ELSE ST_Point(2000.0, 0.0) END AS geom")
          .repartition(4)
        val objects = sparkSession
          .range(0, 101)
          .selectExpr(
            "cast(id as int) AS id",
            "CASE WHEN id < 100 THEN ST_Point(0.0, 0.0) ELSE ST_Point(1000.0, 0.0) END AS geom")
          .repartition(8)
        queries.createOrReplaceTempView("QUERIES")
        objects.createOrReplaceTempView("OBJECTS")

        val df = sparkSession.sql("""SELECT QUERIES.ID AS qid, OBJECTS.ID AS oid
            |FROM QUERIES JOIN OBJECTS
            |ON ST_KNN(QUERIES.GEOM, OBJECTS.GEOM, 1, false, true, true)""".stripMargin)

        df.where("qid = 0").collect().mkString should be("[0,100]")
        findKNNJoinExec(df).exclusive should be(true)
      }
    }

    it("KNN Join with exact algorithms should not fail with null geometries") {
      val df1 = sparkSession.sql(
        "SELECT ST_GeomFromText(col1) as geom1 from values ('POINT (0.0 0.0)'), (null)")
      val df2 = sparkSession.sql("SELECT ST_Point(0.0, 0.0) as geom2")
      df1.cache()
      df2.cache()
      df1.join(df2, expr("ST_KNN(geom1, geom2, 1)")).count() should be(1)
    }

    it("KNN Join with exact algorithms should not fail with empty geometries") {
      val df1 = sparkSession.sql("SELECT ST_GeomFromText('POINT EMPTY') as geom1")
      val df2 = sparkSession.sql("SELECT ST_Point(0.0, 0.0) as geom2")
      df1.cache()
      df2.cache()
      df1.join(df2, expr("ST_KNN(geom1, geom2, 1)")).count() should be(0)
    }

    it("KNN Join should return no matches for empty or all-null candidate sides") {
      val queries = sparkSession
        .range(0, 2)
        .selectExpr("id", "ST_Point(id, 0.0) AS geom")
      val emptyObjects = sparkSession
        .createDataFrame(
          sparkSession.sparkContext.emptyRDD[Row],
          StructType(
            Seq(
              StructField("id", LongType, nullable = false),
              StructField("wkt", StringType, nullable = true))))
        .selectExpr("id", "ST_GeomFromText(wkt) AS geom")
      val nullObjects = sparkSession
        .createDataFrame(
          sparkSession.sparkContext.parallelize(Seq(Row(0L, null), Row(1L, null)), 2),
          StructType(
            Seq(
              StructField("id", LongType, nullable = false),
              StructField("wkt", StringType, nullable = true))))
        .selectExpr("id", "ST_GeomFromText(wkt) AS geom")

      val emptyResult = queries
        .alias("queries")
        .join(
          emptyObjects.alias("emptyObjects"),
          expr("ST_KNN(queries.geom, emptyObjects.geom, 1)"))
      val nullResult = queries
        .alias("queries")
        .join(nullObjects.alias("nullObjects"), expr("ST_KNN(queries.geom, nullObjects.geom, 1)"))

      emptyResult.queryExecution.executedPlan.toString should include("KNNJoin")
      nullResult.queryExecution.executedPlan.toString should include("KNNJoin")
      emptyResult.count() should be(0)
      nullResult.count() should be(0)
    }

    it("KNN Join using spider data source") {
      val dfRandomSquares = sparkSession.read
        .format("spider")
        .option("n", "10000")
        .option("distribution", "parcel")
        .option("dither", "0.5")
        .option("splitRange", "0.5")
        .load()

      dfRandomSquares.createOrReplaceTempView("df_random_squares")

      val dfRandomPoints = sparkSession.read
        .format("spider")
        .option("n", "1000")
        .option("distribution", "uniform")
        .load()

      dfRandomPoints.createOrReplaceTempView("df_random_points")

      // Execute a KNN join query: attribute points to the nearest square
      val knnJoined = sparkSession.sql("""SELECT sq.id, pt.id
          |FROM df_random_squares sq
          |JOIN df_random_points pt
          |ON ST_KNN(sq.geometry, pt.geometry, 1, TRUE)""".stripMargin)

      assert(knnJoined.count() > 0)
    }
  }

  private def withOptimizationMode(mode: String)(body: => Unit): Unit = {
    withConf(Map("sedona.join.optimizationmode" -> mode))(body)
  }

  private def withExportTies(export: Boolean)(body: => Unit): Unit = {
    if (export) {
      withConf(Map("spark.sedona.join.knn.includeTieBreakers" -> "true"))(body)
    } else {
      withConf(Map("spark.sedona.join.knn.includeTieBreakers" -> "false"))(body)
    }
  }

  private def withSpatialPartitionerExport(path: String)(body: => Unit): Unit = {
    withConf(Map("spark.sedona.join.debug.spatialPartitionerSavePath" -> path))(body)
  }

  private def findKNNJoinExec(df: DataFrame): TraitKNNJoinQueryExec = {
    def collect(plan: SparkPlan): Option[TraitKNNJoinQueryExec] =
      plan.collectFirst { case knn: TraitKNNJoinQueryExec => knn }

    val executedPlan = df.queryExecution.executedPlan
    collect(executedPlan)
      .orElse(executedPlan.collectFirst { case adaptive: AdaptiveSparkPlanExec =>
        collect(adaptive.executedPlan)
      }.flatten)
      .get
  }

  def validateQueryPlan(
      df: DataFrame,
      numNeighbors: Int,
      useApproximate: Boolean,
      expressionSize: Int,
      isGeography: Boolean,
      mustInclude: String): Unit = {
    print(df.queryExecution.executedPlan.toString)
    df.queryExecution.executedPlan.toString should include("KNNJoin")
    Option(mustInclude).filter(_.nonEmpty).foreach { text =>
      df.queryExecution.executedPlan.toString should include(text)
    }
    df.queryExecution.executedPlan.collect { case p: KNNJoinExec =>
      p.k should be(Literal(numNeighbors))
      p.isGeography should be(isGeography)
      p.expressions.size should be(expressionSize)
    }
  }

  private def prepareTempViewsForTestData(): (DataFrame, DataFrame) = {
    val df1 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationQueries)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    val df2 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationObjects)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    sparkSession.table("df1").repartition(numPartitions).createOrReplaceTempView("queries")
    sparkSession.table("df2").repartition(numPartitions).createOrReplaceTempView("objects")

    val emptyRdd = sparkSession.sparkContext.emptyRDD[Row]
    val emptyDf = sparkSession.createDataFrame(
      emptyRdd,
      StructType(Seq(StructField("id", IntegerType), StructField("geom", GeometryUDT()))))
    emptyDf.createOrReplaceTempView("EMPTYTABLE")

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    (df1, df2)
  }

  private def prepareTempViewsForDifferentPartitionsTestData(): (DataFrame, DataFrame) = {
    val df1 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationQueries)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    val df2 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationObjects)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    df1.repartition(2).createOrReplaceTempView("queries_par2")
    df2.repartition(4).createOrReplaceTempView("objects_par4")
    (df1, df2)
  }

  private def prepareTempViewsForSkewedTestData() = {
    val df1 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationSkewedObjects)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    df1.repartition(numSkewPartitions).createOrReplaceTempView("queries_skewed")
    df1.repartition(numSkewPartitions).createOrReplaceTempView("objects_skewed")

    val df2 = sparkSession.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", testDataDelimiter)
      .load(knnPointsLocationMultipleSkewedObjects)
      .withColumn("id", col("_c0").cast(IntegerType))
      .withColumn("geom", ST_GeomFromText(new Column("_c1")))
      .withColumn("shape", col("_c1"))
      .select("id", "geom", "shape")
    df2.repartition(numSkewPartitions).createOrReplaceTempView("queries_skewed_multiple")
    df2.repartition(numSkewPartitions).createOrReplaceTempView("objects_skewed_multiple")
  }

  def generateRandomPointsDataFrames(
      sparkSession: SparkSession,
      numRows1: Int,
      numRows2: Int,
      seed: Long,
      coordRange: (Double, Double)): (DataFrame, DataFrame) = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("geom", StringType, nullable = false),
        StructField("shape", StringType, nullable = false)))

    def generateRandomData(numRows: Int, seed: Long, coordRange: (Double, Double)): Seq[Row] = {
      val random = new Random(seed)
      val (minCoord, maxCoord) = coordRange
      (1 to numRows).map { id =>
        val x =
          minCoord + random
            .nextDouble() * (maxCoord - minCoord) // Random x coordinate in the range
        val y =
          minCoord + random
            .nextDouble() * (maxCoord - minCoord) // Random y coordinate in the range
        val geom = s"POINT($x $y)"
        val shape = s"bank$id"
        Row(id, geom, shape)
      }
    }

    val data1 = generateRandomData(numRows1, seed, coordRange)
    val data2 = generateRandomData(
      numRows2,
      seed + 1,
      coordRange
    ) // Use a different seed for the second DataFrame

    val rdd1 = sparkSession.sparkContext.parallelize(data1)
    val rdd2 = sparkSession.sparkContext.parallelize(data2)

    val df1 = sparkSession
      .createDataFrame(rdd1, schema)
      .withColumn("geom", expr("ST_GeomFromText(geom)"))

    val df2 = sparkSession
      .createDataFrame(rdd2, schema)
      .withColumn("geom", expr("ST_GeomFromText(geom)"))

    (df1, df2)
  }

  def createPointsDataFrames(
      sparkSession: SparkSession,
      points1: Seq[(Int, String, String)],
      points2: Seq[(Int, String, String)]): (DataFrame, DataFrame) = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("geom", StringType, nullable = false),
        StructField("shape", StringType, nullable = false)))

    def createDataFrame(points: Seq[(Int, String, String)]): DataFrame = {
      val rows = points.map { case (id, geom, shape) => Row(id, geom, shape) }
      val rdd = sparkSession.sparkContext.parallelize(rows)
      val df = sparkSession
        .createDataFrame(rdd, schema)
        .withColumn("geom", expr("ST_GeomFromText(geom)"))
      df
    }

    val df1 = createDataFrame(points1)
    val df2 = createDataFrame(points2)

    (df1, df2)
  }
}
