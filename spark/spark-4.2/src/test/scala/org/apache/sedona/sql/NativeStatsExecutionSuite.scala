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

import java.nio.file.Files
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.stats.clustering.DBSCAN
import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** Native spatial payloads through statistics and GraphFrames boundaries. */
class NativeStatsExecutionSuite extends FunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = SedonaContext
      .builder()
      .master("local[2]")
      .appName("native-stats-boundaries")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.geospatial.enabled", "true")
      .config("spark.sql.shuffle.partitions", "2")
      .config("sedona.join.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()
    SedonaContext.create(spark)
    spark.sparkContext.setCheckpointDir(
      Files.createTempDirectory("native-stats-checkpoint").toString)
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()
  private def points: DataFrame = spark.sql("""
    SELECT id, ST_SetSRID(ST_Point(x, 0D), 4326) AS geometry,
      named_struct('globe', ST_GeogFromEWKT('SRID=4269;POINT (1 2)'),
                   'missing', CAST(NULL AS GEOMETRY(4326))) AS payload
    FROM VALUES (1, 0D), (2, 0.1D), (3, 10D), (4, 10.1D) AS t(id, x)
  """)

  test("DBSCAN restores native payloads after ID-only GraphFrames component computation") {
    val result = DBSCAN.dbscan(points, 0.2, 2, geometry = "geometry")
    val rows = result
      .selectExpr(
        "id",
        "cluster",
        "isCore",
        "ST_SRID(geometry)",
        "ST_SRID(payload.globe)",
        "payload.missing IS NULL")
      .collect()
    assert(rows.length == 4)
    val byId = rows.map(row => row.getInt(0) -> row).toMap
    assert(byId(1).getLong(1) == byId(2).getLong(1))
    assert(byId(3).getLong(1) == byId(4).getLong(1))
    assert(byId(1).getLong(1) != byId(3).getLong(1))
    rows.foreach { row =>
      assert(row.getBoolean(2))
      assert(row.getInt(3) == 4326)
      assert(row.getInt(4) == 4269)
      assert(row.getBoolean(5))
    }
  }

  test("physical weighting results expose real native nested schemas and values") {
    points.createOrReplaceTempView("native_stats_points")
    Seq(
      "ST_BinaryDistanceBandColumn(geometry, 0.2D, true, false, false, struct(id, geometry, payload))",
      "ST_WeightedDistanceBandColumn(geometry, 0.2D, -1D, true, false, 1D, false, struct(id, geometry, payload))")
      .foreach { function =>
        val result = spark.sql(s"SELECT id, $function AS weights FROM native_stats_points")
        val neighbor = result
          .schema("weights")
          .dataType
          .asInstanceOf[ArrayType]
          .elementType
          .asInstanceOf[StructType]("neighbor")
          .dataType
          .asInstanceOf[StructType]
        assert(neighbor("geometry").dataType == GeometryType(4326))
        val rows = result
          .selectExpr(
            "id",
            "transform(weights, w -> w.neighbor.id)",
            "transform(weights, w -> ST_SRID(w.neighbor.geometry))",
            "transform(weights, w -> ST_SRID(w.neighbor.payload.globe))")
          .collect()
        assert(rows.length == 4)
        rows.foreach { row =>
          val expected = Map(1 -> 2, 2 -> 1, 3 -> 4, 4 -> 3)(row.getInt(0))
          assert(row.getSeq[Int](1) == Seq(expected))
          assert(row.getSeq[Int](2) == Seq(4326))
          assert(row.getSeq[Int](3) == Seq(4269))
        }
      }
  }

  test("LOF and its SQL physical function accept native payload hashing") {
    val result = LocalOutlierFactor.localOutlierFactor(points, k = 1, geometry = "geometry")
    assert(
      result.select("id", "lof").collect().forall(row => math.abs(row.getDouble(1) - 1d) < 1e-9))
    val sql = points.selectExpr("id", "ST_LocalOutlierFactor(geometry, 1, false) AS lof")
    val rows = sql.collect()
    assert(rows.length == 4)
    assert(rows.forall(row => math.abs(row.getDouble(1) - 1d) < 1e-9))
  }
}
