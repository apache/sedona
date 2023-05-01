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

import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.sedona_sql.strategy.join.BroadcastIndexJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class BroadcastIndexJoinSuite extends TestBaseScala {

  describe("Sedona-SQL Broadcast Index Join Test for inner joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Broadcasts the left side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", one())

      var broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", two())

      var broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("ST_Contains(polygonshape, pointshape) AND window_extra <= object_extra")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("ST_Contains(polygonshape, pointshape) AND window_extra > object_extra")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra <= object_extra AND ST_Contains(polygonshape, pointshape)")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra > object_extra AND ST_Contains(polygonshape, pointshape)")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)
    }

    it("Passed Handles multiple extra conditions on a broadcast join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one()).withColumn("window_extra2", one())
      val pointDf = buildPointDf.withColumn("object_extra", two()).withColumn("object_extra2", two())

      var broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra <= object_extra AND window_extra2 <= object_extra2 AND ST_Contains(polygonshape, pointshape)")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra > object_extra AND window_extra2 > object_extra2 AND ST_Contains(polygonshape, pointshape)")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)
    }

    it("Passed ST_Distance <= distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance distance is bound to first expression") {
      val pointDf1 = buildPointDf.withColumn("radius", two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf2.alias("pointDf2").join(
        broadcast(pointDf1).alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf2).alias("pointDf2").join(
        pointDf1.alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set("spark.sql.adaptive.enabled", true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set("spark.sql.adaptive.enabled", false)
    }

    it("Passed broadcast distance join with LineString") {
      assert(sparkSession.sql(
        """
          |select /*+ BROADCAST(a) */ *
          |from (select ST_LineFromText('LineString(1 1, 1 3, 3 3)') as geom) a
          |join (select ST_Point(2.0,2.0) as geom) b
          |on ST_Distance(a.geom, b.geom) < 0.1
          |""".stripMargin).isEmpty)
      assert(sparkSession.sql(
        """
          |select /*+ BROADCAST(a) */ *
          |from (select ST_LineFromText('LineString(1 1, 1 4)') as geom) a
          |join (select ST_Point(1.0,5.0) as geom) b
          |on ST_Distance(a.geom, b.geom) < 1.5
          |""".stripMargin).count() == 1)
    }

    it("Passed validate output rows") {
      val left = sparkSession.createDataFrame(Seq(
        (1.0, 1.0, "left_1"),
        (2.0, 2.0, "left_2")
      )).toDF("l_x", "l_y", "l_data")

      val right = sparkSession.createDataFrame(Seq(
        (2.0, 2.0, "right_2"),
        (3.0, 3.0, "right_3")
      )).toDF("r_x", "r_y", "r_data")

      val joined = left.join(broadcast(right),
        expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))"), "inner")
      assert(joined.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 1)
      assert(rows(0) == Row(2.0, 2.0, "left_2", 2.0, 2.0, "right_2"))

      val joined2 = broadcast(left).join(right,
        expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))"), "inner")
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows2 = joined2.collect()
      assert(rows2.length == 1)
      assert(rows2(0) == Row(2.0, 2.0, "left_2", 2.0, 2.0, "right_2"))

    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for left semi joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias("pointDf").join(
          broadcast(polygonDf.limit(limit)).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
          pointDf.limit(limit).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
          polygonDf.limit(limit).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = polygonDf.alias("polygonDf").join(
          broadcast(pointDf.limit(limit)).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == limit)
      }
    }

    it("Passed Broadcasts the right side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of left side of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", one())

      var broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", two())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("ST_Contains(polygonshape, pointshape) AND window_extra <= object_extra"),
            "left_semi"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("ST_Contains(polygonshape, pointshape) AND window_extra > object_extra"),
            "left_semi"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 0)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra <= object_extra AND ST_Contains(polygonshape, pointshape)"),
            "left_semi"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra > object_extra AND ST_Contains(polygonshape, pointshape)"),
            "left_semi"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 0)
      }
    }

    it("Passed Handles multiple extra conditions on a broadcast join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one()).withColumn("window_extra2", one())
      val pointDf = buildPointDf.withColumn("object_extra", two()).withColumn("object_extra2", two())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra <= object_extra AND window_extra2 <= object_extra2 AND ST_Contains(polygonshape, pointshape)"),
            "left_semi"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra > object_extra AND window_extra2 > object_extra2 AND ST_Contains(polygonshape, pointshape)"),
            "left_semi"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 0)
      }
    }

    it("Passed ST_Distance <= distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)
    }

    it("Passed ST_Distance < distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)
    }

    it("Passed ST_Distance distance is bound to first expression") {
      val pointDf1 = buildPointDf.withColumn("radius", two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = pointDf2.alias("pointDf2").join(
        broadcast(pointDf1).alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf2).alias("pointDf2").join(
        pointDf1.alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_semi")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)
    }

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set("spark.sql.adaptive.enabled", true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_semi")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set("spark.sql.adaptive.enabled", false)
    }

    it("Passed validate output rows") {
      val left = sparkSession.createDataFrame(Seq(
        (1.0, 1.0, "left_1"),
        (2.0, 2.0, "left_2")
      )).toDF("l_x", "l_y", "l_data")

      val right = sparkSession.createDataFrame(Seq(
        (2.0, 2.0, "right_2"),
        (3.0, 3.0, "right_3")
      )).toDF("r_x", "r_y", "r_data")

      val joined = left.join(broadcast(right),
        expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))"), "left_semi")
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 1)
      assert(rows(0) == Row(2.0, 2.0, "left_2"))
    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for left anti joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias("pointDf").join(
          broadcast(polygonDf.limit(limit)).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
          pointDf.limit(limit).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
          polygonDf.limit(limit).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = polygonDf.alias("polygonDf").join(
          broadcast(pointDf.limit(limit)).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)
      }
    }

    it("Passed Broadcasts the right side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 0)
    }

    it("Passed Can access attributes of left side of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", one())

      Seq(500, 900).foreach { limit =>
        var broadcastJoinDf = polygonDf.alias("polygonDf").join(
          broadcast(pointDf.limit(limit)).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000 - limit)

        broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
          pointDf.limit(limit).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000 - limit)

        broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
          polygonDf.limit(limit).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000 - limit)

        broadcastJoinDf = pointDf.alias("pointDf").join(
          broadcast(polygonDf.limit(limit)).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000 - limit)
      }
    }

    it("Passed Handles extra conditions on a broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", two())

      Seq(500, 900).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("ST_Contains(polygonshape, pointshape) AND window_extra <= object_extra"),
            "left_anti"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("ST_Contains(polygonshape, pointshape) AND window_extra > object_extra"),
            "left_anti"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra <= object_extra AND ST_Contains(polygonshape, pointshape)"),
            "left_anti"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra > object_extra AND ST_Contains(polygonshape, pointshape)"),
            "left_anti"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)
      }
    }

    it("Passed Handles multiple extra conditions on a broadcast join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one()).withColumn("window_extra2", one())
      val pointDf = buildPointDf.withColumn("object_extra", two()).withColumn("object_extra2", two())

      var broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra <= object_extra AND window_extra2 <= object_extra2 AND ST_Contains(polygonshape, pointshape)"),
          "left_anti"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra > object_extra AND window_extra2 > object_extra2 AND ST_Contains(polygonshape, pointshape)"),
          "left_anti"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)
    }

    it("Passed ST_Distance < distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)
    }

    it("Passed ST_Distance distance is bound to first expression") {
      val pointDf1 = buildPointDf.withColumn("radius", two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = pointDf2.alias("pointDf2").join(
        broadcast(pointDf1).alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf2).alias("pointDf2").join(
        pointDf1.alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_anti")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)
    }

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set("spark.sql.adaptive.enabled", true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_anti")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)
      sparkSession.conf.set("spark.sql.adaptive.enabled", false)
    }

    it("Passed validate output rows") {
      val left = sparkSession.createDataFrame(Seq(
        (1.0, 1.0, "left_1"),
        (2.0, 2.0, "left_2")
      )).toDF("l_x", "l_y", "l_data")

      val right = sparkSession.createDataFrame(Seq(
        (2.0, 2.0, "right_2"),
        (3.0, 3.0, "right_3")
      )).toDF("r_x", "r_y", "r_data")

      val joined = left.join(broadcast(right),
        expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))"), "left_anti")
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 1)
      assert(rows(0) == Row(1.0, 1.0, "left_1"))
    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for left outer joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias("pointDf").join(
          broadcast(polygonDf.limit(limit)).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"),
          "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
          pointDf.limit(limit).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"),
          "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
          polygonDf.limit(limit).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"),
          "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = polygonDf.alias("polygonDf").join(
          broadcast(pointDf.limit(limit)).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"),
          "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == 1000)
      }
    }

    it("Passed Broadcasts the left side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", one())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = polygonDf.alias("polygonDf").join(
          broadcast(pointDf.limit(limit)).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == limit)
        assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

        broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
          pointDf.limit(limit).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == limit)
        assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

        broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
          polygonDf.limit(limit).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
        assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == limit)

        broadcastJoinDf = pointDf.alias("pointDf").join(
          broadcast(polygonDf.limit(limit)).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
        assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == limit)
      }
    }

    it("Passed Handles extra conditions on a broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", two())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("ST_Contains(polygonshape, pointshape) AND window_extra <= object_extra"),
            "left_outer"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("ST_Contains(polygonshape, pointshape) AND window_extra > object_extra"),
            "left_outer"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra <= object_extra AND ST_Contains(polygonshape, pointshape)"),
            "left_outer"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias("pointDf")
          .join(
            broadcast(polygonDf.limit(limit).alias("polygonDf")),
            expr("window_extra > object_extra AND ST_Contains(polygonshape, pointshape)"),
            "left_outer"
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)
      }
    }

    it("Passed Handles multiple extra conditions on a broadcast join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one()).withColumn("window_extra2", one())
      val pointDf = buildPointDf.withColumn("object_extra", two()).withColumn("object_extra2", two())

      var broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra <= object_extra AND window_extra2 <= object_extra2 AND ST_Contains(polygonshape, pointshape)"),
          "left_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra > object_extra AND window_extra2 > object_extra2 AND ST_Contains(polygonshape, pointshape)"),
          "left_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)
    }

    it("Passed ST_Distance < distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)
    }

    it("Passed ST_Distance distance is bound to first expression") {
      val pointDf1 = buildPointDf.withColumn("radius", two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf2.alias("pointDf2").join(
        broadcast(pointDf1).alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf2).alias("pointDf2").join(
        pointDf1.alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "left_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set("spark.sql.adaptive.enabled", true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "left_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set("spark.sql.adaptive.enabled", false)
    }

    it("Passed validate output rows") {
      val left = sparkSession.createDataFrame(Seq(
        (1.0, 1.0, "left_1"),
        (2.0, 2.0, "left_2")
      )).toDF("l_x", "l_y", "l_data")

      val right = sparkSession.createDataFrame(Seq(
        (2.0, 2.0, "right_2"),
        (3.0, 3.0, "right_3")
      )).toDF("r_x", "r_y", "r_data")

      val joined = left.join(broadcast(right),
        expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))"), "left_outer")
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 2)
      assert(rows(0) == Row(1.0, 1.0, "left_1", null, null, null))
      assert(rows(1) == Row(2.0, 2.0, "left_2", 2.0, 2.0, "right_2"))
    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for right outer joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias("pointDf").join(
          broadcast(polygonDf.limit(limit)).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
          pointDf.limit(limit).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
          polygonDf.limit(limit).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = polygonDf.alias("polygonDf").join(
          broadcast(pointDf.limit(limit)).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)
      }
    }

    it("Passed Broadcasts the left side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", one())

      var broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one())
      val pointDf = buildPointDf.withColumn("object_extra", two())

      var broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("ST_Contains(polygonshape, pointshape) AND window_extra <= object_extra"),
          "right_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf)
        .alias("pointDf")
        .join(
          polygonDf.alias("polygonDf"),
          expr("ST_Contains(polygonshape, pointshape) AND window_extra > object_extra"),
          "right_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf)
        .alias("pointDf")
        .join(
          polygonDf.alias("polygonDf"),
          expr("window_extra <= object_extra AND ST_Contains(polygonshape, pointshape)"),
          "right_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          broadcast(polygonDf.alias("polygonDf")),
          expr("window_extra > object_extra AND ST_Contains(polygonshape, pointshape)"),
          "right_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Handles multiple extra conditions on a broadcast join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", one()).withColumn("window_extra2", one())
      val pointDf = buildPointDf.withColumn("object_extra", two()).withColumn("object_extra2", two())

      var broadcastJoinDf = broadcast(pointDf)
        .alias("pointDf")
        .join(
          polygonDf.alias("polygonDf"),
          expr("window_extra <= object_extra AND window_extra2 <= object_extra2 AND ST_Contains(polygonshape, pointshape)"),
          "right_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf)
        .alias("pointDf")
        .join(
          polygonDf.alias("polygonDf"),
          expr("window_extra > object_extra AND window_extra2 > object_extra2 AND ST_Contains(polygonshape, pointshape)"),
          "right_outer"
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)
    }

    it("Passed ST_Distance < distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2.limit(500)).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.limit(500).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)
    }

    it("Passed ST_Distance distance is bound to first expression") {
      val pointDf1 = buildPointDf.withColumn("radius", two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(
        broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(
        pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf2.alias("pointDf2").join(
        broadcast(pointDf1).alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf2).alias("pointDf2").join(
        pointDf1.alias("pointDf1"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < radius"), "right_outer")
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set("spark.sql.adaptive.enabled", true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(
        broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(
        pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(
        polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(
        broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"), "right_outer")
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set("spark.sql.adaptive.enabled", false)
    }

    it("Passed validate output rows") {
      val left = sparkSession.createDataFrame(Seq(
        (1.0, 1.0, "left_1"),
        (2.0, 2.0, "left_2")
      )).toDF("l_x", "l_y", "l_data")

      val right = sparkSession.createDataFrame(Seq(
        (2.0, 2.0, "right_2"),
        (3.0, 3.0, "right_3")
      )).toDF("r_x", "r_y", "r_data")

      val joined = broadcast(left).join(right,
        expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))"), "right_outer")
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 2)
      assert(rows(0) == Row(2.0, 2.0, "left_2", 2.0, 2.0, "right_2"))
      assert(rows(1) == Row(null, null, null, 3.0, 3.0, "right_3"))
    }
  }

  describe("Sedona-SQL Automatic broadcast") {
    it("Datasets smaller than threshold should be broadcasted") {
      val polygonDf = buildPolygonDf.repartition(3).alias("polygon")
      val pointDf = buildPointDf.repartition(5).alias("point")
      val df = polygonDf.join(pointDf, expr("ST_Contains(polygon.polygonshape, point.pointshape)"))
      sparkSession.conf.set("sedona.global.index", "true")
      sparkSession.conf.set("sedona.join.autoBroadcastJoinThreshold", "10mb")

      assert(df.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      sparkSession.conf.set("sedona.join.autoBroadcastJoinThreshold", "-1")
    }

    it("Datasets larger than threshold should not be broadcasted") {
      val polygonDf = buildPolygonDf.repartition(3).alias("polygon")
      val pointDf = buildPointDf.repartition(5).alias("point")
      val df = polygonDf.join(pointDf, expr("ST_Contains(polygon.polygonshape, point.pointshape)"))
      sparkSession.conf.set("sedona.global.index", "true")
      sparkSession.conf.set("sedona.join.autoBroadcastJoinThreshold", "-1")

      assert(df.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 0)
    }
  }

  describe("Sedona-SQL Broadcast join with null geometries") {
    it("Left outer join with nulls on left side") {
      import sparkSession.implicits._
      val left = Seq(("1", "POINT(1 1)"), ("2", "POINT(1 1)"), ("3", "POINT(1 1)"), ("4", null))
        .toDF("seq", "left_geom")
        .withColumn("left_geom", expr("ST_GeomFromText(left_geom)"))
      val right = Seq("POLYGON((2 0, 2 2, 0 2, 0 0, 2 0))")
        .toDF("right_geom")
        .withColumn("right_geom", expr("ST_GeomFromText(right_geom)"))
      val result = left.join(broadcast(right), expr("ST_Intersects(left_geom, right_geom)"), "left")
      assert(result.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(result.count() == 4)
    }

    it("Left anti join with nulls on left side") {
      import sparkSession.implicits._
      val left = Seq(("1", "POINT(1 1)"), ("2", "POINT(1 1)"), ("3", "POINT(1 1)"), ("4", null))
        .toDF("seq", "left_geom")
        .withColumn("left_geom", expr("ST_GeomFromText(left_geom)"))
      val right = Seq("POLYGON((2 0, 2 2, 0 2, 0 0, 2 0))")
        .toDF("right_geom")
        .withColumn("right_geom", expr("ST_GeomFromText(right_geom)"))
      val result = left.join(broadcast(right), expr("ST_Intersects(left_geom, right_geom)"), "left_anti")
      assert(result.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(result.count() == 1)
    }
  }
}
