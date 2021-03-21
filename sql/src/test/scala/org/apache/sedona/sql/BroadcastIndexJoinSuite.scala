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

import org.apache.spark.sql.sedona_sql.strategy.join.BroadcastIndexJoinExec
import org.apache.spark.sql.functions._

class BroadcastIndexJoinSuite extends TestBaseScala {

  describe("Sedona-SQL Broadcast Index Join Test") {

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(polygonDf.alias("polygonDf").hint("broadcast"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").hint("broadcast").join(pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf.alias("pointDf").hint("broadcast").join(polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(pointDf.alias("pointDf").hint("broadcast"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", lit(1))
      val pointDf = buildPointDf.withColumn("object_extra", lit(1))
      
      var broadcastJoinDf = polygonDf.alias("polygonDf").join(broadcast(pointDf).alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias("polygonDf").join(pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias("pointDf").join(polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias("pointDf").join(broadcast(polygonDf).alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.select(sum("object_extra")).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(sum("window_extra")).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast join") {
      val polygonDf = buildPolygonDf.withColumn("window_extra", expr("ST_X(element_at(ST_DumpPoints(polygonshape), 1))"))
      val pointDf = buildPointDf.withColumn("object_extra", expr("ST_X(pointshape)"))

      var broadcastJoinDf = pointDf
        .alias("pointDf")
        .join(
          polygonDf.alias("polygonDf").hint("broadcast"),
          expr("ST_Contains(polygonshape, pointshape) AND window_extra <= object_extra")
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= radius in a broadcast join") {
      var pointDf1 = buildPointDf
      var pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) <= 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < radius in a broadcast join") {
      var pointDf1 = buildPointDf
      var pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias("pointDf1").join(broadcast(pointDf2).alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias("pointDf1").join(pointDf2.alias("pointDf2"), expr("ST_Distance(pointDf1.pointshape, pointDf2.pointshape) < 2"))
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }
  }
}
