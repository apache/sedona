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

import org.apache.spark.sql.functions._

class BroadcastIndexJoinSuite extends TestBaseScala {

  describe("Sedona-SQL Broadcast Index Join Test") {

    it("Passed Correct partitioning for broadcast join for ST_Polygon and ST_Point") {
      var polygonCsvDf = loadCsv(csvPolygonInputLocation)
      var polygonDf = polygonCsvDf.selectExpr("ST_PolygonFromEnvelope(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20)), cast(_c2 as Decimal(24,20)), cast(_c3 as Decimal(24,20))) as polygonshape")
      polygonDf = polygonDf.repartition(3)

      var pointCsvDf = loadCsv(csvPointInputLocation)
      var pointDf = pointCsvDf.selectExpr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as pointshape")
      pointDf = pointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias("pointDf").join(polygonDf.alias("polygonDf").hint("broadcast"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").hint("broadcast").join(pointDf.alias("pointDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf.alias("pointDf").hint("broadcast").join(polygonDf.alias("polygonDf"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias("polygonDf").join(pointDf.alias("pointDf").hint("broadcast"), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides") {
      var polygonCsvDf = loadCsv(csvPolygonInputLocation)
      var polygonDf = polygonCsvDf.select(expr("ST_PolygonFromEnvelope(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20)), cast(_c2 as Decimal(24,20)), cast(_c3 as Decimal(24,20)))").alias("polygonshape"), lit(1).alias("window_extra"))

      var pointCsvDf = loadCsv(csvPointInputLocation)
      var pointDf = pointCsvDf.select(expr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20)))").alias("pointshape"), lit(1).alias("object_extra"))

      var broadcastJoinDf = polygonDf.alias("polygonDf").join(broadcast(pointDf.alias("pointDf")), expr("ST_Contains(polygonDf.polygonshape, pointDf.pointshape)"))
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
  }
}
