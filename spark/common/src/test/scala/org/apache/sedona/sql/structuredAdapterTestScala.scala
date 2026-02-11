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

import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.CircleRDD
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.adapters.StructuredAdapter
import org.junit.Assert.assertEquals
import org.scalatest.GivenWhenThen

class structuredAdapterTestScala extends TestBaseScala with GivenWhenThen {

  describe("Structured Adapter") {
    it("Should convert DataFrame to SpatialRDD and back") {
      val seq = generateTestData()
      val geom1 = seq.head._3
      val dfOrigin = sparkSession.createDataFrame(seq)
      val rdd = StructuredAdapter.toSpatialRdd(dfOrigin, "_3")
      assertGeometryEquals(geom1, rdd.rawSpatialRDD.take(1).get(0))
      val dfConverted = StructuredAdapter.toDf(rdd, sparkSession)
      intercept[RuntimeException] {
        StructuredAdapter.toSpatialPartitionedDf(rdd, sparkSession)
      }
      assertEquals(seq.size, dfConverted.count())
    }

    it("Should convert DataFrame to SpatialRDD and back, without specifying geometry column") {
      val seq = generateTestData()
      val geom1 = seq.head._3
      val dfOrigin = sparkSession.createDataFrame(seq)
      val rdd = StructuredAdapter.toSpatialRdd(dfOrigin)
      assertGeometryEquals(geom1, rdd.rawSpatialRDD.take(1).get(0))
      val dfConverted = StructuredAdapter.toDf(rdd, sparkSession)
      intercept[RuntimeException] {
        StructuredAdapter.toSpatialPartitionedDf(rdd, sparkSession)
      }
      assertEquals(seq.size, dfConverted.count())
    }

    it("Should convert to Rdd and do spatial partitioning") {
      val seq = generateTestData()
      val dfOrigin = sparkSession.createDataFrame(seq)
      val rdd = StructuredAdapter.toSpatialRdd(dfOrigin, "_3")
      rdd.analyze()
      rdd.spatialPartitioning(GridType.KDBTREE, 10)
      val dfConverted = StructuredAdapter.toSpatialPartitionedDf(rdd, sparkSession)
      assertEquals(seq.size, dfConverted.count())
    }

    it("Should convert a spatial join result back to DataFrame") {
      val pointRdd =
        StructuredAdapter.toSpatialRdd(sparkSession.createDataFrame(generateTestData()))
      val circleRDD = new CircleRDD(pointRdd, 0.0001)
      circleRDD.analyze()
      pointRdd.analyze()
      circleRDD.spatialPartitioning(GridType.KDBTREE)
      pointRdd.spatialPartitioning(circleRDD.getPartitioner)
      circleRDD.buildIndex(IndexType.QUADTREE, true)
      val pairRdd =
        JoinQuery.DistanceJoinQueryFlat(pointRdd, circleRDD, true, SpatialPredicate.INTERSECTS)
      var resultDf =
        StructuredAdapter.toDf(pairRdd, pointRdd.schema, pointRdd.schema, sparkSession)
      assertEquals(pointRdd.rawSpatialRDD.count(), resultDf.count())
      resultDf =
        StructuredAdapter.toDf(pairRdd, pointRdd.schema.json, pointRdd.schema.json, sparkSession)
      assertEquals(pointRdd.rawSpatialRDD.count(), resultDf.count())
    }

    it("Should convert a SpatialRdd to RowRdd and back") {
      val seq = generateTestData()
      val dfOrigin = sparkSession.createDataFrame(seq)
      val spatialRdd = StructuredAdapter.toSpatialRdd(dfOrigin.rdd)
      val rowRdd = StructuredAdapter.toRowRdd(spatialRdd)
      assertEquals(seq.size, StructuredAdapter.toSpatialRdd(rowRdd).rawSpatialRDD.count())
    }

    it("Should not be able to convert an empty Row RDD to SpatialRDD if schema is not provided") {
      val rdd = sparkSession.sparkContext.parallelize(Seq.empty[Row])
      intercept[IllegalArgumentException] {
        StructuredAdapter.toSpatialRdd(rdd)
      }
    }

    it("Should convert an empty Row RDD to SpatialRDD if schema is provided") {
      val rdd = sparkSession.sparkContext.parallelize(Seq.empty[Row])
      val spatialRdd = StructuredAdapter.toSpatialRdd(rdd, null)
      assertEquals(0, spatialRdd.rawSpatialRDD.count())
      assertEquals(0, spatialRdd.schema.size)
    }

    it("can convert spatial RDD to Dataframe preserving spatial partitioning") {
      var pointCsvDF = sparkSession.read
        .format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .load(csvPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql(
        "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      var srcRdd = StructuredAdapter.toSpatialRdd(pointDf, "arealandmark")
      srcRdd.analyze()
      srcRdd.spatialPartitioning(GridType.KDBTREE, 16)
      var numSpatialPartitions = srcRdd.spatialPartitionedRDD.getNumPartitions
      assert(numSpatialPartitions >= 16)

      var partitionedDF = StructuredAdapter.toSpatialPartitionedDf(srcRdd, sparkSession)
      val dfPartitions: Long = partitionedDF.select(spark_partition_id).distinct().count()
      assert(dfPartitions == numSpatialPartitions)
    }

    it("Should repartition by spatial key in one step") {
      val seq = generateTestData()
      val dfOrigin = sparkSession.createDataFrame(seq)
      val partitionedDf =
        StructuredAdapter.repartitionBySpatialKey(dfOrigin, "_3", GridType.KDBTREE, 4)
      assertEquals(seq.size, partitionedDf.count())
      assert(partitionedDf.rdd.getNumPartitions >= 4)
    }

    it("Should repartition by spatial key with auto-detected geometry column") {
      val seq = generateTestData()
      val dfOrigin = sparkSession.createDataFrame(seq)
      val partitionedDf =
        StructuredAdapter.repartitionBySpatialKey(dfOrigin, GridType.KDBTREE)
      assertEquals(seq.size, partitionedDf.count())
    }
  }
}
