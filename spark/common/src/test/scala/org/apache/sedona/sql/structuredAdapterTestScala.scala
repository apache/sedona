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
      val resultDf =
        StructuredAdapter.toDf(pairRdd, pointRdd.schema, pointRdd.schema, sparkSession)
      assertEquals(pointRdd.rawSpatialRDD.count(), resultDf.count())
    }

    it("Should convert a SpatialRdd to RowRdd and back") {
      val seq = generateTestData()
      val dfOrigin = sparkSession.createDataFrame(seq)
      val spatialRdd = StructuredAdapter.toSpatialRdd(dfOrigin.rdd)
      val rowRdd = StructuredAdapter.toRowRdd(spatialRdd)
      assertEquals(seq.size, StructuredAdapter.toSpatialRdd(rowRdd).rawSpatialRDD.count())
    }
  }

}
