#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

context("build index")

sc <- testthat_spark_connection()

test_that("sedona_build_index() works as expected on raw partitions", {
  pt_rdd <- read_point_rdd()

  sedona_build_index(pt_rdd, type = "quadtree")
  indexed_raw_rdd <- invoke(pt_rdd$.jobj, "indexedRawRDD")

  expect_equal(
    indexed_raw_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.quadtree.Quadtree"
  )
  expect_equal(pt_rdd$.state$raw_partitions_index_type, "quadtree")

  sedona_build_index(pt_rdd, type = "rtree")
  indexed_raw_rdd <- invoke(pt_rdd$.jobj, "indexedRawRDD")

  expect_equal(
    indexed_raw_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.strtree.STRtree"
  )
  expect_equal(pt_rdd$.state$raw_partitions_index_type, "rtree")
})

test_that("sedona_build_index() works as expected on spatial partitions", {
  pt_rdd <- read_point_rdd(repartition = 5)
  sedona_apply_spatial_partitioner(pt_rdd, partitioner = "quadtree")
  sedona_build_index(pt_rdd, type = "rtree")
  indexed_rdd <- invoke(pt_rdd$.jobj, "indexedRDD")

  expect_equal(
    indexed_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.strtree.STRtree"
  )
  expect_equal(pt_rdd$.state$spatial_partitions_index_type, "rtree")
})
