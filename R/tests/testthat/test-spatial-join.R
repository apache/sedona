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

context("spatial join")

sc <- testthat_spark_connection()

test_that("sedona_spatial_join() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join(
      pt_rdd, polygon_rdd,
      join_type = "contain", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "pair_rdd"))
  }
})

test_that("sedona_spatial_join() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join(
      pt_rdd, polygon_rdd,
      join_type = "intersect", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "pair_rdd"))
  }
})

test_that("sedona_spatial_join_count_by_key() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join_count_by_key(
      pt_rdd, polygon_rdd,
      join_type = "contain", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "count_by_key_rdd"))
  }
})

test_that("sedona_spatial_join_count_by_key() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join_count_by_key(
      pt_rdd, polygon_rdd,
      join_type = "intersect", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "count_by_key_rdd"))
  }
})
