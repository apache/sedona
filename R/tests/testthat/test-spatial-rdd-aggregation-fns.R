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

context("spatial RDD aggregation functions")

sc <- testthat_spark_connection()
pt_rdd <- read_point_rdd_with_non_spatial_attrs(repartition = 11)

test_that("minimum_bounding_box() works as expected", {
  bb <- minimum_bounding_box(pt_rdd)

  expect_equal(
    c(bb$minX(), bb$maxX(), bb$minY(), bb$maxY()),
    c(-173.120769, -84.965961, 30.244859, 71.355134)
  )
})

test_that("approx_count() works as expected", {
  expect_equal(approx_count(pt_rdd), 3000)
})
