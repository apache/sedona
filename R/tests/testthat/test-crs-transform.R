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

context("CRS transform")

test_that("crs_transform() works as expected", {
  sc <- testthat_spark_connection()
  pt_rdd <- sedona_read_dsv_to_typed_rdd(
    sc, test_data("crs-test-point.csv"),
    type = "point"
  ) %>%
    crs_transform("epsg:4326", "epsg:3857")

  expect_equivalent(
    pt_rdd %>%
      sdf_register() %>%
      head(5) %>%
      dplyr::transmute(x = ST_X(geometry), y = ST_Y(geometry)) %>%
      # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
      # fixed
      sdf_register(name = random_string()) %>%
      collect(),
    tibble::tribble(
      ~x, ~y,
      -9833016.710450118, 3805934.914254189,
      -9815699.961781807, 3810760.096874278,
      -9839413.351030082, 3810273.8140140832,
      -9820728.151861448, 3809444.5432437807,
      -9832182.148227641, 3888758.1926142,
    )
  )
})
