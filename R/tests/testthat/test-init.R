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

context("initialization")

sc <- testthat_spark_connection()

test_that("required runtime configurations are initialized correctly", {
  conf <- spark_session(sc) %>% invoke("conf")

  expect_equal(
    conf %>% invoke("get", "spark.serializer"),
    "org.apache.spark.serializer.KryoSerializer"
  )
  expect_equal(
    conf %>% invoke("get", "spark.kryo.registrator"),
    "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
  )
})

test_that("Sedona UDTs are registered correctly", {
  udts <- c(
    "org.locationtech.jts.geom.Geometry",
    "org.locationtech.jts.index.SpatialIndex"
  )
  for (udt in udts) {
    expect_true(
      invoke_static(
        sc,
        "org.apache.spark.sql.types.UDTRegistration",
        "exists",
        udt
      )
    )
  }
})

test_that("Sedona Spark SQL functions are registered correctly", {
  sdf <- spark_read_csv(
    sc,
    path = test_data("county_small.tsv"),
    columns = c("county_shape", paste0("_c", seq(17))),
    delimiter = "\t"
  )
  spatial_sdf <- sdf %>%
    dplyr::mutate(county_shape = ST_GeomFromWKT(county_shape)) %>%
    dplyr::mutate(
      pt = dplyr::sql(
        "ST_Point(CAST(40 AS DECIMAL(24, 20)), CAST(-40 AS DECIMAL(24, 20)))"
      )
    )
  schema <- spatial_sdf %>% sdf_schema()

  expect_equal(
    schema[[1]],
    list(name = "county_shape", type = "GeometryUDT")
  )
  expect_equal(
    schema[[19]],
    list(name = "pt", type = "GeometryUDT")
  )
})
