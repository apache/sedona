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

context("dbplyr integration")

sc <- testthat_spark_connection()

test_that("ST_Point() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(pt = ST_Point(-40, 40)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string())
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("id", "pt"))
  expect_equal(
    df$pt[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.Point"
  )
  expect_equal(df$pt[[1]] %>% invoke("getX"), -40)
  expect_equal(df$pt[[1]] %>% invoke("getY"), 40)
})

test_that("ST_PolygonFromEnvelope() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(rectangle = ST_PolygonFromEnvelope(-40, -30, 40, 30))
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("id", "rectangle"))
  expect_equal(
    df$rectangle[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.Polygon"
  )
  expect_true(df$rectangle[[1]] %>% invoke("isRectangle"))
  expect_coordinates_equal(
    df$rectangle[[1]],
    list(c(-40, -30), c(-40, 30), c(40, 30), c(40, -30), c(-40, -30))
  )
})

test_that("ST_Buffer() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(pt = ST_Point(-40, 40)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string()) %>%
    dplyr::compute()

  expect_equal(
    sdf %>%
      dplyr::mutate(pt = ST_Buffer(pt, 3L)) %>%
      dbplyr::remote_query(),
    sprintf(
      "SELECT `id`, ST_Buffer(`pt`, CAST(3 AS DOUBLE)) AS `pt`\nFROM `%s`",
      sdf$ops$x$x
    ) %>%
      dbplyr::sql()
  )
})

test_that("ST_PrecisionReduce() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(rectangle = ST_PolygonFromEnvelope(-40.12345678, -30.12345678, 40.11111111, 30.11111111)) %>%
    dplyr::mutate(rectangle = ST_PrecisionReduce(rectangle, 2)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string())

  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("id", "rectangle"))
  expect_equal(
    df$rectangle[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.Polygon"
  )
  expect_true(df$rectangle[[1]] %>% invoke("isRectangle"))
  expect_coordinates_equal(
    df$rectangle[[1]],
    list(c(-40.12, 30.11), c(40.11, 30.11), c(40.11, -30.12), c(-40.12, -30.12), c(-40.12, 30.11))
  )
})

test_that("ST_SimplifyPreserveTopology() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(pt = ST_Point(-40, 40)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string()) %>%
    dplyr::compute()

  expect_equal(
    sdf %>%
      dplyr::mutate(pt = ST_SimplifyPreserveTopology(pt, 1L)) %>%
      dbplyr::remote_query(),
    sprintf(
      "SELECT `id`, ST_SimplifyPreserveTopology(`pt`, CAST(1 AS DOUBLE)) AS `pt`\nFROM `%s`",
      sdf$ops$x$x
    ) %>%
      dbplyr::sql()
  )
})

test_that("ST_GeometryN() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(pts = ST_GeomFromText("MULTIPOINT((1 2), (3 4), (5 6), (8 9))")) %>%
    dplyr::transmute(pt = ST_GeometryN(pts, 2)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string())
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("pt"))
  expect_equal(
    df$pt[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.Point"
  )
  expect_equal(df$pt[[1]] %>% invoke("getX"), 5)
  expect_equal(df$pt[[1]] %>% invoke("getY"), 6)
})

test_that("ST_InteriorRingN() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(polygon = ST_GeomFromText("POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))")) %>%
    dplyr::transmute(interior_ring = ST_InteriorRingN(polygon, 0)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string())
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("interior_ring"))
  expect_equal(
    df$interior_ring[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.LineString"
  )
  expect_coordinates_equal(
    df$interior_ring[[1]],
    list(c(1, 1), c(2, 1), c(2, 2), c(1, 2), c(1, 1))
  )
})

test_that("ST_AddPoint() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(linestring = ST_GeomFromText("LINESTRING(0 0, 1 1, 1 0)")) %>%
    dplyr::transmute(linestring = ST_AddPoint(linestring, ST_GeomFromText("Point(21 52)"), 1)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string())
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("linestring"))
  expect_equal(
    df$linestring[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.LineString"
  )
  expect_coordinates_equal(
    df$linestring[[1]],
    list(c(0, 0), c(21, 52), c(1, 1), c(1, 0))
  )

  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(linestring = ST_GeomFromText("LINESTRING(0 0, 1 1, 1 0)")) %>%
    dplyr::transmute(linestring = ST_AddPoint(linestring, ST_GeomFromText("Point(21 52)")))
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("linestring"))
  expect_equal(
    df$linestring[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.LineString"
  )
  expect_coordinates_equal(
    df$linestring[[1]],
    list(c(0, 0), c(1, 1), c(1, 0), c(21, 52))
  )
})

test_that("ST_RemovePoint() works as expected", {
  sdf <- sdf_len(sc, 1) %>%
    dplyr::mutate(linestring = ST_GeomFromText("LINESTRING(0 0, 21 52, 1 1, 1 0)")) %>%
    dplyr::transmute(linestring = ST_RemovePoint(linestring, 1)) %>%
    # NOTE: the extra `sdf_register()` call is a workaround until SPARK-37202 is
    # fixed
    sdf_register(name = random_string())
  df <- sdf %>% collect()

  expect_equal(nrow(df), 1)
  expect_equal(colnames(df), c("linestring"))
  expect_equal(
    df$linestring[[1]] %>%
      invoke("%>%", list("getClass"), list("getName")),
    "org.locationtech.jts.geom.LineString"
  )
  expect_coordinates_equal(
    df$linestring[[1]],
    list(c(0, 0), c(1, 1), c(1, 0))
  )
})
