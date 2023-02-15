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

context("spatial queries")

sc <- testthat_spark_connection()

knn_query_pt_x <- -84.01
knn_query_pt_y <- 34.01
knn_query_pt_tbl <- sdf_sql(
  sc,
  sprintf(
    "SELECT ST_GeomFromText(\"POINT(%f %f)\") AS `pt`",
    knn_query_pt_x,
    knn_query_pt_y
  )
) %>%
  collect()
knn_query_pt <- knn_query_pt_tbl$pt[[1]]
knn_query_size <- 100
polygon_sdf <- read_polygon_rdd() %>% sdf_register()
expected_knn_dists <- polygon_sdf %>%
  dplyr::mutate(dist = ST_Distance(geometry, ST_Point(knn_query_pt_x, knn_query_pt_y))) %>%
  dplyr::arrange(dist) %>%
  head(knn_query_size) %>%
  dplyr::pull(dist)

range_query_min_x <- -85.01
range_query_max_x <- -60.01
range_query_min_y <- 34.01
range_query_max_y <- 50.01

geom_factory <- invoke_new(
  sc,
  "org.locationtech.jts.geom.GeometryFactory"
)
range_query_polygon <- invoke_new(
  sc,
  "org.locationtech.jts.geom.Envelope",
  range_query_min_x,
  range_query_max_x,
  range_query_min_y,
  range_query_max_y
) %>%
  invoke(geom_factory, "toGeometry", .)

compute_dists <- function(sdf) {
  sdf %>%
    dplyr::mutate(dist = ST_Distance(geometry, ST_Point(knn_query_pt_x, knn_query_pt_y))) %>%
    dplyr::pull(dist)
}

collect_coords <- function(sdf) {
  sdf %>%
    dplyr::transmute(
      coords = transform(
        ST_DumpPoints(geometry), ~ array(ST_X(geometry), ST_Y(geometry))
      )
    ) %>%
    dplyr::pull(coords)
}

expected_coords_for_contain_query <- polygon_sdf %>%
  dplyr::filter(
    ST_Contains(
      ST_PolygonFromEnvelope(
        range_query_min_x,
        range_query_min_y,
        range_query_max_x,
        range_query_max_y
      ),
      geometry
    )
  ) %>%
  collect_coords()

expected_coords_for_intersect_query <- polygon_sdf %>%
  dplyr::filter(
    ST_Intersects(
      geometry,
      ST_PolygonFromEnvelope(
        range_query_min_x,
        range_query_min_y,
        range_query_max_x,
        range_query_max_y
      )
    )
  ) %>%
  collect_coords()

test_that("KNN query works as expected for 'rdd' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    knn_rdd <- sedona_knn_query(
      polygon_rdd,
      x = knn_query_pt,
      k = knn_query_size,
      index_type = index_type,
      result_type = "rdd"
    )

    expect_equal(
      knn_rdd %>% sdf_register() %>% compute_dists(), expected_knn_dists
    )
  }
})

test_that("KNN query works as expected for 'sdf' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    knn_sdf <- sedona_knn_query(
      polygon_rdd,
      x = knn_query_pt,
      k = knn_query_size,
      index_type = index_type,
      result_type = "sdf"
    )

    expect_equal(knn_sdf %>% compute_dists(), expected_knn_dists)
  }
})

test_that("KNN query works as expected for 'raw' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    knn_result <- sedona_knn_query(
      polygon_rdd,
      x = knn_query_pt,
      k = knn_query_size,
      index_type = index_type,
      result_type = "raw"
    )

    expect_equal(
      knn_result %>%
        lapply(function(pt) invoke(pt, "distance", knn_query_pt)) %>%
        unlist(),
      expected_knn_dists
    )
  }
})

test_that("Range query works as expected for 'rdd' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    rdd <- sedona_range_query(
      polygon_rdd,
      x = range_query_polygon,
      query_type = "contain",
      index_type = index_type,
      result_type = "rdd"
    )

    expect_setequal(
      rdd %>% sdf_register() %>% collect_coords(),
      expected_coords_for_contain_query
    )
  }

  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    rdd <- sedona_range_query(
      polygon_rdd,
      x = range_query_polygon,
      query_type = "intersect",
      index_type = index_type,
      result_type = "rdd"
    )

    expect_setequal(
      rdd %>% sdf_register() %>% collect_coords(),
      expected_coords_for_intersect_query
    )
  }
})

test_that("Range query works as expected for 'sdf' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    sdf <- sedona_range_query(
      polygon_rdd,
      x = range_query_polygon,
      query_type = "contain",
      index_type = index_type,
      result_type = "sdf"
    )

    expect_setequal(
      sdf %>% collect_coords(), expected_coords_for_contain_query
    )
  }

  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    sdf <- sedona_range_query(
      polygon_rdd,
      x = range_query_polygon,
      query_type = "intersect",
      index_type = index_type,
      result_type = "sdf"
    )

    expect_setequal(
      sdf %>% collect_coords(), expected_coords_for_intersect_query
    )
  }
})

test_that("Range query works as expected for 'raw' result type", {
  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    polygons <- sedona_range_query(
      polygon_rdd,
      x = range_query_polygon,
      query_type = "contain",
      index_type = index_type,
      result_type = "raw"
    )

    expect_coordinate_lists_setequal(
      polygons, expected_coords_for_contain_query
    )
  }

  for (index_type in list(NULL, "quadtree", "rtree")) {
    polygon_rdd <- read_polygon_rdd()
    sdf <- sedona_range_query(
      polygon_rdd,
      x = range_query_polygon,
      query_type = "intersect",
      index_type = index_type,
      result_type = "raw"
    )

    expect_coordinate_lists_setequal(
      polygons, expected_coords_for_intersect_query
    )
  }
})
