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

context("visualization")

sc <- testthat_spark_connection()

test_that("sedona_render_heatmap() works as expected", {
  pt_rdd <- read_point_rdd_with_non_spatial_attrs()

  sedona_render_heatmap(
    pt_rdd,
    800,
    600,
    output_location = tempfile("arealm-small-"),
    boundary = c(-91, -84, 30, 35),
    blur_radius = 10
  )

  succeed()
})

test_that("sedona_render_scatter_plot() works as expected", {
  pt_rdd <- read_point_rdd()

  sedona_render_scatter_plot(
    pt_rdd,
    1000,
    600,
    output_location = tempfile("scatter-plot-"),
    boundary = c(-126.790180, -64.630926, 24.863836, 50.000),
    base_color = c(255, 255, 255)
  )

  succeed()
})

test_that("sedona_render_choropleth_map() works as expected", {
  pt_rdd <- read_point_rdd()
  polygon_rdd <- read_polygon_rdd()
  invoke(
    pt_rdd$.jobj,
    "spatialPartitioning",
    invoke_static(
      sc,
      "org.apache.sedona.core.enums.GridType",
      "KDBTREE"
    )
  )
  invoke(
    polygon_rdd$.jobj,
    "spatialPartitioning",
    invoke(pt_rdd$.jobj, "getPartitioner")
  )
  invoke(
    pt_rdd$.jobj,
    "buildIndex",
    invoke_static(
      sc,
      "org.apache.sedona.core.enums.IndexType",
      "RTREE"
    ),
    TRUE
  )
  pair_rdd <- invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.JoinQuery",
    "SpatialJoinQueryCountByKey",
    pt_rdd$.jobj,
    polygon_rdd$.jobj,
    TRUE,
    TRUE
  ) %>%
    apache.sedona:::new_spatial_rdd("pair_rdd")

  sedona_render_choropleth_map(
    pair_rdd,
    1000,
    600,
    output_location = tempfile("choropleth-map-"),
    boundary = c(-126.790180, -64.630926, 24.863836, 50.000),
    base_color = c(255, 255, 255)
  )

  succeed()
})

test_that("overlay operator works as expected", {
  resolution_x <- 1000
  resolution_y <- 600
  boundary <- c(-126.790180, -64.630926, 24.863836, 50.000)

  pt_rdd <- read_point_rdd()
  polygon_rdd <- read_polygon_rdd()
  pair_rdd <- sedona_spatial_join_count_by_key(
    pt_rdd,
    polygon_rdd,
    join_type = "intersect"
  )
  overlay <- sedona_render_scatter_plot(
    polygon_rdd,
    resolution_x,
    resolution_y,
    output_location = tempfile("scatter-plot-"),
    boundary = boundary,
    base_color = c(255, 0, 0),
    browse = FALSE
  )

  sedona_render_choropleth_map(
    pair_rdd,
    resolution_x,
    resolution_y,
    output_location = tempfile("choropleth-map-"),
    boundary = boundary,
    base_color = c(255, 0, 0),
    overlay = overlay
  )

  succeed()
})
