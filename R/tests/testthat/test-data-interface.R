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

context("data interface")

sc <- testthat_spark_connection()

shapefile <- function(filename) {
  test_data(file.path("shapefiles", filename))
}

test_rdd_with_non_spatial_attrs <- invoke_new(
  sc,
  "org.apache.sedona.core.spatialRDD.PointRDD",
  java_context(sc),
  test_data("arealm-small.csv"),
  1L, # offset
  sc$state$enums$delimiter$csv,
  TRUE,
  1L, # numPartitions
  sc$state$object_cache$storage_levels$memory_only
) %>%
  apache.sedona:::new_spatial_rdd("point")

expect_result_matches_original <- function(pt_rdd) {
  expect_equal(
    pt_rdd %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    test_rdd_with_non_spatial_attrs$.jobj %>%
      invoke("%>%", list("rawSpatialRDD"), list("count"))
  )
  expect_geom_equal(
    sc,
    test_rdd_with_non_spatial_attrs$.jobj %>%
      invoke("%>%", list("rawSpatialRDD"), list("takeOrdered", 5L), list("toArray")),
    pt_rdd %>%
      invoke("%>%", list("rawSpatialRDD"), list("takeOrdered", 5L), list("toArray"))
  )
}

test_that("sedona_read_dsv_to_typed_rdd() creates PointRDD correctly", {
  pt_rdd <- sedona_read_dsv_to_typed_rdd(
    sc,
    location = test_data("arealm-small.csv"),
    delimiter = ",",
    type = "point",
    first_spatial_col_index = 1,
    has_non_spatial_attrs = TRUE
  )

  expect_equal(class(pt_rdd), c("point_rdd", "spatial_rdd"))
  expect_equal(pt_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(pt_rdd, c(-173.120769, -84.965961, 30.244859, 71.355134))
  for (idx in 0:8) {
    expect_equal(
      pt_rdd$.jobj %>%
        invoke(
          "%>%",
          list("rawSpatialRDD"),
          list("take", 9L),
          list("get", idx),
          list("getUserData")
        ),
      "testattribute0\ttestattribute1\ttestattribute2"
    )
  }
})

test_that("sedona_read_dsv_to_typed_rdd() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_dsv_to_typed_rdd(
    sc,
    location = test_data("primaryroads-polygon.csv"),
    delimiter = ",",
    type = "polygon",
    first_spatial_col_index = 0,
    has_non_spatial_attrs = FALSE
  )

  expect_equal(class(polygon_rdd), c("polygon_rdd", "spatial_rdd"))
  expect_equal(polygon_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(polygon_rdd, c(-158.104182, -66.03575, 17.986328, 48.645133))
})

test_that("sedona_read_dsv_to_typed_rdd() creates LineStringRDD correctly", {
  linestring_rdd <- sedona_read_dsv_to_typed_rdd(
    sc,
    location = test_data("primaryroads-linestring.csv"),
    delimiter = ",",
    type = "linestring",
    first_spatial_col_index = 0,
    has_non_spatial_attrs = FALSE
  )

  expect_equal(class(linestring_rdd), c("linestring_rdd", "spatial_rdd"))
  expect_equal(linestring_rdd$.jobj %>% invoke("approximateTotalCount"), 3000)
  expect_boundary_envelope(linestring_rdd, c(-123.393766, -65.648659, 17.982169, 49.002374))
})

test_that("sedona_read_geojson_to_typed_rdd() creates PointRDD correctly", {
  pt_rdd <- sedona_read_geojson_to_typed_rdd(
    sc,
    location = test_data("points.json"),
    type = "point"
  )

  expect_equal(class(pt_rdd), c("point_rdd", "spatial_rdd"))
  expect_equal(pt_rdd$.jobj %>% invoke("approximateTotalCount"), 7)
  expect_boundary_envelope(pt_rdd, c(-88.1234, -85.3333, 31.3699, 34.9876))
  for (idx in 0:6) {
    expect_equal(
      pt_rdd$.jobj %>%
        invoke(
          "%>%",
          list("rawSpatialRDD"),
          list("take", 9L),
          list("get", idx),
          list("getUserData")
        ),
      "testattribute0\ttestattribute1\ttestattribute2"
    )
  }
})

test_that("sedona_read_geojson_to_typed_rdd() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_geojson_to_typed_rdd(
    sc,
    location = test_data("testPolygon.json"),
    type = "polygon",
    has_non_spatial_attrs = TRUE
  )

  expect_equal(class(polygon_rdd), c("polygon_rdd", "spatial_rdd"))
  expect_equal(polygon_rdd$.jobj %>% invoke("approximateTotalCount"), 1001)
  expect_false(is.null(polygon_rdd$.jobj %>% invoke("boundaryEnvelope")))
  first_2 <- polygon_rdd$.jobj %>%
    invoke("%>%", list("rawSpatialRDD"), list("take", 2L))
  expected_data <- c(
    "01\t077\t011501\t5\t1500000US010770115015\t010770115015\t5\tBG\t6844991\t32636",
    "01\t045\t021102\t4\t1500000US010450211024\t010450211024\t4\tBG\t11360854\t0"
  )
  for (i in seq_along(expected_data)) {
    expect_equal(
      first_2[[i]] %>% invoke("getUserData"),
      expected_data[[i]]
    )
  }
  expect_equal(
    polygon_rdd$.jobj %>%
      invoke("%>%", list("fieldNames"), list("toArray")) %>%
      unlist(),
    c("STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "AFFGEOID", "GEOID", "NAME", "LSAD", "ALAND", "AWATER")
  )
})

test_that("sedona_read_geojson() works as expected on geojson input with 'type' and 'geometry' properties", {
  geojson_rdd <- sedona_read_geojson(sc, test_data("testPolygon.json"))

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 1001
  )
})

test_that("sedona_read_geojson() works as expected on geojson input without 'type' or 'geometry' properties", {
  geojson_rdd <- sedona_read_geojson(sc, test_data("testpolygon-no-property.json"))

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 10
  )
})

test_that("sedona_read_geojson() works as expected on geojson input with null property value", {
  geojson_rdd <- sedona_read_geojson(sc, test_data("testpolygon-with-null-property-value.json"))

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 3
  )
})

test_that("sedona_read_geojson() can skip invalid geometries correctly", {
  geojson_rdd <- sedona_read_geojson(
    sc,
    test_data("testInvalidPolygon.json"),
    allow_invalid_geometries = TRUE,
    skip_syntactically_invalid_geometries = FALSE
  )

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 3
  )

  geojson_rdd <- sedona_read_geojson(
    sc,
    test_data("testInvalidPolygon.json"),
    allow_invalid_geometries = FALSE,
    skip_syntactically_invalid_geometries = FALSE
  )

  expect_equal(
    geojson_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 2
  )
})

test_that("sedona_read_wkb() works as expected", {
  wkb_rdd <- sedona_read_wkb(sc, test_data("county_small_wkb.tsv"))

  expect_equal(
    wkb_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 103
  )
})

test_that("sedona_read_shapefile_to_typed_rdd() creates PointRDD correctly", {
  pt_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = shapefile("point"), type = "point"
  )

  expect_equal(class(pt_rdd), c("point_rdd", "spatial_rdd"))
  expect_equal(
    pt_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    100000
  )
})

test_that("sedona_read_shapefile_to_typed_rdd() creates PolygonRDD correctly", {
  polygon_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = shapefile("polygon"), type = "polygon"
  )

  expect_equal(class(polygon_rdd), c("polygon_rdd", "spatial_rdd"))
  expect_equal(
    polygon_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    20069
  )
})

test_that("sedona_read_shapefile_to_typed_rdd() creates LineStringRDD correctly", {
  linestring_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = shapefile("polyline"), type = "linestring"
  )

  expect_equal(class(linestring_rdd), c("linestring_rdd", "spatial_rdd"))
  expect_equal(
    linestring_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    15137
  )
})

test_that("sedona_read_shapefile() works as expected", {
  wkb_rdd <- sedona_read_shapefile(sc, shapefile("polygon"))

  expect_equal(
    wkb_rdd$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 10000
  )
})

test_that("sedona_write_wkb() works as expected", {
  output_location <- tempfile()
  sedona_write_wkb(test_rdd_with_non_spatial_attrs, output_location)
  pt_rdd <- invoke_new(
    sc,
    "org.apache.sedona.core.spatialRDD.PointRDD",
    java_context(sc),
    output_location,
    0L, # offset
    sc$state$enums$delimiter$wkb,
    TRUE,
    1L, # numPartitions
    sc$state$object_cache$storage_levels$memory_only
  )

  expect_result_matches_original(pt_rdd)
})

test_that("sedona_write_wkt() works as expected", {
  output_location <- tempfile()
  sedona_write_wkt(test_rdd_with_non_spatial_attrs, output_location)
  pt_rdd <- invoke_new(
    sc,
    "org.apache.sedona.core.spatialRDD.PointRDD",
    java_context(sc),
    output_location,
    0L, # offset
    sc$state$enums$delimiter$wkt,
    TRUE,
    1L, # numPartitions
    sc$state$object_cache$storage_levels$memory_only
  )

  expect_result_matches_original(pt_rdd)
})

test_that("sedona_write_geojson() works as expected", {
  output_location <- tempfile()
  sedona_write_geojson(test_rdd_with_non_spatial_attrs, output_location)
  pt_rdd <- invoke_new(
    sc,
    "org.apache.sedona.core.spatialRDD.PointRDD",
    java_context(sc),
    output_location,
    0L, # offset
    sc$state$enums$delimiter$geojson,
    TRUE,
    1L, # numPartitions
    sc$state$object_cache$storage_levels$memory_only
  )

  expect_result_matches_original(pt_rdd)
})

test_that("sedona_save_spatial_rdd() works as expected", {
  for (fmt in c("wkb", "wkt", "geojson")) {
    location <- tempfile(pattern = "pt_", fileext = paste0(".", fmt))
    copy_to(
      sc, tibble::tibble(id = 1, name = "a point", type = "point")
    ) %>%
      dplyr::mutate(
        pt = dplyr::sql(
          "ST_Point(CAST(123 AS Decimal(24, 20)), CAST(456 AS Decimal(24, 20)))"
        )
      ) %>%
      sedona_save_spatial_rdd(
        spatial_col = "pt", output_location = location, output_format = fmt
      )
    sdf <- do.call(paste0("sedona_read_", fmt), list(sc, location))

    expect_equal(
      sdf$.jobj %>% invoke("%>%", list("rawSpatialRDD"), list("count")), 1
    )
    pts <- sdf$.jobj %>%
      invoke("%>%", list("rawSpatialRDD"), list("takeOrdered", 1L))
    expect_equal(pts[[1]] %>% invoke("getX"), 123)
    expect_equal(pts[[1]] %>% invoke("getY"), 456)
    expect_equal(pts[[1]] %>% invoke("getUserData"), "1.0\ta point\tpoint")
  }
})
