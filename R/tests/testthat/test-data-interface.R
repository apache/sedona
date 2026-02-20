# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


sc <- testthat_spark_connection()

shapefile <- function(filename) {
  test_data(file.path("shapefiles", filename))
}

geoparquet <- function(filename) {
  test_data(file.path("geoparquet", filename))
}

test_rdd_with_non_spatial_attrs <- invoke_new(
  sc,
  "org.apache.sedona.core.spatialRDD.PointRDD",
  java_context(sc),
  test_data("arealm-small.csv"),
  1L, # offset
  sc$state$enums$delimiter$csv,
  TRUE,
  1L # numPartitions
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

expect_result_matches_original_geojson <- function(pt_rdd) {
  expect_equal(
    pt_rdd %>% invoke("%>%", list("rawSpatialRDD"), list("count")),
    test_rdd_with_non_spatial_attrs$.jobj %>%
      invoke("%>%", list("rawSpatialRDD"), list("count"))
  )
  expect_geom_equal_geojson(
    sc,
    test_rdd_with_non_spatial_attrs$.jobj %>%
      invoke("%>%", list("rawSpatialRDD"), list("takeOrdered", 5L), list("toArray")),
    pt_rdd %>%
      invoke("%>%", list("rawSpatialRDD"), list("takeOrdered", 5L), list("toArray"))
  )
}
# ------- Read RDD ------------
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
  lifecycle::expect_deprecated({
    pt_rdd <- sedona_read_geojson_to_typed_rdd(
      sc,
      location = test_data("points.json"),
      type = "point"
    )
  })

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
  lifecycle::expect_deprecated({
    polygon_rdd <- sedona_read_geojson_to_typed_rdd(
      sc,
      location = test_data("testPolygon.json"),
      type = "polygon",
      has_non_spatial_attrs = TRUE
    )
  })

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
  lifecycle::expect_deprecated({
    pt_rdd <- sedona_read_shapefile_to_typed_rdd(
      sc,
      location = shapefile("point"), type = "point"
    )
  })

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


# ------- Read SDF ------------

test_that("spark_read_geoparquet() works as expected", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
    geoparquet_sdf <- spark_read_geoparquet(sc, geoparquet("example1.parquet"), name = sdf_name)
  })
  ## Right number of rows
  geoparquet_df <-
    geoparquet_sdf %>%
    spark_dataframe()

  expect_equal(
    invoke(geoparquet_df, 'count'), 5
  )

  ## Right registered name
  expect_equal(geoparquet_sdf %>% dbplyr::remote_name(), sdf_name)

  ## Right schema
  expect_equivalent(
    geoparquet_sdf %>% sdf_schema(),
    list(
      pop_est    = list(name = "pop_est", type = "LongType"),
      continent  = list(name = "continent", type = "StringType"),
      name       = list(name = "name", type = "StringType"),
      iso_a3     = list(name = "iso_a3", type = "StringType"),
      gdp_md_est = list(name = "gdp_md_est", type = "DoubleType"),
      geometry   = list(name = "geometry", type = "GeometryUDT")
    )
  )

  ## Right data (first row)
  expect_equivalent(
    geoparquet_sdf %>% head(1) %>% mutate(geometry = geometry %>% st_astext()) %>% collect() %>% as.list(),
    list(
      pop_est = 920938,
      continent = "Oceania",
      name = "Fiji",
      iso_a3 = "FJI",
      gdp_md_est = 8374,
      geometry = "MULTIPOLYGON (((180 -16.067132663642447, 180 -16.555216566639196, 179.36414266196414 -16.801354076946883, 178.72505936299711 -17.01204167436804, 178.59683859511713 -16.639150000000004, 179.0966093629971 -16.433984277547403, 179.4135093629971 -16.379054277547404, 180 -16.067132663642447)), ((178.12557 -17.50481, 178.3736 -17.33992, 178.71806 -17.62846, 178.55271 -18.15059, 177.93266000000003 -18.28799, 177.38146 -18.16432, 177.28504 -17.72465, 177.67087 -17.381140000000002, 178.12557 -17.50481)), ((-179.79332010904864 -16.020882256741224, -179.9173693847653 -16.501783135649397, -180 -16.555216566639196, -180 -16.067132663642447, -179.79332010904864 -16.020882256741224)))"
    )
  )

  ## Spatial predicate
  filtered <-
    geoparquet_sdf %>%
    filter(ST_Intersects(ST_Point(35.174722, -6.552465), geometry)) %>%
    collect()
  expect_equal(filtered %>% nrow(), 1)
  expect_equal(filtered$name, "Tanzania")

})


test_that("spark_read_geoparquet() works as expected, ex 2", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
    geoparquet_sdf <- spark_read_geoparquet(sc, geoparquet("example2.parquet"), name = sdf_name)
  })

  ## Right data (first row)
  expect_equivalent(
    geoparquet_sdf %>% head(1) %>% select(name, geometry) %>%  mutate(geometry = geometry %>% st_astext()) %>% collect() %>% as.list(),
    list(
      name = "Vatican City",
      geometry = "POINT (12.453386544971766 41.903282179960115)"
    )
  )

})


test_that("spark_read_geoparquet() works as expected, ex 3", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
    geoparquet_sdf <- spark_read_geoparquet(sc, geoparquet("example3.parquet"), name = sdf_name)
  })
  ## Right data (first row)
  expect_equivalent(
    geoparquet_sdf %>% head(1) %>% mutate(geometry = geometry %>% st_astext() %>% substring(1, 26)) %>% collect() %>% as.list(),
    list(
      BoroCode = 5,
      BoroName = "Staten Island",
      Shape_Leng = 330470.010332,
      Shape_Area = 1.62381982381E9,
      geometry = "MULTIPOLYGON (((970217.022"
    )
  )

})


test_that("spark_read_geoparquet() works as expected, ex 1.0.0-beta.1", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
    geoparquet_sdf <- spark_read_geoparquet(sc, geoparquet("example-1.0.0-beta.1.parquet"), name = sdf_name)
  })
  ## Right number of rows
  geoparquet_df <-
    geoparquet_sdf %>%
    spark_dataframe()

  expect_equal(
    invoke(geoparquet_df, 'count'), 5
  )

})



test_that("spark_read_geoparquet() works as expected, multiple geom", {

  ## Load
  sdf_name <- random_string("spatial_sdf")

  test_data <-
    data.frame(
      id = 1:3,
      g0 = c("POINT (1 2)", "POINT Z(1 2 3)", "MULTIPOINT (0 0, 1 1, 2 2)"),
      g1 = c("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON Z((0 0 2, 1 0 2, 1 1 2, 0 1 2, 0 0 2))", "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))")
    )

  geoparquet_sdf <- copy_to(sc, test_data, sdf_name)
  geoparquet_sdf <- geoparquet_sdf %>% mutate(g0 = st_geomfromtext(g0), g1 = st_geomfromtext(g1))

  ## Write
  tmp_dest <- tempfile()
  lifecycle::expect_deprecated({
    spark_write_geoparquet(geoparquet_sdf, path = tmp_dest, mode = "overwrite")
  })
  ## Check
  ### Can't check on geoparquet metadata with available packages

  file <- dir(tmp_dest, full.names = TRUE, pattern = "parquet$")
  lifecycle::expect_deprecated({
    geoparquet_2_sdf <- spark_read_geoparquet(sc, path = file)
  })
  out <- geoparquet_2_sdf %>% sdf_schema()

  expect_match(out$g0$type, "GeometryUDT")
  expect_match(out$g1$type, "GeometryUDT")

  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geoparquet_2_sdf)))

})


test_that("spark_read_geoparquet() throws an error with plain parquet files", {

  expect_error(
    spark_read_geoparquet(sc, geoparquet("plain.parquet")),
    regexp = "not contain valid geo"
  )

})


test_that("spark_read_geojson() works as expected", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testPolygon.json"), name = sdf_name)
  })
  tmp_dest <- tempfile(fileext = ".json")

  ## Right number of rows
  geojson_df <-
    geojson_sdf %>%
    spark_dataframe()

  expect_equal(
    invoke(geojson_df, 'count'), 1001
  )

  ## Right registered name
  expect_equal(geojson_sdf %>% dbplyr::remote_name(), sdf_name)

})


test_that("spark_read_geojson() works as expected, no feat", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testpolygon-no-property.json"), name = sdf_name)
  })
  ## Right number of rows
  expect_equal(
    invoke(geojson_sdf %>% spark_dataframe(), 'count'), 10
  )

  ## Right registered name
  expect_equal(geojson_sdf %>% dbplyr::remote_name(), sdf_name)

  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))

})

test_that("spark_read_geojson() works as expected, null values", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testpolygon-with-null-property-value.json"), name = sdf_name)
  })
  ## Right number of rows
  expect_equal(
    invoke(geojson_sdf %>% spark_dataframe(), 'count'), 3
  )

  ## Right registered name
  expect_equal(geojson_sdf %>% dbplyr::remote_name(), sdf_name)

  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))

})


test_that("spark_read_geojson() works as expected, with id", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testContainsId.json"), name = sdf_name)
  })
  ## Right number of rows
  expect_equal(
    invoke(geojson_sdf %>% spark_dataframe(), 'count'), 1
  )

  ## Right cols
  expect_equal(
    geojson_sdf %>% sdf_schema(),
    list(
      geometry = list(name = "geometry", type = "GeometryUDT"),
      id       = list(name = "id", type = "StringType"),
      zipcode  = list(name = "zipcode", type = "StringType"),
      name     = list(name = "name", type = "StringType")
    )
  )

  ## Right registered name
  expect_equal(geojson_sdf %>% dbplyr::remote_name(), sdf_name)

  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))

})


test_that("spark_read_geojson() works as expected, invalid geom", {
  sdf_name <- random_string("spatial_sdf")

  # Keep invalid
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testInvalidPolygon.json"), name = sdf_name)
  })
  ## Right number of rows
  expect_equal(
    invoke(geojson_sdf %>% spark_dataframe(), 'count'), 3
  )

  ## Right registered name
  expect_equal(geojson_sdf %>% dbplyr::remote_name(), sdf_name)


  # Remove invalid
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testInvalidPolygon.json"), name = sdf_name, options = list(allow_invalid_geometries = FALSE))
  })
  ## Right number of rows
  expect_equal(
    invoke(geojson_sdf %>% spark_dataframe(), 'count'), 2
  )

  ## Right registered name
  expect_equal(geojson_sdf %>% dbplyr::remote_name(), sdf_name)

  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))

})



# ------- Write RDD ------------

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
    1L # numPartitions
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
    1L # numPartitions
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
    1L # numPartitions
  )

  expect_result_matches_original_geojson(pt_rdd)
})

test_that("sedona_save_spatial_rdd() works as expected", {
  for (fmt in c("wkb", "wkt")) {
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


# ------- Write SDF ------------

test_that("spark_write_geoparquet() works as expected", {
  lifecycle::expect_deprecated({
  geoparquet_sdf <- spark_read_geoparquet(sc, geoparquet("example2.parquet"))
  })
  tmp_dest <- tempfile(fileext = ".parquet")

  ## Save
  lifecycle::expect_deprecated({
  geoparquet_sdf %>% spark_write_geoparquet(tmp_dest)
  })
  ### Reload
  lifecycle::expect_deprecated({
  geoparquet_2_sdf <- spark_read_geoparquet(sc, tmp_dest)
  })
  original_cols <- colnames(geoparquet_sdf)
  expect_equivalent(
    geoparquet_sdf %>% mutate(geometry = geometry %>% st_astext()) %>% collect(),
    geoparquet_2_sdf %>% dplyr::select(dplyr::all_of(original_cols)) %>% mutate(geometry = geometry %>% st_astext()) %>% collect()
  )

  unlink(tmp_dest, recursive = TRUE)

})

test_that("spark_write_geojson() works as expected", {
  sdf_name <- random_string("spatial_sdf")
  lifecycle::expect_deprecated({
  geojson_sdf <- spark_read_geojson(sc, path = test_data("testPolygon.json"), name = sdf_name)
  })
  tmp_dest <- tempfile(fileext = ".json")

  ## Save
  lifecycle::expect_deprecated({
  geojson_sdf %>% spark_write_geojson(tmp_dest)
  })
  ### Reload
  lifecycle::expect_deprecated({
  geojson_2_sdf <- spark_read_geojson(sc, path = tmp_dest)
  })
  ## order of columns changes !
  expect_equal(
    colnames(geojson_sdf) %>% sort(),
    colnames(geojson_2_sdf) %>% sort()
  )
  expect_equal(
    geojson_sdf %>% mutate(geometry = geometry %>% st_astext()) %>% collect(),
    geojson_2_sdf %>% mutate(geometry = geometry %>% st_astext()) %>% collect() %>%
      select(colnames(geojson_sdf))
  )


  unlink(tmp_dest, recursive = TRUE)

})
