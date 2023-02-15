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

context("sdf interface")

sc <- testthat_spark_connection()

pt_rdd <- read_point_rdd_with_non_spatial_attrs()

shapefile <- function(filename) {
  test_data(file.path("shapefiles", filename))
}

test_that("sdf_register() works as expected for Spatial RDDs", {
  sdf_name <- random_string("spatial_sdf")
  pt_sdf <- sdf_register(pt_rdd, name = sdf_name)
  
  expect_equivalent(
    pt_sdf %>% sdf_schema(),
    list(
      geometry = list(name = "geometry", type = "GeometryUDT")
      
    )
  )
  expect_equal(pt_sdf %>% dbplyr::remote_name(), dbplyr::ident(sdf_name))
  
  pt_sdf %>% collect()
  succeed()
})


test_that("sdf_register() works as expected for Spatial RDDs with fieldNames", {
  sdf_name <- random_string("spatial_sdf")
  polygon_rdd <- sedona_read_shapefile_to_typed_rdd(
    sc,
    location = shapefile("dbf"), type = "polygon"
  )
  polygon_sdf <- sdf_register(polygon_rdd, name = sdf_name)
  
  expect_equivalent(
    polygon_sdf %>% sdf_schema(),
    list(
      geometry = list(name = "geometry", type = "GeometryUDT"),
      geometry = list(name = "STATEFP", type = "StringType"),
      geometry = list(name = "COUNTYFP", type = "StringType"),
      geometry = list(name = "COUNTYNS", type = "StringType"),
      geometry = list(name = "AFFGEOID", type = "StringType"),
      geometry = list(name = "GEOID", type = "StringType"),
      geometry = list(name = "NAME", type = "StringType"),
      geometry = list(name = "LSAD", type = "StringType"),
      geometry = list(name = "ALAND", type = "StringType"),
      geometry = list(name = "AWATER", type = "StringType")
      
    )
  )
  
  expect_equal(polygon_sdf %>% dbplyr::remote_name(), dbplyr::ident(sdf_name))
  
  polygon_sdf %>% collect()
  succeed()
})


test_that("as.spark.dataframe() works as expected for Spatial RDDs with non-spatial attributes", {
  sdf_name <- random_string("spatial_sdf")
  pt_sdf <- as.spark.dataframe(
    pt_rdd,
    non_spatial_cols = paste0("attr_", seq(3)), name = sdf_name
  )

  expect_equivalent(
    pt_sdf %>% sdf_schema(),
    list(
      geometry = list(name = "geometry", type = "GeometryUDT"),
      attr_1 = list(name = "attr_1", type = "StringType"),
      attr_2 = list(name = "attr_2", type = "StringType"),
      attr_3 = list(name = "attr_3", type = "StringType")
    )
  )
  expect_equal(pt_sdf %>% dbplyr::remote_name(), dbplyr::ident(sdf_name))

  pt_sdf %>% collect()
  succeed()
})
