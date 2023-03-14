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

#' Export a Spark SQL query with a spatial column into a Sedona spatial RDD.
#'
#' Given a Spark dataframe object or a dplyr expression encapsulating a Spark
#' SQL query, build a Sedona spatial RDD that will encapsulate the same query or
#' data source. The input should contain exactly one spatial column and all
#' other non-spatial columns will be treated as custom user-defined attributes
#' in the resulting spatial RDD.
#'
#' @param x A Spark dataframe object in sparklyr or a dplyr expression
#'   representing a Spark SQL query.
#' @param spatial_col The name of the spatial column.
#'
#' @return A SpatialRDD encapsulating the query.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   tbl <- dplyr::tbl(
#'     sc,
#'     dplyr::sql("SELECT ST_GeomFromText('POINT(-71.064544 42.28787)') AS `pt`")
#'   )
#'   rdd <- to_spatial_rdd(tbl, "pt")
#' }
#'
#' @export
to_spatial_rdd <- function(x, spatial_col) {
  sdf <- x %>% spark_dataframe()

  invoke_static(
    spark_connection(sdf),
    "org.apache.sedona.sql.utils.Adapter",
    "toSpatialRdd",
    sdf,
    spatial_col
  ) %>%
    new_spatial_rdd(NULL)
}

#' Spatial RDD aggregation routine
#'
#' Function extracting aggregate statistics from a Sedona spatial RDD.
#'
#' @param x A Sedona spatial RDD.
#'
#' @name sedona_spatial_rdd_aggregation_routine
#' @keywords internal
NULL

#' Find the minimal bounding box of a geometry.
#'
#' Given a Sedona spatial RDD, find the axis-aligned minimal bounding box of the
#' geometry represented by the RDD.
#'
#' @inheritParams sedona_spatial_rdd_aggregation_routine
#'
#' @return A minimum bounding box object.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_shapefile_to_typed_rdd(
#'     sc,
#'     location = input_location, type = "polygon"
#'   )
#'   boundary <- minimum_bounding_box(rdd)
#' }
#'
#' @family Spatial RDD aggregation routine
#'
#' @export
minimum_bounding_box <- function(x) {
  x$.jobj %>%
    invoke("boundary") %>%
    make_bounding_box()
}

#' Find the approximate total number of records within a Spatial RDD.
#'
#' Given a Sedona spatial RDD, find the (possibly approximated) number of total
#' records within it.
#'
#' @inheritParams sedona_spatial_rdd_aggregation_routine
#'
#' @return Approximate number of records within the SpatialRDD.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_shapefile_to_typed_rdd(
#'     sc,
#'     location = input_location, type = "polygon"
#'   )
#'   approx_cnt <- approx_count(rdd)
#' }
#'
#' @family Spatial RDD aggregation routine
#'
#' @export
approx_count <- function(x) {
  x$.jobj %>%
    invoke("approximateTotalCount")
}

#' Perform a CRS transformation.
#'
#' Transform data within a spatial RDD from one coordinate reference system to
#' another.
#'
#' @param x The spatial RDD to be processed.
#' @param src_epsg_crs_code Coordinate reference system to transform from
#'  (e.g., "epsg:4326", "epsg:3857", etc).
#' @param dst_epsg_crs_code Coordinate reference system to transform to.
#'  (e.g., "epsg:4326", "epsg:3857", etc).
#' @param strict If FALSE (default), then ignore the "Bursa-Wolf Parameters
#'   Required" error.
#'
#' @return The transformed SpatialRDD.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_geojson_to_typed_rdd(
#'     sc,
#'     location = input_location, type = "polygon"
#'   )
#'   crs_transform(
#'     rdd,
#'     src_epsg_crs_code = "epsg:4326", dst_epsg_crs_code = "epsg:3857"
#'   )
#' }
#'
#' @export
crs_transform <- function(x,
                          src_epsg_crs_code,
                          dst_epsg_crs_code,
                          strict = FALSE) {
  successful <- x$.jobj %>%
    invoke("CRSTransform", src_epsg_crs_code, dst_epsg_crs_code, !strict)
  if (!successful) {
    stop("Failed to perform CRS transformation.")
  }

  x
}

new_spatial_rdd <- function(jobj, type, ...) {
  structure(
    list(.jobj = jobj, .state = new.env(parent = emptyenv())),
    class = paste0(c(type, "spatial"), "_rdd")
  )
}
