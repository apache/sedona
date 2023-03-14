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

#' Execute a spatial query
#'
#' Given a spatial RDD, run a spatial query parameterized by a spatial object
#' `x`.
#'
#' @param rdd A Sedona spatial RDD.
#' @param x The query object.
#' @param index_type Index to use to facilitate the KNN query. If NULL, then
#'   do not build any additional spatial index on top of `x`. Supported
#'   index types are "quadtree" and "rtree".
#' @param result_type Type of result to return.
#'   If "rdd" (default), then the k nearest objects will be returned in a Sedona
#'   spatial RDD.
#'   If "sdf", then a Spark dataframe containing the k nearest objects will be
#'   returned.
#'   If "raw", then a list of k nearest objects will be returned. Each element
#'   within this list will be a JVM object of type
#'   `org.locationtech.jts.geom.Geometry`.
#'
#' @name spatial_query
#' @keywords internal
NULL


#' Query the k nearest spatial objects.
#'
#' Given a spatial RDD, a query object `x`, and an integer k, find the k
#' nearest spatial objects within the RDD from `x` (distance between
#' `x` and another geometrical object will be measured by the minimum
#' possible length of any line segment connecting those 2 objects).
#'
#' @inheritParams spatial_query
#' @param k Number of nearest spatail objects to return.
#'
#' @return The KNN query result.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   knn_query_pt_x <- -84.01
#'   knn_query_pt_y <- 34.01
#'   knn_query_pt_tbl <- sdf_sql(
#'     sc,
#'     sprintf(
#'       "SELECT ST_GeomFromText(\"POINT(%f %f)\") AS `pt`",
#'       knn_query_pt_x,
#'       knn_query_pt_y
#'     )
#'   ) %>%
#'       collect()
#'   knn_query_pt <- knn_query_pt_tbl$pt[[1]]
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_geojson_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     type = "polygon"
#'   )
#'   knn_result_sdf <- sedona_knn_query(
#'     rdd,
#'     x = knn_query_pt, k = 3, index_type = "rtree", result_type = "sdf"
#'   )
#' }
#'
#' @family Sedona spatial query
#'
#' @export
sedona_knn_query <- function(rdd,
                             x,
                             k,
                             index_type = c("quadtree", "rtree"),
                             result_type = c("rdd", "sdf", "raw")) {
  as.spatial_rdd <- function(sc, query_result) {
    query_result <-
      invoke_static(sc, "java.util.Arrays", "asList", query_result)
    raw_spatial_rdd <- invoke(java_context(sc), "parallelize", query_result)
    spatial_rdd <- invoke_new(
      sc,
      "org.apache.sedona.core.spatialRDD.SpatialRDD"
    )
    invoke(spatial_rdd, "setRawSpatialRDD", raw_spatial_rdd)

    new_spatial_rdd(spatial_rdd, NULL)
  }

  post_process_query_result <- function(sc,
                                        query_result,
                                        result_type = c("rdd", "sdf", "raw")) {
    result_type <- match.arg(result_type)

    switch(result_type,
      rdd = as.spatial_rdd(sc, query_result),
      sdf = as.spatial_rdd(sc, query_result) %>% sdf_register(),
      raw = query_result
    )
  }

  sc <- spark_connection(rdd$.jobj)

  ensure_spatial_indexing(rdd, index_type)
  query_result <- invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.KNNQuery",
    "SpatialKnnQuery",
    rdd$.jobj,
    x,
    as.integer(k),
    has_raw_partition_index(rdd)
  )
  post_process_query_result(sc, query_result, result_type)
}

#' Execute a range query.
#'
#' Given a spatial RDD and a query object `x`, find all spatial objects
#' within the RDD that are covered by `x` or intersect `x`.
#'
#' @inheritParams spatial_query
#' @param query_type Type of spatial relationship involved in the query.
#'   Currently "cover" and "intersect" are supported.
#'
#' @return The range query result.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   range_query_min_x <- -87
#'   range_query_max_x <- -50
#'   range_query_min_y <- 34
#'   range_query_max_y <- 54
#'   geom_factory <- invoke_new(
#'     sc,
#'     "org.locationtech.jts.geom.GeometryFactory"
#'   )
#'   range_query_polygon <- invoke_new(
#'     sc,
#'     "org.locationtech.jts.geom.Envelope",
#'     range_query_min_x,
#'     range_query_max_x,
#'     range_query_min_y,
#'     range_query_max_y
#'   ) %>%
#'     invoke(geom_factory, "toGeometry", .)
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_geojson_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     type = "polygon"
#'   )
#'   range_query_result_sdf <- sedona_range_query(
#'     rdd,
#'     x = range_query_polygon,
#'     query_type = "intersect",
#'     index_type = "rtree",
#'     result_type = "sdf"
#'   )
#' }
#'
#' @family Sedona spatial query
#'
#' @export
sedona_range_query <- function(rdd,
                               x,
                               query_type = c("cover", "intersect"),
                               index_type = c("quadtree", "rtree"),
                               result_type = c("rdd", "sdf", "raw")) {
  as.spatial_rdd <- function(sc, result_rdd) {
    raw_spatial_rdd <- invoke_new(
      sc, "org.apache.sedona.core.spatialRDD.SpatialRDD"
    )
    raw_spatial_rdd %>%
      invoke("setRawSpatialRDD", result_rdd)

    raw_spatial_rdd %>% new_spatial_rdd(NULL)
  }

  post_process_query_result <- function(sc,
                                        result_rdd,
                                        result_type = c("rdd", "sdf", "raw")) {
    result_type <- match.arg(result_type)

    switch(result_type,
      rdd = as.spatial_rdd(sc, result_rdd),
      sdf = as.spatial_rdd(sc, result_rdd) %>% sdf_register(),
      raw = result_rdd %>% invoke("collect")
    )
  }

  sc <- spark_connection(rdd$.jobj)

  ensure_spatial_indexing(rdd, index_type)
  query_result <- invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.RangeQuery",
    "SpatialRangeQuery",
    rdd$.jobj,
    x,
    identical(query_type, "intersect"),
    has_raw_partition_index(rdd)
  )
  post_process_query_result(sc, query_result, result_type)
}

ensure_spatial_indexing <- function(rdd, index_type = c("quadtree", "rtree")) {
  if (!is.null(index_type)) {
    index_type <- match.arg(index_type)
    if (!identical(rdd$.state$raw_partition_index_type, index_type)) {
      sedona_build_index(rdd, index_type, index_spatial_partitions = FALSE)
    }
  }
}

has_raw_partition_index <- function(rdd) {
  !is.null(rdd$.state$raw_partition_index_type)
}
