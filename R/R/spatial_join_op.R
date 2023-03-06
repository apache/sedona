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

#' Spatial join operator
#'
#' R interface for a Sedona spatial join operator
#'
#' @param spatial_rdd Spatial RDD containing geometries to be queried.
#' @param query_window_rdd Spatial RDD containing the query window(s).
#' @param join_type Type of the join query (must be either "contain" or
#'   "intersect").
#'   If `join_type` is "contain", then a geometry from `spatial_rdd` will match
#'   a geometry from the `query_window_rdd` if and only if the former is fully
#'   contained in the latter.
#'   If `join_type` is "intersect", then a geometry from `spatial_rdd` will
#'   match a geometry from the `query_window_rdd` if and only if the former
#'   intersects the latter.
#' @param partitioner Spatial partitioning to apply to both `spatial_rdd` and
#'   `query_window_rdd` to facilitate the join query. Can be either a grid type
#'   (currently "quadtree" and "kdbtree" are supported) or a custom spatial
#'   partitioner object. If `partitioner` is NULL, then assume the same spatial
#'   partitioner has been applied to both `spatial_rdd` and `query_window_rdd`
#'   already and skip the partitioning step.
#' @param index_type Controls how `spatial_rdd` and `query_window_rdd` will be
#'   indexed (unless they are indexed already). If "NONE", then no index will be
#'   constructed and matching geometries will be identified in a doubly nested-
#'   loop iterating through all possible pairs of elements from `spatial_rdd`
#'   and `query_window_rdd`, which will be inefficient for large data sets.
#'
#' @name spatial_join_op
#' @keywords internal
#'
NULL

#' Perform a spatial join operation on two Sedona spatial RDDs.
#'
#' Given `spatial_rdd` and `query_window_rdd`, return a pair RDD containing all
#' pairs of geometrical elements (p, q) such that p is an element of
#' `spatial_rdd`, q is an element of `query_window_rdd`, and (p, q) satisfies
#' the spatial relation specified by `join_type`.
#'
#' @inheritParams spatial_join_op
#' @family Sedona spatial join operator
#'
#' @return A spatial RDD containing the join result.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_dsv_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     delimiter = ",",
#'     type = "point",
#'     first_spatial_col_index = 1L
#'   )
#'   query_rdd_input_location <- "/dev/null" # replace it with the path to your input file
#'   query_rdd <- sedona_read_shapefile_to_typed_rdd(
#'     sc,
#'     location = query_rdd_input_location,
#'     type = "polygon"
#'   )
#'   join_result_rdd <- sedona_spatial_join(
#'     rdd,
#'     query_rdd,
#'     join_type = "intersect",
#'     partitioner = "quadtree"
#'   )
#' }
#' @export
sedona_spatial_join <- function(spatial_rdd,
                                query_window_rdd,
                                join_type = c("contain", "intersect"),
                                partitioner = c("quadtree", "kdbtree"),
                                index_type = c("quadtree", "rtree")) {
  sc <- spark_connection(spatial_rdd$.jobj)
  join_type <- match.arg(join_type)

  ensure_consistent_spatial_partitioning(
    spatial_rdd, query_window_rdd, partitioner
  )
  ensure_spatial_index(
    spatial_rdd, query_window_rdd, index_type
  )

  invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.JoinQuery",
    "SpatialJoinQuery",
    spatial_rdd$.jobj,
    query_window_rdd$.jobj,
    !is.null(index_type),
    identical(join_type, "intersect")
  ) %>%
    new_spatial_rdd(type = "pair")
}

#' Perform a spatial count-by-key operation based on two Sedona spatial RDDs.
#'
#' For each element p from `spatial_rdd`, count the number of unique elements q
#' from `query_window_rdd` such that (p, q) satisfies the spatial relation
#' specified by `join_type`.
#'
#' @inheritParams spatial_join_op
#' @family Sedona spatial join operator
#'
#' @return A spatial RDD containing the join-count-by-key results.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_dsv_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     delimiter = ",",
#'     type = "point",
#'     first_spatial_col_index = 1L
#'   )
#'   query_rdd_input_location <- "/dev/null" # replace it with the path to your input file
#'   query_rdd <- sedona_read_shapefile_to_typed_rdd(
#'     sc,
#'     location = query_rdd_input_location,
#'     type = "polygon"
#'   )
#'   join_result_rdd <- sedona_spatial_join_count_by_key(
#'     rdd,
#'     query_rdd,
#'     join_type = "intersect",
#'     partitioner = "quadtree"
#'   )
#' }
#' @export
sedona_spatial_join_count_by_key <- function(spatial_rdd,
                                             query_window_rdd,
                                             join_type = c("contain", "intersect"),
                                             partitioner = c("quadtree", "kdbtree"),
                                             index_type = c("quadtree", "rtree")) {
  sc <- spark_connection(spatial_rdd$.jobj)
  join_type <- match.arg(join_type)

  ensure_consistent_spatial_partitioning(
    spatial_rdd, query_window_rdd, partitioner
  )
  ensure_spatial_index(
    spatial_rdd, query_window_rdd, index_type
  )

  invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.JoinQuery",
    "SpatialJoinQueryCountByKey",
    spatial_rdd$.jobj,
    query_window_rdd$.jobj,
    !is.null(index_type),
    identical(join_type, "intersect")
  ) %>%
    new_spatial_rdd(type = "count_by_key")
}

ensure_consistent_spatial_partitioning <- function(spatial_rdd,
                                                   query_window_rdd,
                                                   partitioner = c("quadtree", "kdbtree")) {
  if (!is.null(partitioner)) {
    ensure_consistent_spatial_partitioning_impl(
      partitioner = partitioner,
      spatial_rdd = spatial_rdd,
      query_window_rdd = query_window_rdd
    )
  }
}

ensure_consistent_spatial_partitioning_impl <- function(partitioner = c("quadtree", "kdbtree"),
                                                        spatial_rdd = NULL,
                                                        query_window_rdd = NULL) {
  UseMethod("ensure_consistent_spatial_partitioning_impl")
}

ensure_consistent_spatial_partitioning_impl.character <- function(partitioner = c("quadtree", "kdbtree"),
                                                                  spatial_rdd = NULL,
                                                                  query_window_rdd = NULL) {
  partitioner <- match.arg(partitioner)
  if (!identical(spatial_rdd$.state$spatial_partitioner_type, partitioner)) {
    sedona_apply_spatial_partitioner(spatial_rdd, partitioner = partitioner)
  }
  spatial_rdd_partitioner <- spatial_rdd$.jobj %>% invoke("getPartitioner")
  query_window_rdd_partitioner <- query_window_rdd$.jobj %>%
    invoke("getPartitioner")
  if (!spatial_rdd_partitioner %>%
    invoke("equals", query_window_rdd_partitioner)) {
    sedona_apply_spatial_partitioner(
      query_window_rdd,
      partitioner = spatial_rdd_partitioner
    )
  }
}

ensure_consistent_spatial_partitioning_impl.spark_jobj <- function(partitioner = c("quadtree", "kdbtree"),
                                                                   spatial_rdd = NULL,
                                                                   query_window_rdd = NULL) {
  for (x in list(spatial_rdd, query_window_rdd)) {
    existing_partitioner <- x %>% invoke("getPartitioner")
    if (!existing_partitioner %>% invoke("equals", partitioner)) {
      sedona_apply_spatial_partitioner(x, partitioner = partitioner)
    }
  }
}

ensure_spatial_index <- function(spatial_rdd,
                                 query_window_rdd,
                                 index_type = c("quadtree", "rtree")) {
  if (!is.null(index_type)) {
    index_type <- match.arg(index_type)
    for (x in list(spatial_rdd, query_window_rdd)) {
      if (!identical(x$.state$spatial_partitions_index_type, index_type)) {
        sedona_build_index(
          x,
          type = index_type, index_spatial_partitions = TRUE
        )
      }
    }
  }
}
