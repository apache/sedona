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

#' Apply a spatial partitioner to a Sedona spatial RDD.
#'
#' Given a Sedona spatial RDD, partition its content using a spatial
#' partitioner.
#'
#' @param rdd The spatial RDD to be partitioned.
#' @param partitioner The name of a grid type to use (currently "quadtree" and
#'   "kdbtree" are supported) or an
#'   `org.apache.sedona.core.spatialPartitioning.SpatialPartitioner` JVM
#'   object. The latter option is only relevant for advanced use cases involving
#'   a custom spatial partitioner.
#' @param max_levels Maximum number of levels in the partitioning tree data
#'   structure. If NULL (default), then use the current number of partitions
#'   within `rdd` as maximum number of levels.
#'   Specifying `max_levels` is unsupported for use cases involving a
#'   custom spatial partitioner because in these scenarios the partitioner
#'   object already has its own maximum number of levels set and there is no
#'   well-defined way to override this existing setting in the partitioning
#'   data structure.
#'
#' @return A spatially partitioned SpatialRDD.
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
#'   sedona_apply_spatial_partitioner(rdd, partitioner = "kdbtree")
#' }
#'
#' @export
sedona_apply_spatial_partitioner <- function(rdd,
                                             partitioner = c("quadtree", "kdbtree"),
                                             max_levels = NULL) {
  apply_spatial_partitioner_impl(
    partitioner = partitioner,
    rdd = rdd,
    max_levels = max_levels
  )

  rdd
}

apply_spatial_partitioner_impl <- function(partitioner = c("quadtree", "kdbtree"),
                                           rdd = NULL,
                                           max_levels = NULL) {
  UseMethod("apply_spatial_partitioner_impl")
}

apply_spatial_partitioner_impl.character <- function(partitioner = c("quadtree", "kdbtree"),
                                                     rdd = NULL,
                                                     max_levels = NULL) {
  sc <- spark_connection(rdd$.jobj)
  grid_type <- sc$state$enums$grid_type[[match.arg(partitioner)]]

  do.call(
    invoke,
    list(rdd$.jobj, "spatialPartitioning", grid_type) %>%
      append(as.integer(max_levels))
  )
  rdd$.state$has_spatial_partitions <- TRUE
  rdd$.state$spatial_partitioner_type <- partitioner
}

apply_spatial_partitioner_impl.spark_jobj <- function(partitioner = c("quadtree", "kdbtree"),
                                                      rdd = NULL,
                                                      max_levels = NULL) {
  if (!is.null(max_levels)) {
    stop("Cannot specify `max_levels` for custom partitioner")
  }

  invoke(rdd$.jobj, "spatialPartitioning", partitioner)
  rdd$.state$has_spatial_partitions <- TRUE
}

apply_spatial_partitioner_impl.default <- function(partitioner = c("quadtree", "kdbtree"),
                                                   rdd = NULL,
                                                   max_levels = NULL) {
  stop(
    "Unsupported partitioner type '",
    paste(class(partitioner), collapse = " "),
    "'"
  )
}
