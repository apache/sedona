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

#' Build an index on a Sedona spatial RDD.
#'
#' Given a Sedona spatial RDD, build the type of index specified on each of its
#' partition(s).
#'
#' @param rdd The spatial RDD to be indexed.
#' @param type The type of index to build. Currently "quadtree" and "rtree" are
#'   supported.
#' @param index_spatial_partitions If the RDD is already partitioned using a
#'   spatial partitioner, then index each spatial partition within the RDD
#'   instead of partitions within the raw RDD associated with the underlying
#'   spatial data source. Default: TRUE.
#'   Notice this option is irrelevant if the input RDD has not been partitioned
#'   using with a spatial partitioner yet.
#'
#' @return A spatial index object.
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
#'     location = input_location,
#'     type = "polygon"
#'   )
#'   sedona_build_index(rdd, type = "rtree")
#' }
#'
#' @export
sedona_build_index <- function(rdd,
                               type = c("quadtree", "rtree"),
                               index_spatial_partitions = TRUE) {
  sc <- spark_connection(rdd$.jobj)
  type <- match.arg(type)

  index_spatial_partitions <- (
    index_spatial_partitions &&
      !is.null(invoke(rdd$.jobj, "spatialPartitionedRDD"))
  )
  invoke(
    rdd$.jobj,
    "buildIndex",
    sc$state$enums$index_type[[type]],
    index_spatial_partitions
  )
  if (index_spatial_partitions) {
    rdd$.state$spatial_partitions_index_type <- type
  } else {
    rdd$.state$raw_partitions_index_type <- type
  }
}
