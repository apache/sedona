#' Execute a spatial query
#'
#' Given a spatial RDD, run a spatial query parameterized by a spatial object
#' \code{x}.
#'
#' @param rdd A Sedona spatial RDD.
#' @param x The query object.
#' @param index_type Index to use to facilitate the KNN query. If NULL, then
#'   do not build any additional spatial index on top of \code{x}. Supported
#'   index types are "quadtree" and "rtree".
#' @param result_type Type of result to return.
#'   If "rdd" (default), then the k nearest objects will be returned in a Sedona
#'   spatial RDD.
#'   If "sdf", then a Spark dataframe containing the k nearest objects will be
#'   returned.
#'   If "raw", then a list of k nearest objects will be returned. Each element
#'   within this list will be a JVM object of type
#'   \code{org.locationtech.jts.geom.Geometry}.
#'
#' @name spatial_query
NULL


#' Query the k nearest spatial objects.
#'
#' Given a spatial RDD, a query object \code{x}, and an integer k, find the k
#' nearest spatial objects within the RDD from \code{x} (distance between
#' \code{x} and another geometrical object will be measured by the minimum
#' possible length of any line segment connecting those 2 objects).
#'
#' @inheritParams spatial_query
#' @param k Number of nearest spatail objects to return.
#'
#' @family Sedona spatial query
#' @export
sedona_knn_query <- function(
                             rdd,
                             x,
                             k,
                             index_type = c("quadtree", "rtree"),
                             result_type = c("rdd", "sdf", "raw")) {
  as.spatial_rdd <- function(sc, query_result) {
    raw_spatial_rdd <-
      invoke_static(sc, "java.util.Arrays", "asList", query_result) %>%
      invoke(java_context(sc), "parallelize", .)
    spatial_rdd <- invoke_new(
      sc,
      "org.apache.sedona.core.spatialRDD.SpatialRDD"
    )
    invoke(spatial_rdd, "setRawSpatialRDD", raw_spatial_rdd)

    new_spatial_rdd(spatial_rdd, NULL)
  }

  post_process_query_result <- function(
                                        sc,
                                        query_result,
                                        result_type = c("rdd", "sdf", "raw")) {
    result_type <- match.arg(result_type)

    switch(
      result_type,
      rdd = as.spatial_rdd(sc, query_result),
      sdf = as.spatial_rdd(sc, query_result) %>% sdf_register(),
      raw = query_result
    )
  }

  sc <- spark_connection(rdd$.jobj)

  ensure_spatial_indexing(rdd, index_type)
  invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.KNNQuery",
    "SpatialKnnQuery",
    rdd$.jobj,
    x,
    as.integer(k),
    has_raw_partition_index(rdd)
  ) %>%
    post_process_query_result(sc, ., result_type)
}

#' Execute a range query.
#'
#' Given a spatial RDD and a query object \code{x}, find all spatial objects
#' within the RDD that are covered by \code{x} or intersect \code{x}.
#'
#' @inheritParams spatial_query
#' @param query_type Type of spatial relationship involved in the query.
#'   Currently "cover" and "intersect" are supported.
#'
#' @family Sedona spatial query
#' @export
sedona_range_query <- function(
                               rdd,
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

  post_process_query_result <- function(
                                        sc,
                                        result_rdd,
                                        result_type = c("rdd", "sdf", "raw")) {
    result_type <- match.arg(result_type)

    switch(
      result_type,
      rdd = as.spatial_rdd(sc, result_rdd),
      sdf = as.spatial_rdd(sc, result_rdd) %>% sdf_register(),
      raw = result_rdd %>% invoke("collect")
    )
  }

  sc <- spark_connection(rdd$.jobj)

  ensure_spatial_indexing(rdd, index_type)
  invoke_static(
    sc,
    "org.apache.sedona.core.spatialOperator.RangeQuery",
    "SpatialRangeQuery",
    rdd$.jobj,
    x,
    identical(query_type, "intersect"),
    has_raw_partition_index(rdd)
  ) %>%
    post_process_query_result(sc, ., result_type)
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
