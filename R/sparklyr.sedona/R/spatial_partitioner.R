#' Apply a spatial partitioner to a Sedona spatial RDD.
#'
#' Given a Sedona spatial RDD, partition its content using a spatial
#' partitioner.
#'
#' @param rdd The spatial RDD to be partitioned.
#' @param partitioner The name of a grid type to use (currently "quadtree" and
#'   "kdbtree" are supported) or an
#'   \code{org.apache.sedona.core.spatialPartitioning.SpatialPartitioner} JVM
#'   object. The latter option is only relevant for advanced use cases involving
#'   a custom spatial partitioner.
#' @param max_levels Maximum number of levels in the partitioning tree data
#'   structure. If NULL (default), then use the current number of partitions
#'   within \code{rdd} as maximum number of levels.
#'   Specifying \code{max_levels} is unsupported for use cases involving a
#'   custom spatial partitioner because in these scenarios the partitioner
#'   object already has its own maximum number of levels set and there is no
#'   well-defined way to override this existing setting in the partitioning
#'   data structure.
#'
#' @export
sedona_apply_spatial_partitioner <- function(
                                             rdd,
                                             partitioner = c("quadtree", "kdbtree"),
                                             max_levels = NULL) {
  apply_spatial_partitioner_impl(
    partitioner = partitioner,
    rdd = rdd,
    max_levels = max_levels
  )
}

apply_spatial_partitioner_impl <- function(
                                           partitioner = c("quadtree", "kdbtree"),
                                           rdd = NULL,
                                           max_levels = NULL) {
  UseMethod("apply_spatial_partitioner_impl")
}

apply_spatial_partitioner_impl.character <- function(
                                                     partitioner = c("quadtree", "kdbtree"),
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

apply_spatial_partitioner_impl.spark_jobj <- function(
                                                      partitioner = c("quadtree", "kdbtree"),
                                                      rdd = NULL,
                                                      max_levels = NULL) {
  if (!is.null(max_levels)) {
    stop("Cannot specify `max_levels` for custom partitioner")
  }

  invoke(rdd$.jobj, "spatialPartitioning", partitioner)
  rdd$.state$has_spatial_partitions <- TRUE
}

apply_spatial_partitioner_impl.default <- function(
                                                   partitioner = c("quadtree", "kdbtree"),
                                                   rdd = NULL,
                                                   max_levels = NULL) {
  stop(
    "Unsupported partitioner type '",
    paste(class(partitioner), collapse = " "),
    "'"
  )
}
