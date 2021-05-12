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
#' @examples
#' library(sparklyr)
#' library(sparklyr.sedona)
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
