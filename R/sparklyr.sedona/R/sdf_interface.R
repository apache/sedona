#' Import data from a spatial RDD into a Spark Dataframe.
#'
#' @param x A spatial RDD.
#' @param name Name to assign to the resulting Spark temporary view. If
#'   unspecified, then a random name will be assigned.
#'
#' @name as_spark_dataframe
NULL

#' Import data from a spatial RDD into a Spark Dataframe.
#'
#' @inheritParams as_spark_dataframe
#'
#' @importFrom sparklyr sdf_register
#'
#' @examples
#' library(sparklyr)
#' library(sparklyr.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_geojson_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     type = "polygon"
#'   )
#'   sdf <- sdf_register(rdd)
#' }
#' @export
sdf_register.spatial_rdd <- function(x, name = NULL) {
  as.spark.dataframe(x, name = name)
}

#' Import data from a spatial RDD into a Spark Dataframe.
#'
#' Import data from a spatial RDD (possibly with non-spatial attributes) into a
#' Spark Dataframe.
#'
#' @inheritParams as_spark_dataframe
#' @param non_spatial_cols Column names for non-spatial attributes in the
#'   resulting Spark Dataframe.
#'
#' @examples
#' library(sparklyr)
#' library(sparklyr.sedona)
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
#'     first_spatial_col_index = 1L,
#'     repartition = 5
#'   )
#'   sdf <- as.spark.dataframe(rdd, non_spatial_cols = c("attr1", "attr2"))
#' }
#' @export
as.spark.dataframe <- function(x, non_spatial_cols = NULL, name = NULL) {
  sc <- spark_connection(x$.jobj)
  sdf <- invoke_static(
    sc,
    "org.apache.sedona.sql.utils.Adapter",
    "toDf",
    x$.jobj,
    as.list(non_spatial_cols),
    spark_session(sc)
  )
  sdf_register(sdf, name)
}
