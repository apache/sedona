#' @importFrom sparklyr sdf_register
#' @export
sdf_register.spatial_rdd <- function(x, name = NULL) {
  as.spark.dataframe(x, name = name)
}

#' Import data from a spatial RDD into a Spark Dataframe.
#'
#' Import data from a spatial RDD (possibly with non-spatial attributes) into a
#' Spark Dataframe.
#'
#' @param x A spatial RDD.
#' @param non_spatial_cols Column names for non-spatial attributes in the
#'   resulting Spark Dataframe.
#' @param name Name to assign to the resulting Spark temporary view. If
#'   unspecified, then a random name will be assigned.
#'
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
