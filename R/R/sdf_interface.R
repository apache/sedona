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
#' @return A Spark Dataframe containing the imported spatial data.
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
#'     location = input_location,
#'     type = "polygon"
#'   )
#'   sdf <- sdf_register(rdd)
#' }
#'
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
#'   resulting Spark Dataframe. By default (NULL) it will import all field names if that property exists, in particular for shapefiles.
#'
#' @return A Spark Dataframe containing the imported spatial data.
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
#'     first_spatial_col_index = 1L,
#'     repartition = 5
#'   )
#'   sdf <- as.spark.dataframe(rdd, non_spatial_cols = c("attr1", "attr2"))
#' }
#'
#' @export
as.spark.dataframe <- function(x, non_spatial_cols = NULL, name = NULL) {
  sc <- spark_connection(x$.jobj)
  
  # Defaut keep all columns
  if (is.null(non_spatial_cols)) {
    if (!is.null(invoke(x$.jobj, "%>%", list("fieldNames")))) { ## Only if dataset has field names
      non_spatial_cols <- invoke(x$.jobj, "%>%", list("fieldNames"), list("toString")) ### Get columns names
      non_spatial_cols <- gsub("(^\\[|\\]$)", "", non_spatial_cols)  ##### remove brackets
      non_spatial_cols <- strsplit(non_spatial_cols, ", ")[[1]]  ##### turn into list
    }
  } else {
    stopifnot("non_spatial_cols needs to be a charcter vector (or NULL, default)" = is.character(non_spatial_cols))
  }
  
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
