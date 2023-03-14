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


# ------- Read RDD ------------

#' Create a SpatialRDD from an external data source.
#' 
#' Import spatial object from an external data source into a Sedona SpatialRDD.
#'
#' @param sc A `spark_connection`.
#' @param location Location of the data source.
#' @param type Type of the SpatialRDD (must be one of "point", "polygon", or
#'   "linestring".
#' @param has_non_spatial_attrs Whether the input contains non-spatial
#'   attributes.
#' @param storage_level Storage level of the RDD (default: MEMORY_ONLY).
#' @param repartition The minimum number of partitions to have in the resulting
#'   RDD (default: 1).
#'
#' @name sedona_spatial_rdd_data_source
#' @keywords internal
NULL

#' Create a typed SpatialRDD from a delimiter-separated values data source.
#'
#' Create a typed SpatialRDD (namely, a PointRDD, a PolygonRDD, or a
#' LineStringRDD) from a data source containing delimiter-separated values.
#' The data source can contain spatial attributes (e.g., longitude and latidude)
#' and other attributes. Currently only inputs with spatial attributes occupying
#' a contiguous range of columns (i.e.,
#' \[first_spatial_col_index, last_spatial_col_index\]) are supported.
#'
#' @inheritParams sedona_spatial_rdd_data_source
#' @param delimiter Delimiter within each record. Must be one of
#'   ',', '\\t', '?', '\\'', '"', '_', '-', '%', '~', '|', ';'
#' @param first_spatial_col_index Zero-based index of the left-most column
#'   containing spatial attributes (default: 0).
#' @param last_spatial_col_index Zero-based index of the right-most column
#'   containing spatial attributes (default: NULL). Note last_spatial_col_index
#'   does not need to be specified when creating a PointRDD because it will
#'   automatically have the implied value of (first_spatial_col_index + 1).
#'   For all other types of RDDs, if last_spatial_col_index is unspecified, then
#'   it will assume the value of -1 (i.e., the last of all input columns).
#'
#' @return A typed SpatialRDD.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your csv file
#'   rdd <- sedona_read_dsv_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     delimiter = ",",
#'     type = "point",
#'     first_spatial_col_index = 1L
#'   )
#' }
#'
#' @family Sedona RDD data interface functions
#'
#' @export
sedona_read_dsv_to_typed_rdd <- function(sc,
                                         location,
                                         delimiter = c(",", "\t", "?", "'", "\"", "_", "-", "%", "~", "|", ";"),
                                         type = c("point", "polygon", "linestring"),
                                         first_spatial_col_index = 0L,
                                         last_spatial_col_index = NULL,
                                         has_non_spatial_attrs = TRUE,
                                         storage_level = "MEMORY_ONLY",
                                         repartition = 1L) {
  delimiter <- to_delimiter_enum_value(sc, match.arg(delimiter))
  rdd_cls <- rdd_cls_from_type(type)
  first_spatial_col_index <- as.integer(first_spatial_col_index)
  if (type != "point") {
    last_spatial_col_index <- last_spatial_col_index %||% -1L
  } else {
    if (!is.null(last_spatial_col_index)) {
      if (as.integer(last_spatial_col_index) != first_spatial_col_index + 1L) {
        stop(
          "last_spatial_col_index must be either unspecified or be equal to ",
          "(first_spatial_col_index + 1) for PointRDD"
        )
      }
    } else {
      last_spatial_col_index <- first_spatial_col_index + 1L
    }
  }
  last_spatial_col_index <- as.integer(last_spatial_col_index)
  fmt <- (
    if (identical(type, "point")) {
      if (!is.null(last_spatial_col_index) &&
        last_spatial_col_index != first_spatial_col_index + 1L) {
      }
      invoke_new(
        sc,
        "org.apache.sedona.core.formatMapper.PointFormatMapper",
        first_spatial_col_index,
        delimiter,
        has_non_spatial_attrs
      )
    } else {
      fmt_cls <- paste0(
        "org.apache.sedona.core.formatMapper.",
        if (identical(type, "polygon")) {
          "Polygon"
        } else {
          "LineString"
        },
        "FormatMapper"
      )

      invoke_new(
        sc,
        fmt_cls,
        first_spatial_col_index,
        last_spatial_col_index,
        delimiter,
        has_non_spatial_attrs
      )
    })

  invoke_new(
    sc,
    rdd_cls,
    java_context(sc),
    location,
    max(as.integer(repartition %||% 1L), 1L),
    fmt
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(type)
}

#' (Deprecated) Create a typed SpatialRDD from a shapefile or geojson data source.
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#' 
#' Constructors of typed RDD (PointRDD, PolygonRDD, LineStringRDD) are soft deprecated, use non-types versions
#' 
#' Create a typed SpatialRDD (namely, a PointRDD, a PolygonRDD, or a
#' LineStringRDD)
#' * `sedona_read_shapefile_to_typed_rdd`: from a shapefile data source
#' * `sedona_read_geojson_to_typed_rdd`: from a GeoJSON data source
#' 
#'
#' @param sc A `spark_connection`.
#' @param location Location of the data source.
#' @param type Type of the SpatialRDD (must be one of "point", "polygon", or
#'   "linestring".
#' @param has_non_spatial_attrs Whether the input contains non-spatial
#'   attributes.
#' @param storage_level Storage level of the RDD (default: MEMORY_ONLY).
#' @param repartition The minimum number of partitions to have in the resulting
#'   RDD (default: 1).
#'
#' @return A typed SpatialRDD.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your shapefile
#'   rdd <- sedona_read_shapefile_to_typed_rdd(
#'     sc,
#'     location = input_location, type = "polygon"
#'   )
#' }
#'
#' @family Sedona RDD data interface functions
#'
#' @export
sedona_read_shapefile_to_typed_rdd <- function(sc,
                                               location,
                                               type = c("point", "polygon", "linestring"),
                                               storage_level = "MEMORY_ONLY") {
  
  lifecycle::deprecate_soft(
    "1.4.0",
    "sedona_read_shapefile_to_typed_rdd()",
    with = "sedona_read_shapefile()"
  )
  
  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader",
    paste0("readTo", to_camel_case(type), "RDD"),
    java_context(sc),
    location
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(type)
}


#' @export
#' @rdname sedona_read_shapefile_to_typed_rdd
sedona_read_geojson_to_typed_rdd <- function(sc,
                                             location,
                                             type = c("point", "polygon", "linestring"),
                                             has_non_spatial_attrs = TRUE,
                                             storage_level = "MEMORY_ONLY",
                                             repartition = 1L) {
  
  lifecycle::deprecate_soft(
    "1.4.0",
    "sedona_read_geojson_to_typed_rdd()",
    with = "sedona_read_geojson()"
  )
  
  invoke_new(
    sc,
    rdd_cls_from_type(type),
    java_context(sc),
    location,
    sc$state$enums$delimiter$geojson,
    has_non_spatial_attrs,
    max(as.integer(repartition %||% 1L), 1L)
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(type)
}



#' Read geospatial data into a Spatial RDD
#'
#' @description Import spatial object from an external data source into a Sedona SpatialRDD.
#' * `sedona_read_shapefile`: from a shapefile 
#' * `sedona_read_geojson`: from a geojson file 
#' * `sedona_read_wkt`: from a geojson file 
#' * `sedona_read_wkb`: from a geojson file 
#'
#' @param sc A `spark_connection`.
#' @param location Location of the data source.
#' @param storage_level Storage level of the RDD (default: MEMORY_ONLY).
#' @param repartition The minimum number of partitions to have in the resulting
#'   RDD (default: 1).
#' @param allow_invalid_geometries Whether to allow topology-invalid
#'   geometries to exist in the resulting RDD.
#' @param skip_syntactically_invalid_geometries Whether to allows Sedona to
#'   automatically skip syntax-invalid geometries, rather than throwing
#'   errorings.
#' @param wkt_col_idx Zero-based index of column containing hex-encoded WKB data
#'   (default: 0).
#' @param wkb_col_idx Zero-based index of column containing hex-encoded WKB data
#'   (default: 0).
#'
#' @return A SpatialRDD.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_geojson(sc, location = input_location)
#' }
#'
#' @family Sedona RDD data interface functions
#'
#' @export
sedona_read_geojson <- function(sc,
                                location,
                                allow_invalid_geometries = TRUE,
                                skip_syntactically_invalid_geometries = TRUE,
                                storage_level = "MEMORY_ONLY",
                                repartition = 1L) {
  raw_text_rdd <- invoke(
    java_context(sc),
    "textFile",
    location,
    max(as.integer(repartition %||% 1L), 1L)
  )
  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.GeoJsonReader",
    "readToGeometryRDD",
    raw_text_rdd,
    allow_invalid_geometries,
    skip_syntactically_invalid_geometries
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(NULL)
}


#' @export
#' @rdname sedona_read_geojson
sedona_read_wkb <- function(sc,
                            location,
                            wkb_col_idx = 0L,
                            allow_invalid_geometries = TRUE,
                            skip_syntactically_invalid_geometries = TRUE,
                            storage_level = "MEMORY_ONLY",
                            repartition = 1L) {
  raw_text_rdd <- invoke(
    java_context(sc),
    "textFile",
    location,
    max(as.integer(repartition %||% 1L), 1L)
  )

  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.WkbReader",
    "readToGeometryRDD",
    raw_text_rdd,
    as.integer(wkb_col_idx),
    allow_invalid_geometries,
    skip_syntactically_invalid_geometries
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(NULL)
}


#' @export
#' @rdname sedona_read_geojson
sedona_read_wkt <- function(sc,
                            location,
                            wkt_col_idx = 0L,
                            allow_invalid_geometries = TRUE,
                            skip_syntactically_invalid_geometries = TRUE,
                            storage_level = "MEMORY_ONLY",
                            repartition = 1L) {
  raw_text_rdd <- invoke(
    java_context(sc),
    "textFile",
    location,
    max(as.integer(repartition %||% 1L), 1L)
  )

  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.WktReader",
    "readToGeometryRDD",
    raw_text_rdd,
    as.integer(wkt_col_idx),
    allow_invalid_geometries,
    skip_syntactically_invalid_geometries
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(NULL)
}

#' @rdname sedona_read_geojson
#' @export
sedona_read_shapefile <- function(sc,
                                  location,
                                  storage_level = "MEMORY_ONLY") {
  invoke_static(
    sc,
    "org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader",
    paste0("readToGeometryRDD"),
    java_context(sc),
    location
  ) %>%
    set_storage_level(storage_level) %>%
    new_spatial_rdd(NULL)
}


# ------- Read SDF ------------
#' Read geospatial data into a Spark DataFrame.
#' 
#' @description Functions to read geospatial data from a variety of formats into Spark DataFrames.
#' 
#' * `spark_read_shapefile`: from a shapefile 
#' * `spark_read_geojson`: from a geojson file 
#' * `spark_read_geoparquet`: from a geoparquet file 
#' * `spark_read_geotiff`: from a GeoTiff file, or a folder containing GeoTiff files
#'
#' @inheritParams sparklyr::spark_read_source
#'
#'
#' @return A tbl
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- spark_read_shapefile(sc, location = input_location)
#' }
#'
#' @family Sedona DF data interface functions
#'
#' @export
spark_read_shapefile <- function(sc,
                                 name = NULL,
                                 path = name,
                                 options = list(),
                                 ...) {
  
  lapply(names(options), function(name) {
    if (!name %in% c("")) {
      warning(paste0("Ignoring unkown option '", name,"'"))
    }
  })
  
  rdd <- sedona_read_shapefile(sc,
                               location = path,
                               storage_level = "MEMORY_ONLY")
  
  
  
  rdd %>% sdf_register(name = name)
}


#' @export
#' @rdname spark_read_shapefile
spark_read_geojson <- function(sc,
                               name = NULL,
                               path = name,
                               options = list(),
                               repartition = 0,
                               memory = TRUE,
                               overwrite = TRUE) {
  
  # check options
  if ("allow_invalid_geometries" %in% names(options)) final_allow_invalid <- options[["allow_invalid_geometries"]] else final_allow_invalid <- TRUE
  if ("skip_syntactically_invalid_geometries" %in% names(options)) final_skip <- options[["skip_syntactically_invalid_geometries"]] else final_skip <- TRUE
  lapply(names(options), function(name) {
    if (!name %in% c("allow_invalid_geometries", "skip_syntactically_invalid_geometries")) {
      warning(paste0("Ignoring unkown option '", name,"'"))
    }
  })
  
  final_repartition <- max(as.integer(repartition), 1L)
  
  rdd <- sedona_read_geojson(sc,
                             location = path,
                             allow_invalid_geometries = final_allow_invalid,
                             skip_syntactically_invalid_geometries = final_skip,
                             storage_level = "MEMORY_ONLY",
                             repartition = final_repartition)
  
  
  
  rdd %>% sdf_register(name = name)
}

#' @export
#' @rdname spark_read_shapefile
#' @importFrom sparklyr spark_read_source
spark_read_geoparquet <- function(sc,
                                  name = NULL,
                                  path = name,
                                  options = list(),
                                  repartition = 0,
                                  memory = TRUE,
                                  overwrite = TRUE) {
  
  spark_read_source(sc, 
                    name = name,
                    path = path,
                    source = "geoparquet",
                    options = options,
                    repartition = repartition,
                    memory = memory,
                    overwrite = overwrite,
                    columns = NULL)
}


#' @export
#' @rdname spark_read_shapefile
#' @importFrom sparklyr spark_read_source
spark_read_geotiff <- function(sc,
                               name = NULL,
                               path = name,
                               options = list(),
                               repartition = 0,
                               memory = TRUE,
                               overwrite = TRUE) {
  
  spark_read_source(sc, 
                    name = name,
                    path = path,
                    source = "geotiff",
                    options = options,
                    repartition = repartition,
                    memory = memory,
                    overwrite = overwrite,
                    columns = NULL)
}


# ------- Write RDD ------------


#' Write SpatialRDD into a file.
#'
#' Export serialized data from a Sedona SpatialRDD into a file.
#' * `sedona_write_wkb`:
#' * `sedona_write_wkt`:
#' * `sedona_write_geojson`:
#'
#' @param x The SpatialRDD object.
#' @param output_location Location of the output file.
#'
#' @return No return value.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_wkb(
#'     sc,
#'     location = input_location,
#'     wkb_col_idx = 0L
#'   )
#'   sedona_write_wkb(rdd, "/tmp/wkb_output.tsv")
#' }
#'
#' @family Sedona RDD data interface functions
#'
#' @export
sedona_write_wkb <- function(x, output_location) {
  invoke(x$.jobj, "saveAsWKB", output_location)
}


#' @export
#' @rdname sedona_write_wkb
sedona_write_wkt <- function(x, output_location) {
  invoke(x$.jobj, "saveAsWKT", output_location)
}


#' @export
#' @rdname sedona_write_wkb
sedona_write_geojson <- function(x, output_location) {
  invoke(x$.jobj, "saveAsGeoJSON", output_location)
}

#' Save a Spark dataframe containing exactly 1 spatial column into a file.
#'
#' Export serialized data from a Spark dataframe containing exactly 1 spatial
#' column into a file.
#'
#' @param x A Spark dataframe object in sparklyr or a dplyr expression
#'   representing a Spark SQL query.
#' @param spatial_col The name of the spatial column.
#' @param output_location Location of the output file.
#' @param output_format Format of the output.
#'
#' @return No return value.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   tbl <- dplyr::tbl(
#'     sc,
#'     dplyr::sql("SELECT ST_GeomFromText('POINT(-71.064544 42.28787)') AS `pt`")
#'   )
#'   sedona_save_spatial_rdd(
#'     tbl %>% dplyr::mutate(id = 1),
#'     spatial_col = "pt",
#'     output_location = "/tmp/pts.wkb",
#'     output_format = "wkb"
#'   )
#' }
#'
#' @family Sedona RDD data interface functions
#'
#' @export
sedona_save_spatial_rdd <- function(x,
                                    spatial_col,
                                    output_location,
                                    output_format = c("wkb", "wkt", "geojson")) {
  spatial_rdd <- to_spatial_rdd(x, spatial_col)
  output_format <- match.arg(output_format)

  do.call(
    paste0("sedona_write_", output_format),
    list(spatial_rdd, output_location)
  )
}


# ------- Write SDF ------------


### No shapefile writer in Sedona



#' Write geospatial data from a Spark DataFrame.
#'
#' @description Functions to write geospatial data into a variety of formats from Spark DataFrames.
#' 
#' * `spark_write_geojson`: to GeoJSON
#' * `spark_write_geoparquet`: to GeoParquet
#' * `spark_write_geotiff`: to GeoTiff
#'
#'
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3a://"} and \samp{"file://"} protocols.
#' @inheritParams sparklyr::spark_write_source
#'
#'
#' @return NULL
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   tbl <- dplyr::tbl(
#'     sc,
#'     dplyr::sql("SELECT ST_GeomFromText('POINT(-71.064544 42.28787)') AS `pt`")
#'   )
#'   spark_write_geojson(
#'     tbl %>% dplyr::mutate(id = 1),
#'     output_location = "/tmp/pts.geojson"
#'   )
#' }
#'
#' @family Sedona DF data interface functions
#'
#' @importFrom sparklyr spark_write_source
#' @export
spark_write_geojson <- function(x,
                                path,
                                mode = NULL,
                                options = list(),
                                partition_by = NULL,
                                ...) {
  
  ## find geometry column if not specified
  if (!"spatial_col" %in% names(options)) {
    schema <- x %>% sdf_schema()
    potential_cols <- which(sapply(schema, function(x) x$type == "GeometryUDT"))
    
    if (length(potential_cols) == 0) {
      cli::cli_abort("No geometry column found")
    } else if (length(potential_cols) > 1) {
      spatial_col = names(potential_cols)[1]
      cli::cli_warn("Multiple geometry columns found, using {spatial_col}")
    } else {
      spatial_col = names(potential_cols)
    }
    
  } else {
    spatial_col = options[["spatial_col"]]
  }
  
  rdd <- x %>% to_spatial_rdd(spatial_col = spatial_col)
  
  sedona_write_geojson(x = rdd, output_location = path)
  
}


#' @export
#' @rdname spark_write_geojson
#' @importFrom sparklyr spark_write_source
spark_write_geoparquet <- function(x,
                                   path,
                                   mode = NULL,
                                   options = list(),
                                   partition_by = NULL,
                                   ...) {
  
  spark_write_source(
    x = x,
    source = "geoparquet",
    mode = mode,
    options = options,
    partition_by = partition_by,
    save_args = list(path),
    ...
  )
  
}

#' @export
#' @rdname spark_write_geojson
#' @importFrom sparklyr spark_write_source
spark_write_geotiff <- function(x,
                                   path,
                                   mode = NULL,
                                   options = list(),
                                   partition_by = NULL,
                                   ...) {
  
  spark_write_source(
    x = x,
    source = "geotiff",
    mode = mode,
    options = options,
    partition_by = partition_by,
    save_args = list(path),
    ...
  )
  
}

# ------- Utilities ------------
rdd_cls_from_type <- function(type = c("point", "polygon", "linestring")) {
  type <- match.arg(type)

  paste0(
    "org.apache.sedona.core.spatialRDD.",
    to_camel_case(type),
    "RDD"
  )
}

to_camel_case <- function(type) {
  switch(type,
    point = "Point",
    polygon = "Polygon",
    linestring = "LineString"
  )
}

to_delimiter_enum_value <- function(sc, delimiter) {
  delimiter <- switch(delimiter,
    "," = "csv",
    "\t" = "tsv",
    "?" = "questionmark",
    "'" = "singlequote",
    "\"" = "quote",
    "_" = "underscore",
    "-" = "dash",
    "%" = "percent",
    "~" = "tilde",
    "|" = "pipe",
    ";" = "semicolon",
    stop("Unsupported delimiter '", delimiter, "'")
  )

  sc$state$enums$delimiter[[delimiter]]
}

set_storage_level <- function(rdd, storage_level) {
  sc <- spark_connection(rdd)
  storage_level <- sc$state$object_cache$storage_levels[[storage_level]] %||% {
    storage_level_obj <- invoke_static(
      sc, "org.apache.spark.storage.StorageLevel", storage_level
    )
    sc$state$object_cache$storage_levels[[storage_level]] <- storage_level_obj

    storage_level_obj
  }
  invoke(rdd, "analyze", storage_level)

  rdd
}
