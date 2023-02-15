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

spark_dependencies <- function(spark_version, scala_version, ...) {
  if (spark_version[1, 1] == "3") {
    spark_version <- "3.0"
    scala_version <- scala_version %||% "2.12"
  } else if (spark_version[1, 1] == "2" && spark_version[1, 2] == "4") {
    spark_version <- "2.4"
    scala_version <- scala_version %||% "2.11"
  } else {
    stop("Unsupported Spark version: ", spark_version)
  }

  packages <- c(
    "org.datasyslab:geotools-wrapper:1.3.0-27.2",
    "edu.ucar:cdm-core:5.4.2",
    "org.locationtech.jts:jts-core:1.19.0",
    "org.wololo:jts2geojson:0.16.1"
  )
  jars <- NULL

  sedona_jar_files <- Sys.getenv("SEDONA_JAR_FILES")
  if (nchar(sedona_jar_files) > 0) {
    jars <- strsplit(sedona_jar_files, ":")[[1]]
  } else {
    packages <- c(
      paste0(
        "org.apache.sedona:sedona-",
        c("core", "sql", "viz"),
        sprintf("-%s_%s:1.3.1-incubating", spark_version, scala_version)
      ),
      packages
    )
  }

  spark_dependency(
    jars = jars,
    packages = packages,
    initializer = sedona_initialize_spark_connection,
    repositories = c("https://artifacts.unidata.ucar.edu/repository/unidata-all"),
    dbplyr_sql_variant = sedona_dbplyr_sql_variant()
  )
}

sedona_initialize_spark_connection <- function(sc) {
  invoke_static(
    sc,
    "org.apache.sedona.sql.utils.SedonaSQLRegistrator",
    "registerAll",
    spark_session(sc)
  )

  # Instantiate all enum objects and store them immutably under
  # sc$state$enums
  for (x in c(
    "csv",
    "tsv",
    "geojson",
    "wkt",
    "wkb",
    "comma",
    "tab",
    "questionmark",
    "singlequote",
    "quote",
    "underscore",
    "dash",
    "percent",
    "tilde",
    "pipe",
    "semicolon"
  )) {
    sc$state$enums$delimiter[[x]] <- invoke_static(
      sc, "org.apache.sedona.core.enums.FileDataSplitter", toupper(x)
    )
  }
  for (x in c(
    "point",
    "polygon",
    "linestring",
    "multipoint",
    "multipolygon",
    "multilinestring",
    "geometrycollection",
    "circle",
    "rectangle"
  )) {
    sc$state$enums$geometry_type[[x]] <- invoke_static(
      sc, "org.apache.sedona.core.enums.GeometryType", toupper(x)
    )
  }
  for (x in c("quadtree", "rtree")) {
    sc$state$enums$index_type[[x]] <- invoke_static(
      sc, "org.apache.sedona.core.enums.IndexType", toupper(x)
    )
  }
  for (x in c("quadtree", "kdbtree")) {
    sc$state$enums$grid_type[[x]] <- invoke_static(
      sc, "org.apache.sedona.core.enums.GridType", toupper(x)
    )
  }
  for (x in c("png", "gif", "svg")) {
    sc$state$enums$image_types[[x]] <- invoke_static(
      sc, "org.apache.sedona.viz.utils.ImageType", toupper(x)
    )
  }
  for (x in c("red", "green", "blue")) {
    sc$state$enums$awt_color[[x]] <- invoke_static(
      sc, "java.awt.Color", toupper(x)
    )
  }
  lockBinding(sym = "enums", env = sc$state)

  # Other JVM objects that can be cached and evicted are stored mutably
  # under sc$state$object_cache
  sc$state$object_cache$storage_levels$memory_only <- invoke_static(
    sc, "org.apache.spark.storage.StorageLevel", "MEMORY_ONLY"
  )
}

sedona_dbplyr_sql_variant <- function() {
  list(
    scalar = list(
      ST_Point = function(x, y) {
        dbplyr::build_sql(
          "ST_Point(CAST(",
          x,
          " AS DECIMAL(24, 20)), CAST(",
          y,
          " AS DECIMAL(24, 20)))"
        )
      },
      ST_PolygonFromEnvelope = function(min_x, min_y, max_x, max_y) {
        dbplyr::build_sql(
          "ST_PolygonFromEnvelope(CAST(",
          min_x,
          " AS DECIMAL(24, 20)), CAST(",
          min_y,
          " AS DECIMAL(24, 20)), CAST(",
          max_x,
          " AS DECIMAL(24, 20)), CAST(",
          max_y,
          " AS DECIMAL(24, 20)))"
        )
      },
      ST_Buffer = function(geometry, buffer) {
        dbplyr::build_sql(
          "ST_Buffer(", geometry, ", CAST(", buffer, " AS DOUBLE))"
        )
      },
      ST_PrecisionReduce = function(geometry, precision) {
        dbplyr::build_sql(
          "ST_PrecisionReduce(", geometry, ", CAST(", precision, " AS INTEGER))"
        )
      },
      ST_SimplifyPreserveTopology = function(geometry, distance_tolerance) {
        dbplyr::build_sql(
          "ST_SimplifyPreserveTopology(",
          geometry,
          ", CAST(",
          distance_tolerance,
          " AS DOUBLE))"
        )
      },
      ST_GeometryN = function(geometry, n) {
        dbplyr::build_sql(
          "ST_GeometryN(", geometry, ", CAST(", n, " AS INTEGER))"
        )
      },
      ST_InteriorRingN = function(geometry, n) {
        dbplyr::build_sql(
          "ST_InteriorRingN(", geometry, ", CAST(", n, " AS INTEGER))"
        )
      },
      ST_AddPoint = function(geometry, point, position = NULL) {
        if (is.null(position)) {
          dbplyr::build_sql("ST_AddPoint(", geometry, ", ", point, ")")
        } else {
          dbplyr::build_sql(
            "ST_AddPoint(",
            geometry,
            ", ",
            point,
            ", CAST(",
            position,
            " AS INTEGER))"
          )
        }
      },
      ST_RemovePoint = function(geometry, position = NULL) {
        if (is.null(position)) {
          dbplyr::build_sql("ST_RemovePoint(", geometry, ")")
        } else {
          dbplyr::build_sql(
            "ST_RemovePoint(", geometry, ", CAST(", position, " AS INTEGER))"
          )
        }
      }
    )
  )
}

.onAttach <- function(libname, pkgname) {
  options(spark.serializer = "org.apache.spark.serializer.KryoSerializer")
  options(
    spark.kryo.registrator = "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
  )
}
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
