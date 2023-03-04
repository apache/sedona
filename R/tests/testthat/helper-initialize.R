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

testthat_spark_connection <- function(conn_retry_interval_s = 2) {
  conn_key <- ".testthat_spark_connection"
  if (!exists(conn_key, envir = .GlobalEnv)) {
    version <- Sys.getenv("SPARK_VERSION")
    spark_installed <- spark_installed_versions()
    if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
      spark_install(version)
    }

    conn_attempts <- 3
    for (attempt in seq(conn_attempts)) {
      success <- tryCatch(
        {
          
          config <- spark_config()
          config[["sparklyr.connect.timeout"]] <- 300
          
          sc <- spark_connect(
            master = "local",
            method = "shell",
            config = config,
            app_name = paste0("testthat-", uuid::UUIDgenerate()),
            version = version
          )
          assign(conn_key, sc, envir = .GlobalEnv)
          TRUE
        },
        error = function(e) {
          if (attempt < conn_attempts) {
            Sys.sleep(conn_retry_interval_s)
            FALSE
          } else {
            warning("spark_connect() failed: ", e)
            e
          }
        }
      )
      if (success) break
    }
  }

  get(conn_key, envir = .GlobalEnv)
}

test_data <- function(file_name) {
  file.path(
    normalizePath(getwd()),
    "..",
    "..",
    "..",
    "core",
    "src",
    "test",
    "resources",
    file_name
  )
}

read_point_rdd <- function(repartition = NULL) {
  sedona_read_dsv_to_typed_rdd(
    testthat_spark_connection(),
    location = test_data("arealm.csv"),
    type = "point",
    repartition = repartition
  )
}

read_point_rdd_with_non_spatial_attrs <- function(repartition = NULL) {
  sedona_read_dsv_to_typed_rdd(
    testthat_spark_connection(),
    location = test_data("arealm-small.csv"),
    type = "point",
    first_spatial_col_index = 1,
    repartition = repartition
  )
}

read_polygon_rdd <- function(repartition = NULL) {
  polygon_rdd <- sedona_read_dsv_to_typed_rdd(
    testthat_spark_connection(),
    location = test_data("primaryroads-polygon.csv"),
    type = "polygon",
    repartition = repartition
  )
}

expect_boundary_envelope <- function(rdd, expected) {
  actual <- lapply(
    paste0("get", c("MinX", "MaxX", "MinY", "MaxY")),
    function(getter) {
      rdd$.jobj %>% invoke("%>%", list("boundaryEnvelope"), list(getter))
    }
  ) %>%
    unlist()

  testthat::expect_equal(actual, expected)
}

expect_geom_equal <- function(sc, lhs, rhs) {
  testthat::expect_equal(length(lhs), length(rhs))
  for (i in seq_along(lhs)) {
    testthat::expect_true(
      invoke_static(
        sc,
        "org.apache.sedona.common.utils.GeomUtils",
        "equalsExactGeom",
        lhs[[i]],
        rhs[[i]]
      )
    )
  }
}

expect_geom_equal_geojson <- function(sc, lhs, rhs) {
  testthat::expect_equal(length(lhs), length(rhs))
  for (i in seq_along(lhs)) {
    testthat::expect_true(
      invoke_static(
        sc,
        "org.apache.sedona.common.utils.GeomUtils",
        "equalsExactGeomUnsortedUserData",
        lhs[[i]],
        rhs[[i]]
      )
    )
  }
}

as.coordinate_list <- function(geometry) {
  geometry %>%
    invoke("getCoordinates") %>%
    lapply(function(pt) c(pt %>% invoke("getX"), pt %>% invoke("getY")))
}

expect_coordinates_equal <- function(geometry, coords) {
  testthat::expect_equal(as.coordinate_list(geometry), coords)
}

expect_coordinate_lists_setequal <- function(geometries, coords_list) {
  testthat::expect_setequal(
    geometries %>%
      lapply(
        function(geometry) {
          geometry %>%
            invoke("getCoordinates") %>%
            lapply(function(pt) list(pt %>% invoke("getX"), pt %>% invoke("getY")))
        }
      ),
    coords_list
  )
}
