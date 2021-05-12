testthat_spark_connection <- function(conn_attempts, conn_retry_interval_s = 2) {
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
          sc <- spark_connect(
            master = "local",
            method = "shell",
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
  file.path(normalizePath(getwd()), "data", file_name)
}

read_polygon_rdd <- function() {
  polygon_rdd <- sedona_read_dsv_to_typed_rdd(
    testthat_spark_connection(),
    location = test_data("primaryroads-polygon.csv"),
    delimiter = ",",
    type = "polygon"
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
        "org.apache.sedona.core.utils.GeomUtils",
        "equalsExactGeom",
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
