context("CRS transform")

test_that("crs_transform() works as expected", {
  sc <- testthat_spark_connection()
  pt_rdd <- sedona_read_dsv_to_typed_rdd(
    sc, test_data("crs-test-point.csv"),
    type = "point"
  ) %>%
    crs_transform("epsg:4326", "epsg:3857")

  expect_equivalent(
    pt_rdd %>%
      sdf_register() %>%
      head(5) %>%
      dplyr::transmute(x = ST_X(geometry), y = ST_Y(geometry)) %>%
      collect(),
    tibble::tribble(
      ~x, ~y,
      -9833016.710450118, 3805934.914254189,
      -9815699.961781807, 3810760.096874278,
      -9839413.351030082, 3810273.8140140832,
      -9820728.151861448, 3809444.5432437807,
      -9832182.148227641, 3888758.1926142,
    )
  )
})
