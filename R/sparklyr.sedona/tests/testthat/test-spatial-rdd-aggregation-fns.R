context("spatial RDD aggregation functions")

sc <- testthat_spark_connection()
pt_rdd <- sedona_read_dsv_to_typed_rdd(
  sc,
  location = test_data("arealm-small.csv"),
  type = "point",
  first_spatial_col_index = 1,
  repartition = 11
)

test_that("minimum_bounding_box() works as expected", {
  bb <- minimum_bounding_box(pt_rdd)

  expect_equal(
    c(bb$minX(), bb$maxX(), bb$minY(), bb$maxY()),
    c(-173.120769, -84.965961, 30.244859, 71.355134)
  )
})

test_that("approx_count() works as expected", {
  expect_equal(approx_count(pt_rdd), 3000)
})
