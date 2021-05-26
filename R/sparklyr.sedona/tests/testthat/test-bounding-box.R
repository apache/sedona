context("bounding box")

test_that("bounding box object works as expected", {
  sc <- testthat_spark_connection()

  b <- new_bounding_box(
    sc,
    min_x = -3,
    max_x = 3,
    min_y = -4,
    max_y = 4
  )

  expect_equal(b$minX(), -3)
  expect_equal(b$maxX(), 3)
  expect_equal(b$minY(), -4)
  expect_equal(b$maxY(), 4)
  expect_equal(b$width(), 6)
  expect_equal(b$height(), 8)
  expect_equal(b$diameter(), 10)
  expect_equal(b$minExtent(), 6)
  expect_equal(b$maxExtent(), 8)
  expect_equal(b$area(), 48)

  b_inf <- new_bounding_box(sc)

  expect_equal(b_inf$minX(), -Inf)
  expect_equal(b_inf$maxX(), Inf)
  expect_equal(b_inf$minY(), -Inf)
  expect_equal(b_inf$maxY(), Inf)
  expect_equal(b_inf$width(), Inf)
  expect_equal(b_inf$height(), Inf)
  expect_equal(b_inf$diameter(), Inf)
  expect_equal(b_inf$minExtent(), Inf)
  expect_equal(b_inf$maxExtent(), Inf)
  expect_equal(b_inf$area(), Inf)
})
