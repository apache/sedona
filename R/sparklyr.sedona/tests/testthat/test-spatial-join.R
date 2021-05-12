context("spatial join")

sc <- testthat_spark_connection()

test_that("sedona_spatial_join() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join(
      pt_rdd, polygon_rdd,
      join_type = "contain", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "pair_rdd"))
  }
})

test_that("sedona_spatial_join() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join(
      pt_rdd, polygon_rdd,
      join_type = "intersect", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "pair_rdd"))
  }
})

test_that("sedona_spatial_join_count_by_key() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join_count_by_key(
      pt_rdd, polygon_rdd,
      join_type = "contain", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "count_by_key_rdd"))
  }
})

test_that("sedona_spatial_join_count_by_key() works as expected with 'contain' as join type", {
  for (partitioner in c("quadtree", "kdbtree")) {
    pt_rdd <- read_point_rdd()
    polygon_rdd <- read_polygon_rdd()
    pair_rdd <- sedona_spatial_join_count_by_key(
      pt_rdd, polygon_rdd,
      join_type = "intersect", partitioner = partitioner
    )

    expect_equal(invoke(pair_rdd$.jobj, "count"), 1207)
    expect_true(inherits(pair_rdd, "count_by_key_rdd"))
  }
})
