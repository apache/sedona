context("apply spatial partition")

sc <- testthat_spark_connection()

test_that("sedona_apply_spatial_partitioner() works as expected for quadtree partitioner", {
  pt_rdd <- read_point_rdd(repartition = 4L)
  sedona_apply_spatial_partitioner(pt_rdd, partitioner = "quadtree")

  expect_gt(
    pt_rdd$.jobj %>%
      invoke("%>%", list("spatialPartitionedRDD"), list("getNumPartitions")),
    0
  )
  expect_true(pt_rdd$.state$has_spatial_partitions)
  expect_equal(pt_rdd$.state$spatial_partitioner_type, "quadtree")
})

test_that("sedona_apply_spatial_partitioner() works as expected for kdbtree partitioner", {
  pt_rdd <- read_point_rdd(repartition = 4L)
  sedona_apply_spatial_partitioner(pt_rdd, partitioner = "kdbtree")

  expect_gt(
    pt_rdd$.jobj %>%
      invoke("%>%", list("spatialPartitionedRDD"), list("getNumPartitions")),
    0
  )
  expect_true(pt_rdd$.state$has_spatial_partitions)
  expect_equal(pt_rdd$.state$spatial_partitioner_type, "kdbtree")
})

test_that("sedona_apply_spatial_partitioner() works with custom max_levels setting", {
  pt_rdd <- read_point_rdd(repartition = 4L)
  sedona_apply_spatial_partitioner(
    pt_rdd,
    partitioner = "kdbtree", max_levels = 8
  )

  expect_gt(
    pt_rdd$.jobj %>%
      invoke("%>%", list("spatialPartitionedRDD"), list("getNumPartitions")),
    0
  )
  expect_true(pt_rdd$.state$has_spatial_partitions)
})

test_that("sedona_apply_spatial_partitioner() works with custom partitioner", {
  pt_rdd <- read_point_rdd(repartition = 4L)
  polygon_rdd <- read_polygon_rdd()
  sedona_apply_spatial_partitioner(
    pt_rdd,
    partitioner = "kdbtree", max_levels = 8
  )
  sedona_apply_spatial_partitioner(
    polygon_rdd,
    partitioner = invoke(pt_rdd$.jobj, "getPartitioner")
  )

  expect_gt(
    polygon_rdd$.jobj %>%
      invoke("%>%", list("spatialPartitionedRDD"), list("getNumPartitions")),
    0
  )
  expect_true(polygon_rdd$.state$has_spatial_partitions)
})
