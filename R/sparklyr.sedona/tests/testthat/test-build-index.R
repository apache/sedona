context("build index")

sc <- testthat_spark_connection()

test_that("sedona_build_index() works as expected on raw partitions", {
  pt_rdd <- read_point_rdd()

  sedona_build_index(pt_rdd, type = "quadtree")
  indexed_raw_rdd <- invoke(pt_rdd$.jobj, "indexedRawRDD")

  expect_equal(
    indexed_raw_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.quadtree.Quadtree"
  )
  expect_equal(pt_rdd$.state$raw_partitions_index_type, "quadtree")

  sedona_build_index(pt_rdd, type = "rtree")
  indexed_raw_rdd <- invoke(pt_rdd$.jobj, "indexedRawRDD")

  expect_equal(
    indexed_raw_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.strtree.STRtree"
  )
  expect_equal(pt_rdd$.state$raw_partitions_index_type, "rtree")
})

test_that("sedona_build_index() works as expected on spatial partitions", {
  pt_rdd <- read_point_rdd(repartition = 5)
  sedona_apply_spatial_partitioner(pt_rdd, partitioner = "quadtree")
  sedona_build_index(pt_rdd, type = "rtree")
  indexed_rdd <- invoke(pt_rdd$.jobj, "indexedRDD")

  expect_equal(
    indexed_rdd %>%
      invoke("%>%", list("first"), list("getClass"), list("getName")),
    "org.locationtech.jts.index.strtree.STRtree"
  )
  expect_equal(pt_rdd$.state$spatial_partitions_index_type, "rtree")
})
