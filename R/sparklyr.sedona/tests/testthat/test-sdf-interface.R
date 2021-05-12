context("sdf interface")

sc <- testthat_spark_connection()

pt_rdd <- read_point_rdd_with_non_spatial_attrs()

test_that("sdf_register() works as expected for Spatial RDDs", {
  sdf_name <- random_string("spatial_sdf")
  pt_sdf <- sdf_register(pt_rdd, name = sdf_name)

  expect_equivalent(
    pt_sdf %>% sdf_schema(),
    list(geometry = list(name = "geometry", type = "GeometryUDT"))
  )
  expect_equal(pt_sdf %>% dbplyr::remote_name(), dbplyr::ident(sdf_name))

  pt_sdf %>% collect()
  succeed()
})

test_that("as.spark.dataframe() works as expected for Spatial RDDs with non-spatial attributes", {
  sdf_name <- random_string("spatial_sdf")
  pt_sdf <- as.spark.dataframe(
    pt_rdd,
    non_spatial_cols = paste0("attr_", seq(3)), name = sdf_name
  )

  expect_equivalent(
    pt_sdf %>% sdf_schema(),
    list(
      geometry = list(name = "geometry", type = "GeometryUDT"),
      attr_1 = list(name = "attr_1", type = "StringType"),
      attr_2 = list(name = "attr_2", type = "StringType"),
      attr_3 = list(name = "attr_3", type = "StringType")
    )
  )
  expect_equal(pt_sdf %>% dbplyr::remote_name(), dbplyr::ident(sdf_name))

  pt_sdf %>% collect()
  succeed()
})
