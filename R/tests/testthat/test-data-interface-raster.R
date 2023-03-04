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

context("data interface: raster")

sc <- testthat_spark_connection()

# Read geotiff ---------------
test_that("Should Pass geotiff loading without readFromCRS and readToCRS", {
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  
  expect_equal(
    row  %>% select(Geom, height, width, bands) %>% as.list(),
    list(
      Geom = "POLYGON ((-13095782 4021226.5, -13095782 3983905, -13058822 3983905, -13058822 4021226.5, -13095782 4021226.5))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  
})

test_that("Should Pass geotiff loading with readToCRS", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  
  expect_equal(
    row  %>% select(Geom, height, width, bands) %>% as.list(),
    list(
      Geom = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  
})

test_that("Should Pass geotiff loading with readFromCRS", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readFromCRS = "EPSG:4499"))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  
  expect_equal(
    row  %>% select(Geom, height, width, bands) %>% as.list(),
    list(
      Geom = "POLYGON ((-13095782 4021226.5, -13095782 3983905, -13058822 3983905, -13058822 4021226.5, -13095782 4021226.5))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  
})

test_that("Should Pass geotiff loading with readFromCRS and readToCRS", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326", readFromCRS = "EPSG:4499"))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  
  expect_equal(
    row  %>% select(Geom, height, width, bands) %>% as.list(),
    list(
      Geom = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  
})

test_that("Should Pass geotiff loading with all read options", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, 
                                    options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326", readFromCRS = "EPSG:4499", disableErrorInCRS = TRUE))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  
  expect_equal(
    row  %>% select(Geom, height, width, bands) %>% as.list(),
    list(
      Geom = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})


# Raster functions ---------------
test_that("should pass RS_GetBand", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
            RS_GetBand(image.data, 1, image.nBands) as targetBand
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  
  expect_equal(
    row$targetBand[[1]] %>% length(),
    517 * 512
  )
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})

test_that("should pass RS_Base64", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  expect_no_error(
    sc %>% 
      DBI::dbGetQuery("SELECT RS_base64(height, width, targetBand, RS_Array(height*width, 0.0), RS_Array(height*width, 0.0)) as encodedstring
                     FROM (
                       SELECT RS_GetBand(image.data, 1, image.nBands) as targetBand, image.height as height, image.width as width
                       FROM ?) tmp
                     LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))# %>% print()
  )
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})

test_that("should pass RS_HTML", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  expect_no_error(
    # row <- 
    sc %>% 
      DBI::dbGetQuery("SELECT RS_HTML(RS_base64(height, width, targetBand, RS_Array(height*width, 0.0), RS_Array(height*width, 0.0)), '300') as htmlstring
                     FROM (
                       SELECT RS_GetBand(image.data, 1, image.nBands) as targetBand, image.height as height, image.width as width
                       FROM ?) tmp
                     LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  )
  
  
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})

test_that("should pass RS_GetBand for length of Band 2", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/test3.tif"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  row <-
    sc %>% 
    DBI::dbGetQuery("SELECT RS_GetBand(image.data, 2, image.nBands) as targetBand, image.height as height, image.width as width
                       FROM ?  LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  expect_equal(
    row$targetBand[[1]] %>% length(),
    32*32
  )
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})

test_that("should pass RS_GetBand for elements of Band 2", {
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/test3.tif"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  row <-
    sc %>% 
    DBI::dbGetQuery("SELECT RS_GetBand(image.data, 2, image.nBands) as targetBand, image.height as height, image.width as width
                       FROM ?  LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  expect_equal(
    row$targetBand[[1]][2],
    956.0
  )
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})

test_that("should pass RS_GetBand for elements of Band 4", {
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/test3.tif"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  row <-
    sc %>% 
    DBI::dbGetQuery("SELECT RS_GetBand(image.data, 4, image.nBands) as targetBand, image.height as height, image.width as width
                       FROM ?  LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  expect_equal(
    row$targetBand[[1]][3],
    0
  )
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})


# Write geotiff ---------------
test_that("Should Pass geotiff file writing with coalesce", {
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands"))
  
  geotiff_df %>% 
    sdf_coalesce(1L) %>% 
    spark_write_geotiff(path = tmp_dest)
  
  ## not clear what the issue is here
  for (file in dir(path = tmp_dest, full.names = TRUE)) load_path <- path.expand(file)
  
  geotiff_2_sdf <- spark_read_geotiff(sc, path = load_path, options = list(dropInvalid = TRUE))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, dbplyr::remote_name(geotiff_2_sdf)))
  
  expect_equal(
    row  %>% select(height, width, bands) %>% as.list(),
    list(
      # Geom = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
})

test_that("Should Pass geotiff file writing with writeToCRS", {
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands"))
  
  geotiff_df %>% 
    sdf_coalesce(1L) %>% 
    spark_write_geotiff(path = tmp_dest, options = list(writeToCRS = "EPSG:4499"))
  
  ## not clear what the issue is here
  for (file in dir(path = tmp_dest, full.names = TRUE)) load_path <- path.expand(file)
  
  geotiff_2_sdf <- spark_read_geotiff(sc, path = load_path, options = list(dropInvalid = TRUE))
  
  row <- sc %>% 
    DBI::dbGetQuery("SELECT 
             image.geometry as Geom, 
             image.height as height, 
             image.width as width, 
             image.data as data,
             image.nBands as bands 
             FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, dbplyr::remote_name(geotiff_2_sdf)))
  
  expect_equal(
    row  %>% select(height, width, bands) %>% as.list(),
    list(
      # Geom = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))",
      height = 517,
      width = 512,
      bands = 1
    )
  )
  
  line1 <- row$data[[1]][1:512] 
  line2 <- row$data[[1]][513:1024]
  expect_equal(line1[0 + 1], 0) 
  expect_equal(line2[159 + 1], 0)
  expect_equal(line2[160 + 1], 123)
  
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
  
})

test_that("Should Pass geotiff file writing without coalesce", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands"))
  geotiff_2_sdf <- geotiff_df %>% sdf_register()
  
  geotiff_2_sdf %>% 
    spark_write_geotiff(path = tmp_dest)
  
  
  ## Count created files
  files <- dir(path = tmp_dest, recursive = TRUE, pattern = "tiff?$")
  
  expect_equal(length(files), 3)
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
})

test_that("Should Pass geotiff file writing with nested schema", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_sdf %>% 
    spark_write_geotiff(path = tmp_dest)
  
  
  ## Count created files
  files <- dir(path = tmp_dest, recursive = TRUE, pattern = "tiff?$")
  
  expect_equal(length(files), 3)
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})


test_that("Should Pass geotiff file writing with renamed fields", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image.origin as source","image.geometry as geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands"))
  geotiff_2_sdf <- geotiff_df %>% sdf_register()
  
  geotiff_2_sdf %>% 
    spark_write_geotiff(path = tmp_dest, 
                        mode = "overwrite",
                        options = list(
                          fieldOrigin   = "source",
                          fieldGeometry = "geom",
                          fieldNBands   = "bands"
                        ))
  
  
  ## Count created files
  files <- dir(path = tmp_dest, recursive = TRUE, pattern = "tiff?$")
  
  expect_equal(length(files), 3)
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
})

test_that("Should Pass geotiff file writing with nested schema and renamed fields", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image as tiff_image"))
  geotiff_2_sdf <- geotiff_df %>% sdf_register()
  
  geotiff_2_sdf %>% 
    spark_write_geotiff(path = tmp_dest, 
                        mode = "overwrite",
                        options = list(
                          fieldImage   = "tiff_image"
                        ))
  
  
  ## Count created files
  files <- dir(path = tmp_dest, recursive = TRUE, pattern = "tiff?$")
  
  expect_equal(length(files), 3)
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
})

test_that("Should Pass geotiff file writing with converted geometry", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image.origin as source","ST_GeomFromWkt(image.geometry) as geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands"))
  geotiff_2_sdf <- geotiff_df %>% sdf_register()
  
  geotiff_2_sdf %>% 
    spark_write_geotiff(path = tmp_dest, 
                        mode = "overwrite",
                        options = list(
                          fieldOrigin   = "source",
                          fieldGeometry = "geom",
                          fieldNBands   = "bands"
                        ))
  
  
  ## Count created files
  files <- dir(path = tmp_dest, recursive = TRUE, pattern = "tiff?$")
  
  expect_equal(length(files), 3)
  
  ## Cleanup
  unlink(tmp_dest, recursive = TRUE)
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
})

test_that("Should Pass geotiff file writing with handling invalid schema", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  ## Write
  tmp_dest <- tempfile()
  
  geotiff_df <- geotiff_sdf %>% spark_dataframe()
  geotiff_df <- invoke(geotiff_df, "selectExpr", list("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data"))
  geotiff_2_sdf <- geotiff_df %>% sdf_register()
  
  expect_error(
    geotiff_2_sdf %>% 
      spark_write_geotiff(path = tmp_dest, 
                          mode = "overwrite",
                          options = list(
                            fieldImage   = "tiff_image"
                          )),
    regexp = "Invalid GeoTiff Schema"
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(geotiff_2_sdf)))
})


# Binary and RS_functions  -----------------
# Only functions related to reading

test_that("Passed RS_FromGeoTiff from binary", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster/test1.tiff"), name = sdf_name)
  
  raster_sdf <- 
    binary_sdf %>% 
    mutate(raster = RS_FromGeoTiff(content))
  
  expect_equal(
    raster_sdf %>% sdf_schema() ,
    list(
      path             = list(name = "path", type = "StringType"),
      modificationTime = list(name = "modificationTime", type = "TimestampType"),
      length           = list(name = "length", type = "LongType"),
      content          = list(name = "content", type = "BinaryType"),
      raster           = list(name = "raster", type = "RasterUDT"))
  )
  
  a <- raster_sdf %>% head(1) %>%  collect()
  expect_equal(
    a$raster[[1]] %>% sparklyr::invoke("getClass") %>% sparklyr::invoke("getSimpleName"),
    "GridCoverage2D"
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  # sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(raster_sdf)))
  rm(a)
  
})

test_that("Passed RS_FromArcInfoAsciiGrid from binary", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster_asc/test1.asc"), name = sdf_name)
  
  raster_sdf <- 
    binary_sdf %>% 
    mutate(raster = RS_FromArcInfoAsciiGrid(content))
  
  expect_equal(
    raster_sdf %>% sdf_schema() ,
    list(
      path             = list(name = "path", type = "StringType"),
      modificationTime = list(name = "modificationTime", type = "TimestampType"),
      length           = list(name = "length", type = "LongType"),
      content          = list(name = "content", type = "BinaryType"),
      raster           = list(name = "raster", type = "RasterUDT"))
  )
  
  a <- raster_sdf %>% head(1) %>%  collect()
  expect_equal(
    a$raster[[1]] %>% sparklyr::invoke("getClass") %>% sparklyr::invoke("getSimpleName"),
    "GridCoverage2D"
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  # sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(raster_sdf)))
  rm(a)
  
})


test_that("Passed RS_Envelope with raster", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster/test1.tiff"), name = sdf_name)
  
  raster_sdf <- 
    binary_sdf %>% 
    mutate(
      raster = RS_FromGeoTiff(content),
      env = RS_Envelope(raster)
    )
  
  expect_equal(
    raster_sdf %>% sdf_schema() ,
    list(
      path             = list(name = "path", type = "StringType"),
      modificationTime = list(name = "modificationTime", type = "TimestampType"),
      length           = list(name = "length", type = "LongType"),
      content          = list(name = "content", type = "BinaryType"),
      raster           = list(name = "raster", type = "RasterUDT"),
      env              = list(name = "env", type = "GeometryUDT")
    )
  )
  
  a <- 
    raster_sdf %>% 
    mutate(env = env %>% st_astext()) %>% 
    select(env) %>% 
    head(1) %>%  collect()
  expect_equal(
    a$env[1] %>% substr(1, 129),
    "POLYGON ((-13095817.809482181 3983868.8560156375, -13095817.809482181 4021262.7487925636, -13058785.559768861 4021262.7487925636,"
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  # sc %>% DBI::dbExecute(paste0("DROP TABLE ", dbplyr::remote_name(raster_sdf)))
  rm(a)
})


test_that("Passed RS_NumBands with raster", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  a <-
    binary_sdf %>% 
    mutate(
      raster = RS_FromGeoTiff(content),
      nbands = RS_NumBands(raster)
    ) %>% 
    select(nbands) %>% 
    collect()
  
  expect_equal(
    a %>% as.list(),
    list(nbands = c(1, 1, 4))
    
  )
  
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  rm(a)
})


test_that("Passed RS_Value with raster", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster/test1.tiff"), name = sdf_name)
  
  a <-
    binary_sdf %>% 
    mutate(
      raster = RS_FromGeoTiff(content),
      val = RS_Value(raster, ST_Point(-13077301.685, 4002565.802))
    ) %>% 
    select(val) %>% 
    collect()
  
  expect_equal(
    a %>% as.list(),
    list(val = c(255))
    
  )
  
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  rm(a)
})

test_that("Passed RS_Values with raster", {
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  a <-
    binary_sdf %>% 
    mutate(
      raster = RS_FromGeoTiff(content),
      val = RS_Values(raster, array(ST_Point(-13077301.685, 4002565.802), NULL))
    ) %>% 
    select(val) %>%
    collect()
  
  expect_equal(
    a %>% as.list(),
    list(val = list(c(255, NA_real_), c(255, NA_real_), c(NA_real_, NA_real_)))
    
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  rm(a)
})
