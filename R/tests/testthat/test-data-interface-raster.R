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


# Read Binary and RS_functions  -----------------
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


# Write Binary and RS_functions  -----------------
test_that("Should read geotiff using binary source and write geotiff back to disk using raster source", {
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  tmp_dest <- tempfile()
  
  binary_sdf %>% 
    spark_write_raster(path = tmp_dest)
  
  sdf_name_2 <- random_string("spatial_sdf_2")
  binary_2_sdf <- spark_read_binary(sc, dir = tmp_dest, name = sdf_name_2, recursive_file_lookup = TRUE)
  
  expect_equal(
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name)),
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM (SELECT RS_FromGeoTiff(content) as raster FROM ?) LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name_2))
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name_2))
  rm(binary_sdf, binary_2_sdf, tmp_dest)
  
  

})

test_that("Should read and write geotiff using given options", {
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  tmp_dest <- tempfile()
  
  binary_sdf %>% 
    spark_write_raster(path = tmp_dest, 
                       options = list("rasterField" = "content", 
                                      "fileExtension" = ".tiff",
                                      "pathField" = "path"
                                      ))
  
  sdf_name_2 <- random_string("spatial_sdf_2")
  binary_2_sdf <- spark_read_binary(sc, dir = tmp_dest, name = sdf_name_2, recursive_file_lookup = TRUE)
  
  
  expect_equal(
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name)),
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM (SELECT RS_FromGeoTiff(content) as raster FROM ?) LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name_2))
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name_2))
  rm(binary_sdf, binary_2_sdf, tmp_dest)
  
})

test_that("Should read and write via RS_FromGeoTiff and RS_AsGeoTiff", {
  
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  raster_sdf <- 
    binary_sdf %>% 
    mutate(raster = RS_FromGeoTiff(content)) %>% 
    mutate(content = RS_AsGeoTiff(raster))
  
  
  tmp_dest <- tempfile()
  
  raster_sdf %>% 
    spark_write_raster(path = tmp_dest, 
                       options = list("rasterField" = "content", 
                                      "fileExtension" = ".tiff",
                                      "pathField" = "path"
                       ))
  
  sdf_name_2 <- random_string("spatial_sdf_2")
  binary_2_sdf <- spark_read_binary(sc, dir = tmp_dest, name = sdf_name_2, recursive_file_lookup = TRUE)
  
  
  expect_equal(
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name)),
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM (SELECT RS_FromGeoTiff(content) as raster FROM ?) LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name_2))
  rm(raster_sdf, binary_sdf, binary_2_sdf, tmp_dest)
  
})

test_that("Should handle null", {
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  raster_sdf <- 
    binary_sdf %>% 
    mutate(raster = RS_FromGeoTiff(NULL)) %>% 
    mutate(content = RS_AsGeoTiff(raster))
  
  tmp_dest <- tempfile()
  
  raster_sdf %>% 
    spark_write_raster(path = tmp_dest)
  
  sdf_name_2 <- random_string("spatial_sdf_2")
  binary_2_sdf <- spark_read_binary(sc, dir = tmp_dest, name = sdf_name_2, recursive_file_lookup = TRUE)
  
  out <- sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM (SELECT RS_FromGeoTiff(content) as raster FROM ?) LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name_2))
  
  expect_equal(
    out$n,
    0
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name_2))
  rm(raster_sdf, binary_sdf, binary_2_sdf, tmp_dest)
  
})

test_that("Should read RS_FromGeoTiff and write RS_AsArcGrid", {
  
  ## Load
  sdf_name <- random_string("spatial_sdf")
  binary_sdf <- spark_read_binary(sc, dir = test_data("raster"), name = sdf_name)
  
  raster_sdf <- 
    binary_sdf %>% 
    mutate(raster = RS_FromGeoTiff(content)) %>% 
    mutate(content = RS_AsArcGrid(raster)) %>% 
    sdf_register()
  
  tmp_dest <- tempfile()
  
  raster_sdf %>% 
    spark_write_raster(path = tmp_dest, 
                       options = list("rasterField" = "content", 
                                      "fileExtension" = ".asc",
                                      "pathField" = "path"
                       ))
  
  sdf_name_2 <- random_string("spatial_sdf_2")
  binary_2_sdf <- spark_read_binary(sc, dir = tmp_dest, name = sdf_name_2, recursive_file_lookup = TRUE)
  
  
  
  expect_equal(
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, dbplyr::remote_name(raster_sdf))),
    sc %>% DBI::dbGetQuery("SELECT count(*) as n FROM (SELECT RS_FromGeoTiff(content) as raster FROM ?) LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name_2))
  )
  
  ## Cleanup
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name_2))
  rm(raster_sdf, binary_sdf, binary_2_sdf, tmp_dest)
  
})
