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

test_that("FIX: should pass RS_Base64", {
  
  sdf_name <- random_string("spatial_sdf")
  geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  
  expect_no_error(
    # row <- 
    sc %>% 
      DBI::dbGetQuery("SELECT RS_base64(height, width, targetBand, RS_Array(height*width, 0.0), RS_Array(height*width, 0.0)) as encodedstring
                     FROM (
                       SELECT RS_GetBand(image.data, 1, image.nBands) as targetBand, image.height as height, image.width as width
                       FROM ?) tmp
                     LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  )
  
  
  
  sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
})

test_that("FIX: should pass RS_HTML", {
  
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
test_that("TODO: Should Pass geotiff file writing with coalesce", {
  
  # sdf_name <- random_string("spatial_sdf")
  # geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE, readToCRS = "EPSG:4326"))
  # 
  # row <- sc %>% 
  #   DBI::dbGetQuery("SELECT 
  #            image.geometry as Geom, 
  #            image.height as height, 
  #            image.width as width, 
  #            image.data as data,
  #            image.nBands as bands 
  #            FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  # 
  # expect_equal(
  #   row  %>% select(Geom, height, width, bands) %>% as.list(),
  #   list(
  #     Geom = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))",
  #     height = 517,
  #     width = 512,
  #     bands = 1
  #   )
  # )
  # 
  # line1 <- row$data[[1]][1:512] 
  # line2 <- row$data[[1]][513:1024]
  # expect_equal(line1[0 + 1], 0) 
  # expect_equal(line2[159 + 1], 0)
  # expect_equal(line2[160 + 1], 123)
  # 
  # sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  # 
  # 
  # val savePath = resourceFolder + "raster-written/"
  # df.coalesce(1).write.mode("overwrite").format("geotiff").save(savePath)
  # 
  # var loadPath = savePath
  # val tempFile = new File(loadPath)
  # val fileList = tempFile.listFiles()
  # for (i <- 0 until fileList.length) {
  #   if (fileList(i).isDirectory) loadPath = fileList(i).getAbsolutePath
  # }
  # 
  # var dfWritten = sparkSession.read.format("geotiff").option("dropInvalid", true).load(loadPath)
  # dfWritten = dfWritten.selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
  # val rowFirst = dfWritten.first()
  # assert(rowFirst.getInt(2) == 517)
  # assert(rowFirst.getInt(3) == 512)
  # assert(rowFirst.getInt(5) == 1)
  # 
  # val blackBand = rowFirst.getAs[mutable.WrappedArray[Double]](4)
  # val line1 = blackBand.slice(0, 512)
  # val line2 = blackBand.slice(512, 1024)
  # assert(line1(0) == 0.0) // The first value at line 1 is black
  # assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
})

test_that("TODO: Should Pass geotiff file writing with writeToCRS", {
  # sdf_name <- random_string("spatial_sdf")
  # geotiff_sdf <- spark_read_geotiff(sc, path = test_data("raster/"), name = sdf_name, options = list(dropInvalid = TRUE))
  # 
  # row <- sc %>%
  #   DBI::dbGetQuery("SELECT
  #            image.geometry as Geom,
  #            image.height as height,
  #            image.width as width,
  #            image.data as data,
  #            image.nBands as bands
  #            FROM ? LIMIT 1", DBI::dbQuoteIdentifier(sc, sdf_name))
  # 
  # expect_equal(
  #   row$targetBand[[1]] %>% length(),
  #   517 * 512
  # )
  # 
  # sc %>% DBI::dbExecute(paste0("DROP TABLE ", sdf_name))
  # 
  # 
  # 
  # df = df.selectExpr("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands")
  # val savePath = resourceFolder + "raster-written/"
  # df.coalesce(1).write.mode("overwrite").format("geotiff").option("writeToCRS", "EPSG:4499").save(savePath)
  # 
  # var loadPath = savePath
  # val tempFile = new File(loadPath)
  # val fileList = tempFile.listFiles()
  # for (i <- 0 until fileList.length) {
  #   if (fileList(i).isDirectory) loadPath = fileList(i).getAbsolutePath
  # }
  # 
  # var dfWritten = sparkSession.read.format("geotiff").option("dropInvalid", true).load(loadPath)
  # dfWritten = dfWritten.selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
  # val rowFirst = dfWritten.first()
  # assert(rowFirst.getInt(2) == 517)
  # assert(rowFirst.getInt(3) == 512)
  # assert(rowFirst.getInt(5) == 1)
  # 
  # val blackBand = rowFirst.getAs[mutable.WrappedArray[Double]](4)
  # val line1 = blackBand.slice(0, 512)
  # val line2 = blackBand.slice(512, 1024)
  # assert(line1(0) == 0.0) // The first value at line 1 is black
  # assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
})

test_that("TODO: Should Pass geotiff file writing without coalesce", {
  # var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
  # df = df.selectExpr("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data", "image.nBands as nBands")
  # val savePath = resourceFolder + "raster-written/"
  # df.write.mode("overwrite").format("geotiff").save(savePath)
  # 
  # var imageCount = 0
  # def getFile(loadPath: String): Unit ={
  #   val tempFile = new File(loadPath)
  #   val fileList = tempFile.listFiles()
  #   if (fileList == null) return
  #   for (i <- 0 until fileList.length) {
  #     if (fileList(i).isDirectory) getFile(fileList(i).getAbsolutePath)
  #     else if (fileList(i).getAbsolutePath.endsWith(".tiff") || fileList(i).getAbsolutePath.endsWith(".tif")) imageCount += 1
  #   }
  # }
  # 
  # getFile(savePath)
  # assert(imageCount == 3)
})

test_that("TODO: Should Pass geotiff file writing with nested schema", {
  # val df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
  # val savePath = resourceFolder + "raster-written/"
  # df.write.mode("overwrite").format("geotiff").save(savePath)
  # 
  # var imageCount = 0
  # def getFile(loadPath: String): Unit ={
  #   val tempFile = new File(loadPath)
  #   val fileList = tempFile.listFiles()
  #   if (fileList == null) return
  #   for (i <- 0 until fileList.length) {
  #     if (fileList(i).isDirectory) getFile(fileList(i).getAbsolutePath)
  #     else if (fileList(i).getAbsolutePath.endsWith(".tiff") || fileList(i).getAbsolutePath.endsWith(".tif")) imageCount += 1
  #   }
  # }
  # 
  # getFile(savePath)
  # assert(imageCount == 3)
})

test_that("TODO: Should Pass geotiff file writing with renamed fields", {
  # var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
  # df = df.selectExpr("image.origin as source","image.geometry as geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
  # val savePath = resourceFolder + "raster-written/"
  # df.write
  # .mode("overwrite")
  # .format("geotiff")
  # .option("fieldOrigin", "source")
  # .option("fieldGeometry", "geom")
  # .option("fieldNBands", "bands")
  # .save(savePath)
  # 
  # var imageCount = 0
  # def getFile(loadPath: String): Unit ={
  #   val tempFile = new File(loadPath)
  #   val fileList = tempFile.listFiles()
  #   if (fileList == null) return
  #   for (i <- 0 until fileList.length) {
  #     if (fileList(i).isDirectory) getFile(fileList(i).getAbsolutePath)
  #     else if (fileList(i).getAbsolutePath.endsWith(".tiff") || fileList(i).getAbsolutePath.endsWith(".tif")) imageCount += 1
  #   }
  # }
  # 
  # getFile(savePath)
  # assert(imageCount == 3)
})

test_that("TODO: Should Pass geotiff file writing with nested schema and renamed fields", {
  # var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
  # df = df.selectExpr("image as tiff_image")
  # val savePath = resourceFolder + "raster-written/"
  # df.write
  # .mode("overwrite")
  # .format("geotiff")
  # .option("fieldImage", "tiff_image")
  # .save(savePath)
  # 
  # var imageCount = 0
  # def getFile(loadPath: String): Unit ={
  #   val tempFile = new File(loadPath)
  #   val fileList = tempFile.listFiles()
  #   if (fileList == null) return
  #   for (i <- 0 until fileList.length) {
  #     if (fileList(i).isDirectory) getFile(fileList(i).getAbsolutePath)
  #     else if (fileList(i).getAbsolutePath.endsWith(".tiff") || fileList(i).getAbsolutePath.endsWith(".tif")) imageCount += 1
  #   }
  # }
  # 
  # getFile(savePath)
  # assert(imageCount == 3)
})

test_that("TODO: Should Pass geotiff file writing with converted geometry", {
  # var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
  # df = df.selectExpr("image.origin as source","ST_GeomFromWkt(image.geometry) as geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
  # val savePath = resourceFolder + "raster-written/"
  # df.write
  # .mode("overwrite")
  # .format("geotiff")
  # .option("fieldOrigin", "source")
  # .option("fieldGeometry", "geom")
  # .option("fieldNBands", "bands")
  # .save(savePath)
  # 
  # var imageCount = 0
  # def getFile(loadPath: String): Unit ={
  #   val tempFile = new File(loadPath)
  #   val fileList = tempFile.listFiles()
  #   if (fileList == null) return
  #   for (i <- 0 until fileList.length) {
  #     if (fileList(i).isDirectory) getFile(fileList(i).getAbsolutePath)
  #     else if (fileList(i).getAbsolutePath.endsWith(".tiff") || fileList(i).getAbsolutePath.endsWith(".tif")) imageCount += 1
  #   }
  # }
  # 
  # getFile(savePath)
  # assert(imageCount == 3)
})

test_that("TODO: Should Pass geotiff file writing with handling invalid schema", {
  # var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(rasterdatalocation)
  # df = df.selectExpr("image.origin as origin","image.geometry as geometry", "image.height as height", "image.width as width", "image.data as data")
  # val savePath = resourceFolder + "raster-written/"
  # 
  # try {
  #   df.write
  #   .mode("overwrite")
  #   .format("geotiff")
  #   .option("fieldImage", "tiff_image")
  #   .save(savePath)
  # }
  # catch {
  #   case e: IllegalArgumentException => {
  #     assert(e.getMessage == "Invalid GeoTiff Schema")
  #   }
  # }
})



