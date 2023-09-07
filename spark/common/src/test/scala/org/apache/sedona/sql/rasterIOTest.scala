/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import java.io.File
import java.nio.file.Files

class rasterIOTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  var rasterdatalocation: String = resourceFolder + "raster/"
  val tempDir: String = Files.createTempDirectory("sedona_raster_io_test_").toFile.getAbsolutePath

  describe("Raster IO test") {
    it("should read geotiff using binary source and write geotiff back to disk using raster source") {
      var rasterDf = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should read and write geotiff using given options") {
      var rasterDf = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").option("rasterField", "content").option("fileExtension", ".tiff").option("pathField", "path").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should read and write via RS_FromGeoTiff and RS_AsGeoTiff") {
      var df = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      var rasterDf = df.selectExpr("RS_FromGeoTiff(content) as raster", "path").selectExpr("RS_AsGeoTiff(raster) as content", "path")
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").option("rasterField", "content").option("fileExtension", ".tiff").option("pathField", "path").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should handle null") {
      var df = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      var rasterDf = df.selectExpr("RS_FromGeoTiff(null) as raster", "length").selectExpr("RS_AsGeoTiff(raster) as content", "length")
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterCount == 3)
      assert(rasterDf.count() == 0)
    }

    it("Passed RS_AsRaster with empty raster"){
      var df = sparkSession.sql("SELECT RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))') as geom")
      var rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', 255, 0d) as rasterized")
      var actual = rasterized.selectExpr("RS_AsGeoTiff(rasterized)").first().get(0).asInstanceOf[Array[Byte]].mkString("Array(", ", ", ")")
      var expected = "Array(77, 77, 0, 42, 0, 0, 0, 8, 0, 16, 1, 0, 0, 3, 0, 0, 0, 1, 0, 4, 0, 0, 1, 1, 0, 3, 0, 0, 0, 1, 0, 5, 0, 0, 1, 2, 0, 3, 0, 0, 0, 1, 0, 64, 0, 0, 1, 3, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 6, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 17, 0, 4, 0, 0, 0, 1, 0, 0, 1, -128, 1, 21, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 22, 0, 3, 0, 0, 0, 1, 0, 5, 0, 0, 1, 23, 0, 4, 0, 0, 0, 1, 0, 0, 0, -96, 1, 26, 0, 5, 0, 0, 0, 1, 0, 0, 0, -48, 1, 27, 0, 5, 0, 0, 0, 1, 0, 0, 0, -40, 1, 40, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 83, 0, 3, 0, 0, 0, 1, 0, 3, 0, 0, -123, -40, 0, 12, 0, 0, 0, 16, 0, 0, 0, -32, -121, -81, 0, 3, 0, 0, 0, 16, 0, 0, 1, 96, -92, -127, 0, 2, 0, 0, 0, 4, 48, 46, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 64, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 2, 0, 3, 4, 0, 0, 0, 0, 1, 0, 2, 4, 1, 0, 0, 0, 1, 0, 1, 8, 0, 0, 0, 0, 1, 16, -26, 64, 111, -32, 0, 0, 0, 0, 0, 64, 111, -32, 0, 0, 0, 0, 0, 64, 111, -32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 111, -32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 111, -32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd') as rasterized")
      actual = rasterized.selectExpr("RS_AsGeoTiff(rasterized)").first().get(0).asInstanceOf[Array[Byte]].mkString("Array(", ", ", ")")
      expected = "Array(77, 77, 0, 42, 0, 0, 0, 8, 0, 15, 1, 0, 0, 3, 0, 0, 0, 1, 0, 4, 0, 0, 1, 1, 0, 3, 0, 0, 0, 1, 0, 5, 0, 0, 1, 2, 0, 3, 0, 0, 0, 1, 0, 64, 0, 0, 1, 3, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 6, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 17, 0, 4, 0, 0, 0, 1, 0, 0, 1, 116, 1, 21, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 22, 0, 3, 0, 0, 0, 1, 0, 5, 0, 0, 1, 23, 0, 4, 0, 0, 0, 1, 0, 0, 0, -96, 1, 26, 0, 5, 0, 0, 0, 1, 0, 0, 0, -60, 1, 27, 0, 5, 0, 0, 0, 1, 0, 0, 0, -52, 1, 40, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 1, 83, 0, 3, 0, 0, 0, 1, 0, 3, 0, 0, -123, -40, 0, 12, 0, 0, 0, 16, 0, 0, 0, -44, -121, -81, 0, 3, 0, 0, 0, 16, 0, 0, 1, 84, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 64, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 2, 0, 3, 4, 0, 0, 0, 0, 1, 0, 2, 4, 1, 0, 0, 0, 1, 0, 1, 8, 0, 0, 0, 0, 1, 16, -26, 63, -16, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
      assertEquals(expected, actual)
    }

    it("should read RS_FromGeoTiff and write RS_AsArcGrid") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster_geotiff_color/*")
      var rasterDf = df.selectExpr("RS_FromGeoTiff(content) as raster", "path").selectExpr("RS_AsArcGrid(raster, 1) as content", "path")
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").option("rasterField", "content").option("fileExtension", ".asc").option("pathField", "path").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromArcInfoAsciiGrid(content)")
      assert(rasterDf.count() == rasterCount)
    }
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(tempDir))
}