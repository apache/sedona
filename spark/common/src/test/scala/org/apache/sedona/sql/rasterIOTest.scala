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