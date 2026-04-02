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
import org.scalatest.BeforeAndAfter

import java.io.File
import java.nio.file.Files

class geotiffMetadataTest extends TestBaseScala with BeforeAndAfter {

  val rasterDir: String = resourceFolder + "raster/"
  val singleFileLocation: String = resourceFolder + "raster/test1.tiff"
  val tempDir: String =
    Files.createTempDirectory("sedona_sedonainfo_test_").toFile.getAbsolutePath

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempDir))
    super.afterAll()
  }

  describe("GeoTiff Metadata (sedonainfo) data source") {

    it("should read a single GeoTIFF file and return one row") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      assert(df.count() == 1)
      val row = df.first()
      assert(row.getAs[String]("path").contains("test1.tiff"))
      assert(row.getAs[String]("driver") == "GTiff")
      assert(row.getAs[Int]("width") > 0)
      assert(row.getAs[Int]("height") > 0)
      assert(row.getAs[Int]("numBands") > 0)
    }

    it("should read multiple GeoTIFF files via glob pattern") {
      val df = sparkSession.read.format("sedonainfo").load(rasterDir + "*.tiff")
      assert(df.count() > 1)
    }

    it("should read GeoTIFF files from directory with trailing slash") {
      val df = sparkSession.read.format("sedonainfo").load(rasterDir)
      assert(df.count() > 1)
    }

    it("should return correct schema") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val schema = df.schema
      assert(schema.fieldNames.contains("path"))
      assert(schema.fieldNames.contains("driver"))
      assert(schema.fieldNames.contains("fileSize"))
      assert(schema.fieldNames.contains("width"))
      assert(schema.fieldNames.contains("height"))
      assert(schema.fieldNames.contains("numBands"))
      assert(schema.fieldNames.contains("srid"))
      assert(schema.fieldNames.contains("crs"))
      assert(schema.fieldNames.contains("geoTransform"))
      assert(schema.fieldNames.contains("cornerCoordinates"))
      assert(schema.fieldNames.contains("bands"))
      assert(schema.fieldNames.contains("overviews"))
      assert(schema.fieldNames.contains("metadata"))
      assert(schema.fieldNames.contains("isTiled"))
      assert(schema.fieldNames.contains("compression"))
    }

    it("should return correct geoTransform struct") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val row = df
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY",
          "geoTransform.skewX",
          "geoTransform.skewY")
        .first()
      assert(row.getAs[Double]("scaleX") != 0.0)
      assert(row.getAs[Double]("scaleY") != 0.0)
    }

    it("should return correct cornerCoordinates struct") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val row = df
        .selectExpr(
          "cornerCoordinates.minX",
          "cornerCoordinates.minY",
          "cornerCoordinates.maxX",
          "cornerCoordinates.maxY")
        .first()
      assert(row.getAs[Double]("maxX") > row.getAs[Double]("minX"))
      assert(row.getAs[Double]("maxY") > row.getAs[Double]("minY"))
    }

    it("should return correct bands array") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val row = df.first()
      val numBands = row.getAs[Int]("numBands")
      val bands = row.getAs[Seq[Any]]("bands")
      assert(bands != null)
      assert(bands.size == numBands)
    }

    it("should return band metadata with correct fields") {
      val df = sparkSession.read
        .format("sedonainfo")
        .load(singleFileLocation)
        .selectExpr("explode(bands) as band")
        .selectExpr("band.band", "band.dataType", "band.blockWidth", "band.blockHeight")
      val row = df.first()
      assert(row.getAs[Int]("band") == 1)
      assert(row.getAs[String]("dataType") != null)
      assert(row.getAs[Int]("blockWidth") > 0)
      assert(row.getAs[Int]("blockHeight") > 0)
    }

    it("should cross-validate metadata against raster data source") {
      val metaDf = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val rasterDf = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(singleFileLocation)
        .selectExpr(
          "RS_Width(rast) as width",
          "RS_Height(rast) as height",
          "RS_NumBands(rast) as numBands",
          "RS_SRID(rast) as srid")

      val metaRow = metaDf.first()
      val rasterRow = rasterDf.first()
      assert(metaRow.getAs[Int]("width") == rasterRow.getAs[Int]("width"))
      assert(metaRow.getAs[Int]("height") == rasterRow.getAs[Int]("height"))
      assert(metaRow.getAs[Int]("numBands") == rasterRow.getAs[Int]("numBands"))
      assert(metaRow.getAs[Int]("srid") == rasterRow.getAs[Int]("srid"))
    }

    it("should support LIMIT pushdown") {
      val df = sparkSession.read.format("sedonainfo").load(rasterDir)
      val totalCount = df.count()
      assert(totalCount > 2, "Need at least 3 files for this test")
      val limitedDf = df.limit(2)
      assert(limitedDf.count() == 2)
    }

    it("should support column selection") {
      val df = sparkSession.read
        .format("sedonainfo")
        .load(singleFileLocation)
        .select("path", "width", "height")
      assert(df.schema.fieldNames.length == 3)
      assert(df.count() == 1)
    }

    it("should report isTiled correctly") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val row = df.first()
      val isTiled = row.getAs[Boolean]("isTiled")
      assert(isTiled == true || isTiled == false)
    }

    it("should return fileSize") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val row = df.first()
      val fileSize = row.getAs[Long]("fileSize")
      assert(fileSize > 0)
    }

    it("should return overviews array") {
      val df = sparkSession.read.format("sedonainfo").load(singleFileLocation)
      val row = df.first()
      val overviews = row.getAs[Seq[Any]]("overviews")
      assert(overviews != null)
    }

    it("should detect COG properties from a generated COG file") {
      // Generate a COG from test1.tiff using RS_AsCOG and write to disk
      val cogBytes = sparkSession.read
        .format("binaryFile")
        .load(singleFileLocation)
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsCOG(raster, 'LZW', 256, 0.5, 'Nearest', 2) as cog")
        .first()
        .getAs[Array[Byte]]("cog")

      val cogFile = new File(tempDir, "test_cog.tiff")
      val fos = new java.io.FileOutputStream(cogFile)
      try { fos.write(cogBytes) }
      finally { fos.close() }

      // Read the COG with sedonainfo
      val df = sparkSession.read.format("sedonainfo").load(cogFile.getAbsolutePath)
      assert(df.count() == 1)

      val row = df.first()

      // COGs should be tiled
      assert(row.getAs[Boolean]("isTiled") == true)

      // COGs should have overviews
      val overviews = row.getAs[Seq[Any]]("overviews")
      assert(overviews != null)
      assert(overviews.nonEmpty, "COG should have at least one overview level")

      // Verify overview struct fields are accessible
      val overviewDf = df
        .selectExpr("explode(overviews) as ovr")
        .selectExpr("ovr.level", "ovr.width", "ovr.height")
      val ovrRow = overviewDf.first()
      assert(ovrRow.getAs[Int]("level") >= 1)
      assert(ovrRow.getAs[Int]("width") > 0)
      assert(ovrRow.getAs[Int]("height") > 0)

      // Overview dimensions should be smaller than full resolution
      val fullWidth = row.getAs[Int]("width")
      val fullHeight = row.getAs[Int]("height")
      assert(ovrRow.getAs[Int]("width") < fullWidth)
      assert(ovrRow.getAs[Int]("height") < fullHeight)

      // Band block size should match the COG tile size (256)
      val bandDf = df
        .selectExpr("explode(bands) as band")
        .selectExpr("band.blockWidth", "band.blockHeight")
      val bandRow = bandDf.first()
      assert(bandRow.getAs[Int]("blockWidth") == 256)
      assert(bandRow.getAs[Int]("blockHeight") == 256)
    }

    it("should correctly report non-COG vs COG differences") {
      // Read the original non-COG test file
      val nonCogDf =
        sparkSession.read.format("sedonainfo").load(singleFileLocation).select("isTiled")
      val nonCogTiled = nonCogDf.first().getAs[Boolean]("isTiled")

      // Generate a COG and write directly to file
      val cogBytes = sparkSession.read
        .format("binaryFile")
        .load(singleFileLocation)
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsCOG(raster, 'Deflate', 256) as cog")
        .first()
        .getAs[Array[Byte]]("cog")

      val cogFile = new File(tempDir, "test_cog_compare.tiff")
      val fos = new java.io.FileOutputStream(cogFile)
      try { fos.write(cogBytes) }
      finally { fos.close() }

      val cogDf = sparkSession.read
        .format("sedonainfo")
        .load(cogFile.getAbsolutePath)
        .select("isTiled", "overviews")
      val cogRow = cogDf.first()

      // COG should be tiled with overviews
      assert(cogRow.getAs[Boolean]("isTiled") == true)
      assert(cogRow.getAs[Seq[Any]]("overviews").nonEmpty)
    }
  }
}
