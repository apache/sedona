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
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files

class geotiffMetadataTest extends TestBaseScala with BeforeAndAfterAll {

  val rasterDir: String = resourceFolder + "raster/"
  val singleFileLocation: String = resourceFolder + "raster/test1.tiff"
  val tempDir: String =
    Files.createTempDirectory("sedona_geotiffmetadata_test_").toFile.getAbsolutePath

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempDir))
    super.afterAll()
  }

  describe("GeoTiffMetadata data source") {

    it("should read test1.tiff with exact metadata values") {
      val df = sparkSession.read.format("geotiff.metadata").load(singleFileLocation)
      assertEquals(1L, df.count())

      val row = df.first()
      assert(row.getAs[String]("path").endsWith("test1.tiff"))
      assertEquals("GTiff", row.getAs[String]("driver"))
      assertEquals(174803L, row.getAs[Long]("fileSize"))
      assertEquals(512, row.getAs[Int]("width"))
      assertEquals(517, row.getAs[Int]("height"))
      assertEquals(1, row.getAs[Int]("numBands"))
      assertEquals(3857, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("EPSG"))
      // test1.tiff has TileWidth/TileLength TIFF tags (internally tiled)
      assertEquals(true, row.getAs[Boolean]("isTiled"))
    }

    it("should return exact geoTransform for test1.tiff") {
      val row = sparkSession.read
        .format("geotiff.metadata")
        .load(singleFileLocation)
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY",
          "geoTransform.skewX",
          "geoTransform.skewY")
        .first()
      assertEquals(-1.3095817809482181e7, row.getAs[Double]("upperLeftX"), 0.01)
      assertEquals(4021262.7487925636, row.getAs[Double]("upperLeftY"), 0.01)
      assertEquals(72.32861272132695, row.getAs[Double]("scaleX"), 1e-10)
      assertEquals(-72.32861272132695, row.getAs[Double]("scaleY"), 1e-10)
      assertEquals(0.0, row.getAs[Double]("skewX"), 1e-15)
      assertEquals(0.0, row.getAs[Double]("skewY"), 1e-15)
    }

    it("should return exact cornerCoordinates for test1.tiff") {
      val row = sparkSession.read
        .format("geotiff.metadata")
        .load(singleFileLocation)
        .selectExpr(
          "cornerCoordinates.minX",
          "cornerCoordinates.minY",
          "cornerCoordinates.maxX",
          "cornerCoordinates.maxY")
        .first()
      assertEquals(-1.3095817809482181e7, row.getAs[Double]("minX"), 0.01)
      assertEquals(3983868.8560156375, row.getAs[Double]("minY"), 0.01)
      assertEquals(-1.3058785559768861e7, row.getAs[Double]("maxX"), 0.01)
      assertEquals(4021262.7487925636, row.getAs[Double]("maxY"), 0.01)
    }

    it("should return exact band metadata for test1.tiff") {
      val row = sparkSession.read
        .format("geotiff.metadata")
        .load(singleFileLocation)
        .selectExpr("explode(bands) as b")
        .selectExpr(
          "b.band",
          "b.dataType",
          "b.colorInterpretation",
          "b.noDataValue",
          "b.blockWidth",
          "b.blockHeight",
          "b.description",
          "b.unit")
        .first()
      assertEquals(1, row.getAs[Int]("band"))
      assertEquals("UNSIGNED_8BITS", row.getAs[String]("dataType"))
      assertEquals("Gray", row.getAs[String]("colorInterpretation"))
      assert(row.isNullAt(row.fieldIndex("noDataValue")))
      assertEquals(256, row.getAs[Int]("blockWidth"))
      assertEquals(256, row.getAs[Int]("blockHeight"))
      assertEquals("GRAY_INDEX", row.getAs[String]("description"))
      assert(row.isNullAt(row.fieldIndex("unit")))
    }

    it("should return empty overviews for non-COG test1.tiff") {
      // test1.tiff has only 1 IFD (no internal overviews)
      val row = sparkSession.read
        .format("geotiff.metadata")
        .load(singleFileLocation)
        .selectExpr("size(overviews) as overviewCount")
        .first()
      assertEquals(0, row.getAs[Int]("overviewCount"))
    }

    it("should cross-validate against raster data source") {
      val metaRow = sparkSession.read.format("geotiff.metadata").load(singleFileLocation).first()
      val rasterRow = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(singleFileLocation)
        .selectExpr(
          "RS_Width(rast) as width",
          "RS_Height(rast) as height",
          "RS_NumBands(rast) as numBands",
          "RS_SRID(rast) as srid")
        .first()
      assertEquals(metaRow.getAs[Int]("width"), rasterRow.getAs[Int]("width"))
      assertEquals(metaRow.getAs[Int]("height"), rasterRow.getAs[Int]("height"))
      assertEquals(metaRow.getAs[Int]("numBands"), rasterRow.getAs[Int]("numBands"))
      assertEquals(metaRow.getAs[Int]("srid"), rasterRow.getAs[Int]("srid"))
    }

    it("should read multiple files via glob") {
      val df = sparkSession.read.format("geotiff.metadata").load(rasterDir + "*.tiff")
      // 7 .tiff files in the raster directory (excludes test3.tif)
      assertEquals(7L, df.count())
    }

    it("should read files from directory with trailing slash") {
      val df = sparkSession.read.format("geotiff.metadata").load(rasterDir)
      // Recursive lookup finds all .tif/.tiff files including subdirectories
      assertEquals(9L, df.count())
    }

    it("should support LIMIT pushdown") {
      val df = sparkSession.read.format("geotiff.metadata").load(rasterDir).limit(2)
      assertEquals(2L, df.count())
    }

    it("should support column pruning") {
      val df = sparkSession.read
        .format("geotiff.metadata")
        .load(singleFileLocation)
        .select("path", "width", "height")
      assertEquals(3, df.schema.fieldNames.length)
      val row = df.first()
      assertEquals(512, row.getAs[Int]("width"))
      assertEquals(517, row.getAs[Int]("height"))
    }

    it("should detect COG properties from a generated COG") {
      // Generate a COG with known parameters: LZW compression, 256 tile size, 2 overviews
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

      val df = sparkSession.read.format("geotiff.metadata").load(cogFile.getAbsolutePath)
      val row = df.first()

      // COG preserves original dimensions and CRS
      assertEquals(512, row.getAs[Int]("width"))
      assertEquals(517, row.getAs[Int]("height"))
      assertEquals(1, row.getAs[Int]("numBands"))
      assertEquals(3857, row.getAs[Int]("srid"))

      // COG must be tiled
      assertEquals(true, row.getAs[Boolean]("isTiled"))

      // COG must have overviews (requested 2)
      val overviews = df
        .selectExpr("explode(overviews) as o")
        .selectExpr("o.level", "o.width", "o.height")
        .collect()
      assertEquals(2, overviews.length)
      // Each overview is progressively smaller
      assert(overviews(0).getAs[Int]("width") < 512)
      assert(overviews(1).getAs[Int]("width") < overviews(0).getAs[Int]("width"))

      // Block size should match the requested 256
      val bandRow = df
        .selectExpr("explode(bands) as b")
        .selectExpr("b.blockWidth", "b.blockHeight")
        .first()
      assertEquals(256, bandRow.getAs[Int]("blockWidth"))
      assertEquals(256, bandRow.getAs[Int]("blockHeight"))
    }
  }
}
