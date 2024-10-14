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
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.SaveMode
import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import java.io.File
import java.nio.file.Files

class rasterIOTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  var rasterdatalocation: String = resourceFolder + "raster/"
  val tempDir: String = Files.createTempDirectory("sedona_raster_io_test_").toFile.getAbsolutePath

  describe("Raster IO test") {
    it(
      "should read geotiff using binary source and write geotiff back to disk using raster source") {
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
      rasterDf.write
        .format("raster")
        .option("rasterField", "content")
        .option("fileExtension", ".tiff")
        .option("pathField", "path")
        .option("useDirectCommitter", "false")
        .mode(SaveMode.Overwrite)
        .save(tempDir + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should read and write via RS_FromGeoTiff and RS_AsGeoTiff") {
      var df = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      var rasterDf = df
        .selectExpr("RS_FromGeoTiff(content) as raster", "path")
        .selectExpr("RS_AsGeoTiff(raster) as content", "path")
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .option("rasterField", "content")
        .option("fileExtension", ".tiff")
        .option("pathField", "path")
        .mode(SaveMode.Overwrite)
        .save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it("should handle null") {
      var df = sparkSession.read.format("binaryFile").load(rasterdatalocation)
      var rasterDf = df
        .selectExpr("RS_FromGeoTiff(null) as raster", "length")
        .selectExpr("RS_AsGeoTiff(raster) as content", "length")
      val rasterCount = rasterDf.count()
      rasterDf.write.format("raster").mode(SaveMode.Overwrite).save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterCount == 6)
      assert(rasterDf.count() == 0)
    }

    it("Passed RS_AsRaster with empty raster") {
      var df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))') as geom")
      var rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', 255, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected =
        "Array(255.0, 255.0, 255.0, 255.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0, 255.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', 3093151) as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected =
        "Array(3093151.0, 3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd') as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected =
        "Array(1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsRaster LineString") {
      var df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('LINESTRING(1 1, 2 1, 10 1)') as geom")
      var rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', 255, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected = "Array(255.0, 255.0, 255.0, 255.0, 255.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr(
        "RS_AsRaster(ST_GeomFromWKT('LINESTRING(1 1, 1 2, 1 10)'), raster, 'd', 255, 0d) as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected = "Array(255.0, 255.0, 255.0, 255.0, 255.0)"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsRaster with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df =
        df.selectExpr("ST_GeomFromWKT('POINT(5 5)') as geom", "RS_FromGeoTiff(content) as raster")
      var rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', 61784, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected = "Array(61784.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', 255) as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected = "Array(255.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd') as rasterized")
      actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      expected = "Array(1.0)"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsRaster with raster extent") {
      var df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 0) as raster, ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))') as geom")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', 255, 0d, false) as rasterized")
      var actualSeq =
        rasterized.selectExpr("RS_BandAsArray(rasterized, 1)").first().getSeq[Double](0)
      var actualMax = actualSeq.max
      var actualSum = actualSeq.sum
      var expectedMax = 255.0d
      var expectedSum = 255.0 * 9
      assertEquals(expectedMax, actualMax, 1e-5)
      assertEquals(expectedSum, actualSum, 1e-5)

      var actualWidth = rasterized.selectExpr("RS_Width(rasterized)").first().getInt(0)
      var actualHeight = rasterized.selectExpr("RS_Height(rasterized)").first().getInt(0)
      assertEquals(255, actualWidth)
      assertEquals(255, actualHeight)
    }

    it("should read RS_FromGeoTiff and write RS_AsArcGrid") {
      var df =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster_geotiff_color/*")
      var rasterDf = df
        .selectExpr("RS_FromGeoTiff(content) as raster", "path")
        .selectExpr("RS_AsArcGrid(raster, 1) as content", "path")
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .option("rasterField", "content")
        .option("fileExtension", ".asc")
        .option("pathField", "path")
        .mode(SaveMode.Overwrite)
        .save(tempDir + "/raster-written")
      df = sparkSession.read.format("binaryFile").load(tempDir + "/raster-written/*")
      rasterDf = df.selectExpr("RS_FromArcInfoAsciiGrid(content)")
      assert(rasterDf.count() == rasterCount)
    }

    it(
      "should read geotiff using binary source and write geotiff back to hdfs using raster source") {
      val miniHDFS: (MiniDFSCluster, String) = creatMiniHdfs()
      var rasterDf =
        sparkSession.read.format("binaryFile").load(rasterdatalocation).repartition(3)
      val rasterCount = rasterDf.count()
      rasterDf.write
        .format("raster")
        .mode(SaveMode.Overwrite)
        .save(miniHDFS._2 + "/raster-written")
      rasterDf = sparkSession.read.format("binaryFile").load(miniHDFS._2 + "/raster-written/*")
      rasterDf = rasterDf.selectExpr("RS_FromGeoTiff(content)")
      assert(rasterDf.count() == rasterCount)
      miniHDFS._1.shutdown()
    }
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(tempDir))
}
