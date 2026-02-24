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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.LimitExec
import org.apache.spark.sql.execution.SampleExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.geotools.coverage.grid.GridCoverage2D
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
      assert(rasterCount == 8)
      assert(rasterDf.count() == 0)
    }

    it("Passed RS_AsRaster with empty raster") {
      val df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))') as geom")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected =
        "Array(255.0, 255.0, 255.0, 255.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0, 255.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 3093151) as rasterized")
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
      val df = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 4326) as raster, ST_GeomFromWKT('LINESTRING(1 1, 2 1, 10 1)') as geom")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected = "Array(0.0, 0.0, 0.0, 0.0, 255.0, 255.0, 255.0, 255.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr(
        "RS_AsRaster(ST_GeomFromWKT('LINESTRING(4 1, 4 2, 4 10)'), raster, 'd', false, 255, 0d) as rasterized")
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
      df = df.selectExpr(
        "ST_GeomFromText('POINT (-13085817.809482181 3993868.8560156375)', 3857) as geom",
        "RS_FromGeoTiff(content) as raster")
      var rasterized =
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 61784, 0d) as rasterized")
      var actual = rasterized
        .selectExpr("RS_BandAsArray(rasterized, 1)")
        .first()
        .getSeq(0)
        .mkString("Array(", ", ", ")")
      var expected = "Array(61784.0)"
      assertEquals(expected, actual)

      rasterized = df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255) as rasterized")
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
        df.selectExpr("RS_AsRaster(geom, raster, 'd', false, 255, 0d, false) as rasterized")
      var actualSeq =
        rasterized.selectExpr("RS_BandAsArray(rasterized, 1)").first().getSeq[Double](0)
      var actualMax = actualSeq.max
      var actualSum = actualSeq.sum
      var expectedMax = 255.0d
      var expectedSum = 255.0 * 7
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

  describe("Raster read test") {
    it("should read geotiff using raster source with explicit tiling") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true", "tileWidth" -> "64"))
        .load(rasterdatalocation)
      assert(rasterDf.count() > 100)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[GridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width <= 64)
        assert(raster.getGridGeometry.getGridRange2D.height <= 64)
        val x = row.getInt(1)
        val y = row.getInt(2)
        assert(x >= 0 && y >= 0)
        raster.dispose(true)
      }

      // Test projection push-down
      rasterDf.selectExpr("y", "rast as r").collect().foreach { row =>
        val raster = row.getAs[Object](1).asInstanceOf[GridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width <= 64)
        assert(raster.getGridGeometry.getGridRange2D.height <= 64)
        val y = row.getInt(0)
        assert(y >= 0)
        raster.dispose(true)
      }
    }

    it("should tile geotiff using raster source with padding enabled") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true", "tileWidth" -> "64", "padWithNoData" -> "true"))
        .load(rasterdatalocation)
      assert(rasterDf.count() > 100)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[GridCoverage2D]
        assert(raster.getGridGeometry.getGridRange2D.width == 64)
        assert(raster.getGridGeometry.getGridRange2D.height == 64)
        val x = row.getInt(1)
        val y = row.getInt(2)
        assert(x >= 0 && y >= 0)
        raster.dispose(true)
      }
    }

    it("should push down limit and sample to data source") {
      FileUtils.cleanDirectory(new File(tempDir))

      val sourceDir = new File(rasterdatalocation)
      val files = sourceDir.listFiles().filter(_.isFile)
      var numUniqueFiles = 0
      var numTotalFiles = 0
      files.foreach { file =>
        if (file.getPath.endsWith(".tif") || file.getPath.endsWith(".tiff")) {
          // Create 4 copies for each file
          for (i <- 0 until 4) {
            val destFile = new File(tempDir + "/" + file.getName + "_" + i)
            FileUtils.copyFile(file, destFile)
            numTotalFiles += 1
          }
          numUniqueFiles += 1
        }
      }

      val df = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(tempDir)
        .withColumn("width", expr("RS_Width(rast)"))

      val dfWithLimit = df.limit(numUniqueFiles)
      val plan = queryPlan(dfWithLimit)
      // Global/local limits are all pushed down to data source
      assert(plan.collect { case e: LimitExec => e }.isEmpty)
      assert(dfWithLimit.count() == numUniqueFiles)

      val dfWithSample = df.sample(0.3, seed = 42)
      val planSample = queryPlan(dfWithSample)
      // Sample is pushed down to data source
      assert(planSample.collect { case e: SampleExec => e }.isEmpty)
      val count = dfWithSample.count()
      assert(count >= numTotalFiles * 0.1 && count <= numTotalFiles * 0.5)

      val dfWithSampleAndLimit = df.sample(0.5, seed = 42).limit(numUniqueFiles)
      val planBoth = queryPlan(dfWithSampleAndLimit)
      assert(planBoth.collect { case e: LimitExec => e }.isEmpty)
      assert(planBoth.collect { case e: SampleExec => e }.isEmpty)
      assert(dfWithSampleAndLimit.count() == numUniqueFiles)

      // Limit and sample cannot be fully pushed down when retile is enabled
      val dfReTiledWithSampleAndLimit = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true"))
        .load(tempDir)
        .sample(0.5, seed = 42)
        .limit(numUniqueFiles)
      dfReTiledWithSampleAndLimit.explain(true)
      val planRetiled = queryPlan(dfReTiledWithSampleAndLimit)
      assert(planRetiled.collect { case e: LimitExec => e }.nonEmpty)
      assert(planRetiled.collect { case e: SampleExec => e }.nonEmpty)
    }

    it("should read geotiff using raster source without tiling") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(rasterdatalocation)
      assert(rasterDf.schema.fields.length == 2)
      rasterDf.collect().foreach { row =>
        val raster = row.getAs[Object](0).asInstanceOf[GridCoverage2D]
        assert(raster != null)
        raster.dispose(true)
        // Should load name correctly
        val name = row.getString(1)
        assert(name != null)
      }
    }

    it("should read geotiff using raster source with auto-tiling") {
      val rasterDf = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "true"))
        .load(rasterdatalocation)
      val rasterDfNoTiling = sparkSession.read
        .format("raster")
        .options(Map("retile" -> "false"))
        .load(rasterdatalocation)
      assert(rasterDf.count() > rasterDfNoTiling.count())
    }

    it("should throw exception when only tileHeight is specified") {
      assertThrows[IllegalArgumentException] {
        val df = sparkSession.read
          .format("raster")
          .options(Map("retile" -> "true", "tileHeight" -> "64"))
          .load(rasterdatalocation)
        df.collect()
      }
    }

    it("should throw exception when the geotiff is badly tiled") {
      val exception = intercept[Exception] {
        val rasterDf = sparkSession.read
          .format("raster")
          .options(Map("retile" -> "true"))
          .load(resourceFolder + "raster_geotiff_color/*")
        rasterDf.collect()
      }
      assert(
        exception.getMessage.contains(
          "To resolve this issue, you can try one of the following methods"))
    }

    it("read partitioned directory") {
      FileUtils.cleanDirectory(new File(tempDir))
      Files.createDirectory(new File(tempDir + "/part=1").toPath)
      Files.createDirectory(new File(tempDir + "/part=2").toPath)
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test1.tiff"),
        new File(tempDir + "/part=1/test1.tiff"))
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test2.tiff"),
        new File(tempDir + "/part=1/test2.tiff"))
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test4.tiff"),
        new File(tempDir + "/part=2/test4.tiff"))
      FileUtils.copyFile(
        new File(resourceFolder + "raster/test4.tiff"),
        new File(tempDir + "/part=2/test5.tiff"))

      val rasterDf = sparkSession.read
        .format("raster")
        .load(tempDir)
      val rows = rasterDf.collect()
      assert(rows.length >= 4)
      rows.foreach { row =>
        val name = row.getAs[String]("name")
        if (name.startsWith("test1") || name.startsWith("test2")) {
          assert(row.getAs[Int]("part") == 1)
        } else {
          assert(row.getAs[Int]("part") == 2)
        }
      }
    }

    it("read directory recursively from a temp directory with subdirectories") {
      // Create temp subdirectories in tempDir
      FileUtils.cleanDirectory(new File(tempDir))
      val subDir1 = tempDir + "/subdir1"
      val subDir2 = tempDir + "/nested/subdir2"
      new File(subDir1).mkdirs()
      new File(subDir2).mkdirs()

      // Copy raster files from resourceFolder/raster to the temp subdirectories
      val sourceDir = new File(resourceFolder + "raster")
      val files = sourceDir.listFiles().filter(_.isFile)
      files.zipWithIndex.foreach { case (file, idx) =>
        idx % 3 match {
          case 0 => FileUtils.copyFile(file, new File(tempDir, file.getName))
          case 1 => FileUtils.copyFile(file, new File(subDir1, file.getName))
          case 2 => FileUtils.copyFile(file, new File(subDir2, file.getName))
        }
      }

      val rasterDfNonRecursive = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(sourceDir.getPath)

      val rasterDfRecursive = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(tempDir + "/")

      val rowsNonRecursive = rasterDfNonRecursive.collect()
      val rowsRecursive = rasterDfRecursive.collect()
      assert(rowsRecursive.length == rowsNonRecursive.length)
    }

    it("read directory suffixed by /*.tif") {
      val df = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(resourceFolder + "raster")

      val dfTif = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(resourceFolder + "raster/*.tif")

      val dfTiff = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(resourceFolder + "raster/*.tiff")

      assert(df.count() == dfTif.count() + dfTiff.count())
      queryPlan(dfTif).collect { case scan: BatchScanExec => scan }.foreach { scan =>
        val table = scan.table.asInstanceOf[RasterTable]
        assert(!table.paths.head.endsWith("*.tif"))
        assert(table.options.get("pathGlobFilter") == "*.tif")
      }
      queryPlan(dfTiff).collect { case scan: BatchScanExec => scan }.foreach { scan =>
        val table = scan.table.asInstanceOf[RasterTable]
        assert(!table.paths.head.endsWith("*.tiff"))
        assert(table.options.get("pathGlobFilter") == "*.tiff")
      }

      var dfComplexGlob = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(resourceFolder + "raster/test*.tiff")
      queryPlan(dfComplexGlob).collect { case scan: BatchScanExec => scan }.foreach { scan =>
        val table = scan.table.asInstanceOf[RasterTable]
        assert(!table.paths.head.endsWith("*.tiff"))
        assert(table.options.get("pathGlobFilter") == "test*.tiff")
      }
      dfComplexGlob = sparkSession.read
        .format("raster")
        .option("retile", "false")
        .load(resourceFolder + "raster/*1.tiff")
      queryPlan(dfComplexGlob).collect { case scan: BatchScanExec => scan }.foreach { scan =>
        val table = scan.table.asInstanceOf[RasterTable]
        assert(!table.paths.head.endsWith("*1.tiff"))
        assert(table.options.get("pathGlobFilter") == "*1.tiff")
      }
    }
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempDir))
    super.afterAll()
  }

  private def queryPlan(df: DataFrame): SparkPlan = {
    df.queryExecution.executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.initialPlan
      case plan: SparkPlan => plan
    }
  }
}
