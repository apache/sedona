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
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.sedona_sql.io.geotiffmetadata.GeoTiffMetadataScan
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files
import java.util.Collections

class geotiffMetadataTest extends TestBaseScala with BeforeAndAfterAll {

  val rasterDir: String = resourceFolder + "raster/"
  val singleFileLocation: String = resourceFolder + "raster/test1.tiff"
  val tempDir: String =
    Files.createTempDirectory("sedona_geotiffmetadata_test_").toFile.getAbsolutePath

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempDir))
    super.afterAll()
  }

  private def withSqlConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = sparkSession.conf
    val originals = pairs.map { case (k, _) => k -> conf.getOption(k) }
    pairs.foreach { case (k, v) => conf.set(k, v) }
    try f
    finally
      originals.foreach {
        case (k, Some(v)) => conf.set(k, v)
        case (k, None) => conf.unset(k)
      }
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
      // TIFF tag 259 = LZW compression
      assertEquals("LZW", row.getAs[String]("compression"))

      // metadata map should be non-null and contain common TIFF tags
      val metadata = row.getAs[Map[String, String]]("metadata")
      assert(metadata != null)
      assert(metadata.nonEmpty)
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

    it("should read files from directory without trailing slash") {
      // Directory detection via Hadoop FS should apply recursive lookup regardless of slash
      val df = sparkSession.read.format("geotiff.metadata").load(rasterDir.stripSuffix("/"))
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
      // Requested LZW compression should be reflected
      assertEquals("LZW", row.getAs[String]("compression"))

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

    it("should support Hive-style partition discovery when recursiveFileLookup is disabled") {
      val base = new File(tempDir, "partitioned")
      val d2020 = new File(base, "year=2020")
      val d2021 = new File(base, "year=2021")
      d2020.mkdirs()
      d2021.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(d2020, "a.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(d2021, "b.tiff"))

      // Force both files into a single bin-packed Spark partition; otherwise each file gets
      // its own partition and the per-file partition-value handling is never exercised
      withSqlConf(
        "spark.sql.files.openCostInBytes" -> "0",
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.maxPartitionBytes" -> "1073741824") {
        // Mixed-case option spelling: data source options are case-insensitive, so this must
        // suppress the lowercase recursiveFileLookup default just the same
        val df = sparkSession.read
          .format("geotiff.metadata")
          .option("RecursiveFileLookup", "false")
          .load(base.getAbsolutePath)
        // Partition inference stays available because the explicit option is respected
        assert(df.schema.fieldNames.contains("year"))
        assertEquals(1, df.rdd.getNumPartitions)
        val rows = df.selectExpr("path", "year").collect()
        assertEquals(2, rows.length)
        // Every row must carry the partition value of ITS OWN file, even though both files
        // are bin-packed into one Spark partition
        rows.foreach { r =>
          assert(r.getAs[String]("path").contains(s"year=${r.getAs[Int]("year")}"))
        }
      }
    }

    it("should not filter explicitly named files in a mixed directory-and-file load") {
      // The extension filter is a directory-scan default; Spark applies pathGlobFilter to
      // every root, so it must not be installed when an explicitly named file (here without
      // an extension) is loaded alongside a directory.
      val dir = new File(tempDir, "mixedRoots")
      dir.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "a.tiff"))
      val explicitFile = new File(tempDir, "explicit_geotiff_no_extension")
      FileUtils.copyFile(new File(singleFileLocation), explicitFile)

      val df = sparkSession.read
        .format("geotiff.metadata")
        .load(dir.getAbsolutePath, explicitFile.getAbsolutePath)
      assertEquals(2L, df.count())
      // Both files must parse, including the extensionless explicit one
      assertEquals(2, df.select("width").collect().length)
    }

    it("should hash scans from every field used by equality") {
      val scan = sparkSession.read
        .format("geotiff.metadata")
        .load(singleFileLocation)
        .queryExecution
        .executedPlan
        .collectFirst { case exec: BatchScanExec =>
          exec.scan.asInstanceOf[GeoTiffMetadataScan]
        }
        .getOrElse(fail("GeoTIFF metadata query did not contain a BatchScanExec"))

      val equalScan = scan.copy()
      val limitedScan = scan.copy(pushedLimit = Some(1))
      val differentOptionsScan = scan.copy(options =
        new CaseInsensitiveStringMap(Collections.singletonMap("hash-test", "different")))

      assert(scan == equalScan)
      assertEquals(scan.hashCode(), equalScan.hashCode())
      assert(scan != limitedScan)
      assert(scan.hashCode() != limitedScan.hashCode())
      assert(scan != differentOptionsScan)
      assert(scan.hashCode() != differentOptionsScan.hashCode())
    }

    it("should apply both a glob path and an explicit pathGlobFilter") {
      // Native Spark semantics: both constraints apply, so a*.tiff restricted to b*.tiff is
      // empty.
      val dir = new File(tempDir, "globAndFilter")
      dir.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "a.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "b.tiff"))

      val both = sparkSession.read
        .format("geotiff.metadata")
        .option("pathGlobFilter", "b*.tiff")
        .load(dir.getAbsolutePath + "/a*.tiff")
      assertEquals(0L, both.count())

      // Sanity: without the explicit filter, the glob path alone matches a.tiff
      val globOnly =
        sparkSession.read.format("geotiff.metadata").load(dir.getAbsolutePath + "/a*.tiff")
      assertEquals(1L, globOnly.count())
    }

    it("should not widen a file glob into nested files under explicit recursion") {
      // GH-3131: the glob used to be rewritten to path=dir + pathGlobFilter, and
      // pathGlobFilter matches leaf names at any depth, so recursiveFileLookup=true pulled
      // nested a2.tiff into a load of dir/a*.tiff. The glob path now reaches Spark natively
      // and matches direct children of the directory only.
      val dir = new File(tempDir, "globRecursive")
      val sub = new File(dir, "sub")
      sub.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "a1.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(sub, "a2.tiff"))

      val df = sparkSession.read
        .format("geotiff.metadata")
        .option("recursiveFileLookup", "true")
        .load(dir.getAbsolutePath + "/a*.tiff")
      assertEquals(1L, df.count())
      assert(df.select("path").first().getString(0).endsWith("a1.tiff"))
    }

    it("should apply the directory-scan defaults to a glob that matches directories") {
      // GH-3131: region* expands to directories, which must get the same defaults as loading
      // them by explicit path — recursive lookup plus the extension filter.
      val root = new File(tempDir, "dirGlob")
      val region1 = new File(root, "region1")
      val region2Sub = new File(root, "region2/sub")
      region1.mkdirs()
      region2Sub.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(region1, "x.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(region2Sub, "y.tiff"))
      // The extension filter must exclude this; without it the reader would surface a row
      FileUtils.writeStringToFile(new File(region1, "readme.txt"), "not a geotiff", "UTF-8")

      val df = sparkSession.read
        .format("geotiff.metadata")
        .load(root.getAbsolutePath + "/region*")
      assertEquals(2L, df.count())
    }

    it("should keep the directory defaults when a glob also matches a _SUCCESS marker") {
      // Entries Spark's own listing discards (_SUCCESS, dotfiles, *._COPYING_) must not
      // sway the all-directories classification: root/* still stands for directories, so
      // the recursive-scan and extension-filter defaults apply and the nested file is found.
      val root = new File(tempDir, "markerGlob")
      val region1Sub = new File(root, "region1/sub")
      region1Sub.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(root, "region1/x.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(region1Sub, "y.tiff"))
      FileUtils.writeStringToFile(new File(root, "_SUCCESS"), "", "UTF-8")

      val df = sparkSession.read.format("geotiff.metadata").load(root.getAbsolutePath + "/*")
      assertEquals(2L, df.count())
    }

    it("should apply the directory defaults to an explicitly loaded underscore directory") {
      // Spark's hidden-name rule applies to listing entries, never to explicitly given
      // roots — so probing a literal _staging root must not filter it either.
      val root = new File(tempDir, "underscoreDir")
      val staging = new File(root, "_staging")
      val sub = new File(staging, "sub")
      sub.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(staging, "x.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(sub, "y.tiff"))

      val df = sparkSession.read.format("geotiff.metadata").load(staging.getAbsolutePath)
      assertEquals(2L, df.count())
    }

    it("should probe the path literally when glob expansion is disabled") {
      // With __globPaths__=false Spark treats the path literally, so a directory whose
      // name contains glob metacharacters must be probed with getFileStatus, not expanded
      // as a pattern — otherwise it loses the directory-scan defaults.
      val root = new File(tempDir, "literalGlobChars")
      val dir = new File(root, "[region]")
      val sub = new File(dir, "sub")
      sub.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "x.tiff"))
      FileUtils.copyFile(new File(singleFileLocation), new File(sub, "y.tiff"))

      val df = sparkSession.read
        .format("geotiff.metadata")
        .option("__globPaths__", "false")
        .load(dir.getAbsolutePath)
      assertEquals(2L, df.count())
    }

    it("should report a file glob that matches nothing as a missing path") {
      // Native Spark glob semantics, aligned with every other file source: an empty glob is
      // an error, not a silently empty result (the pre-GH-3131 rewrite returned no rows).
      val dir = new File(tempDir, "emptyGlob")
      dir.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "a.tiff"))
      intercept[org.apache.spark.sql.AnalysisException] {
        sparkSession.read
          .format("geotiff.metadata")
          .load(dir.getAbsolutePath + "/nomatch*.tiff")
          .count()
      }
    }

    it("should filter non-GeoTIFF files when loading multiple directories") {
      val dirA = new File(tempDir, "multiA")
      val dirB = new File(tempDir, "multiB")
      dirA.mkdirs()
      dirB.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dirA, "a.tiff"))
      FileUtils.writeStringToFile(new File(dirA, "junk.txt"), "not a geotiff", "UTF-8")
      FileUtils.copyFile(new File(singleFileLocation), new File(dirB, "b.tiff"))

      val df = sparkSession.read
        .format("geotiff.metadata")
        .load(dirA.getAbsolutePath, dirB.getAbsolutePath)
      assertEquals(2L, df.count())
      // Force a parse of every listed file — junk.txt would fail here if not filtered out
      assertEquals(2, df.select("width").collect().length)
    }

    it("should fail catalog table creation with a clear error instead of an NPE") {
      def messages(t: Throwable): Seq[String] =
        if (t == null) Nil else Option(t.getMessage).toSeq ++ messages(t.getCause)

      sparkSession.sql("DROP TABLE IF EXISTS geotiff_meta_tbl")
      try {
        val err = intercept[Exception] {
          sparkSession.sql(
            s"CREATE TABLE geotiff_meta_tbl USING `geotiff.metadata` LOCATION '$singleFileLocation'")
        }
        assert(
          messages(err).exists(_.contains("does not support catalog table operations")),
          s"unexpected error: $err")
      } finally {
        sparkSession.sql("DROP TABLE IF EXISTS geotiff_meta_tbl")
      }
    }

    it("should reject catalog table creation with an explicit schema") {
      // With a user-supplied schema Spark skips FileFormat.inferSchema, so rejection relies on
      // the stub fallback format failing on instantiation. Depending on the Spark version the
      // error surfaces at CREATE or at first SELECT — either way it must be the clear message,
      // never an NPE or a silently unusable table.
      def messages(t: Throwable): Seq[String] =
        if (t == null) Nil else Option(t.getMessage).toSeq ++ messages(t.getCause)

      sparkSession.sql("DROP TABLE IF EXISTS geotiff_meta_schema_tbl")
      try {
        val err = intercept[Exception] {
          sparkSession.sql(
            "CREATE TABLE geotiff_meta_schema_tbl (path STRING) " +
              s"USING `geotiff.metadata` LOCATION '$singleFileLocation'")
          sparkSession.sql("SELECT * FROM geotiff_meta_schema_tbl").collect()
        }
        assert(
          messages(err).exists(_.contains("does not support catalog table operations")),
          s"unexpected error: $err")
      } finally {
        sparkSession.sql("DROP TABLE IF EXISTS geotiff_meta_schema_tbl")
      }
    }
  }
}
