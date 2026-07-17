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
import org.apache.spark.sql.Row
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files

class netcdfMetadataTest extends TestBaseScala with BeforeAndAfterAll {

  val netcdfDir: String = resourceFolder + "raster/netcdf/"
  val singleFileLocation: String = netcdfDir + "test.nc"
  val variantsDir: String = resourceFolder + "raster/netcdf_variants/"
  val tempDir: String =
    Files.createTempDirectory("sedona_netcdfmetadata_test_").toFile.getAbsolutePath

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempDir))
    super.afterAll()
  }

  describe("NetCdfMetadata data source") {

    it("should read test.nc with exact metadata values") {
      val df = sparkSession.read.format("netcdf.metadata").load(singleFileLocation)
      assertEquals(1L, df.count())

      val row = df.first()
      assert(row.getAs[String]("path").endsWith("test.nc"))
      assertEquals("NetCDF", row.getAs[String]("driver"))
      assertEquals(124336L, row.getAs[Long]("fileSize"))
      assertEquals("NetCDF", row.getAs[String]("format"))
      assertEquals(80, row.getAs[Int]("width"))
      assertEquals(48, row.getAs[Int]("height"))
      // test.nc carries no grid mapping or CRS attributes
      assert(row.isNullAt(row.fieldIndex("srid")))
      assert(row.isNullAt(row.fieldIndex("crs")))
    }

    it("should return exact geoTransform for test.nc") {
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY",
          "geoTransform.skewX",
          "geoTransform.skewY")
        .first()
      // Coordinate values are pixel centers: lon 5..14.875, lat 45..50.875, spacing 0.125.
      // The transform origin is the outer corner of the top-left pixel (GDAL convention),
      // matching RS_FromNetCDF.
      assertEquals(4.9375, row.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(50.9375, row.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(0.125, row.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-0.125, row.getAs[Double]("scaleY"), 1e-6)
      assertEquals(0.0, row.getAs[Double]("skewX"), 1e-15)
      assertEquals(0.0, row.getAs[Double]("skewY"), 1e-15)
    }

    it("should return exact cornerCoordinates for test.nc") {
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .selectExpr(
          "cornerCoordinates.minX",
          "cornerCoordinates.minY",
          "cornerCoordinates.maxX",
          "cornerCoordinates.maxY")
        .first()
      assertEquals(4.9375, row.getAs[Double]("minX"), 1e-6)
      assertEquals(44.9375, row.getAs[Double]("minY"), 1e-6)
      assertEquals(14.9375, row.getAs[Double]("maxX"), 1e-6)
      assertEquals(50.9375, row.getAs[Double]("maxY"), 1e-6)
    }

    it("should return exact dimensions for test.nc") {
      val dims = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .selectExpr("explode(dimensions) as d")
        .selectExpr("d.name", "d.length", "d.isUnlimited")
        .collect()
        .map(r =>
          (r.getAs[String]("name"), r.getAs[Int]("length"), r.getAs[Boolean]("isUnlimited")))

      assertEquals(4, dims.length)
      assert(dims.contains(("time", 2, true)))
      assert(dims.contains(("z", 2, false)))
      assert(dims.contains(("lat", 48, false)))
      assert(dims.contains(("lon", 80, false)))
    }

    it("should return exact variable metadata for test.nc") {
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .selectExpr("explode(variables) as v")
        .selectExpr(
          "v.name",
          "v.dataType",
          "v.dimensions",
          "v.shape",
          "v.units",
          "v.longName",
          "v.standardName",
          "v.noDataValue",
          "v.isCoordinate",
          "v.attributes")

      val byName = df.collect().map(r => r.getAs[String]("name") -> r).toMap
      assertEquals(6, byName.size)
      assertEquals(Set("time", "z", "lat", "lon", "O3", "NO2"), byName.keySet)

      // Data variable: 4-D, not a coordinate, all CF attributes surfaced
      val o3 = byName("O3")
      assertEquals("float", o3.getAs[String]("dataType"))
      assertEquals(Seq("time", "z", "lat", "lon"), o3.getAs[Seq[String]]("dimensions"))
      assertEquals(Seq(2, 2, 48, 80), o3.getAs[Seq[Int]]("shape"))
      assertEquals("Ozone concentration", o3.getAs[String]("longName"))
      assertEquals("mass_concentration_of_ozone_in_air", o3.getAs[String]("standardName"))
      // test.nc uses the non-CF attribute name "unit", so the CF `units` column is null,
      // but the raw attribute is still available in the attributes map
      assert(o3.isNullAt(o3.fieldIndex("units")))
      assertEquals("microgram/m3", o3.getAs[Map[String, String]]("attributes")("unit"))
      // missing_value = NaN
      assert(o3.getAs[Double]("noDataValue").isNaN)
      assertEquals(false, o3.getAs[Boolean]("isCoordinate"))

      // Coordinate variable: 1-D, flagged as coordinate, CF units surfaced
      val lat = byName("lat")
      assertEquals("float", lat.getAs[String]("dataType"))
      assertEquals(Seq("lat"), lat.getAs[Seq[String]]("dimensions"))
      assertEquals(Seq(48), lat.getAs[Seq[Int]]("shape"))
      assertEquals("degrees_north", lat.getAs[String]("units"))
      assertEquals("latitudes", lat.getAs[String]("longName"))
      assertEquals(true, lat.getAs[Boolean]("isCoordinate"))
      assert(lat.isNullAt(lat.fieldIndex("noDataValue")))
    }

    it("should return empty globalAttributes for test.nc") {
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .selectExpr("size(globalAttributes) as attrCount")
        .first()
      assertEquals(0, row.getAs[Int]("attrCount"))
    }

    it("should cross-validate extent against RS_FromNetCDF") {
      val metaRow = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .selectExpr(
          "width",
          "height",
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()

      val rasterMeta = sparkSession.read
        .format("binaryFile")
        .load(singleFileLocation)
        .selectExpr("RS_FromNetCDF(content, 'O3') as raster")
        .selectExpr("RS_Metadata(raster) as metadata")
        .first()
        .getStruct(0)
      val rasterMetaSeq = metadataStructToSeq(rasterMeta)

      // RS_Metadata: (upperLeftX, upperLeftY, gridWidth, gridHeight, scaleX, scaleY, ...)
      assertEquals(rasterMetaSeq(0), metaRow.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(rasterMetaSeq(1), metaRow.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(rasterMetaSeq(2), metaRow.getAs[Int]("width").toDouble, 1e-6)
      assertEquals(rasterMetaSeq(3), metaRow.getAs[Int]("height").toDouble, 1e-6)
      assertEquals(rasterMetaSeq(4), metaRow.getAs[Double]("scaleX"), 1e-6)
      assertEquals(rasterMetaSeq(5), metaRow.getAs[Double]("scaleY"), 1e-6)
    }

    it("should read files via glob pattern") {
      val df = sparkSession.read.format("netcdf.metadata").load(netcdfDir + "*.nc")
      assertEquals(1L, df.count())
    }

    it("should read files from directory with trailing slash") {
      val df = sparkSession.read.format("netcdf.metadata").load(netcdfDir)
      assertEquals(1L, df.count())
    }

    it("should read files from directory without trailing slash") {
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(netcdfDir.stripSuffix("/"))
      assertEquals(1L, df.count())
    }

    it("should support LIMIT pushdown across multiple files") {
      // Populate a directory with three files so the pushed-limit truncation branch (files in a
      // partition exceeding the remaining limit) actually runs.
      val multiDir = new File(tempDir, "limit")
      multiDir.mkdirs()
      for (i <- 1 to 3) {
        FileUtils.copyFile(new File(singleFileLocation), new File(multiDir, s"copy_$i.nc"))
      }
      val df =
        sparkSession.read.format("netcdf.metadata").load(multiDir.getAbsolutePath).limit(2)
      assertEquals(2L, df.count())
    }

    it("should support column pruning") {
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .select("path", "width", "height")
      assertEquals(3, df.schema.fieldNames.length)
      val row = df.first()
      assertEquals(80, row.getAs[Int]("width"))
      assertEquals(48, row.getAs[Int]("height"))
    }

    it("should serve cheap-only projections without opening the file") {
      // Oracle: a file of pure garbage bytes cannot be parsed as NetCDF. A cheap-only
      // projection (path/driver/fileSize) must still succeed because the file is never opened,
      // while selecting a header field must fail because it forces a parse.
      val corrupt = new File(tempDir, "corrupt.nc")
      FileUtils.writeByteArrayToFile(corrupt, Array.fill[Byte](2048)(0x7f))

      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(corrupt.getAbsolutePath)
        .select("path", "driver", "fileSize")
        .first()
      assert(row.getAs[String]("path").endsWith("corrupt.nc"))
      assertEquals("NetCDF", row.getAs[String]("driver"))
      assertEquals(2048L, row.getAs[Long]("fileSize"))

      // Requesting a header field opens the file, which must fail on garbage bytes
      intercept[Exception] {
        sparkSession.read
          .format("netcdf.metadata")
          .load(corrupt.getAbsolutePath)
          .select("format")
          .collect()
      }
    }

    it("should read a NetCDF-4 (HDF5) file including unsigned attributes") {
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_nc4.nc4")
      val row = df.first()
      assertEquals("NetCDF-4", row.getAs[String]("format"))
      assertEquals(5, row.getAs[Int]("width"))
      assertEquals(4, row.getAs[Int]("height"))

      // Regular grid: lat 10..13, lon 100..104, spacing 1.0
      val gt = df
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()
      assertEquals(99.5, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(13.5, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(1.0, gt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-1.0, gt.getAs[Double]("scaleY"), 1e-6)

      val temp = df
        .selectExpr("explode(variables) as v")
        .where("v.name = 'temperature'")
        .selectExpr(
          "v.dataType",
          "v.shape",
          "v.units",
          "v.standardName",
          "v.noDataValue",
          "v.attributes")
        .first()
      assertEquals("float", temp.getAs[String]("dataType"))
      assertEquals(Seq(4, 5), temp.getAs[Seq[Int]]("shape"))
      assertEquals("K", temp.getAs[String]("units"))
      assertEquals("air_temperature", temp.getAs[String]("standardName"))
      // _FillValue = -999
      assertEquals(-999.0, temp.getAs[Double]("noDataValue"), 1e-6)
      // valid_max is an unsigned byte 250; it must surface as 250, not -6
      assertEquals("250", temp.getAs[Map[String, String]]("attributes")("valid_max"))
    }

    it("should resolve CRS and SRID from a grid_mapping variable") {
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_crs.nc")
        .select("srid", "crs")
        .first()
      assertEquals(4326, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs") != null)
      assert(row.getAs[String]("crs").contains("WGS 84"))
    }

    it("should null geoTransform for an irregular grid but still report cornerCoordinates") {
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_irregular.nc")
      val row = df.first()
      // Irregular spacing -> no faithful affine transform
      assert(row.isNullAt(row.fieldIndex("geoTransform")))
      // width/height still come from dimension lengths
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(4, row.getAs[Int]("height"))
      // cornerCoordinates cover the coordinate centers only (lon 0..20, lat 0..10)
      val corners = df
        .selectExpr(
          "cornerCoordinates.minX",
          "cornerCoordinates.minY",
          "cornerCoordinates.maxX",
          "cornerCoordinates.maxY")
        .first()
      assertEquals(0.0, corners.getAs[Double]("minX"), 1e-6)
      assertEquals(0.0, corners.getAs[Double]("minY"), 1e-6)
      assertEquals(20.0, corners.getAs[Double]("maxX"), 1e-6)
      assertEquals(10.0, corners.getAs[Double]("maxY"), 1e-6)
    }

    it("should null grid fields for a file with no gridded variable") {
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_novar.nc")
        .first()
      assert(row.isNullAt(row.fieldIndex("width")))
      assert(row.isNullAt(row.fieldIndex("height")))
      assert(row.isNullAt(row.fieldIndex("geoTransform")))
      assert(row.isNullAt(row.fieldIndex("cornerCoordinates")))
      // Dimensions and variables are still populated
      val counts = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_novar.nc")
        .selectExpr("size(dimensions) as dimCount", "size(variables) as varCount")
        .first()
      assertEquals(1, counts.getAs[Int]("dimCount"))
      assertEquals(1, counts.getAs[Int]("varCount"))
    }
  }

  private def metadataStructToSeq(struct: Row): Seq[Double] = {
    (0 until struct.length).map { k =>
      struct(k) match {
        case value: Int => value.toDouble
        case value: Double => value
      }
    }
  }
}
