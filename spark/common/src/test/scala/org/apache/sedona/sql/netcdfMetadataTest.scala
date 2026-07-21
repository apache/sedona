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
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.sedona_sql.io.netcdfmetadata.{NetCdfMetadataPartitionReader, NetCdfMetadataScan}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.Assert.assertEquals
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files
import java.util.Collections

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

    it("should hash scans from every field used by equality") {
      val scan = sparkSession.read
        .format("netcdf.metadata")
        .load(singleFileLocation)
        .queryExecution
        .executedPlan
        .collectFirst { case exec: BatchScanExec =>
          exec.scan.asInstanceOf[NetCdfMetadataScan]
        }
        .getOrElse(fail("NetCDF metadata query did not contain a BatchScanExec"))

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

    it("should skip CF bounds variables when selecting the grid variable") {
      // lat_bnds(lat, nv) and lon_bnds(lon, nv) are rank-2 and declared BEFORE temp(lat, lon);
      // grid selection must skip them and anchor the grid on temp.
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_bounds.nc")
      val row = df.first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      val gt = df
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()
      // lat 10..12, lon 20..23, spacing 1.0
      assertEquals(19.5, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(12.5, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(1.0, gt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-1.0, gt.getAs[Double]("scaleY"), 1e-6)
    }

    it("should decode packed and unsigned coordinate variables") {
      // lat: short, scale_factor=0.5, add_offset=40 -> 40, 40.5, 41
      // lon: byte with _Unsigned="true" (raw 100, 150, 200), scale_factor=0.1 -> 10, 15, 20
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_packed.nc")
      val gt = df
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()
      assertEquals(7.5, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(41.25, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(5.0, gt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-0.5, gt.getAs[Double]("scaleY"), 1e-6)
      val corners = df
        .selectExpr(
          "cornerCoordinates.minX",
          "cornerCoordinates.minY",
          "cornerCoordinates.maxX",
          "cornerCoordinates.maxY")
        .first()
      assertEquals(7.5, corners.getAs[Double]("minX"), 1e-6)
      assertEquals(39.75, corners.getAs[Double]("minY"), 1e-6)
      assertEquals(22.5, corners.getAs[Double]("maxX"), 1e-6)
      assertEquals(41.25, corners.getAs[Double]("maxY"), 1e-6)

      // The lon variable declares _FillValue = -1 (byte) with _Unsigned = "true":
      // the reported no-data value must be the unsigned reinterpretation, 255
      val lon = df
        .selectExpr("explode(variables) as v")
        .where("v.name = 'lon'")
        .selectExpr("v.noDataValue")
        .first()
      assertEquals(255.0, lon.getAs[Double]("noDataValue"), 1e-6)
    }

    it("should resolve coordinates within the data variable's group, not globally") {
      // Root group has lat(2)/lon(2); the data variable lives in /sub with lat(3)/lon(5).
      // Group-relative resolution must bind /sub coordinates (45..47, 100..104).
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_groups.nc4")
      val row = df.first()
      assertEquals(5, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      // The grid mapping is referenced with an absolute path (temp:grid_mapping = "/crs"),
      // which must resolve to the root crs variable
      assertEquals(4326, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("WGS 84"))
      val gt = df
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()
      assertEquals(99.5, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(47.5, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(1.0, gt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-1.0, gt.getAs[Double]("scaleY"), 1e-6)
      // Nested-group dimensions are reported with their group prefix
      val dimNames = df
        .selectExpr("explode(dimensions) as d")
        .selectExpr("d.name", "d.length")
        .collect()
        .map(r => (r.getAs[String]("name"), r.getAs[Int]("length")))
      assert(dimNames.contains(("lat", 2)))
      assert(dimNames.contains(("sub/lat", 3)))
      assert(dimNames.contains(("sub/lon", 5)))
      // A nested variable's dimension list uses the same qualified names as the inventory,
      // so the two columns can be joined reliably
      val subTemp = df
        .selectExpr("explode(variables) as v")
        .where("v.name = 'sub/temp'")
        .selectExpr("v.dimensions")
        .first()
      assertEquals(Seq("sub/lat", "sub/lon"), subTemp.getAs[Seq[String]]("dimensions"))
    }

    it("should find coordinate variables laterally by dimension identity") {
      // Dimensions are declared at the root; the coordinate variables live in /coords and the
      // data variable in the sibling group /data. The lateral lookup must bind them because
      // they reference the very same root dimensions.
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_lateral.nc4")
      val row = df.first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      val gt = df
        .selectExpr("geoTransform.upperLeftX", "geoTransform.upperLeftY")
        .first()
      // lat 10..12, lon 20..23, spacing 1.0
      assertEquals(19.5, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(12.5, gt.getAs[Double]("upperLeftY"), 1e-6)
    }

    it("should search lateral coordinate variables width-wise from the local apex") {
      // The first branch contains coordinates two levels below /domain, while the later branch
      // contains coordinates only one level below it. The lateral lookup proceeds width-wise,
      // level by level, so the shallower coordinates win despite occurring later in file order.
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_lateral_width.nc4")
      val row = df.select("width", "height").first()
      assertEquals(3, row.getAs[Int]("width"))
      assertEquals(2, row.getAs[Int]("height"))
      val gt = df
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()
      // Shallow x = 1,2,3 and y = 10,20. The deeper decoys are x = 1000..3000, y = 100,200.
      assertEquals(0.5, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(25.0, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(1.0, gt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-10.0, gt.getAs[Double]("scaleY"), 1e-6)

      // The raster loader must use the same apex-bounded, width-wise coordinate lookup as the
      // metadata reader. A depth-first file scan would bind the deep decoys instead.
      val rasterMetadata = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_lateral_width.nc4")
        .selectExpr("RS_FromNetCDF(content, 'domain/data/temp') as raster")
        .selectExpr("RS_Metadata(raster) as metadata")
        .first()
        .getStruct(0)
      val rasterGt = metadataStructToSeq(rasterMetadata)
      assertEquals(0.5, rasterGt(0), 1e-6)
      assertEquals(25.0, rasterGt(1), 1e-6)
      assertEquals(1.0, rasterGt(4), 1e-6)
      assertEquals(-10.0, rasterGt(5), 1e-6)
    }

    it("should skip declared ancillary variables when selecting the grid variable") {
      // quality(lat, lon) is declared BEFORE temperature(lat, lon), but temperature declares
      // ancillary_variables = "quality", so temperature must be selected and its grid_mapping
      // (with crs_wkt) must drive the CRS.
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_ancillary.nc")
        .select("width", "height", "srid", "crs")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      assertEquals(4326, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("WGS 84"))
    }

    it("should load a nested variable through RS_FromNetCDF using the discovered full name") {
      // The documented workflow: discover the variable name with netcdf.metadata, pass it to
      // RS_FromNetCDF — including full names of variables in nested groups.
      val rasterRow = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_groups.nc4")
        .selectExpr("RS_FromNetCDF(content, 'sub/temp') as raster")
        .selectExpr("RS_Width(raster) as width", "RS_Height(raster) as height")
        .first()
      assertEquals(5, rasterRow.getAs[Int]("width"))
      assertEquals(3, rasterRow.getAs[Int]("height"))
    }

    it("should parse the extended grid_mapping form and identify WGS 84 by datum name") {
      // temp:grid_mapping = "crs: lat lon" (CF extended form); the crs variable declares
      // grid_mapping_name = latitude_longitude and horizontal_datum_name = "WGS84" (positive
      // identification), but no crs_wkt.
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_gridmapping_ext.nc")
        .select("srid", "crs")
        .first()
      assertEquals(4326, row.getAs[Int]("srid"))
      // The file carries no CRS WKT; crs carries the WKT derived from the grid mapping
      // parameters (GH-3121)
      assert(row.getAs[String]("crs").startsWith("GEOGCRS["))
    }

    it("should not infer an SRID from the latitude_longitude mapping name alone") {
      // The mapping name does not fix the datum: without a positive WGS 84 identification
      // by name, srid must stay null.
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_gridmapping_plain.nc")
        .select("srid", "crs")
        .first()
      assert(row.isNullAt(row.fieldIndex("srid")))
      assert(row.isNullAt(row.fieldIndex("crs")))
    }

    it("should not report EPSG:4326 for a non-WGS datum on the WGS 84 ellipsoid") {
      // The crs variable declares horizontal_datum_name = "Costa_Rica_2005" with WGS 84
      // ellipsoid parameters. The ellipsoid defines the Earth figure, not the datum, so
      // srid must stay null; crs reports the parameter-defined figure with its datum
      // unresolved, never an EPSG:4326 identity.
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_gridmapping_nonwgs.nc")
        .select("srid", "crs")
        .first()
      assert(row.isNullAt(row.fieldIndex("srid")))
      val crs = row.getAs[String]("crs")
      assert(crs.startsWith("GEOGCRS["))
      assert(!crs.contains("4326"))
    }

    it("should select the grid mapping matching the grid coordinates in the expanded form") {
      // temp:grid_mapping = "geographic: lat lon projected: x y" and temp's trailing dims are
      // (y, x) — the projected mapping (EPSG:3857) must be reported, never the geographic one.
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_multimapping.nc")
      val row = df.select("srid", "crs").first()
      assertEquals(3857, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("Pseudo-Mercator"))
      // The extent is computed from the projected metre coordinates
      val gt = df
        .selectExpr("geoTransform.upperLeftX", "geoTransform.upperLeftY", "geoTransform.scaleX")
        .first()
      assertEquals(250.0, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(3500.0, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(500.0, gt.getAs[Double]("scaleX"), 1e-6)
    }

    it("should not exclude a root variable shadowed by a path reference's basename") {
      // temperature:ancillary_variables = "/aux/temperature" references a rank-1 variable in
      // /aux; the root grid variable "temperature" shares only the basename and must remain
      // the selected grid (excluding by name would leave the unreferenced 2x2 /aux/grid).
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_pathref.nc4")
        .select("width", "height", "srid")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      assertEquals(4326, row.getAs[Int]("srid"))
    }

    it("should decode unsigned 64-bit coordinates above 2^63 as positive values") {
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_uint64.nc4")
      val gt = df
        .selectExpr("geoTransform.upperLeftX", "geoTransform.scaleX", "geoTransform.scaleY")
        .first()
      // lon = 2^63 + {0, 4096, 8192, 12288}: spacing 4096, all positive
      assertEquals(4096.0, gt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(-2048.0, gt.getAs[Double]("scaleY"), 1e-6)
      assertEquals(9.223372036854774e18, gt.getAs[Double]("upperLeftX"), 4096.0)
      assert(gt.getAs[Double]("upperLeftX") > 0)
      val corners = df
        .selectExpr("cornerCoordinates.minX", "cornerCoordinates.maxY")
        .first()
      assert(corners.getAs[Double]("minX") > 9.0e18)
      assert(corners.getAs[Double]("maxY") > 9.0e18)
    }

    it("should agree with RS_FromNetCDF on packed coordinate georeferencing") {
      // The loader must decode _Unsigned/scale_factor/add_offset the same way the metadata
      // reader does, or rasters would be silently mis-georeferenced.
      val metaGt = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_packed.nc")
        .selectExpr(
          "geoTransform.upperLeftX",
          "geoTransform.upperLeftY",
          "geoTransform.scaleX",
          "geoTransform.scaleY")
        .first()

      val rasterRow = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_packed.nc")
        .selectExpr("RS_FromNetCDF(content, 'temp') as raster")
        .selectExpr(
          "RS_Metadata(raster) as metadata",
          "RS_Value(raster, 0, 0, 1) as upperLeft",
          "RS_Value(raster, 2, 0, 1) as upperRight",
          "RS_Value(raster, 0, 2, 1) as lowerLeft",
          "RS_Value(raster, 2, 2, 1) as lowerRight",
          "RS_Value(raster, 1, 0, 1) as distinctMissing",
          "RS_Value(raster, 1, 1, 1) as centerNoData",
          "RS_Value(raster, 1, 2, 1) as validCollision",
          "RS_Count(raster, 1) as validCount",
          "RS_BandNoDataValue(raster, 1) as noData")
        .first()
      val rasterMeta = rasterRow.getStruct(rasterRow.fieldIndex("metadata"))
      val rasterMetaSeq = metadataStructToSeq(rasterMeta)

      assertEquals(rasterMetaSeq(0), metaGt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(rasterMetaSeq(1), metaGt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(rasterMetaSeq(4), metaGt.getAs[Double]("scaleX"), 1e-6)
      assertEquals(rasterMetaSeq(5), metaGt.getAs[Double]("scaleY"), 1e-6)
      // Latitude is stored in descending order while the raster transform is north-up. The
      // record values are packed independently of the coordinates and must be decoded with
      // temp's own scale/offset.
      assertEquals(10.5, rasterRow.getAs[Double]("upperLeft"), 1e-6)
      assertEquals(11.5, rasterRow.getAs[Double]("upperRight"), 1e-6)
      assertEquals(13.5, rasterRow.getAs[Double]("lowerLeft"), 1e-6)
      assertEquals(14.5, rasterRow.getAs[Double]("lowerRight"), 1e-6)
      // Both missing-value attributes are checked in packed storage units. A finite sentinel absent
      // from the decoded samples keeps a valid decoded value equal to a raw sentinel valid.
      assert(rasterRow.isNullAt(rasterRow.fieldIndex("distinctMissing")))
      assert(rasterRow.isNullAt(rasterRow.fieldIndex("centerNoData")))
      assertEquals(-999.0, rasterRow.getAs[Double]("validCollision"), 1e-6)
      assertEquals(7L, rasterRow.getAs[Long]("validCount"))
      // The synthetic finite sentinel remains observable to downstream no-data consumers.
      assert(java.lang.Double.isFinite(rasterRow.getAs[Double]("noData")))
    }

    it("should bind RS_FromNetCDF coordinates by dimension identity, not declaration order") {
      // /wrong/lat,lon (2x2, over /wrong's own dims) are declared before /coords/lat,lon
      // (which reference the root dims used by /data/temperature). The loader must bind the
      // /coords variables and produce a 4x3 raster, not a 2x2 one.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_lateral.nc4")
        .selectExpr("RS_FromNetCDF(content, 'data/temperature') as raster")
        .selectExpr(
          "RS_Width(raster) as width",
          "RS_Height(raster) as height",
          "RS_Metadata(raster) as metadata")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      // Extent from /coords: lon 20..23, lat 10..12
      assertEquals(19.5, row.getStruct(row.fieldIndex("metadata")).getDouble(0), 1e-6)
    }

    it("should validate RS_FromNetCDF coordinates against the requested axis") {
      // /data/lat(time) is a rank-1 numeric decoy over the record's time dimension, declared
      // in the data variable's own group; the true lat(lat)/lon(lon) live laterally in
      // /coords. Validation against the requested dimension must reject the decoy.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_axis.nc4")
        .selectExpr("RS_FromNetCDF(content, 'data/temp') as raster")
        .selectExpr(
          "RS_Width(raster) as width",
          "RS_Height(raster) as height",
          "RS_NumBands(raster) as numBands",
          "RS_Metadata(raster) as metadata")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      assertEquals(2, row.getAs[Int]("numBands"))
      // Extent from /coords: lon 20..23 with spacing 1 -> upperLeftX 19.5
      assertEquals(19.5, row.getStruct(row.fieldIndex("metadata")).getDouble(0), 1e-6)
    }

    it("should reject explicitly named coordinates over unrelated dimensions") {
      // wrong/lat and wrong/lon are attached to /wrong's own dimensions, not the record
      // variable's — path-resolved coordinates must be validated too, failing loudly instead
      // of silently producing a 2x2 raster.
      def messages(t: Throwable): Seq[String] =
        if (t == null) Nil else Option(t.getMessage).toSeq ++ messages(t.getCause)
      val err = intercept[Exception] {
        sparkSession.read
          .format("binaryFile")
          .load(variantsDir + "test_reader_lateral.nc4")
          .selectExpr("RS_FromNetCDF(content, 'data/temperature', 'wrong/lat', 'wrong/lon') as r")
          .collect()
      }
      assert(messages(err).exists(_.toLowerCase.contains("invalid")), s"unexpected error: $err")
    }

    it("should resolve expanded grid-mapping coordinates by identity, not basename") {
      // temp:grid_mapping = "geographic: /geo/x /geo/y projected: x y" — the geographic
      // clause's coordinates share the basenames x/y but resolve to /geo variables, while the
      // grid's actual coordinates are the root x/y. The projected mapping must be selected.
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_multimapping_paths.nc4")
      val row = df.select("srid", "crs").first()
      assertEquals(3857, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("Pseudo-Mercator"))
      val gt = df.selectExpr("geoTransform.scaleX").first()
      assertEquals(500.0, gt.getAs[Double]("scaleX"), 1e-6)
    }

    it("should recognize the WGS 84 ensemble datum name") {
      // EPSG:6326 is officially named "World Geodetic System 1984 ensemble"
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_gridmapping_ensemble.nc")
        .select("srid", "crs")
        .first()
      assertEquals(4326, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").startsWith("GEOGCRS["))
    }

    it("should validate explicitly named RS_FromNetCDF coordinates against their axes") {
      // Long form: the named lat coordinate must be attached to the record variable's Y
      // dimension. The decoy /data/lat(time) is rejected and the lateral /coords/lat(lat)
      // is bound instead, exactly as in the short form.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_axis.nc4")
        .selectExpr("RS_FromNetCDF(content, 'data/temp', 'lon', 'lat') as raster")
        .selectExpr(
          "RS_Width(raster) as width",
          "RS_Height(raster) as height",
          "RS_NumBands(raster) as numBands")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      assertEquals(2, row.getAs[Int]("numBands"))
    }

    it("should extract a permuted raster when a named coordinate serves another axis") {
      // Naming the time coordinate as the Y axis is the documented escape hatch: the axis is
      // derived from the coordinate's dimension (time), and the remaining lat dimension
      // becomes the band dimension.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_axis.nc4")
        .selectExpr("RS_FromNetCDF(content, 'data/temp', 'lon', 'time') as raster")
        .selectExpr(
          "RS_Width(raster) as width",
          "RS_Height(raster) as height",
          "RS_NumBands(raster) as numBands")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(2, row.getAs[Int]("height"))
      assertEquals(3, row.getAs[Int]("numBands"))
    }

    it("should extract non-trailing spatial axes with permuted strides and correct values") {
      // temp(lat, lon, time) with value = 100*lat_index + 10*lon_index + time_index; naming
      // lat/lon (dims 0 and 1) leaves time as the band dimension. Values verify the plane is
      // walked with the correct strides, not the trailing-plane arithmetic.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_permuted.nc")
        .selectExpr("RS_FromNetCDF(content, 'temp', 'lon', 'lat') as raster")
        .selectExpr(
          "RS_Width(raster) as width",
          "RS_Height(raster) as height",
          "RS_NumBands(raster) as numBands",
          "RS_Value(raster, 0, 0, 1) as upperLeftBand1",
          "RS_Value(raster, 0, 0, 2) as upperLeftBand2",
          "RS_Value(raster, 3, 0, 1) as upperRightBand1",
          "RS_Value(raster, 0, 2, 1) as lowerLeftBand1",
          "RS_Value(raster, 3, 2, 1) as lowerRightBand1",
          "RS_Metadata(raster) as metadata")
        .first()
      assertEquals(4, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      assertEquals(2, row.getAs[Int]("numBands"))
      // Latitude increases while longitude decreases in the file. The raster is normalized to
      // north-up, west-to-east order, and packed time coordinates must not scale the samples.
      assertEquals(230.0, row.getAs[Double]("upperLeftBand1"), 1e-6)
      assertEquals(231.0, row.getAs[Double]("upperLeftBand2"), 1e-6)
      assertEquals(200.0, row.getAs[Double]("upperRightBand1"), 1e-6)
      assertEquals(30.0, row.getAs[Double]("lowerLeftBand1"), 1e-6)
      assertEquals(0.0, row.getAs[Double]("lowerRightBand1"), 1e-6)
      // Extent from lon 20..23, lat 10..12
      assertEquals(19.5, row.getStruct(row.fieldIndex("metadata")).getDouble(0), 1e-6)
    }

    it("should resolve a repeated dimension to its trailing occurrence") {
      // temp(n, y, n): the X coordinate x(n) is attached to a dimension that occurs twice;
      // the trailing occurrence is the X axis and the leading one becomes the band dimension.
      // Value = 100*band_index + 10*y_index + x_index.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_repeated.nc")
        .selectExpr("RS_FromNetCDF(content, 'temp', 'x', 'y') as raster")
        .selectExpr(
          "RS_Width(raster) as width",
          "RS_Height(raster) as height",
          "RS_NumBands(raster) as numBands",
          "RS_Value(raster, 0, 0, 1) as upperLeftBand1",
          "RS_Value(raster, 0, 0, 2) as upperLeftBand2",
          "RS_Value(raster, 1, 2, 1) as lowerRightBand1")
        .first()
      assertEquals(2, row.getAs[Int]("width"))
      assertEquals(3, row.getAs[Int]("height"))
      assertEquals(2, row.getAs[Int]("numBands"))
      // Upper-left = max y (index 2), first x (index 0)
      assertEquals(20.0, row.getAs[Double]("upperLeftBand1"), 1e-6)
      assertEquals(120.0, row.getAs[Double]("upperLeftBand2"), 1e-6)
      // Bottom-right = min y (index 0), last x (index 1)
      assertEquals(1.0, row.getAs[Double]("lowerRightBand1"), 1e-6)
    }

    it("should not label an x/y extent with a mapping scoped to auxiliary coordinates") {
      // temp(y, x) has 1-D metre coordinates x/y and 2-D auxiliary lat/lon listed in
      // `coordinates`; grid_mapping = "geographic: lat lon" applies only to the auxiliary
      // coordinates, so no CRS may be attached to the metre extent.
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_auxcrs.nc")
      val row = df.select("srid", "crs").first()
      assert(row.isNullAt(row.fieldIndex("srid")))
      assert(row.isNullAt(row.fieldIndex("crs")))
      // The extent itself is still reported, just unlabeled
      val gt = df
        .selectExpr("geoTransform.upperLeftX", "geoTransform.upperLeftY", "geoTransform.scaleX")
        .first()
      assertEquals(250.0, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(3500.0, gt.getAs[Double]("upperLeftY"), 1e-6)
      assertEquals(500.0, gt.getAs[Double]("scaleX"), 1e-6)
    }

    it("should match expanded grid-mapping clauses to lateral sibling-group coordinates") {
      // The grid coordinates /coords/x,/coords/y are found laterally by dimension identity;
      // the clause references "x y" (plain names, not visible in the data variable's scope)
      // must match them through the same CF-aware resolution.
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_multimapping_lateral.nc4")
      val row = df.select("srid", "crs").first()
      assertEquals(3857, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("Pseudo-Mercator"))
      val gt = df.selectExpr("geoTransform.scaleX").first()
      assertEquals(500.0, gt.getAs[Double]("scaleX"), 1e-6)
    }

    it("should skip incompatible shadows when matching expanded grid-mapping coordinates") {
      // /data/y(time) has the same name as the grid's Y dimension but cannot serve that axis.
      // Both extent and clause matching must skip it and bind the lateral /coords/y(y).
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_multimapping_shadow.nc4")
      val row = df
        .selectExpr("srid", "geoTransform.upperLeftX", "geoTransform.upperLeftY")
        .first()
      assertEquals(4326, row.getAs[Int]("srid"))
      assertEquals(19.5, row.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(11.5, row.getAs[Double]("upperLeftY"), 1e-6)
    }

    it("should stop expanded grid-mapping coordinate lookup at the local apex") {
      // /domain declares the grid's x/y dimensions and /domain/coords contains their NUG
      // coordinate variables. Unrelated root x/y coordinates are above the local apex and must
      // not shadow the lateral coordinates referenced by the plain "projected: x y" clause.
      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_multimapping_apex.nc4")
      val row = df.select("srid", "crs").first()
      assertEquals(3857, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("Pseudo-Mercator"))
      val gt = df.selectExpr("geoTransform.upperLeftX", "geoTransform.scaleX").first()
      assertEquals(250.0, gt.getAs[Double]("upperLeftX"), 1e-6)
      assertEquals(500.0, gt.getAs[Double]("scaleX"), 1e-6)
    }

    it("should translate CF grid mapping parameters to a CRS (GH-3121)") {
      // The crs variable defines the CRS in the canonical CF form: grid_mapping_name =
      // lambert_conformal_conic plus projection parameters, no crs_wkt.
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_cf_lcc.nc")
        .select("srid", "crs")
        .first()
      val crs = row.getAs[String]("crs")
      assert(crs.startsWith("PROJCRS["))
      assert(crs.contains("Lambert Conformal Conic"))
      assert(crs.contains("\"Latitude of 1st standard parallel\",33"))
      assert(crs.contains("\"Latitude of 2nd standard parallel\",45"))
      // A parameter-defined LCC has no EPSG identity to derive
      assert(row.isNullAt(row.fieldIndex("srid")))
    }

    it("should derive the EPSG code for a CDM universal_transverse_mercator mapping") {
      // netCDF-Java convention: utm_zone_number = 33 (positive = northern hemisphere)
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_cf_utm.nc")
        .select("srid", "crs")
        .first()
      assertEquals(32633, row.getAs[Int]("srid"))
      assert(row.getAs[String]("crs").contains("Transverse Mercator"))
    }

    it("should convert kilometre false origins when translating a CF grid mapping") {
      // x/y units are km, so CF's false_easting/false_northing (2000) are kilometres;
      // the derived CRS must carry them in its declared unit, not misread them as metres.
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_cf_ps_km.nc")
        .select("srid", "crs")
        .first()
      val crs = row.getAs[String]("crs")
      assert(crs.contains("Polar Stereographic"))
      assert(crs.contains("\"False easting\",2000.0,LENGTHUNIT[\"kilometre\",1000.0]"))
      assert(row.isNullAt(row.fieldIndex("srid")))
    }

    it("should return neither derived CRS nor SRID when WKT serialization fails") {
      var sridEvaluated = false
      val (crs, srid) = NetCdfMetadataPartitionReader.derivedCrs(
        throw new IllegalArgumentException("WKT serialization failed"), {
          sridEvaluated = true
          Some(4326)
        })

      assert(crs == null)
      assert(srid.isEmpty)
      assert(!sridEvaluated)
    }

    it("should veto WGS 84 inference on a contradicting ellipsoid name") {
      // horizontal_datum_name = WGS84 but reference_ellipsoid_name = GRS_1980
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_gm_badellipsoid.nc")
        .select("srid")
        .first()
      assert(row.isNullAt(row.fieldIndex("srid")))
    }

    it("should veto WGS 84 inference on a non-Greenwich prime meridian name") {
      // geographic_crs_name = WGS 84 but prime_meridian_name = Paris
      val row = sparkSession.read
        .format("netcdf.metadata")
        .load(variantsDir + "test_gm_badpm.nc")
        .select("srid")
        .first()
      assert(row.isNullAt(row.fieldIndex("srid")))
    }

    it("should resolve RS_FromNetCDF band dimensions in the record variable's scope") {
      // Root declares time(1) with a coordinate variable; /sub/temp(time, lat, lon) uses
      // /sub/time(2). Binding the root variable would fail on the second band.
      val row = sparkSession.read
        .format("binaryFile")
        .load(variantsDir + "test_reader_bands.nc4")
        .selectExpr("RS_FromNetCDF(content, 'sub/temp') as raster")
        .selectExpr("RS_NumBands(raster) as numBands")
        .first()
      assertEquals(2, row.getAs[Int]("numBands"))
    }

    it("should not emit a geoTransform for locally uneven spacing near the best-fit line") {
      // lat = 0, 1.4, 2: steps of 1.4 and 0.6 around a mean of 1.0 — within half a pixel of
      // the best-fit line, but materially irregular, so no affine transform.
      val df = sparkSession.read.format("netcdf.metadata").load(variantsDir + "test_uneven.nc")
      val row = df.first()
      assert(row.isNullAt(row.fieldIndex("geoTransform")))
      val corners = df
        .selectExpr("cornerCoordinates.minY", "cornerCoordinates.maxY")
        .first()
      // Centers-only extent for the irregular axis pair
      assertEquals(0.0, corners.getAs[Double]("minY"), 1e-6)
      assertEquals(2.0, corners.getAs[Double]("maxY"), 1e-6)
    }

    it("should support Hive-style partition discovery when recursiveFileLookup is disabled") {
      val base = new File(tempDir, "partitioned")
      val d2020 = new File(base, "year=2020")
      val d2021 = new File(base, "year=2021")
      d2020.mkdirs()
      d2021.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(d2020, "a.nc"))
      FileUtils.copyFile(new File(singleFileLocation), new File(d2021, "b.nc"))

      // Force both files into a single bin-packed Spark partition; otherwise each file gets
      // its own partition and the per-file partition-value handling is never exercised
      withSqlConf(
        "spark.sql.files.openCostInBytes" -> "0",
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.maxPartitionBytes" -> "1073741824") {
        // Mixed-case option spelling: data source options are case-insensitive, so this must
        // suppress the lowercase recursiveFileLookup default just the same
        val df = sparkSession.read
          .format("netcdf.metadata")
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
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "a.nc"))
      val explicitFile = new File(tempDir, "explicit_netcdf_no_extension")
      FileUtils.copyFile(new File(singleFileLocation), explicitFile)

      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(dir.getAbsolutePath, explicitFile.getAbsolutePath)
      assertEquals(2L, df.count())
      // Both files must parse, including the extensionless explicit one
      assertEquals(2, df.select("format").collect().length)
    }

    it("should apply both a glob path and an explicit pathGlobFilter") {
      // Native Spark semantics: both constraints apply, so a*.nc restricted to b*.nc is empty.
      // The internal glob-to-filter rewrite must not discard either constraint.
      val dir = new File(tempDir, "globAndFilter")
      dir.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "a.nc"))
      FileUtils.copyFile(new File(singleFileLocation), new File(dir, "b.nc"))

      val both = sparkSession.read
        .format("netcdf.metadata")
        .option("pathGlobFilter", "b*.nc")
        .load(dir.getAbsolutePath + "/a*.nc")
      assertEquals(0L, both.count())

      // Sanity: without the explicit filter, the glob path alone matches a.nc
      val globOnly =
        sparkSession.read.format("netcdf.metadata").load(dir.getAbsolutePath + "/a*.nc")
      assertEquals(1L, globOnly.count())
    }

    it("should filter non-NetCDF files when loading multiple directories") {
      val dirA = new File(tempDir, "multiA")
      val dirB = new File(tempDir, "multiB")
      dirA.mkdirs()
      dirB.mkdirs()
      FileUtils.copyFile(new File(singleFileLocation), new File(dirA, "a.nc"))
      FileUtils.writeStringToFile(new File(dirA, "junk.txt"), "not netcdf", "UTF-8")
      FileUtils.copyFile(new File(singleFileLocation), new File(dirB, "b.nc"))

      val df = sparkSession.read
        .format("netcdf.metadata")
        .load(dirA.getAbsolutePath, dirB.getAbsolutePath)
      assertEquals(2L, df.count())
      // Force a parse of every listed file — junk.txt would fail here if not filtered out
      assertEquals(2, df.select("format").collect().length)
    }

    it("should fail catalog table creation with a clear error instead of an NPE") {
      sparkSession.sql("DROP TABLE IF EXISTS netcdf_meta_tbl")
      val err = intercept[Exception] {
        sparkSession.sql(
          s"CREATE TABLE netcdf_meta_tbl USING `netcdf.metadata` LOCATION '$singleFileLocation'")
      }
      try {
        def messages(t: Throwable): Seq[String] =
          if (t == null) Nil else Option(t.getMessage).toSeq ++ messages(t.getCause)
        assert(
          messages(err).exists(_.contains("does not support catalog table operations")),
          s"unexpected error: $err")
      } finally {
        sparkSession.sql("DROP TABLE IF EXISTS netcdf_meta_tbl")
      }
    }

    it("should reject catalog table creation with an explicit schema") {
      // With a user-supplied schema Spark skips FileFormat.inferSchema, so rejection relies on
      // the stub fallback format failing on instantiation. Depending on the Spark version the
      // error surfaces at CREATE or at first SELECT — either way it must be the clear message,
      // never an NPE or a silently unusable table.
      def messages(t: Throwable): Seq[String] =
        if (t == null) Nil else Option(t.getMessage).toSeq ++ messages(t.getCause)

      sparkSession.sql("DROP TABLE IF EXISTS netcdf_meta_schema_tbl")
      try {
        val err = intercept[Exception] {
          sparkSession.sql(
            "CREATE TABLE netcdf_meta_schema_tbl (path STRING) " +
              s"USING `netcdf.metadata` LOCATION '$singleFileLocation'")
          sparkSession.sql("SELECT * FROM netcdf_meta_schema_tbl").collect()
        }
        assert(
          messages(err).exists(_.contains("does not support catalog table operations")),
          s"unexpected error: $err")
      } finally {
        sparkSession.sql("DROP TABLE IF EXISTS netcdf_meta_schema_tbl")
      }
    }

    it("should stop on zero-progress channel writes instead of spinning") {
      val file = new File(tempDir, "raf_zero.bin")
      val payload = Array.tabulate[Byte](64 * 1024)(i => (i % 127).toByte)
      FileUtils.writeByteArrayToFile(file, payload)

      val raf = new org.apache.spark.sql.sedona_sql.io.netcdfmetadata.HadoopRandomAccessFile(
        new org.apache.hadoop.fs.Path(file.toURI),
        new org.apache.hadoop.conf.Configuration(),
        file.length(),
        file.lastModified())
      try {
        // Accepts 7 bytes for the first 3 calls, then permanently reports zero progress
        val stalling = new java.nio.channels.WritableByteChannel {
          private var calls = 0
          private var open = true
          override def isOpen: Boolean = open
          override def close(): Unit = { open = false }
          override def write(src: java.nio.ByteBuffer): Int = {
            calls += 1
            if (calls > 3) return 0
            val n = math.min(7, src.remaining())
            src.position(src.position() + n)
            n
          }
        }
        val copied = raf.readToByteChannel(stalling, 0, file.length())
        // Must report exactly the bytes the channel accepted, and must not hang
        assertEquals(21L, copied)
      } finally {
        raf.close()
      }
    }

    it("should drain channels that accept partial writes in HadoopRandomAccessFile") {
      val file = new File(tempDir, "raf.bin")
      val payload = Array.tabulate[Byte](100 * 1024)(i => (i % 251).toByte)
      FileUtils.writeByteArrayToFile(file, payload)

      val raf = new org.apache.spark.sql.sedona_sql.io.netcdfmetadata.HadoopRandomAccessFile(
        new org.apache.hadoop.fs.Path(file.toURI),
        new org.apache.hadoop.conf.Configuration(),
        file.length(),
        file.lastModified())
      try {
        val out = new java.io.ByteArrayOutputStream()
        // A channel that accepts at most 7 bytes per write call
        val trickle = new java.nio.channels.WritableByteChannel {
          private var open = true
          override def isOpen: Boolean = open
          override def close(): Unit = { open = false }
          override def write(src: java.nio.ByteBuffer): Int = {
            val n = math.min(7, src.remaining())
            val tmp = new Array[Byte](n)
            src.get(tmp)
            out.write(tmp)
            n
          }
        }
        val copied = raf.readToByteChannel(trickle, 0, file.length())
        assertEquals(file.length(), copied)
        assert(java.util.Arrays.equals(payload, out.toByteArray))
      } finally {
        raf.close()
      }
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
