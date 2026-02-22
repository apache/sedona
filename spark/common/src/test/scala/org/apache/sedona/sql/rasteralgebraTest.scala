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

import org.apache.sedona.common.raster.MapAlgebra
import org.apache.sedona.common.utils.RasterUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.geotools.coverage.grid.GridCoverage2D
import org.junit.Assert.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import java.awt.image.{DataBuffer, SinglePixelPackedSampleModel}
import java.io.File
import java.net.URLConnection
import scala.collection.mutable

class rasteralgebraTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  import sparkSession.implicits._

  describe("should pass all the arithmetic operations on bands") {
    it("Passed RS_Add") {
      var inputDf =
        Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(400.0, 900.0, 1400.0))).toDF("sumOfBands")
      inputDf = inputDf.selectExpr("RS_Add(Band1,Band2) as sumOfBands")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_Subtract") {
      var inputDf =
        Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(0.0, 100.0, 200.0))).toDF("differenceOfBands")
      inputDf = inputDf.selectExpr("RS_Subtract(Band1,Band2) as differenceOfBands")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_Multiply") {
      var inputDf =
        Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(40000.0, 200000.0, 480000.0))).toDF("MultiplicationOfBands")
      inputDf = inputDf.selectExpr("RS_Multiply(Band1,Band2) as multiplicationOfBands")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_Divide") {
      var inputDf = Seq(
        (Seq(200.0, 400.0, 600.0), Seq(200.0, 200.0, 500.0)),
        ((Seq(0.4, 0.26, 0.27), Seq(0.3, 0.32, 0.43)))).toDF("Band1", "Band2")
      val expectedList = List(List(1.0, 2.0, 1.2), List(1.33, 0.81, 0.63))
      val inputList = inputDf
        .selectExpr("RS_Divide(Band1,Band2) as divisionOfBands")
        .as[List[Double]]
        .collect()
        .toList
      val resultList = inputList zip expectedList
      for ((actual, expected) <- resultList) {
        assert(actual == expected)
      }

    }

    it("Passed RS_MultiplyFactor") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0))).toDF("Band")
      val expectedDF = Seq((Seq(600.0, 1200.0, 1800.0))).toDF("multiply")
      inputDf = inputDf.selectExpr("RS_MultiplyFactor(Band, 3) as multiply")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_MultiplyFactor with double factor") {
      val inputDf = Seq((Seq(200.0, 400.0, 600.0))).toDF("Band")
      val expectedDF = Seq((Seq(20.0, 40.0, 60.0))).toDF("multiply")
      val actualDF = inputDf.selectExpr("RS_MultiplyFactor(Band, 0.1) as multiply")
      assert(
        actualDF.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }
  }

  describe("Should pass basic statistical tests") {
    it("Passed RS_Mode") {
      val inputDf =
        Seq((Seq(200.0, 400.0, 600.0, 200.0)), (Seq(200.0, 400.0, 600.0, 700.0))).toDF("Band")
      val expectedResult = List(List(200.0), List(200.0, 400.0, 600.0, 700.0))
      val actualResult =
        inputDf.selectExpr("sort_array(RS_Mode(Band)) as mode").as[List[Double]].collect().toList
      val resultList = actualResult zip expectedResult
      for ((actual, expected) <- resultList) {
        assert(actual == expected)
      }

    }

    it("Passed RS_Mean") {
      var inputDf = Seq(
        (Seq(200.0, 400.0, 600.0, 200.0)),
        (Seq(200.0, 400.0, 600.0, 700.0)),
        (Seq(0.43, 0.36, 0.73, 0.56))).toDF("Band")
      val expectedList = List(350.0, 475.0, 0.52)
      val actualList = inputDf.selectExpr("RS_Mean(Band) as mean").as[Double].collect().toList
      val resultList = actualList zip expectedList
      for ((actual, expected) <- resultList) {
        assert(actual == expected)
      }
    }

    it("Passed RS_NormalizedDifference") {
      var inputDf =
        Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(0.0, 0.11, 0.14))).toDF("normalizedDifference")
      inputDf = inputDf.selectExpr("RS_NormalizedDifference(Band1,Band2) as normalizedDifference")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_CountValue") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0, 200.0, 600.0, 600.0, 800.0))).toDF("Band")
      val expectedDF = Seq(3).toDF("CountValue")
      inputDf = inputDf.selectExpr("RS_CountValue(Band, 600.0) as CountValue")
      assert(inputDf.first().getAs[Int](0) == expectedDF.first().getAs[Int](0))
    }
  }

  describe("Should pass operator tests") {
    it("Passed RS_GreaterThan") {
      var inputDf = Seq(
        (Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)),
        (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF =
        Seq((Seq(1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0)), (Seq(0.0, 0.0, 0.0, 0.0, 1.0, 0.0))).toDF(
          "GreaterThan")
      inputDf = inputDf.selectExpr("RS_GreaterThan(Band, 0.2) as GreaterThan")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_GreaterThanEqual") {
      var inputDf = Seq(
        (Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)),
        (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF =
        Seq((Seq(1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0)), (Seq(0.0, 0.0, 0.0, 0.0, 1.0, 0.0))).toDF(
          "GreaterThanEqual")
      inputDf = inputDf.selectExpr("RS_GreaterThanEqual(Band, 0.2) as GreaterThanEqual")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_LessThan") {
      var inputDf = Seq(
        (Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)),
        (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF =
        Seq((Seq(0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0)), (Seq(1.0, 1.0, 1.0, 0.0, 1.0))).toDF(
          "LessThan")
      inputDf = inputDf.selectExpr("RS_LessThan(Band, 0.2) as LessThan")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_LessThanEqual") {
      var inputDf = Seq(
        (Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)),
        (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF =
        Seq((Seq(0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0)), (Seq(1.0, 1.0, 1.0, 0.0, 1.0))).toDF(
          "LessThanEqual")
      inputDf = inputDf.selectExpr("RS_LessThanEqual(Band, 0.2) as LessthanEqual")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_Modulo") {
      var inputDf = Seq(
        (Seq(100.0, 260.0, 189.0, 106.0, 230.0, 169.0, 196.0)),
        (Seq(230.0, 345.0, 136.0, 106.0, 134.0, 105.0))).toDF("Band")
      val expectedDF = Seq(
        (Seq(10.0, 80.0, 9.0, 16.0, 50.0, 79.0, 16.0)),
        (Seq(50.0, 75.0, 46.0, 16.0, 44.0, 15.0))).toDF("Modulo")
      inputDf = inputDf.selectExpr("RS_Modulo(Band, 90.0) as Modulo")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_BitwiseAND") {
      var inputDf = Seq((Seq(10.0, 20.0, 30.0), Seq(10.0, 20.0, 30.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(10.0, 20.0, 30.0))).toDF("AND")
      inputDf = inputDf.selectExpr("RS_BitwiseAND(Band1, Band2) as AND")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_BitwiseOR") {
      var inputDf = Seq((Seq(10.0, 20.0, 30.0), Seq(40.0, 22.0, 62.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(42.0, 22.0, 62.0))).toDF("OR")
      inputDf = inputDf.selectExpr("RS_BitwiseOR(Band1, Band2) as OR")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_SquareRoot") {
      var inputDf = Seq((Seq(8.0, 16.0, 24.0))).toDF("Band")
      val expectedDF = Seq((Seq(2.83, 4.0, 4.90))).toDF("SquareRoot")
      inputDf = inputDf.selectExpr("RS_SquareRoot(Band) as SquareRoot")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_LogicalDifference") {
      var inputDf = Seq((Seq(10.0, 20.0, 30.0), Seq(40.0, 20.0, 50.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(10.0, 0.0, 30.0))).toDF("LogicalDifference")
      inputDf = inputDf.selectExpr("RS_LogicalDifference(Band1, Band2) as LogicalDifference")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_LogicalOver") {
      var inputDf = Seq((Seq(0.0, 0.0, 30.0), Seq(40.0, 20.0, 50.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(40.0, 20.0, 30.0))).toDF("LogicalOR")
      inputDf = inputDf.selectExpr("RS_LogicalOver(Band1, Band2) as LogicalOR")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_FetchRegion") {
      var inputDf =
        Seq((Seq(100.0, 260.0, 189.0, 106.0, 230.0, 169.0, 196.0, 200.0, 460.0))).toDF("Band")
      val expectedDF = Seq(Seq(100.0, 260.0, 189.0, 106.0, 230.0, 169.0)).toDF("Region")
      inputDf = inputDf.selectExpr("RS_FetchRegion(Band,Array(0, 0, 1, 2),Array(3, 3)) as Region")
      assert(
        inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF
          .first()
          .getAs[mutable.WrappedArray[Double]](0))
    }

    it("should pass RS_Normalize") {
      var df =
        Seq((Seq(800.0, 900.0, 0.0, 255.0)), (Seq(100.0, 200.0, 700.0, 900.0))).toDF("Band")
      df = df.selectExpr("RS_Normalize(Band) as normalizedBand")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0)(1) == 255)
    }

    it("should pass RS_NormalizeAll") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result1 = df.selectExpr("RS_NormalizeAll(raster) as normalized").first().get(0)
      val result2 = df.selectExpr("RS_NormalizeAll(raster, 0, 255) as normalized").first().get(0)
      val result3 =
        df.selectExpr("RS_NormalizeAll(raster, 0, 255, false) as normalized").first().get(0)
      val result4 =
        df.selectExpr("RS_NormalizeAll(raster, 0, 255, false, 255) as normalized").first().get(0)
      assert(result1.isInstanceOf[GridCoverage2D])
      assert(result2.isInstanceOf[GridCoverage2D])
      assert(result3.isInstanceOf[GridCoverage2D])
      assert(result4.isInstanceOf[GridCoverage2D])
    }

    it("should pass RS_SetPixelType") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")

      val df1 = df.selectExpr("RS_SetPixelType(raster, 'D') as modifiedRaster")
      val result1 = df1.selectExpr("RS_BandPixelType(modifiedRaster)").first().get(0).toString
      assert(result1 == "REAL_64BITS")

      val df2 = df.selectExpr("RS_SetPixelType(raster, 'F') as modifiedRaster")
      val result2 = df2.selectExpr("RS_BandPixelType(modifiedRaster)").first().get(0).toString
      assert(result2 == "REAL_32BITS")

      val df3 = df.selectExpr("RS_SetPixelType(raster, 'I') as modifiedRaster")
      val result3 = df3.selectExpr("RS_BandPixelType(modifiedRaster)").first().get(0).toString
      assert(result3 == "SIGNED_32BITS")

      val df4 = df.selectExpr("RS_SetPixelType(raster, 'S') as modifiedRaster")
      val result4 = df4.selectExpr("RS_BandPixelType(modifiedRaster)").first().get(0).toString
      assert(result4 == "SIGNED_16BITS")

      val df5 = df.selectExpr("RS_SetPixelType(raster, 'US') as modifiedRaster")
      val result5 = df5.selectExpr("RS_BandPixelType(modifiedRaster)").first().get(0).toString
      assert(result5 == "UNSIGNED_16BITS")

      val df6 = df.selectExpr("RS_SetPixelType(raster, 'B') as modifiedRaster")
      val result6 = df6.selectExpr("RS_BandPixelType(modifiedRaster)").first().get(0).toString
      assert(result6 == "UNSIGNED_8BITS")
    }

    it("should pass RS_Array") {
      val df = sparkSession.sql("SELECT RS_Array(6, 1e-6) as band")
      val result = df.first().getAs[mutable.WrappedArray[Double]](0)
      assert(result.length == 6)
      assert(result sameElements Array.fill[Double](6)(1e-6))
    }
  }

  describe("Should pass all raster function tests") {

    it("Passed RS_FromGeoTiff should handle null values") {
      val result = sparkSession.sql("select RS_FromGeoTiff(null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_FromGeoTiff from binary") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_FromGeoTiff(content)").first().get(0)
      assert(result != null)
      assert(result.isInstanceOf[GridCoverage2D])
    }

    it("Passed RS_FromArcInfoAsciiGrid should handle null values") {
      val result = sparkSession.sql("select RS_FromArcInfoAsciiGrid(null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_FromArcInfoAsciiGrid from binary") {
      val df =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster_asc/test1.asc")
      val result = df.selectExpr("RS_FromArcInfoAsciiGrid(content)").first().get(0)
      assert(result != null)
    }

    it("Passed RS_Envelope should handle null values") {
      val result = sparkSession.sql("select RS_Envelope(null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_Envelope with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_Envelope(RS_FromGeoTiff(content))").first().get(0)
      assert(result != null)
      assert(result.isInstanceOf[Geometry])
    }

    it("Passed RS_NumBands should handle null values") {
      val result = sparkSession.sql("select RS_NumBands(null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_NumBands with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_NumBands(RS_FromGeoTiff(content))").first().getInt(0)
      assert(result == 1)
    }

    it("Passed RS_Width with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_Width(RS_FromGeoTiff(content))").first().getInt(0)
      assertEquals(512, result)
    }

    it("Passed RS_Height with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_Height(RS_FromGeoTiff(content))").first().getInt(0)
      assertEquals(517, result)
    }

    it("Passed RS_SetSRID should handle null values") {
      val result = sparkSession.sql("select RS_SetSRID(null, 0)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_SetSRID with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val dfRaster = df.selectExpr(
        "RS_FromGeoTiff(content) as rast",
        "RS_SetSRID(RS_FromGeoTiff(content), 4326) as rast_4326")
      val dfResult = dfRaster.selectExpr(
        "RS_SRID(rast_4326) as srid_4326",
        "RS_Metadata(rast) as metadata",
        "RS_Metadata(rast_4326) as metadata_4326")
      val result = dfResult.first()
      assert(result.getInt(0) == 4326)

      // Assert SRID
      assert(result.getInt(0) == 4326)

      // Extract metadata and metadata_4326
      val metadata = result.getStruct(1)
      val metadata_4326 = result.getStruct(2)

      // Assert metadata schema
      val expectedSchema = StructType(
        Seq(
          StructField("upperLeftX", DoubleType, nullable = false),
          StructField("upperLeftY", DoubleType, nullable = false),
          StructField("gridWidth", IntegerType, nullable = false),
          StructField("gridHeight", IntegerType, nullable = false),
          StructField("scaleX", DoubleType, nullable = false),
          StructField("scaleY", DoubleType, nullable = false),
          StructField("skewX", DoubleType, nullable = false),
          StructField("skewY", DoubleType, nullable = false),
          StructField("srid", IntegerType, nullable = false),
          StructField("numSampleDimensions", IntegerType, nullable = false),
          StructField("tileWidth", IntegerType, nullable = false),
          StructField("tileHeight", IntegerType, nullable = false)))

      assert(dfResult.schema("metadata").dataType == expectedSchema)
      assert(dfResult.schema("metadata_4326").dataType == expectedSchema)

      // Assert metadata fields
      assert(metadata(0) == metadata_4326(0))
      assert(metadata(1) == metadata_4326(1))
      assert(metadata(2) == metadata_4326(2))
      assert(metadata(3) == metadata_4326(3))
      assert(metadata(4) == metadata_4326(4))
      assert(metadata(5) == metadata_4326(5))
      assert(metadata(6) == metadata_4326(6))
      assert(metadata(7) == metadata_4326(7))
      assert(metadata(9) == metadata_4326(9))
    }

    it("Passed RS_SetGeoReference should handle null values") {
      val result = sparkSession.sql("select RS_SetGeoReference(null, null)").first().get(0)
      assertNull(result)
    }

    it("Passed RS_SetGeoReference with raster") {
      val dfFile =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val df = dfFile.selectExpr("RS_FromGeoTiff(content) as raster")
      var actual = df
        .selectExpr("RS_GeoReference(RS_SetGeoReference(raster, '56 1 1 -56 23 34'))")
        .first()
        .getString(0)
      var expected = "56.000000 \n1.000000 \n1.000000 \n-56.000000 \n23.000000 \n34.000000"
      assertEquals(expected, actual)

      actual = df
        .selectExpr(
          "RS_GeoReference(RS_SetGeoReference(raster, '80 1 1 -82 -13095810 4021255', 'ESRI'))")
        .first()
        .getString(0)
      expected =
        "80.000000 \n1.000000 \n1.000000 \n-82.000000 \n-13095850.000000 \n4021296.000000"
      assertEquals(expected, actual)

      actual = df
        .selectExpr(
          "RS_GeoReference(RS_SetGeoReference(raster, -13095810, 4021255, 80, -82, 1, 1))")
        .first()
        .getString(0)
      expected =
        "80.000000 \n1.000000 \n1.000000 \n-82.000000 \n-13095810.000000 \n4021255.000000"
      assertEquals(expected, actual)
    }

    it("Passed RS_SetGeoReference with empty raster") {
      var df =
        sparkSession.sql("Select RS_MakeEmptyRaster(1, 20, 20, 2, 22, 2, 3, 1, 1, 0) as raster")
      var actual = df
        .selectExpr("RS_GeoReference(RS_SetGeoReference(raster, '3 1.5 1.5 2 22 3'))")
        .first()
        .getString(0)
      var expected = "3.000000 \n1.500000 \n1.500000 \n2.000000 \n22.000000 \n3.000000"
      assertEquals(expected, actual)

      actual = df
        .selectExpr("RS_GeoReference(RS_SetGeoReference(raster, '3 1.5 2.5 3 22 3', 'ESRI'))")
        .first()
        .getString(0)
      expected = "3.000000 \n1.500000 \n2.500000 \n3.000000 \n20.500000 \n1.500000"
      assertEquals(expected, actual)

      actual = df
        .selectExpr("RS_GeoReference(RS_SetGeoReference(raster, 22, 8, 3, 4, 2, 4))")
        .first()
        .getString(0)
      expected = "3.000000 \n4.000000 \n2.000000 \n4.000000 \n22.000000 \n8.000000"
      assertEquals(expected, actual)
    }

    it("Passed RS_Band") {
      val inputDf = Seq(
        Seq(16, 0, 24, 33, 43, 49, 64, 0, 76, 77, 79, 89, 0, 116, 118, 125, 135, 0, 157, 190, 215,
          229, 241, 248, 249)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 3, -215, 2, -2, 2, 2, 0), band, 1, 0d) as emptyRaster")
      val resultDf = df.selectExpr("RS_Band(emptyRaster, array(1,1,1)) as raster")
      val actual = resultDf.selectExpr("RS_NumBands(raster)").first().get(0)
      val expected = 3
      assertEquals(expected, actual)

      assertRSMetadata(
        df,
        "RS_Metadata(emptyRaster) as metadata",
        resultDf,
        "RS_Metadata(raster) as metadata")

      val actualBandValues = resultDf.selectExpr("RS_BandAsArray(raster, 1)").first().getSeq(0)
      val expectedBandValues = df.selectExpr("RS_BandAsArray(emptyRaster, 1)").first().getSeq(0)
      assertEquals(expectedBandValues.toString(), actualBandValues.toString())
    }

    it("Passed RS_Band with raster") {
      val dfFile = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      val df = dfFile.selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_Band(raster, array(1,2,2,2,1)) as resultRaster")
      val actual = resultDf.selectExpr("RS_NumBands(resultRaster)").first().getInt(0)
      val expected = 5
      assertEquals(expected, actual)

      assertRSMetadata(
        resultDf,
        "RS_Metadata(resultRaster) as metadata",
        df,
        "RS_Metadata(raster) as metadata")
    }

    it("Passed RS_Union") {
      val inputDf = Seq(
        (
          Seq(13, 80, 49, 15, 4, 46, 47, 94, 58, 37, 6, 22, 98, 26, 78, 66, 86, 79, 5, 65, 7, 12,
            89, 67),
          Seq(37, 4, 5, 15, 60, 83, 24, 19, 23, 87, 98, 89, 59, 71, 42, 46, 0, 80, 27, 73, 66,
            100, 78, 64),
          Seq(35, 68, 56, 87, 49, 20, 73, 90, 45, 96, 52, 98, 2, 82, 88, 74, 77, 60, 5, 61, 81,
            32, 9, 15))).toDF("band1", "band2", "band3")

      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_AddBandFromArray(RS_MakeEmptyRaster(2, 4, 6, 1, -1, 1, 1, 0, 0, 0), band1, 1), band2, 2) as raster1",
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 4, 6, 1, -1, 1, 1, 0, 0, 0), band3, 1) as raster2",
        "RS_AddBandFromArray(RS_AddBandFromArray(RS_MakeEmptyRaster(2, 4, 6, 1, -1, 1, 1, 0, 0, 0), band1, 1), band3, 2) as raster3")

      var actualDf = df.selectExpr("RS_Union(raster1, raster2) as actualRaster")

      var actualNumBands = actualDf.selectExpr("RS_NumBands(actualRaster)").first().getInt(0)
      var expectedNumBands = 3
      assertEquals(expectedNumBands, actualNumBands)

      var actualBandValues =
        actualDf.selectExpr("RS_BandAsArray(actualRaster,3)").first().getSeq(0)
      var expectedBandValues = df.selectExpr("RS_BandAsArray(raster2, 1)").first().getSeq(0)
      assertTrue(expectedBandValues.equals(actualBandValues))

      assertRSMetadata(
        actualDf,
        "RS_Metadata(actualRaster) as metadata",
        df,
        "RS_Metadata(raster1) as metadata")

      actualDf = df.selectExpr("RS_Union(raster1, raster2, raster3) as actualRaster")

      actualNumBands = actualDf.selectExpr("RS_NumBands(actualRaster)").first().getInt(0)
      expectedNumBands = 5
      assertEquals(expectedNumBands, actualNumBands)

      actualBandValues = actualDf.selectExpr("RS_BandAsArray(actualRaster,3)").first().getSeq(0)
      expectedBandValues = df.selectExpr("RS_BandAsArray(raster2, 1)").first().getSeq(0)
      assertTrue(expectedBandValues.equals(actualBandValues))
      actualBandValues = actualDf.selectExpr("RS_BandAsArray(actualRaster,4)").first().getSeq(0)
      expectedBandValues = df.selectExpr("RS_BandAsArray(raster3, 1)").first().getSeq(0)
      assertTrue(expectedBandValues.equals(actualBandValues))

      assertRSMetadata(
        actualDf,
        "RS_Metadata(actualRaster) as metadata",
        df,
        "RS_Metadata(raster1) as metadata")

      actualDf = df.selectExpr(
        "RS_Union(raster1, raster2, raster3, raster1, raster2, raster3, raster1) as actualRaster")

      actualNumBands = actualDf.selectExpr("RS_NumBands(actualRaster)").first().getInt(0)
      expectedNumBands = 12
      assertEquals(expectedNumBands, actualNumBands)

      actualBandValues = actualDf.selectExpr("RS_BandAsArray(actualRaster,11)").first().getSeq(0)
      expectedBandValues = df.selectExpr("RS_BandAsArray(raster1, 1)").first().getSeq(0)
      assertTrue(expectedBandValues.equals(actualBandValues))
      actualBandValues = actualDf.selectExpr("RS_BandAsArray(actualRaster,10)").first().getSeq(0)
      expectedBandValues = df.selectExpr("RS_BandAsArray(raster3, 2)").first().getSeq(0)
      assertTrue(expectedBandValues.equals(actualBandValues))

      assertRSMetadata(
        actualDf,
        "RS_Metadata(actualRaster) as metadata",
        df,
        "RS_Metadata(raster1) as metadata")
    }

    it("Passed RS_AddBand with empty raster") {
      val inputDf = Seq(
        (
          Seq(13, 80, 49, 15, 4, 46, 47, 94, 58, 37, 6, 22, 98, 26, 78, 66, 86, 79, 5, 65, 7, 12,
            89, 67),
          Seq(37, 4, 5, 15, 60, 83, 24, 19, 23, 87, 98, 89, 59, 71, 42, 46, 0, 80, 27, 73, 66,
            100, 78, 64),
          Seq(35, 68, 56, 87, 49, 20, 73, 90, 45, 96, 52, 98, 2, 82, 88, 74, 77, 60, 5, 61, 81,
            32, 9, 15))).toDF("band1", "band2", "band3")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_AddBandFromArray(RS_MakeEmptyRaster(2, 4, 6, 1, -1, 1, 1, 0, 0, 0), band1, 1), band2, 2) as toRaster",
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 4, 6, 1, -1, 1, 1, 0, 0, 0), band3, 1) as fromRaster")

      val actualDf = df.selectExpr("RS_AddBand(toRaster, fromRaster, 1, 3) as actualRaster")

      val actualNumBands = actualDf.selectExpr("RS_NumBands(actualRaster)").first().getInt(0)
      val expectedNumBands = 3
      assertEquals(expectedNumBands, actualNumBands)

      val actualBandValues =
        actualDf.selectExpr("RS_BandAsArray(actualRaster,3)").first().getSeq(0)
      val expectedBandValues = df.selectExpr("RS_BandAsArray(fromRaster, 1)").first().getSeq(0)
      assertTrue(expectedBandValues.equals(actualBandValues))

      assertRSMetadata(
        df,
        "RS_Metadata(toRaster) as metadata",
        actualDf,
        "RS_Metadata(actualRaster) as metadata")
    }

    it("Passed RS_SetValues with raster and implicit CRS transformation") {
      val dfFile = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      val df = dfFile.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "ST_GeomFromWKT('POLYGON ((-8682522.873537656 4572703.890837922, -8673439.664183248 4572993.532747675, -8673155.57366801 4563873.2099182755, -8701890.325907696 4562931.7093397, -8682522.873537656 4572703.890837922))', 3857) as geom")
      val resultDf = df.selectExpr("RS_SetValues(raster, 1, geom, 10, false, false) as result")

      var actual = resultDf
        .selectExpr("RS_Value(result, ST_GeomFromWKT('POINT (-77.9146 37.8916)'))")
        .first()
        .get(0)
      val expected = 10.0
      assertEquals(expected, actual)

      actual = resultDf
        .selectExpr("RS_Value(result, ST_GeomFromWKT('POINT (235749.0869 4200557.7397)', 26918))")
        .first()
        .get(0)
      assertEquals(expected, actual)
    }

    it("Passed RS_SetValues with empty raster") {
      var inputDf = Seq(
        (
          Seq(1, 1, 1, 0, 0, 0, 1, 2, 3, 3, 5, 6, 7, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0),
          Seq(11, 12, 13, 14, 15, 16, 17, 18, 19))).toDF("band", "newValues")
      var df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0), band, 1, 0d) as raster",
        "newValues")
      var actual = df
        .selectExpr("RS_BandAsArray(RS_SetValues(raster,  1, 2, 2, 3, 3, newValues, true), 1)")
        .first()
        .getSeq(0)
      var expected = Seq(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 11.0, 12.0, 13.0, 3.0, 5.0, 14.0, 15.0,
        0.0, 0.0, 3.0, 0.0, 0.0, 19.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assert(expected.equals(actual))

      actual = df
        .selectExpr("RS_BandAsArray(RS_SetValues(raster,  1, 2, 2, 3, 3, newValues), 1)")
        .first()
        .getSeq(0)
      expected = Seq(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 11.0, 12.0, 13.0, 3.0, 5.0, 14.0, 15.0, 16.0,
        0.0, 3.0, 17.0, 18.0, 19.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assert(expected.equals(actual))
    }

    it("Passed RS_SetValues with empty raster geom variant") {
      var inputDf =
        Seq((Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
          .toDF("band")
      inputDf = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 5, 5, 1, -1, 1, -1, 0, 0, 0), band, 1) as raster")
      var actual = inputDf
        .selectExpr(
          "RS_BandAsArray(RS_SetValues(raster, 1, ST_GeomFromWKT('LINESTRING(1 -1, 1 -4, 2 -2, 3 -3, 4 -4, 5 -4, 6 -6)'), 255), 1)")
        .first()
        .getSeq(0)
      var expected = Seq(255.0, 0.0, 0.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0, 255.0, 0.0,
        255.0, 0.0, 0.0, 255.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0, 0.0, 255.0)
      assert(expected.equals(actual))

      actual = inputDf
        .selectExpr(
          "RS_BandAsArray(RS_SetValues(raster, 1, ST_GeomFromWKT('POINT(2 -2)'), 25), 1)")
        .first()
        .getSeq(0)
      expected = Seq(25.0, 25.0, 0.0, 0.0, 0.0, 25.0, 25.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assert(expected.equals(actual))

      actual = inputDf
        .selectExpr(
          "RS_BandAsArray(RS_SetValues(raster, 1, ST_GeomFromWKT('MULTIPOINT((2 -2), (2 -1), (3 -3))'), 400), 1)")
        .first()
        .getSeq(0)
      expected = Seq(400.0, 400.0, 0.0, 0.0, 0.0, 400.0, 400.0, 400.0, 0.0, 0.0, 0.0, 400.0,
        400.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assert(expected.equals(actual))

      actual = inputDf
        .selectExpr(
          "RS_BandAsArray(RS_SetValues(raster, 1, ST_GeomFromWKT('POLYGON((1 -1, 3 -3, 6 -6, 4 -1, 1 -1))'), 150), 1)")
        .first()
        .getSeq(0)
      expected = Seq(150.0, 150.0, 150.0, 0.0, 0.0, 0.0, 150.0, 150.0, 150.0, 0.0, 0.0, 0.0,
        150.0, 150.0, 0.0, 0.0, 0.0, 0.0, 150.0, 0.0, 0.0, 0.0, 0.0, 0.0, 150.0)
      assert(expected.equals(actual))

      actual = inputDf
        .selectExpr(
          "RS_BandAsArray(RS_SetValues(raster, 1, ST_GeomFromWKT('POLYGON((1 -1, 3 -3, 6 -6, 4 -1, 1 -1))'), 150, true), 1)")
        .first()
        .getSeq(0)
      expected = Seq(150.0, 150.0, 150.0, 150.0, 0.0, 0.0, 150.0, 150.0, 150.0, 0.0, 0.0, 0.0,
        150.0, 150.0, 150.0, 0.0, 0.0, 0.0, 150.0, 150.0, 0.0, 0.0, 0.0, 0.0, 150.0)
      assert(expected.equals(actual))
    }

    it("Passed RS_SetValue with empty raster") {
      var inputDF =
        Seq((Seq(1, 1, 1, 0, 0, 0, 1, 2, 3, 3, 5, 6, 7, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0)))
          .toDF("band")
      var df = inputDF.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0), " +
          "band, 1, 0d) as raster")
      var actual =
        df.selectExpr("RS_BandAsArray(RS_SetValue(raster,  1, 2, 2, 255), 1)").first().getSeq(0)
      var expected = Seq(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 255.0, 2.0, 3.0, 3.0, 5.0, 6.0, 7.0, 0.0,
        0.0, 3.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assert(expected.equals(actual))

      actual =
        df.selectExpr("RS_BandAsArray(RS_SetValue(raster, 2, 2, 156), 1)").first().getSeq(0)
      expected = Seq(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 156.0, 2.0, 3.0, 3.0, 5.0, 6.0, 7.0, 0.0, 0.0,
        3.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assert(expected.equals(actual))
    }

    it("Passed RS_SetBandNoDataValue with raster") {
      val dfFile =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val df = dfFile.selectExpr("RS_FromGeoTiff(content) as raster")
      val actual = df
        .selectExpr("RS_BandNoDataValue(RS_SetBandNoDataValue(raster, 1, -999))")
        .first()
        .getDouble(0)
      val expected = -999
      assertEquals(expected, actual, 0.001d)

      val actualNull =
        df.selectExpr("RS_BandNoDataValue(RS_SetBandNoDataValue(raster, 1, null))").first().get(0)
      assertNull(actualNull)
    }

    it("Passed RS_SetBandNoDataValue with empty raster") {
      val df =
        sparkSession.sql("Select RS_MakeEmptyRaster(1, 20, 20, 2, 22, 2, 3, 1, 1, 0) as raster")
      val actual = df
        .selectExpr("RS_BandNoDataValue(RS_SetBandNoDataValue(raster, 1, -999.999))")
        .first()
        .getDouble(0)
      val expected = -999.999
      assertEquals(expected, actual, 0.001d)
    }

    it("Passed RS_SetBandNoDataValue with empty multiple band raster") {
      var df = sparkSession.sql(
        "Select RS_MakeEmptyRaster(2, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 0) as raster")
      df = df.selectExpr(
        "RS_SetBandNoDataValue(RS_SetBandNoDataValue(raster, -999.99), 2, 444) as raster")
      var actual = df.selectExpr("RS_BandNoDataValue(raster)").first().getDouble(0)
      var expected = -999.99
      assertEquals(expected, actual, 0.001d)

      actual = df.selectExpr("RS_BandNoDataValue(raster, 2)").first().getDouble(0)
      expected = 444
      assertEquals(expected, actual, 0.1d)
    }

    it("Passed RS_SRID should handle null values") {
      val result = sparkSession.sql("select RS_SRID(null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_SRID with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_SRID(RS_FromGeoTiff(content))").first().getInt(0)
      assert(result == 3857)
    }

    it("Passed RS_Value should handle null values") {
      val result = sparkSession.sql("select RS_Value(null, null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_Value with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df
        .selectExpr(
          "RS_Value(RS_FromGeoTiff(content), ST_SetSRID(ST_Point(-13077301.685, 4002565.802), 3857))")
        .first()
        .getDouble(0)
      assert(result == 255d)
    }

    it("Passed RS_Value with raster and implicit CRS transformation") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      val result = df
        .selectExpr(
          "RS_Value(RS_FromGeoTiff(content), ST_GeomFromWKT('POINT (-77.9146 37.8916)', 4326))")
        .first()
        .get(0)
      assert(result == 99d)
    }

    it("Passed RS_Value with raster and coordinates") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result =
        df.selectExpr("RS_Value(RS_FromGeoTiff(content), 4, 4, 1)").first().getDouble(0)
      assert(result == 123d)
    }

    it("Passed RS_Values should handle null values") {
      val result = sparkSession.sql("select RS_Values(null, null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_Values with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df
        .selectExpr(
          "RS_Values(RS_FromGeoTiff(content), array(ST_SetSRID(ST_Point(-13077301.685, 4002565.802), 3857), null))")
        .first()
        .getList[Any](0)
      assert(result.size() == 2)
      assert(result.get(0) == 255d)
      assert(result.get(1) == null)
    }

    it("Passed RS_Values with raster and serialized point array") {
      // https://issues.apache.org/jira/browse/SEDONA-266
      // A shuffle changes the internal type for the geometry array (point in this case) from GenericArrayData to UnsafeArrayData.
      // UnsafeArrayData.array() throws UnsupportedOperationException.
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val points = sparkSession
        .createDataFrame(Seq(("POINT (-13077301.685 4002565.802)", 1), ("POINT (0 0)", 2)))
        .toDF("point", "id")
        .withColumn("point", expr("ST_GeomFromText(point, 3857)"))
        .groupBy()
        .agg(collect_list("point").alias("point"))

      val result = df
        .crossJoin(points)
        .selectExpr("RS_Values(RS_FromGeoTiff(content), point, 1)")
        .first()
        .getList[Any](0)

      assert(result.size() == 2)
      assert(result.get(0) == 255d)
      assert(result.get(1) == null)
    }

    it("Passed RS_Values with raster, serialized point array and implicit CRS transformation") {
      // https://issues.apache.org/jira/browse/SEDONA-266
      // A shuffle changes the internal type for the geometry array (point in this case) from GenericArrayData to UnsafeArrayData.
      // UnsafeArrayData.array() throws UnsupportedOperationException.
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      val points = sparkSession
        .createDataFrame(Seq(("POINT (-77.9146 37.8916)", 1), ("POINT (-100 100)", 2)))
        .toDF("point", "id")
        .withColumn("point", expr("ST_GeomFromText(point, 4326)"))
        .groupBy()
        .agg(collect_list("point").alias("point"))

      val result = df
        .crossJoin(points)
        .selectExpr("RS_Values(RS_FromGeoTiff(content), point, 1)")
        .first()
        .getList[Any](0)

      assert(result.size() == 2)
      assert(result.get(0) == 99d)
      assert(result.get(1) == null)
    }

    it("Passed RS_Values with raster and Grid Coordinates") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df
        .selectExpr("RS_Values(RS_FromGeoTiff(content), array(1,2), array(3,2), 1)")
        .first()
        .getList[Double](0)

      assert(result.size() == 2)
      assert(result.get(0) == 132.0)
      assert(result.get(1) == 132.0)
    }

    it("Passed RS_Clip with implicit geometry transformation") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
        .selectExpr(
          "RS_FromGeoTiff(content) as raster",
          "ST_GeomFromWKT('POLYGON ((-8682522.873537656 4572703.890837922, -8673439.664183248 4572993.532747675, -8673155.57366801 4563873.2099182755, -8701890.325907696 4562931.7093397, -8682522.873537656 4572703.890837922))', 3857) as geom")
      val clippedDf = df.selectExpr("RS_Clip(raster, 1, geom, false, 200, false) as clipped")

      // Extract the metadata for the original raster
      val clippedMetadata = df
        .selectExpr("RS_Metadata(raster) as metadata")
        .first()
        .getStruct(0)

      // Extract the metadata for the clipped raster
      val originalMetadata = clippedDf
        .selectExpr("RS_Metadata(clipped) as metadata")
        .first()
        .getStruct(0)

      // Compare the metadata
      val clippedMetadataSeq = metadataStructToSeq(clippedMetadata).slice(0, 9)
      val originalMetadataSeq = metadataStructToSeq(originalMetadata).slice(0, 9)
      assert(
        clippedMetadataSeq == originalMetadataSeq,
        s"Expected: $originalMetadataSeq, but got: $clippedMetadataSeq")

      // Extract the RS_Values for the specified points
      val actualValues = clippedDf
        .selectExpr(
          "RS_Values(clipped, " +
            "Array(ST_GeomFromWKT('POINT(223802 4.21769e+06)', 26918),ST_GeomFromWKT('POINT(224759 4.20453e+06)', 26918)," +
            "ST_GeomFromWKT('POINT(237201 4.20429e+06)', 26918),ST_GeomFromWKT('POINT(237919 4.20357e+06)', 26918)," +
            "ST_GeomFromWKT('POINT(254668 4.21769e+06)', 26918)), 1)")
        .first()
        .get(0)
      var expectedValues = Seq(null, null, 0.0, 0.0, null)
      assertTrue(expectedValues.equals(actualValues))
    }

    it("Passed RS_Clip with raster") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
        .selectExpr(
          "RS_FromGeoTiff(content) as raster",
          "ST_GeomFromWKT('POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 221170 4197590, 236722 4204770))', 26918) as geom")
      val clippedDf = df.selectExpr("RS_Clip(raster, 1, geom, false, 200, false) as clipped")

      val clippedMetadata =
        df.selectExpr("RS_Metadata(raster)").first().getStruct(0).toSeq.slice(0, 9)
      val originalMetadata =
        clippedDf.selectExpr("RS_Metadata(clipped)").first().getStruct(0).toSeq.slice(0, 9)
      assertTrue(originalMetadata.equals(clippedMetadata))

      var actualValues = clippedDf
        .selectExpr(
          "RS_Values(clipped, " +
            "Array(ST_GeomFromWKT('POINT(223802 4.21769e+06)', 26918),ST_GeomFromWKT('POINT(224759 4.20453e+06)', 26918)," +
            "ST_GeomFromWKT('POINT(237201 4.20429e+06)', 26918),ST_GeomFromWKT('POINT(237919 4.20357e+06)', 26918)," +
            "ST_GeomFromWKT('POINT(254668 4.21769e+06)', 26918)), 1)")
        .first()
        .get(0)
      var expectedValues = Seq(null, null, 0.0, 0.0, null)
      assertTrue(expectedValues.equals(actualValues))

      val croppedDf = df.selectExpr("RS_Clip(raster, 1, geom, false, 200, false) as cropped")
      actualValues = croppedDf
        .selectExpr(
          "RS_Values(cropped, " +
            "Array(ST_GeomFromWKT('POINT(236842 4.20465e+06)', 26918),ST_GeomFromWKT('POINT(236961 4.20453e+06)', 26918)," +
            "ST_GeomFromWKT('POINT(237201 4.20429e+06)', 26918),ST_GeomFromWKT('POINT(237919 4.20357e+06)', 26918)," +
            "ST_GeomFromWKT('POINT(223802 4.20465e+06)', 26918)), 1)")
        .first()
        .get(0)
      expectedValues = Seq(0.0, 0.0, 0.0, 0.0, null)
      assertTrue(expectedValues.equals(actualValues))

      // Test with a polygon that does not intersect the raster in lenient mode
      val actual = df
        .selectExpr(
          "RS_Clip(raster, 1, ST_GeomFromWKT('POLYGON((274157 4174899,263510 4174947,269859 4183348,274157 4174899))', 26918))")
        .first()
        .get(0)
      assertNull(actual)
    }

    it("Passed RS_AsGeoTiff") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val resultRaw = df.selectExpr("RS_FromGeoTiff(content) as raster").first().get(0)
      val resultLoaded = df
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsGeoTiff(raster) as geotiff")
        .selectExpr("RS_FromGeoTiff(geotiff) as raster_new")
        .first()
        .get(0)
      assert(resultLoaded != null)
      assert(resultLoaded.isInstanceOf[GridCoverage2D])
      assertEquals(
        resultRaw.asInstanceOf[GridCoverage2D].getEnvelope.toString,
        resultLoaded.asInstanceOf[GridCoverage2D].getEnvelope.toString)
    }

    it("Passed RS_AsGeoTiff with different compression types") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val resultRaw = df.selectExpr("RS_FromGeoTiff(content) as raster").first().get(0)
      val resultLoadedDf = df
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .withColumn("geotiff", expr("RS_AsGeoTiff(raster, 'LZW', 1)"))
        .withColumn("geotiff2", expr("RS_AsGeoTiff(raster, 'Deflate', 0.5)"))
        .withColumn("raster_new", expr("RS_FromGeoTiff(geotiff)"))
      val resultLoaded = resultLoadedDf.first().getAs[GridCoverage2D]("raster_new")
      val writtenBinary1 = resultLoadedDf.first().getAs[Array[Byte]]("geotiff")
      val writtenBinary2 = resultLoadedDf.first().getAs[Array[Byte]]("geotiff2")
      assertEquals(
        resultRaw.asInstanceOf[GridCoverage2D].getEnvelope.toString,
        resultLoaded.getEnvelope.toString)
      assert(writtenBinary1.length > writtenBinary2.length)
    }

    it("Passed RS_AsBase64") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      val resultRaw =
        df.selectExpr("RS_AsBase64(RS_FromGeoTiff(content)) as raster").first().getString(0)
      assert(
        resultRaw.startsWith(
          "iVBORw0KGgoAAAANSUhEUgAABaAAAALQCAMAAABR+ye1AAADAFBMVEXE9/W48vOq7PGa5u6L3"))
    }

    it("Passed RS_AsCOG with defaults") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val rasterDf = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val original = rasterDf.first().getAs[GridCoverage2D]("raster")

      // Round-trip: GeoTiff -> raster -> COG bytes -> raster
      val roundTripped = rasterDf
        .selectExpr("RS_AsCOG(raster) as cog")
        .selectExpr("RS_FromGeoTiff(cog) as raster_new")
        .first()
        .getAs[GridCoverage2D]("raster_new")
      assert(roundTripped != null)
      assertEquals(original.getEnvelope.toString, roundTripped.getEnvelope.toString)
      assertEquals(original.getRenderedImage.getWidth, roundTripped.getRenderedImage.getWidth)
      assertEquals(original.getRenderedImage.getHeight, roundTripped.getRenderedImage.getHeight)
    }

    it("Passed RS_AsCOG round-trip with compression") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val rasterDf = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val original = rasterDf.first().getAs[GridCoverage2D]("raster")

      val roundTripped = rasterDf
        .selectExpr("RS_AsCOG(raster, 'LZW', 256) as cog")
        .selectExpr("RS_FromGeoTiff(cog) as raster_new")
        .first()
        .getAs[GridCoverage2D]("raster_new")
      assert(roundTripped != null)
      assertEquals(original.getEnvelope.toString, roundTripped.getEnvelope.toString)
      assertEquals(original.getRenderedImage.getWidth, roundTripped.getRenderedImage.getWidth)
      assertEquals(original.getRenderedImage.getHeight, roundTripped.getRenderedImage.getHeight)
    }

    it("Passed RS_AsCOG with compression") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val resultDf = df
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr(
          "RS_AsCOG(raster, 'LZW') as cog_lzw",
          "RS_AsCOG(raster, 'Deflate') as cog_deflate")
      val row = resultDf.first()
      val cogLzw = row.getAs[Array[Byte]]("cog_lzw")
      val cogDeflate = row.getAs[Array[Byte]]("cog_deflate")
      assert(cogLzw.length > 0)
      assert(cogDeflate.length > 0)
      assert(cogLzw.length != cogDeflate.length)
    }

    it("Passed RS_AsCOG with compression and tileSize") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val cogBytes = df
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsCOG(raster, 'Deflate', 512) as cog")
        .first()
        .getAs[Array[Byte]]("cog")
      assert(cogBytes != null)
      assert(cogBytes.length > 0)
    }

    it("Passed RS_AsCOG with all arguments") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val cogBytes = df
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsCOG(raster, 'LZW', 256, 0.5, 'Nearest', 2) as cog")
        .first()
        .getAs[Array[Byte]]("cog")
      assert(cogBytes != null)
      assert(cogBytes.length > 0)
    }

    it("Passed RS_AsCOG case-insensitive args") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val cogBytes = df
        .selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsCOG(raster, 'lzw', 256, 0.5, 'BILINEAR', 2) as cog")
        .first()
        .getAs[Array[Byte]]("cog")
      assert(cogBytes != null)
      assert(cogBytes.length > 0)
    }

    it("Passed RS_AsArcGrid") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster_asc/*")
      val resultRaw = df.selectExpr("RS_FromArcInfoAsciiGrid(content) as raster").first().get(0)
      val resultLoaded = df
        .selectExpr("RS_FromArcInfoAsciiGrid(content) as raster")
        .selectExpr("RS_AsArcGrid(raster) as arcgrid")
        .selectExpr("RS_FromArcInfoAsciiGrid(arcgrid) as raster_new")
        .first()
        .get(0)
      assert(resultLoaded != null)
      assert(resultLoaded.isInstanceOf[GridCoverage2D])
      assertEquals(
        resultRaw.asInstanceOf[GridCoverage2D].getEnvelope.toString,
        resultLoaded.asInstanceOf[GridCoverage2D].getEnvelope.toString)
    }

    it("Passed RS_AsPNG") {
      val dirPath = System.getProperty("user.dir") + "/target/testAsPNGFunction/"
      new File(dirPath).mkdirs()
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      df = df.selectExpr("RS_AsPNG(RS_FromGeoTiff(content)) as raster")
      df.write
        .format("raster")
        .option("rasterField", "raster")
        .option("fileExtension", ".png")
        .mode(SaveMode.Overwrite)
        .save(dirPath)
      val f = new File(dirPath + "part-*/*.png")
      val mimeType = URLConnection.guessContentTypeFromName(f.getName)
      assertEquals("image/png", mimeType)
    }

    it("Passed RS_AsArcGrid with different bands") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/*")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val rasterDf =
        df.selectExpr("RS_AsArcGrid(raster, 0) as arc", "RS_AsArcGrid(raster, 1) as arc2")
      val binaryDf = rasterDf.selectExpr(
        "RS_FromArcInfoAsciiGrid(arc) as raster",
        "RS_FromArcInfoAsciiGrid(arc2) as raster2")
      assertEquals(rasterDf.count(), binaryDf.count())
    }

    it("Passed RS_UpperLeftX") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_UpperLeftX(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = -1.3095817809482181e7
      assertEquals(expected, result, 1e-12)
    }

    it("Passed RS_UpperLeftY") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_UpperLeftY(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = 4021262.7487925636
      assertEquals(expected, result, 1e-8)
    }

    it("Passed RS_GeoReference") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      var result = df.selectExpr("RS_GeoReference(RS_FromGeoTiff(content))").first().getString(0)
      var expected: String =
        "72.328613 \n0.000000 \n0.000000 \n-72.328613 \n-13095817.809482 \n4021262.748793"
      assertEquals(expected, result)

      result =
        df.selectExpr("RS_GeoReference(RS_FromGeoTiff(content), 'ESRI')").first().getString(0)
      expected =
        "72.328613 \n0.000000 \n0.000000 \n-72.328613 \n-13095781.645176 \n4021226.584486"
      assertEquals(expected, result)
    }

    it("Passed RS_Rotation") {
      val df =
        sparkSession.sql("SELECT RS_MakeEmptyRaster(2, 10, 15, 1, 2, 1, -2, 1, 2, 0) as raster")
      val actual = df.selectExpr("RS_Rotation(raster)").first().get(0)
      val expected = -1.1071487177940904
      assertEquals(expected, actual)
    }

    it("Passed RS_GeoTransform") {
      val df =
        sparkSession.sql("SELECT RS_MakeEmptyRaster(2, 10, 15, 1, 2, 1, -2, 1, 2, 0) as raster")
      val actual = df.selectExpr("RS_GeoTransform(raster)").first().getStruct(0).toSeq.slice(0, 6)
      val expected = mutable.ArraySeq(2.23606797749979, 2.23606797749979, -1.1071487177940904,
        -2.214297435588181, 1.0, 2.0)
      assertTrue(expected.equals(actual))
    }

    it("Passed RS_SkewX") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_SkewX(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = 0.0
      assertEquals(expected, result, 0.1)
    }

    it("Passed RS_SkewY") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_SkewY(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = 0.0
      assertEquals(expected, result, 0.1)
    }

    it("Passed RS_Metadata") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")

      // Extract the metadata struct
      val result =
        df.selectExpr("RS_Metadata(RS_FromGeoTiff(content)) as metadata").first().getStruct(0)

      assert(result.length == 12)
      assert(result(2) == 512)
      assert(result(3) == 517)
      assert(result(9) == 1)
      assert(result(10) == 256)
      assert(result(11) == 256)
    }

    it("Passed RS_MakeEmptyRaster") {
      val widthInPixel = 10
      val heightInPixel = 10
      val upperLeftX = 0.0
      val upperLeftY = 0.0
      val cellSize = 1.0
      val numBands = 2
      // Test without skewX, skewY, srid and datatype
      var result = sparkSession
        .sql(
          s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize))")
        .first()
        .getStruct(0)
      assertEquals(numBands, result.getInt(9))

      // Test without skewX, skewY, srid
      result = sparkSession
        .sql(
          s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, 'I', $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize))")
        .first()
        .getStruct(0)
      assertEquals(numBands, result.getInt(9))

      // Test with integer type input
      result = sparkSession
        .sql(
          s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, ${upperLeftX.toInt}, ${upperLeftY.toInt}, ${cellSize.toInt}))")
        .first()
        .getStruct(0)
      assertEquals(numBands, result.getInt(9))

      // Test with skewX, skewY, srid but WITHOUT datatype
      val skewX = 0.0
      val skewY = 0.0
      val srid = 0
      result = sparkSession
        .sql(
          s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize, -$cellSize, $skewX, $skewY, $srid))")
        .first()
        .getStruct(0)
      assertEquals(numBands, result.getInt(9))

      // Test with skewX, skewY, srid and datatype
      result = sparkSession
        .sql(
          s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, 'I', $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize, -$cellSize, $skewX, $skewY, $srid))")
        .first()
        .getStruct(0)
      assertEquals(numBands, result.getInt(9))
    }

    it("Passed RS_MakeRaster") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test1.tiff")
        .withColumn("rast", expr("RS_FromGeoTiff(content)"))
      val metadata = df.selectExpr("RS_Metadata(rast)").first().getStruct(0).toSeq
      val width = metadata(2).asInstanceOf[Int]
      val height = metadata(3).asInstanceOf[Int]
      val values = Array.tabulate(width * height) { i => i * i }

      // Attach values as a new column to the dataframe
      val dfWithValues = df.withColumn("values", lit(values))

      // Call RS_MakeRaster to create a new raster with the values
      val result =
        dfWithValues.selectExpr("RS_MakeRaster(rast, 'D', values) AS rast").first().get(0)
      val rast = result.asInstanceOf[GridCoverage2D].getRenderedImage
      val r = RasterUtils.getRaster(rast)
      for (i <- values.indices) {
        assertEquals(values(i), r.getSampleDouble(i % width, i / width, 0), 0.001)
      }
    }

    it("Passed RS_MakeRasterForTesting") {
      val result = sparkSession
        .sql("SELECT RS_MakeRasterForTesting(4, 'I', 'SinglePixelPackedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        .first()
        .get(0)
      assert(result.isInstanceOf[GridCoverage2D])
      val gridCoverage2D = result.asInstanceOf[GridCoverage2D]
      assert(
        gridCoverage2D.getRenderedImage.getSampleModel.isInstanceOf[SinglePixelPackedSampleModel])
    }

    it("Passed RS_BandAsArray") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val metadata =
        df.selectExpr("RS_Metadata(RS_FromGeoTiff(content))").first().getStruct(0).toSeq
      val width = metadata(2).asInstanceOf[Int]
      val height = metadata(3).asInstanceOf[Int]
      val result = df.selectExpr("RS_BandAsArray(RS_FromGeoTiff(content), 1)").first().getSeq(0)
      assertEquals(width * height, result.length)
      val resultOutOfBound =
        df.selectExpr("RS_BandAsArray(RS_FromGeoTiff(content), 100)").first().isNullAt(0)
      assertEquals(true, resultOutOfBound)
    }

    it("Passed RS_AddBandFromArray - 3 parameters [default no data value]") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2) as band")
      val bandNewExpected: Seq[Double] = df.selectExpr("band").first().getSeq(0)
      val bandNewActual: Seq[Double] =
        df.selectExpr("RS_BandAsArray(RS_AddBandFromArray(raster, band, 1), 1)").first().getSeq(0)
      for (i <- bandNewExpected.indices) {
        // The band value needs to be mod 256 because the ColorModel will mod 256.
        assertEquals(bandNewExpected(i) % 256, bandNewActual(i), 0.001)
      }
      // Test with out of bound band index. It should fail.
      intercept[Exception] {
        df.selectExpr("RS_BandAsArray(RS_AddBandFromArray(raster, band, 100), 1)").collect()
      }
    }

    it("Passed RS_AddBandFromArray - adding a new band with 2 parameter convenience function") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2) as band")
      val bandNewExpected: Seq[Double] = df.selectExpr("band").first().getSeq(0)
      val bandNewActual: Seq[Double] =
        df.selectExpr("RS_BandAsArray(RS_AddBandFromArray(raster, band), 2)").first().getSeq(0)
      for (i <- bandNewExpected.indices) {
        // The band value needs to be mod 256 because the ColorModel will mod 256.
        assertEquals(bandNewExpected(i) % 256, bandNewActual(i), 0.001)
      }
    }

    it("Passed RS_AddBandFromArray - adding a new band with a custom no data value") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2) as band")
      val raster =
        df.selectExpr("RS_AddBandFromArray(raster, band, 2, 2)").first().getAs[GridCoverage2D](0)
      assertEquals(2, RasterUtils.getNoDataValue(raster.getSampleDimension(1)), 1e-9)
    }

    it("Passed RS_Intersects") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test1.tiff")
        .selectExpr("path", "RS_FromGeoTiff(content) as raster")

      // query window without SRID, will be assumed to be in WGS84
      assert(
        df.selectExpr("RS_Intersects(raster, ST_Point(-117.47993, 33.81798))")
          .first()
          .getBoolean(0))
      assert(
        !df
          .selectExpr("RS_Intersects(raster, ST_Point(-117.27868, 33.97896))")
          .first()
          .getBoolean(0))

      // query window and raster are in the same CRS
      assert(
        df.selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(-13067806,4009116), 3857))")
          .first()
          .getBoolean(0))
      assert(
        !df
          .selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(-13057457,4027023), 3857))")
          .first()
          .getBoolean(0))

      // query window and raster not in the same CRS
      assert(
        df.selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(-117.47993, 33.81798), 4326))")
          .first()
          .getBoolean(0))
      assert(
        !df
          .selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(-117.27868, 33.97896), 4326))")
          .first()
          .getBoolean(0))

      // geom-raster
      assert(
        df.selectExpr("RS_Intersects(ST_SetSRID(ST_Point(-117.47993, 33.81798), 4326), raster)")
          .first()
          .getBoolean(0))
      assert(
        !df
          .selectExpr("RS_Intersects(ST_SetSRID(ST_Point(-117.27868, 33.97896), 4326), raster)")
          .first()
          .getBoolean(0))

      // raster-raster
      assert(df.selectExpr("RS_Intersects(raster, raster)").first().getBoolean(0))
      assert(
        !df
          .selectExpr("RS_Intersects(raster, RS_MakeEmptyRaster(1, 10, 10, 0, 0, 1))")
          .first()
          .getBoolean(0))
      assert(
        df.selectExpr("RS_Intersects(raster, RS_MakeEmptyRaster(1, 10, 10, -118, 34, 1))")
          .first()
          .getBoolean(0))
    }

    it("Passed RS_AddBandFromArray collect generated raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "RS_BandAsArray(RS_FromGeoTiff(content), 1) as band")
      df = df.selectExpr("RS_AddBandFromArray(raster, band, 1) as raster", "band")
      var raster = df.collect().head.getAs[GridCoverage2D](0)
      assert(raster.getNumSampleDimensions == 1)
      df = df.selectExpr("RS_AddBandFromArray(raster, band, 2) as raster", "band")
      raster = df.collect().head.getAs[GridCoverage2D](0)
      assert(raster.getNumSampleDimensions == 2)
    }

    it("Passed RS_ScaleX with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_ScaleX(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = 72.32861272132695
      assertEquals(expected, result, 1e-9)
    }

    it("Passed RS_ScaleY with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_ScaleY(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = -72.32861272132695
      assertEquals(expected, result, 1e-9)
    }

    it("Passed RS_MinConvexHull") {
      val inputDf = Seq(
        (
          Seq(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
          Seq(0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0)))
        .toDF("values2", "values1")

      val minConvexHullAll = inputDf
        .selectExpr("ST_AsText(RS_MinConvexHull(RS_AddBandFromArray(" +
          "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), values1, 1, 0), values2, 2, 0))) as minConvexHullAll")
        .first()
        .getString(0)
      val minConvexHull1 = inputDf
        .selectExpr("ST_AsText(RS_MinConvexHull(RS_AddBandFromArray(" +
          "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), values1, 1, 0), values2, 2, 0), 1)) as minConvexHull1")
        .first()
        .getString(0)
      val expectedMinConvexHullAll = "POLYGON ((0 -1, 4 -1, 4 -5, 0 -5, 0 -1))"
      val expectedMinConvexHull1 = "POLYGON ((1 -1, 4 -1, 4 -5, 1 -5, 1 -1))"
      assertEquals(expectedMinConvexHull1, minConvexHull1)
      assertEquals(expectedMinConvexHullAll, minConvexHullAll)
    }

    it("Passed RS_ConvexHull with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result =
        df.selectExpr("RS_ConvexHull(RS_FromGeoTiff(content))").first().getAs[Geometry](0)
      val coordinates = result.getCoordinates

      val expectedCoordOne = new Coordinate(-13095817.809482181, 4021262.7487925636)
      val expectedCoordTwo = new Coordinate(-13058785.559768861, 4021262.7487925636)
      val expectedCoordThree = new Coordinate(-13058785.559768861, 3983868.8560156375)
      val expectedCoordFour = new Coordinate(-13095817.809482181, 3983868.8560156375)

      assertEquals(expectedCoordOne.getX, coordinates(0).getX, 0.5d)
      assertEquals(expectedCoordOne.getY, coordinates(0).getY, 0.5d)

      assertEquals(expectedCoordTwo.getX, coordinates(1).getX, 0.5d)
      assertEquals(expectedCoordTwo.getY, coordinates(1).getY, 0.5d)

      assertEquals(expectedCoordThree.getX, coordinates(2).getX, 0.5d)
      assertEquals(expectedCoordThree.getY, coordinates(2).getY, 0.5d)

      assertEquals(expectedCoordFour.getX, coordinates(3).getX, 0.5d)
      assertEquals(expectedCoordFour.getY, coordinates(3).getY, 0.5d)

      assertEquals(expectedCoordOne.getX, coordinates(4).getX, 0.5d)
      assertEquals(expectedCoordOne.getY, coordinates(4).getY, 0.5d)

    }

    it("Passed RS_Count with raster") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      var actual = df.selectExpr("RS_Count(raster, 1, false)").first().getLong(0)
      var expected = 1036800
      assertEquals(expected, actual)

      actual = df.selectExpr("RS_Count(raster, 1)").first().getLong(0)
      expected = 928192
      assertEquals(expected, actual)

      actual = df.selectExpr("RS_Count(raster)").first().getLong(0)
      expected = 928192
      assertEquals(expected, actual)
    }

    it("Passed RS_Union_Aggr") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test1.tiff")
        .union(
          sparkSession.read
            .format("binaryFile")
            .load(resourceFolder + "raster/test1.tiff")
            .union(
              sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")))
        .withColumn("raster", expr("RS_FromGeoTiff(content) as raster"))
        .withColumn("index", row_number().over(Window.orderBy("raster")))
        .selectExpr("raster", "index")

      val dfTest = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test1.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")

      df = df.selectExpr("RS_Union_aggr(raster, index) as rasters")

      val actualBands = df.selectExpr("RS_NumBands(rasters)").first().get(0)
      val expectedBands = 3
      assertEquals(expectedBands, actualBands)

      val actualMetadata =
        df.selectExpr("RS_Metadata(rasters)").first().getStruct(0).toSeq.slice(0, 9)
      val expectedMetadata =
        dfTest.selectExpr("RS_Metadata(raster)").first().getStruct(0).toSeq.slice(0, 9)
      assertTrue(expectedMetadata.equals(actualMetadata))
    }

    it("Passed multi-band RS_Union_Aggr") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test4.tiff")
        .withColumn("raster", expr("RS_FromGeoTiff(content)"))
        .withColumn("group", lit(1))
        .withColumn("index", lit(1))
        .select("raster", "group", "index")
        .union(
          sparkSession.read
            .format("binaryFile")
            .load(resourceFolder + "raster/test5.tiff")
            .withColumn("raster", expr("RS_FromGeoTiff(content)"))
            .withColumn("group", lit(1))
            .withColumn("index", lit(2))
            .select("raster", "group", "index"))
        .union(
          sparkSession.read
            .format("binaryFile")
            .load(resourceFolder + "raster/test4.tiff")
            .withColumn("raster", expr("RS_FromGeoTiff(content)"))
            .withColumn("group", lit(2))
            .withColumn("index", lit(1))
            .select("raster", "group", "index"))
        .union(
          sparkSession.read
            .format("binaryFile")
            .load(resourceFolder + "raster/test6.tiff")
            .withColumn("raster", expr("RS_FromGeoTiff(content)"))
            .withColumn("group", lit(2))
            .withColumn("index", lit(2))
            .select("raster", "group", "index"))
      df = df.withColumn("meta", expr("RS_MetaData(raster)"))
      df = df.withColumn("summary", expr("RS_SummaryStatsAll(raster)"))

      // Aggregate rasters based on their indexes to create two separate 2-banded rasters
      var aggregatedDF1 = df
        .groupBy("group")
        .agg(expr("RS_Union_Aggr(raster, index) as multi_band_raster"))
        .orderBy("group")
      aggregatedDF1 = aggregatedDF1.withColumn("meta", expr("RS_MetaData(multi_band_raster)"))
      aggregatedDF1 = aggregatedDF1
        .withColumn("summary1", expr("RS_SummaryStatsAll(multi_band_raster, 1)"))
        .withColumn("summary2", expr("RS_SummaryStatsAll(multi_band_raster, 2)"))

      // Aggregate rasters based on their group to create one 4-banded raster
      var aggregatedDF2 =
        aggregatedDF1.selectExpr("RS_Union_Aggr(multi_band_raster, group) as raster")
      aggregatedDF2 = aggregatedDF2.withColumn("meta", expr("RS_MetaData(raster)"))
      aggregatedDF2 = aggregatedDF2
        .withColumn("summary1", expr("RS_SummaryStatsALl(raster, 1)"))
        .withColumn("summary2", expr("RS_SummaryStatsALl(raster, 2)"))
        .withColumn("summary3", expr("RS_SummaryStatsALl(raster, 3)"))
        .withColumn("summary4", expr("RS_SummaryStatsALl(raster, 4)"))

      val rowsExpected = df.selectExpr("summary").collect()
      val rowsActual = df.selectExpr("summary").collect()

      val expectedMetadata = df.selectExpr("meta").first().getStruct(0).toSeq.slice(0, 9)
      val expectedSummary1 = rowsExpected(0).getStruct(0).toSeq.slice(0, 6)
      val expectedSummary2 = rowsExpected(1).getStruct(0).toSeq.slice(0, 6)
      val expectedSummary3 = rowsExpected(2).getStruct(0).toSeq.slice(0, 6)
      val expectedSummary4 = rowsExpected(3).getStruct(0).toSeq.slice(0, 6)

      val expectedNumBands = mutable.WrappedArray.make(Array(4.0))

      val actualMetadata = aggregatedDF2.selectExpr("meta").first().getStruct(0).toSeq.slice(0, 9)
      val actualNumBands =
        aggregatedDF2.selectExpr("meta").first().getStruct(0).toSeq.slice(9, 10)
      val actualSummary1 = rowsActual(0).getStruct(0).toSeq.slice(0, 6)
      val actualSummary2 = rowsActual(1).getStruct(0).toSeq.slice(0, 6)
      val actualSummary3 = rowsActual(2).getStruct(0).toSeq.slice(0, 6)
      val actualSummary4 = rowsActual(3).getStruct(0).toSeq.slice(0, 6)

      assertTrue(expectedMetadata.equals(actualMetadata))
      assertTrue(actualNumBands == expectedNumBands)
      assertTrue(expectedSummary1.equals(actualSummary1))
      assertTrue(expectedSummary2.equals(actualSummary2))
      assertTrue(expectedSummary3.equals(actualSummary3))
      assertTrue(expectedSummary4.equals(actualSummary4))
    }

    it("Passed RS_ZonalStats edge case") {
      val df = sparkSession.sql("""
          |with data as (
          | SELECT array(3, 7, 5, 40, 61, 70, 60, 80, 27, 55, 35, 44, 21, 36, 53, 54, 86, 28, 45, 24, 99, 22, 18, 98, 10) as pixels,
          |   ST_GeomFromWKT('POLYGON ((5.822754 -6.620957, 6.965332 -6.620957, 6.965332 -5.834616, 5.822754 -5.834616, 5.822754 -6.620957))', 4326) as geom
          |)
          |
          |SELECT RS_SetSRID(RS_AddBandFromArray(RS_MakeEmptyRaster(1, "D", 5, 5, 1, -1, 1), pixels, 1), 4326) as raster, geom FROM data
          |""".stripMargin)

      var actual = df.selectExpr("RS_ZonalStats(raster, geom, 1, 'mode')").first().get(0)
      assertNull(actual)

      val statsDf = df.selectExpr("RS_ZonalStatsAll(raster, geom) as stats")
      actual = statsDf.first().getStruct(0).toSeq.slice(0, 9)
      val expected = Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      assertTrue(expected.equals(actual))
    }

    it("Passed RS_ZonalStats") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "ST_GeomFromWKT('POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 221170 4197590, 236722 4204770))', 26918) as geom")
      var actual =
        df.selectExpr("RS_ZonalStats(raster, geom, 1, 'sum', false, true)").first().get(0)
      assertEquals(1.0795427e7, actual)

      actual =
        df.selectExpr("RS_ZonalStats(raster, geom, 1, 'count', false, false)").first().get(0)
      assertEquals(185104.0, actual)

      actual = df.selectExpr("RS_ZonalStats(raster, geom, 1, 'mean', true, false)").first().get(0)
      assertEquals(58.650240700685295, actual)

      actual = df.selectExpr("RS_ZonalStats(raster, geom, 1, 'variance')").first().get(0)
      assertEquals(8534.098251841822, actual)

      actual = df.selectExpr("RS_ZonalStats(raster, geom, 'sd')").first().get(0)
      assertEquals(92.3801832204387, actual)

      // Test with a polygon in EPSG:4326
      actual = df
        .selectExpr(
          "RS_ZonalStats(raster, ST_GeomFromWKT('POLYGON ((-77.96672569800863073 37.91971182746296876, -77.9688630154902711 37.89620133516485367, -77.93936803424354309 37.90517806858776595, -77.96672569800863073 37.91971182746296876))'), 1, 'mean', false)")
        .first()
        .get(0)
      assertNotNull(actual)

      // Test with a polygon that does not intersect the raster in lenient mode
      actual = df
        .selectExpr(
          "RS_ZonalStats(raster, ST_GeomFromWKT('POLYGON ((-78.22106647832458748 37.76411511479908967, -78.20183062098976734 37.72863564460374874, -78.18088490966962922 37.76753482276972562, -78.22106647832458748 37.76411511479908967))'), 1, 'mean', false)")
        .first()
        .get(0)
      assertNull(actual)
    }

    it("Passed RS_ZonalStats - Raster with no data") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "ST_GeomFromWKT('POLYGON((-167.750000 87.750000, -155.250000 87.750000, -155.250000 40.250000, -180.250000 40.250000, -167.750000 87.750000))', 4326) as geom")
      var actual =
        df.selectExpr("RS_ZonalStats(raster, geom, 1, 'sum', false, true)").first().get(0)
      assertEquals(3229013.0, actual)

      actual = df.selectExpr("RS_ZonalStats(raster, geom, 1, 'count', true, true)").first().get(0)
      assertEquals(14648.0, actual)

      actual =
        df.selectExpr("RS_ZonalStats(raster, geom, 1, 'mean', false, false)").first().get(0)
      assertEquals(226.54970883322787, actual)

      actual = df.selectExpr("RS_ZonalStats(raster, geom, 1, 'variance', false)").first().get(0)
      assertEquals(5596.966403503485, actual)

      actual = df.selectExpr("RS_ZonalStats(raster, geom, 'sd')").first().get(0)
      assertEquals(74.81287592054916, actual)
    }

    it("Passed RS_ZonalStatsAll") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "ST_GeomFromWKT('POLYGON ((-8673439.6642 4572993.5327, -8673155.5737 4563873.2099, -8701890.3259 4562931.7093, -8682522.8735 4572703.8908, -8673439.6642 4572993.5327))', 3857) as geom")
      val actual = df
        .selectExpr("RS_ZonalStatsAll(raster, geom, 1, false, true)")
        .first()
        .getStruct(0)
        .toSeq
        .slice(0, 9)
      val expected = Seq(185104.0, 1.0795427e7, 58.32087367104147, 0.0, 0.0, 92.3801832204387,
        8534.098251841822, 0.0, 255.0)
      assertTrue(expected.equals(actual))

      // Test with a polygon that does not intersect the raster in lenient mode
      val actual2 = df
        .selectExpr(
          "RS_ZonalStatsAll(raster, ST_GeomFromWKT('POLYGON ((-78.22106647832458748 37.76411511479908967, -78.20183062098976734 37.72863564460374874, -78.18088490966962922 37.76753482276972562, -78.22106647832458748 37.76411511479908967))'), 1, true, false)")
        .first()
        .getStruct(0)
      assertNull(actual2)
    }

    it("Passed RS_ZonalStatsAll - Raster with no data") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr(
        "RS_FromGeoTiff(content) as raster",
        "ST_GeomFromWKT('POLYGON((-167.750000 87.750000, -155.250000 87.750000, -155.250000 40.250000, -180.250000 40.250000, -167.750000 87.750000))', 0) as geom")
      val actual = df
        .selectExpr("RS_ZonalStatsAll(raster, geom, 1, false, true)")
        .first()
        .getStruct(0)
        .toSeq
        .slice(0, 9)
      val expected = Seq(14249.0, 3229013.0, 226.61330619692416, 255.0, 255.0, 74.81287592054916,
        5596.966403503485, 1.0, 255.0)
      assertTrue(expected.equals(actual))
    }

    it("Passed RS_SummaryStats with raster") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")

      var actual = df.selectExpr("RS_SummaryStats(raster, 'count')").first().getDouble(0)
      assertEquals(928192.0, actual, 0.1d)

      actual = df.selectExpr("RS_SummaryStats(raster, 'sum', 1)").first().getDouble(0)
      assertEquals(2.06233487e8, actual, 0.1d)

      actual = df.selectExpr("RS_SummaryStats(raster, 'mean', 1, false)").first().getDouble(0)
      assertEquals(198.91347125771605, actual, 0.1d)

      actual = df.selectExpr("RS_SummaryStats(raster, 'stddev', 1, false)").first().getDouble(0)
      assertEquals(95.09054096106192, actual, 0.1d)

      actual = df.selectExpr("RS_SummaryStats(raster, 'min', 1, false)").first().getDouble(0)
      assertEquals(0.0, actual, 0.1d)

      actual = df.selectExpr("RS_SummaryStats(raster, 'max', 1, false)").first().getDouble(0)
      assertEquals(255.0, actual, 0.1d)
    }

    it("Passed RS_SummaryStatsAll with raster") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      var actual = df.selectExpr("RS_SummaryStatsAll(raster, 1, false)").first().getStruct(0)
      assertEquals(1036800.0, actual.getDouble(0), 0.1d)
      assertEquals(2.06233487e8, actual.getDouble(1), 0.1d)
      assertEquals(198.91347125771605, actual.getDouble(2), 1e-6d)
      assertEquals(95.09054096106192, actual.getDouble(3), 1e-6d)
      assertEquals(0.0, actual.getDouble(4), 0.1d)
      assertEquals(255.0, actual.getDouble(5), 0.1d)

      actual = df.selectExpr("RS_SummaryStatsAll(raster, 1)").first().getStruct(0)
      assertEquals(928192.0, actual.getDouble(0), 0.1d)
      assertEquals(2.06233487e8, actual.getDouble(1), 0.1d)
      assertEquals(222.18839097945252, actual.getDouble(2), 1e-6d)
      assertEquals(70.20559521132097, actual.getDouble(3), 1e-6d)
      assertEquals(1.0, actual.getDouble(4), 0.1d)
      assertEquals(255.0, actual.getDouble(5), 0.1d)

      actual = df.selectExpr("RS_SummaryStatsAll(raster)").first().getStruct(0)
      assertEquals(928192.0, actual.getDouble(0), 0.1d)
      assertEquals(2.06233487e8, actual.getDouble(1), 0.1d)
      assertEquals(222.18839097945252, actual.getDouble(2), 1e-6d)
      assertEquals(70.20559521132097, actual.getDouble(3), 1e-6d)
      assertEquals(1.0, actual.getDouble(4), 0.1d)
      assertEquals(255.0, actual.getDouble(5), 0.1d)
    }

    it("Passed RS_BandIsNoData") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      assert(!df.selectExpr("RS_BandIsNoData(raster, 1)").first().getBoolean(0))
      val bandDf = Seq(Seq.fill(9)(10.0)).toDF("data")
      val noDataDf = bandDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 3, 3, 0, 0, 1), data, 1, 10d) as raster")
      assert(noDataDf.selectExpr("RS_BandIsNoData(raster, 1)").first().getBoolean(0))
    }

    it("Passed RS_PixelAsPoint with raster") {
      val widthInPixel = 5
      val heightInPixel = 10
      val upperLeftX = 123.19
      val upperLeftY = -12
      val cellSize = 4
      val numBands = 2
      val result = sparkSession
        .sql(
          s"SELECT RS_PixelAsPoint(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 2, 1)")
        .first()
        .getAs[Geometry](0)
      val expectedX = 127.19
      val expectedY = -12
      val actualCoordinates = result.getCoordinate
      assertEquals(expectedX, actualCoordinates.x, 1e-5)
      assertEquals(expectedY, actualCoordinates.y, 1e-5)
    }

    it("Passed RS_PixelAsPoints with empty raster") {
      val widthInPixel = 5
      val heightInPixel = 10
      val upperLeftX = 123.19
      val upperLeftY = -12
      val cellSize = 4
      val numBands = 2
      val result = sparkSession
        .sql(
          s"SELECT RS_PixelAsPoints(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 1)")
        .first()
        .getList(0)
      val expected = "[POINT (127.19 -12),0.0,2,1]"
      assertEquals(expected, result.get(1).toString)
    }

    it("Passed RS_PixelAsPoints with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")

      var result = df
        .selectExpr("explode(RS_PixelAsPoints(raster, 1)) as exploded")
        .selectExpr(
          "exploded.geom as geom",
          "exploded.value as value",
          "exploded.x as x",
          "exploded.y as y")

      assert(result.count() == 264704)
    }

    it("Passed RS_PixelAsPolygon with raster") {
      val widthInPixel = 5
      val heightInPixel = 10
      val upperLeftX = 123.19
      val upperLeftY = -12
      val cellSize = 4
      val numBands = 2
      val result = sparkSession
        .sql(
          s"SELECT ST_AsText(RS_PixelAsPolygon(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 2, 3))")
        .first()
        .getString(0)
      val expected =
        "POLYGON ((127.19 -20, 131.19 -20, 131.19 -24, 127.19 -24, 127.19 -20))"
      assertEquals(expected, result)
    }

    it("Passed RS_PixelAsPolygons with empty raster") {
      val widthInPixel = 5
      val heightInPixel = 10
      val upperLeftX = 123.19
      val upperLeftY = -12
      val cellSize = 4
      val numBands = 2
      val result = sparkSession
        .sql(
          s"SELECT RS_PixelAsPolygons(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 1)")
        .first()
        .getList(0)
      val expected =
        "[POLYGON ((127.19 -20, 131.19 -20, 131.19 -24, 127.19 -24, 127.19 -20)),0.0,2,3]"
      assertEquals(expected, result.get(11).toString)
    }

    it("Passed RS_PixelAsPolygons with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")

      var result = df
        .selectExpr("explode(RS_PixelAsPolygons(raster, 1)) as exploded")
        .selectExpr(
          "exploded.geom as geom",
          "exploded.value as value",
          "exploded.x as x",
          "exploded.y as y")

      assert(result.count() == 264704)
    }

    it("Passed RS_PixelAsCentroid with raster") {
      val widthInPixel = 12
      val heightInPixel = 13
      val upperLeftX = 240
      val upperLeftY = -193
      val cellSize = 9
      val numBands = 2
      val result = sparkSession
        .sql(
          s"SELECT ST_AsText(RS_PixelAsCentroid(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 2, 3))")
        .first()
        .getString(0)
      val expected = "POINT (253.5 -215.5)"
      assertEquals(expected, result)
    }

    it("Passed RS_PixelAsCentroids with empty raster") {
      val widthInPixel = 12
      val heightInPixel = 13
      val upperLeftX = 240
      val upperLeftY = -193
      val cellSize = 9
      val numBands = 2
      val result = sparkSession
        .sql(
          s"SELECT RS_PixelAsCentroids(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 1)")
        .first()
        .getList(0)
      val expected = "[POINT (253.5 -215.5),0.0,2,3]"
      assertEquals(expected, result.get(25).toString)
    }

    it("Passed RS_PixelAsCentroids with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")

      var result = df
        .selectExpr("explode(RS_PixelAsCentroids(raster, 1)) as exploded")
        .selectExpr(
          "exploded.geom as geom",
          "exploded.value as value",
          "exploded.x as x",
          "exploded.y as y")

      assert(result.count() == 264704)
    }

    it("Passed RS_RasterToWorldCoordX with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_RasterToWorldCoordX(raster, 1, 1)").first().getDouble(0)
      assertEquals(-13095817.809482181, result, 0.5d)
    }

    it("Passed RS_RasterToWorldCoordY with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_RasterToWorldCoordY(raster, 1, 1)").first().getDouble(0)
      assertEquals(4021262.7487925636, result, 0.5d)
    }

    it("Passed RS_RasterToWorldCoord with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result =
        df.selectExpr("RS_RasterToWorldCoord(raster, 1, 1)").first().get(0).asInstanceOf[Geometry]
      val actualCoordinate = result.getCoordinate
      assertEquals(-13095817.809482181, actualCoordinate.x, 0.5d)
      assertEquals(4021262.7487925636, actualCoordinate.y, 0.5d)
    }

    it("Passed RS_WorldToRasterCoord with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df
        .selectExpr(
          "ST_AsText(RS_WorldToRasterCoord(raster, -13095817.809482181, 4021262.7487925636))")
        .first()
        .getString(0)
      val expected = "POINT (1 1)"
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoordX with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df
        .selectExpr("RS_WorldToRasterCoordX(raster, -13095817.809482181, 4021262.7487925636)")
        .first()
        .getInt(0)
      val expected = 1
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoordY with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df
        .selectExpr("RS_WorldToRasterCoordY(raster, -13095817.809482181, 4021262.7487925636)")
        .first()
        .getInt(0)
      val expected = 1
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoord with raster - geom parameter") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val results = df
        .selectExpr(
          "ST_AsText(RS_WorldToRasterCoord(raster, ST_GeomFromText('POINT (-13095817.809482181 4021262.7487925636)', 3857)))",
          "ST_AsText(RS_WorldToRasterCoord(raster, ST_GeomFromText('POINT (-117.64173 33.943833)')))")
        .first()
      val expected = "POINT (1 1)"
      assertEquals(expected, results.getString(0))
      assertEquals(expected, results.getString(1))
    }

    it("Passed RS_WorldToRasterCoordX with raster - geom parameter") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val results = df
        .selectExpr(
          "RS_WorldToRasterCoordX(raster, ST_GeomFromText('POINT (-13095817.809482181 4021262.7487925636)', 3857))",
          "RS_WorldToRasterCoordX(raster, ST_GeomFromText('POINT (-117.64173 33.943833)'))")
        .first()
      val expected = 1
      assertEquals(expected, results.getInt(0))
      assertEquals(expected, results.getInt(1))
    }

    it("Passed RS_WorldToRasterCoordY with raster - geom parameter") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val results = df
        .selectExpr(
          "RS_WorldToRasterCoordY(raster, ST_GeomFromText('POINT (-13095817.809482181 4021262.7487925636)', 3857))",
          "RS_WorldToRasterCoordY(raster, ST_GeomFromText('POINT (-117.64173 33.943833)'))")
        .first()
      val expected = 1
      assertEquals(expected, results.getInt(0))
      assertEquals(expected, results.getInt(1))
    }

    it("Passed RS_Contains") {
      assert(
        sparkSession
          .sql("SELECT RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((5 5, 5 10, 10 10, 10 5, 5 5))'))")
          .first()
          .getBoolean(0))
      assert(
        !sparkSession
          .sql("SELECT RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((2 2, 2 25, 20 25, 20 2, 2 2))'))")
          .first()
          .getBoolean(0))
      assert(
        sparkSession
          .sql("SELECT RS_Contains(ST_GeomFromWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), RS_MakeEmptyRaster(1, 5, 5, 0, 5, 1))")
          .first()
          .getBoolean(0))
      assert(
        !sparkSession
          .sql("SELECT RS_Contains(ST_GeomFromWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), RS_MakeEmptyRaster(1, 20, 20, 0, 20, 1))")
          .first()
          .getBoolean(0))
      assert(
        sparkSession
          .sql("SELECT RS_Contains(RS_MakeEmptyRaster(1, 30, 30, 0, 30, 1), RS_MakeEmptyRaster(1, 20, 20, 0, 20, 1))")
          .first()
          .getBoolean(0))
      assert(
        !sparkSession
          .sql("SELECT RS_Contains(RS_MakeEmptyRaster(1, 10, 10, 0, 10, 1), RS_MakeEmptyRaster(1, 20, 20, 0, 20, 1))")
          .first()
          .getBoolean(0))
    }

    it("Passed RS_Within") {
      assert(
        sparkSession
          .sql("SELECT RS_WITHIN(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((0 0, 0 50, 100 50, 100 0, 0 0))'))")
          .first()
          .getBoolean(0))
      assert(
        !sparkSession
          .sql("SELECT RS_WITHIN(RS_MakeEmptyRaster(1, 100, 100, 0, 50, 1), ST_GeomFromWKT('POLYGON ((2 2, 2 25, 20 25, 20 2, 2 2))'))")
          .first()
          .getBoolean(0))
      assert(
        sparkSession
          .sql("SELECT RS_Within(ST_GeomFromWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), RS_MakeEmptyRaster(1, 20, 20, 0, 20, 1))")
          .first()
          .getBoolean(0))
      assert(
        !sparkSession
          .sql("SELECT RS_Within(ST_GeomFromWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'), RS_MakeEmptyRaster(1, 5, 5, 0, 5, 1))")
          .first()
          .getBoolean(0))
      assert(
        sparkSession
          .sql("SELECT RS_Within(RS_MakeEmptyRaster(1, 20, 20, 0, 20, 1), RS_MakeEmptyRaster(1, 30, 30, 0, 30, 1))")
          .first()
          .getBoolean(0))
      assert(
        !sparkSession
          .sql("SELECT RS_Within(RS_MakeEmptyRaster(1, 20, 20, 0, 20, 1), RS_MakeEmptyRaster(1, 10, 10, 0, 10, 1))")
          .first()
          .getBoolean(0))
    }

    it("Passed RS_BandNoDataValue - noDataValueFor for raster from geotiff - default band") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_BandNoDataValue(raster)").first().getDouble(0)
      assertEquals(0, result, 1e-9)
    }

    it("Passed RS_BandNoDataValue - null noDataValueFor for raster from geotiff") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_BandNoDataValue(raster)").first().get(0)
      assertNull(result)
    }

    it("Passed RS_BandNoDataValue - noDataValueFor for raster from geotiff - explicit band") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_BandNoDataValue(raster, 1)").first().getDouble(0)
      assertEquals(0, result, 1e-9)
    }

    it("Passed RS_BandPixelType from raster") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_BandPixelType(raster, 1)").first().getString(0)
      assertEquals("UNSIGNED_8BITS", result)
    }

    it("Passed RS_BandPixelType from raster - default band value") {
      var df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_BandPixelType(raster)").first().getString(0)
      assertEquals("UNSIGNED_8BITS", result)
    }

    it("Passed RS_Tile - in-db raster") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_Tile(raster, 100, 100) as tiles")
      val result = resultDf.first().get(0)
      assert(result.isInstanceOf[mutable.WrappedArray[GridCoverage2D]])
      val tiles = result.asInstanceOf[mutable.WrappedArray[GridCoverage2D]]
      assert(tiles.exists(tile =>
        tile.getRenderedImage.getWidth < 100 || tile.getRenderedImage.getHeight < 100))
      tiles.foreach { tile =>
        assert(tile.getRenderedImage.getData != null)
        assert(RasterUtils.getNoDataValue(tile.getSampleDimension(0)) == 0)
        tile.dispose(true)
      }
      val rsValuesDf = resultDf
        .selectExpr("explode(tiles) as tile")
        .selectExpr("RS_Value(tile, ST_Centroid(RS_Envelope(tile))) as value")
        .withColumn("is_non_null", expr("value is not null"))
      assert(rsValuesDf.count() == 120)
      assert((90 to 110) contains rsValuesDf.where("is_non_null").count())
    }

    it("Passed RS_Tile - in-db raster with padding") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_Tile(raster, null, 100, 100, true, 3) as tiles")
      val result = resultDf.first().get(0)
      assert(result.isInstanceOf[mutable.WrappedArray[GridCoverage2D]])
      val tiles = result.asInstanceOf[mutable.WrappedArray[GridCoverage2D]]
      assert(tiles.exists(tile => RasterUtils.getNoDataValue(tile.getSampleDimension(0)) == 3))
      tiles.foreach { tile =>
        assert(tile.getRenderedImage.getData != null)
        assert(tile.getRenderedImage.getWidth == 100 && tile.getRenderedImage.getHeight == 100)
        tile.dispose(true)
      }
    }

    it("Passed RS_Tile - in-db raster with band index") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_Tile(raster, array(2), 100, 100, false) as tiles")
      val result = resultDf.first().get(0)
      assert(result.isInstanceOf[mutable.WrappedArray[GridCoverage2D]])
      val tiles = result.asInstanceOf[mutable.WrappedArray[GridCoverage2D]]
      tiles.foreach { tile =>
        assert(tile.getRenderedImage.getData != null)
        assert(tile.getNumSampleDimensions == 1)
        tile.dispose(true)
      }
    }

    it("Passed RS_Tile - in-db raster with band indices") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_Tile(raster, array(2, 1), 100, 100) as tiles")
      val result = resultDf.first().get(0)
      assert(result.isInstanceOf[mutable.WrappedArray[GridCoverage2D]])
      val tiles = result.asInstanceOf[mutable.WrappedArray[GridCoverage2D]]
      tiles.foreach { tile =>
        assert(tile.getRenderedImage.getData != null)
        assert(tile.getNumSampleDimensions == 2)
        tile.dispose(true)
      }
    }

    it("Passed RS_TileExplode - in-db raster") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_TileExplode(raster, 100, 100) AS (x, y, tile)")
      val result = resultDf.collect()
      assert(result.length == 120)
      result.foreach { row =>
        val tile = row.getAs[GridCoverage2D]("tile")
        assert(tile.isInstanceOf[GridCoverage2D])
        assert(tile.getRenderedImage.getData != null)
        tile.dispose(true)
      }
    }

    it("Passed RS_TileExplode - in-db raster with padding") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/raster_with_no_data/test5.tiff")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      Seq("3", "3.0", "3.0d").foreach { noData =>
        val resultDf =
          df.selectExpr(s"RS_TileExplode(raster, 100, 100, true, $noData) AS (x, y, tile)")
        val result = resultDf.collect()
        val tiles = result.map(_.getAs[GridCoverage2D]("tile"))
        assert(tiles.exists(tile => RasterUtils.getNoDataValue(tile.getSampleDimension(0)) == 3))
        tiles.foreach { tile =>
          assert(tile.getRenderedImage.getData != null)
          assert(tile.getRenderedImage.getWidth == 100 && tile.getRenderedImage.getHeight == 100)
          tile.dispose(true)
        }
      }
    }

    it("Passed RS_TileExplode - in-db raster with band index") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf = df.selectExpr("RS_TileExplode(raster, 2, 100, 100, true, 5) AS (x, y, tile)")
      val result = resultDf.collect()
      val tiles = result.map(_.getAs[GridCoverage2D]("tile"))
      assert(tiles.exists(tile => RasterUtils.getNoDataValue(tile.getSampleDimension(0)) == 5))
      tiles.foreach { tile =>
        assert(tile.getRenderedImage.getData != null)
        assert(tile.getNumSampleDimensions == 1)
        tile.dispose(true)
      }
    }

    it("Passed RS_TileExplode - in-db raster with band indices") {
      val df = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif")
        .selectExpr("RS_FromGeoTiff(content) as raster")
      val resultDf =
        df.selectExpr("RS_TileExplode(raster, array(2, 1), 100, 100) AS (x, y, tile)")
      val result = resultDf.collect()
      val tiles = result.map(_.getAs[GridCoverage2D]("tile"))
      tiles.foreach { tile =>
        assert(tile.getRenderedImage.getData != null)
        assert(tile.getNumSampleDimensions == 2)
        tile.dispose(true)
      }
    }

    it("Passed RS_MapAlgebra with two raster columns") {
      var df = sparkSession.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.tif*")
        .load(resourceFolder + "raster")
        .selectExpr("path", "RS_FromGeoTiff(content) as rast")
      df = df
        .as("r1")
        .join(df.as("r2"), col("r1.path") === col("r2.path"), "inner")
        .select(col("r1.rast").alias("rast0"), col("r2.rast").alias("rast1"), col("r1.path"))
      Seq(null, "b", "s", "i", "f", "d").foreach { pixelType =>
        val pixelTypeExpr = if (pixelType == null) null else s"'$pixelType'"
        val dfResult = df
          .withColumn(
            "rast_2",
            expr(
              s"RS_MapAlgebra(rast0, rast1, $pixelTypeExpr, 'out[0] = rast0[0] * 0.5 + rast1[0] * 0.5;', null)"))
          .select("path", "rast0", "rast1", "rast_2")
        dfResult.collect().foreach { row =>
          val rast0 = row.getAs[GridCoverage2D]("rast0")
          val rast1 = row.getAs[GridCoverage2D]("rast1")
          val resultRaster = row.getAs[GridCoverage2D]("rast_2")
          assert(
            rast0.getGridGeometry.getGridToCRS2D == resultRaster.getGridGeometry.getGridToCRS2D)
          val dataType = pixelType match {
            case null => rast0.getRenderedImage.getSampleModel.getDataType
            case _ => RasterUtils.getDataTypeCode(pixelType)
          }
          assert(resultRaster.getRenderedImage.getSampleModel.getDataType == dataType)
          val noDataValue = RasterUtils.getNoDataValue(resultRaster.getSampleDimension(0))
          assert(noDataValue.isNaN)
          val band0 = MapAlgebra.bandAsArray(rast0, 1)
          val band1 = MapAlgebra.bandAsArray(rast1, 1)
          val bandResult = MapAlgebra.bandAsArray(resultRaster, 1)
          assert(band0.size == bandResult.size)
          for (i <- band0.indices) {
            pixelType match {
              case "b" =>
                assert(((band0(i) * 0.5 + band1(i) * 0.5).toInt & 0xff) == bandResult(i).toInt)
              case "s" => assert((band0(i) * 0.5 + band1(i) * 0.5).toShort == bandResult(i))
              case "i" | null => assert((band0(i) * 0.5 + band1(i) * 0.5).toInt == bandResult(i))
              case "f" | "d" =>
                if (band0(i) != 0) {
                  assert((band0(i) * 0.5 + band1(i) * 0.5) == bandResult(i))
                } else {
                  // If the source image has NoDataContainer.GC_NODATA property, Jiffle may convert nodata values in
                  // source raster to NaN.
                  assert(bandResult(i) == 0 || bandResult(i).isNaN)
                }
            }
          }
        }
      }
    }

    it("Passed RS_MapAlgebra") {
      val df = sparkSession.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.tif*")
        .load(resourceFolder + "raster")
        .selectExpr("path", "RS_FromGeoTiff(content) as rast")
      Seq(null, "b", "s", "i", "f", "d").foreach { pixelType =>
        val pixelTypeExpr = if (pixelType == null) null else s"'$pixelType'"
        val dfResult = df
          .withColumn(
            "rast_2",
            expr(s"RS_MapAlgebra(rast, $pixelTypeExpr, 'out[0] = rast[0] * 0.5;')"))
          .select("path", "rast", "rast_2")
        dfResult.collect().foreach { row =>
          val rast = row.getAs[GridCoverage2D]("rast")
          val rast2 = row.getAs[GridCoverage2D]("rast_2")
          assert(rast.getGridGeometry.getGridToCRS2D == rast2.getGridGeometry.getGridToCRS2D)
          val dataType = pixelType match {
            case null => rast.getRenderedImage.getSampleModel.getDataType
            case _ => RasterUtils.getDataTypeCode(pixelType)
          }
          assert(rast2.getRenderedImage.getSampleModel.getDataType == dataType)
          val noDataValue = RasterUtils.getNoDataValue(rast2.getSampleDimension(0))
          assert(noDataValue.isNaN)
          val band = MapAlgebra.bandAsArray(rast, 1)
          val band2 = MapAlgebra.bandAsArray(rast2, 1)
          assert(band.size == band2.size)
          for (i <- band.indices) {
            pixelType match {
              case "b" => assert(((band(i) * 0.5).toInt & 0xff) == band2(i).toInt)
              case "s" => assert((band(i) * 0.5).toShort == band2(i))
              case "i" | null => assert((band(i) * 0.5).toInt == band2(i))
              case "f" | "d" =>
                if (band(i) != 0) {
                  assert((band(i) * 0.5) == band2(i))
                } else {
                  // If the source image has NoDataContainer.GC_NODATA property, Jiffle may convert nodata values in
                  // source raster to NaN.
                  assert(band2(i) == 0 || band2(i).isNaN)
                }
            }
          }
        }
      }
    }

    it("Passed RS_MapAlgebra with nodata value") {
      val df = sparkSession.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.tif*")
        .load(resourceFolder + "raster")
        .selectExpr("RS_FromGeoTiff(content) as rast")
      val dfResult = df
        .withColumn("rast_2", expr("RS_MapAlgebra(rast, 'us', 'out[0] = rast[0] * 0.2;', 10)"))
        .select("rast", "rast_2")
      dfResult.collect().foreach { row =>
        val rast = row.getAs[GridCoverage2D]("rast")
        val rast2 = row.getAs[GridCoverage2D]("rast_2")
        assert(rast2.getRenderedImage.getSampleModel.getDataType == DataBuffer.TYPE_USHORT)
        val noDataValue = RasterUtils.getNoDataValue(rast2.getSampleDimension(0))
        assert(noDataValue == 10)
        val band = MapAlgebra.bandAsArray(rast, 1)
        val band2 = MapAlgebra.bandAsArray(rast2, 1)
        assert(band.size == band2.size)
        for (i <- band.indices) {
          assert((band(i) * 0.2).toShort == band2(i))
        }
      }
    }

    it("Passed RS_AsMatrix with given band and precision") {
      val inputDf =
        Seq(Seq(1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0d) as emptyRaster")
      val resultDf = df.selectExpr("RS_AsMatrix(emptyRaster, 1, 5) as matrix")
      val actual = resultDf.first().getString(0)
      val expected =
        "| 1.00000   3.33333   4.00000   0.00010|\n" + "| 2.22220   9.00000  10.00000  11.11111|\n" + "| 3.00000   4.00000   5.00000   6.00000|\n"
      assertEquals(expected, actual)
    }

    it("Passed RS_AsMatrix with given band") {
      val inputDf =
        Seq(Seq(1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0d) as emptyRaster")
      val resultDf = df.selectExpr("RS_AsMatrix(emptyRaster, 1) as matrix")
      val actual = resultDf.first().getString(0)
      val expected =
        "| 1.000000   3.333333   4.000000   0.000100|\n| 2.222200   9.000000  10.000000  11.111111|\n| 3.000000   4.000000   5.000000   6.000000|\n"
          .format()
      assertEquals(expected, actual)
    }

    it("Passed RS_AsMatrix with default band") {
      val inputDf =
        Seq(Seq(1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0d) as emptyRaster")
      val resultDf = df.selectExpr("RS_AsMatrix(emptyRaster) as matrix")
      val actual = resultDf.first().getString(0)
      val expected =
        "| 1.000000   3.333333   4.000000   0.000100|\n| 2.222200   9.000000  10.000000  11.111111|\n| 3.000000   4.000000   5.000000   6.000000|\n"
          .format()
      assertEquals(expected, actual)
    }

    it("Passed RS_AsImage with default width") {
      val inputDf =
        Seq(Seq(13, 200, 255, 1, 4, 100, 13, 224, 11, 12, 76, 98, 97, 56, 45, 21, 35, 67, 43, 75))
          .toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'b', 5, 4, 0, 0, 1, -1, 0, 0, 0), band, 1, 0d) as emptyRaster")
      val resultDf = df.selectExpr("RS_AsImage(emptyRaster) as html")
      val actual = resultDf.first().getString(0)
      val expectedStart =
        "<img src=\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAECAAAAABjWKqcAAAAIElEQV"
      val expectedEnd = "width=\"200\" />"
      assertTrue(actual.startsWith(expectedStart))
      assertTrue(actual.endsWith(expectedEnd))
    }

    it("Passed RS_AsImage with custom width") {
      val inputDf =
        Seq(Seq(13, 200, 255, 1, 4, 100, 13, 224, 11, 12, 76, 98, 97, 56, 45, 21, 35, 67, 43, 75))
          .toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'b', 5, 4, 0, 0, 1, -1, 0, 0, 0), band, 1, 0d) as emptyRaster")
      val resultDf = df.selectExpr("RS_AsImage(emptyRaster, 500) as html")
      val actual = resultDf.first().getString(0)
      val expectedStart =
        "<img src=\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAECAAAAABjWKqcAAAAIElEQV"
      val expectedEnd = "width=\"500\" />"
      assertTrue(actual.startsWith(expectedStart))
      assertTrue(actual.endsWith(expectedEnd))
    }

    it("Passed RS_Interpolate") {
      val inputDf =
        Seq(Seq(1, 34, 1, 1, 23, 1, 1, 1, 1, 1, 1, 1, 1, 56, 1, 1, 1, 67, 1, 1)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'b', 5, 4, 0, 0, 1, -1, 0, 0, 0), band, 1, 1d) as emptyRaster")
      val result1 = df.selectExpr("RS_Interpolate(emptyRaster)").first().get(0)
      val result2 = df.selectExpr("RS_Interpolate(emptyRaster, 4)").first().get(0)
      val result3 = df.selectExpr("RS_Interpolate(emptyRaster, 4, 'variable', 2)").first().get(0)
      val result4 = df.selectExpr("RS_Interpolate(emptyRaster, 2, 'variable', 2)").first().get(0)
      val result5 =
        df.selectExpr("RS_Interpolate(emptyRaster, 4, 'variable', 2, 5)").first().get(0)
      val result6 =
        df.selectExpr("RS_Interpolate(emptyRaster, 5, 'variable', 3, 5, 1)").first().get(0)

      assert(result1.isInstanceOf[GridCoverage2D])
      assert(result2.isInstanceOf[GridCoverage2D])
      assert(result3.isInstanceOf[GridCoverage2D])
      assert(result4.isInstanceOf[GridCoverage2D])
      assert(result5.isInstanceOf[GridCoverage2D])
      assert(result6.isInstanceOf[GridCoverage2D])
    }

    it("Passed RS_Resample full version") {
      val inputDf = Seq(Seq(1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 2, -2, 0, 0, 0), band, 1, null) as raster")
      val rasterDf = df.selectExpr("RS_Resample(raster, 6, 5, 1, -1, false, null) as raster")
      val rasterOutput = rasterDf.selectExpr("RS_AsMatrix(raster)").first().getString(0)
      val rasterMetadata = rasterDf.selectExpr("RS_Metadata(raster)").first().getStruct(0)
      val rasterMetadataSeq = metadataStructToSeq(rasterMetadata)
      val expectedOutput =
        "| 1.0   1.0   2.0   3.0   3.0   5.0|\n" + "| 1.0   1.0   2.0   3.0   3.0   5.0|\n" + "| 4.0   4.0   5.0   6.0   6.0   9.0|\n" + "| 7.0   7.0   8.0   9.0   9.0  10.0|\n" + "| 7.0   7.0   8.0   9.0   9.0  10.0|\n"
      val expectedMetadata: Seq[Double] =
        Seq(-0.33333333333333326, 0.19999999999999996, 6, 5, 1.388888888888889, -1.24, 0, 0, 0, 1)
      assertEquals(expectedOutput, rasterOutput)
      for (i <- expectedMetadata.indices) {
        assertEquals(expectedMetadata(i), rasterMetadataSeq(i), 1e-6)
      }
    }

    it("Passed RS_Resample resize flavor") {
      val inputDf = Seq(Seq(1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 2, -2, 0, 0, 0), band, 1, null) as raster")
      val rasterDf = df.selectExpr("RS_Resample(raster, 1.2, -1.4, true, null) as raster")
      val rasterOutput = rasterDf.selectExpr("RS_AsMatrix(raster)").first().getString(0)
      val rasterMetadata = rasterDf.selectExpr("RS_Metadata(raster)").first().getStruct(0)
      val rasterMetadataSeq = metadataStructToSeq(rasterMetadata)
      val expectedOutput =
        "|  1.0    1.0    2.0    3.0    3.0    5.0    5.0|\n" + "|  4.0    4.0    5.0    6.0    6.0    9.0    9.0|\n" + "|  4.0    4.0    5.0    6.0    6.0    9.0    9.0|\n" + "|  7.0    7.0    8.0    9.0    9.0   10.0   10.0|\n" + "|  NaN    NaN    NaN    NaN    NaN    NaN    NaN|\n"
      val expectedMetadata: Seq[Double] = Seq(0, 0, 7, 5, 1.2, -1.4, 0, 0, 0, 1)
      assertEquals(expectedOutput, rasterOutput)
      for (i <- expectedMetadata.indices) {
        assertEquals(expectedMetadata(i), rasterMetadataSeq(i), 1e-6)
      }
    }

    it("Passed RS_Resample with ref raster") {
      val inputDf = Seq(Seq(1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10)).toDF("band")
      val df = inputDf.selectExpr(
        "RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 2, -2, 0, 0, 0), band, 1, null) as raster",
        "RS_MakeEmptyRaster(2, 'd', 6, 5, 1, -1, 1.2, -1.4, 0, 0, 0) as refRaster")
      val rasterDf = df.selectExpr("RS_Resample(raster, refRaster, true, null) as raster")
      val rasterOutput = rasterDf.selectExpr("RS_AsMatrix(raster)").first().getString(0)
      val rasterMetadata = rasterDf.selectExpr("RS_Metadata(raster)").first().getStruct(0)
      val rasterMetadataSeq = metadataStructToSeq(rasterMetadata)
      val expectedOutput =
        "| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|\n" + "| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|\n" + "| 4.0   4.0   5.0   6.0   6.0   9.0   9.0|\n" + "| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|\n" + "| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|\n"
      val expectedMetadata: Seq[Double] =
        Seq(-0.20000000298023224, 0.4000000059604645, 7.0, 5.0, 1.2, -1.4, 0.0, 0.0, 0.0, 1.0)
      assertEquals(expectedOutput, rasterOutput)
      for (i <- expectedMetadata.indices) {
        assertEquals(expectedMetadata(i), rasterMetadataSeq(i), 1e-6)
      }
    }

    it("Passed RS_ReprojectMatch") {
      val rasterDf = sparkSession.read
        .format("binaryFile")
        .load(resourceFolder + "raster/test1.tiff")
        .selectExpr("RS_FromGeoTiff(content) as rast")
      val df = rasterDf.selectExpr(
        "rast",
        "RS_MakeEmptyRaster(1, 300, 300, 453926, 3741637, 100, -100, 0, 0, 32611) as align_rast")
      val results =
        df.selectExpr("RS_ReprojectMatch(rast, align_rast) as trans_rast", "align_rast").first()
      val transformedRast = results.get(0).asInstanceOf[GridCoverage2D]
      val alignRast = results.get(1).asInstanceOf[GridCoverage2D]
      assertEquals(alignRast.getEnvelope2D, transformedRast.getEnvelope2D)
      assertEquals(
        alignRast.getCoordinateReferenceSystem,
        transformedRast.getCoordinateReferenceSystem)
      assertEquals(
        alignRast.getGridGeometry.getGridToCRS2D,
        transformedRast.getGridGeometry.getGridToCRS2D)
    }

    it("Passed RS_FromNetCDF with NetCDF classic") {
      val df =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster/netcdf/test.nc")
      val rasterDf = df.selectExpr("RS_FromNetCDF(content, 'O3') as raster")
      val expectedMetadata = Seq(4.9375, 50.9375, 80, 48, 0.125, -0.125, 0, 0, 0, 4)
      val actualMetadata =
        rasterDf.selectExpr("RS_Metadata(raster) as metadata").first().getStruct(0)
      val actualMetadataSeq = metadataStructToSeq(actualMetadata)
      for (i <- expectedMetadata.indices) {
        assertEquals(expectedMetadata(i), actualMetadataSeq(i), 1e-6)
      }

      val expectedFirstVal = 60.95357131958008
      val actualFirstVal =
        rasterDf.selectExpr("RS_Value(raster, 0, 0, 1) as raster").first().getDouble(0)
      assertEquals(expectedFirstVal, actualFirstVal, 1e-6)
    }

    it("Passed RS_FromNetCDF with NetCDF classic long form") {
      val df =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster/netcdf/test.nc")
      val rasterDf = df.selectExpr("RS_FromNetCDF(content, 'O3', 'lon', 'lat') as raster")
      val expectedMetadata = Seq(4.9375, 50.9375, 80, 48, 0.125, -0.125, 0, 0, 0, 4)
      val actualMetadata =
        rasterDf.selectExpr("RS_Metadata(raster) as metadata").first().getStruct(0)
      val actualMetadataSeq = metadataStructToSeq(actualMetadata)
      for (i <- expectedMetadata.indices)
        assertEquals(expectedMetadata(i), actualMetadataSeq(i), 1e-6)

      val expectedFirstVal = 60.95357131958008
      val actualFirstVal =
        rasterDf.selectExpr("RS_Value(raster, 0, 0, 1) as raster").first().getDouble(0)
      assertEquals(expectedFirstVal, actualFirstVal, 1e-6)
    }

    it("Passed RS_NetCDFInfo") {
      val df =
        sparkSession.read.format("binaryFile").load(resourceFolder + "raster/netcdf/test.nc")
      val recordInfo = df.selectExpr("RS_NetCDFInfo(content) as record_info").first().getString(0)
      val expectedRecordInfo = "O3(time=2, z=2, lat=48, lon=80)\n" +
        "\n" +
        "NO2(time=2, z=2, lat=48, lon=80)"
      assertEquals(expectedRecordInfo, recordInfo)
    }
  }

  private def assertRSMetadata(
      expectedDf: DataFrame,
      expectedCol: String,
      actualDf: DataFrame,
      actualCol: String) = {
    // Extract the actual metadata struct
    val actualMetadataStruct = actualDf.selectExpr(actualCol).first().getStruct(0)

    // Extract the expected metadata struct
    val expectedMetadataStruct = expectedDf.selectExpr(expectedCol).first().getStruct(0)

    // Convert Structs to Seqs
    val actualMetadata = metadataStructToSeq(actualMetadataStruct).slice(0, 9)
    val expectedMetadata = metadataStructToSeq(expectedMetadataStruct).slice(0, 9)

    // Compare the actual and expected metadata
    assert(
      actualMetadata == expectedMetadata,
      s"Expected: $expectedMetadata, but got: $actualMetadata")
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
