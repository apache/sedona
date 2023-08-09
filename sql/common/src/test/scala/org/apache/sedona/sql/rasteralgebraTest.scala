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

import org.apache.spark.sql.functions.{collect_list, expr}
import org.geotools.coverage.grid.GridCoverage2D
import org.junit.Assert.assertEquals
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import scala.collection.mutable


class rasteralgebraTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen{

  import sparkSession.implicits._

  describe("should pass all the arithmetic operations on bands") {
    it("Passed RS_Add") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(400.0, 900.0, 1400.0))).toDF("sumOfBands")
      inputDf = inputDf.selectExpr("RS_Add(Band1,Band2) as sumOfBands")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_Subtract") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(0.0, 100.0, 200.0))).toDF("differenceOfBands")
      inputDf = inputDf.selectExpr("RS_Subtract(Band1,Band2) as differenceOfBands")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_Multiply") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(40000.0, 200000.0, 480000.0))).toDF("MultiplicationOfBands")
      inputDf = inputDf.selectExpr("RS_Multiply(Band1,Band2) as multiplicationOfBands")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_Divide") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 200.0, 500.0)), ((Seq(0.4, 0.26, 0.27), Seq(0.3, 0.32, 0.43)))).toDF("Band1", "Band2")
      val expectedList = List(List(1.0, 2.0, 1.2), List(1.33, 0.81, 0.63))
      val inputList = inputDf.selectExpr("RS_Divide(Band1,Band2) as divisionOfBands").as[List[Double]].collect().toList
      val resultList = inputList zip expectedList
      for((actual, expected) <- resultList) {
        assert(actual == expected)
      }

    }

    it("Passed RS_MultiplyFactor") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0))).toDF("Band")
      val expectedDF = Seq((Seq(600.0, 1200.0, 1800.0))).toDF("multiply")
      inputDf = inputDf.selectExpr("RS_MultiplyFactor(Band, 3) as multiply")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_MultiplyFactor with double factor") {
      val inputDf = Seq((Seq(200.0, 400.0, 600.0))).toDF("Band")
      val expectedDF = Seq((Seq(20.0, 40.0, 60.0))).toDF("multiply")
      val actualDF = inputDf.selectExpr("RS_MultiplyFactor(Band, 0.1) as multiply")
      assert(actualDF.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }
  }

  describe("Should pass basic statistical tests") {
    it("Passed RS_Mode") {
      val inputDf = Seq((Seq(200.0, 400.0, 600.0, 200.0)), (Seq(200.0, 400.0, 600.0, 700.0))).toDF("Band")
      val expectedResult = List(List(200.0), List(200.0, 400.0, 600.0, 700.0))
      val actualResult = inputDf.selectExpr("sort_array(RS_Mode(Band)) as mode").as[List[Double]].collect().toList
      val resultList = actualResult zip expectedResult
      for((actual, expected) <- resultList) {
        assert(actual == expected)
      }

    }

    it("Passed RS_Mean") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0, 200.0)), (Seq(200.0, 400.0, 600.0, 700.0)), (Seq(0.43, 0.36, 0.73, 0.56)) ).toDF("Band")
      val expectedList = List(350.0,475.0,0.52)
      val actualList = inputDf.selectExpr("RS_Mean(Band) as mean").as[Double].collect().toList
      val resultList = actualList zip expectedList
      for((actual, expected) <- resultList) {
        assert(actual == expected)
      }
    }

    it("Passed RS_NormalizedDifference") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(0.0, 0.11, 0.14))).toDF("normalizedDifference")
      inputDf = inputDf.selectExpr("RS_NormalizedDifference(Band1,Band2) as normalizedDifference")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_Count") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0, 200.0, 600.0, 600.0, 800.0))).toDF("Band")
      val expectedDF = Seq(3).toDF("Count")
      inputDf = inputDf.selectExpr("RS_Count(Band, 600.0) as Count")
      assert(inputDf.first().getAs[Int](0) == expectedDF.first().getAs[Int](0))
    }
  }

  describe("Should pass operator tests") {
    it("Passed RS_GreaterThan") {
      var inputDf = Seq((Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)), (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF = Seq((Seq(1.0,1.0,0.0,0.0,1.0,1.0,0.0)), (Seq(0.0,0.0,0.0,0.0,1.0,0.0))).toDF("GreaterThan")
      inputDf = inputDf.selectExpr("RS_GreaterThan(Band, 0.2) as GreaterThan")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_GreaterThanEqual") {
      var inputDf = Seq((Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)), (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF = Seq((Seq(1.0,1.0,0.0,1.0,1.0,1.0,0.0)), (Seq(0.0,0.0,0.0,0.0,1.0,0.0))).toDF("GreaterThanEqual")
      inputDf = inputDf.selectExpr("RS_GreaterThanEqual(Band, 0.2) as GreaterThanEqual")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_LessThan") {
      var inputDf = Seq((Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)), (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF = Seq((Seq(0.0,0.0,1.0,0.0,0.0,0.0,1.0)), (Seq(1.0,1.0,1.0,0.0,1.0))).toDF("LessThan")
      inputDf = inputDf.selectExpr("RS_LessThan(Band, 0.2) as LessThan")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_LessThanEqual") {
      var inputDf = Seq((Seq(0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19)), (Seq(0.14, 0.13, 0.10, 0.86, 0.01))).toDF("Band")
      val expectedDF = Seq((Seq(0.0,0.0,1.0,1.0,0.0,0.0,1.0)), (Seq(1.0,1.0,1.0,0.0,1.0))).toDF("LessThanEqual")
      inputDf = inputDf.selectExpr("RS_LessThanEqual(Band, 0.2) as LessthanEqual")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_Modulo") {
      var inputDf = Seq((Seq(100.0, 260.0, 189.0, 106.0, 230.0, 169.0, 196.0)), (Seq(230.0, 345.0, 136.0, 106.0, 134.0, 105.0))).toDF("Band")
      val expectedDF = Seq((Seq(10.0, 80.0, 9.0, 16.0, 50.0, 79.0, 16.0)), (Seq(50.0, 75.0, 46.0, 16.0, 44.0, 15.0))).toDF("Modulo")
      inputDf = inputDf.selectExpr("RS_Modulo(Band, 90.0) as Modulo")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_BitwiseAND") {
      var inputDf = Seq((Seq(10.0, 20.0, 30.0), Seq(10.0, 20.0, 30.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(10.0, 20.0, 30.0))).toDF("AND")
      inputDf = inputDf.selectExpr("RS_BitwiseAND(Band1, Band2) as AND")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_BitwiseOR") {
      var inputDf = Seq((Seq(10.0, 20.0, 30.0), Seq(40.0, 22.0, 62.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(42.0, 22.0, 62.0))).toDF("OR")
      inputDf = inputDf.selectExpr("RS_BitwiseOR(Band1, Band2) as OR")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_SquareRoot") {
      var inputDf = Seq((Seq(8.0, 16.0, 24.0))).toDF("Band")
      val expectedDF = Seq((Seq(2.83, 4.0, 4.90))).toDF("SquareRoot")
      inputDf = inputDf.selectExpr("RS_SquareRoot(Band) as SquareRoot")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_LogicalDifference") {
      var inputDf = Seq((Seq(10.0, 20.0, 30.0), Seq(40.0, 20.0, 50.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(10.0, 0.0, 30.0))).toDF("LogicalDifference")
      inputDf = inputDf.selectExpr("RS_LogicalDifference(Band1, Band2) as LogicalDifference")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_LogicalOver") {
      var inputDf = Seq((Seq(0.0, 0.0, 30.0), Seq(40.0, 20.0, 50.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(40.0, 20.0, 30.0))).toDF("LogicalOR")
      inputDf = inputDf.selectExpr("RS_LogicalOver(Band1, Band2) as LogicalOR")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))

    }

    it("Passed RS_FetchRegion") {
      var inputDf = Seq((Seq(100.0, 260.0, 189.0, 106.0, 230.0, 169.0, 196.0, 200.0, 460.0))).toDF("Band")
      val expectedDF = Seq(Seq(100.0, 260.0, 189.0, 106.0, 230.0, 169.0)).toDF("Region")
      inputDf = inputDf.selectExpr("RS_FetchRegion(Band,Array(0, 0, 1, 2),Array(3, 3)) as Region")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("should pass RS_Normalize") {
      var df = Seq((Seq(800.0, 900.0, 0.0, 255.0)), (Seq(100.0, 200.0, 700.0, 900.0))).toDF("Band")
      df = df.selectExpr("RS_Normalize(Band) as normalizedBand")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0)(1) == 255)
    }

    it("should pass RS_Array") {
      val df = sparkSession.sql("SELECT RS_Array(6, 1e-6) as band")
      val result = df.first().getAs[mutable.WrappedArray[Double]](0)
      assert(result.length == 6)
      assert(result sameElements Array.fill[Double](6)(1e-6))
    }
  }

  describe("Should pass all transformation tests") {
    it("Passed RS_Append for new data length and new band elements") {
      var df = Seq(Seq(200.0, 400.0, 600.0, 800.0, 100.0, 500.0, 800.0, 600.0)).toDF("data")
      df = df.selectExpr("data", "2 as nBands")
      var rowFirst = df.first()
      val nBands = rowFirst.getAs[Int](1)
      val lengthInitial = rowFirst.getAs[mutable.WrappedArray[Double]](0).length
      val lengthBand = lengthInitial / nBands

      df = df.selectExpr("data", "nBands", "RS_GetBand(data, 1, nBands) as band1", "RS_GetBand(data, 2, nBands) as band2")
      df = df.selectExpr("data", "nBands", "RS_NormalizedDifference(band2, band1) as normalizedDifference")
      df = df.selectExpr("RS_Append(data, normalizedDifference, nBands) as targetData")

      rowFirst = df.first()
      assert(rowFirst.getAs[mutable.WrappedArray[Double]](0).length == lengthInitial + lengthBand)
      assert((rowFirst.getAs[mutable.WrappedArray[Double]](0)(lengthInitial) == 0.33) &&
        (rowFirst.getAs[mutable.WrappedArray[Double]](0)(lengthInitial + lengthBand - 1) == 0.14))
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
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster_asc/test1.asc")
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
      val result = df.selectExpr("RS_SRID(RS_SetSRID(RS_FromGeoTiff(content), 4326))").first().getInt(0)
      assert(result == 4326)
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
      val result = df.selectExpr("RS_Value(RS_FromGeoTiff(content), ST_Point(-13077301.685, 4002565.802))").first().getDouble(0)
      assert(result == 255d)
    }

    it("Passed RS_Values should handle null values") {
      val result = sparkSession.sql("select RS_Values(null, null)").first().get(0)
      assert(result == null)
    }

    it("Passed RS_Values with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_Values(RS_FromGeoTiff(content), array(ST_Point(-13077301.685, 4002565.802), null))").first().getList[Any](0)
      assert(result.size() == 2)
      assert(result.get(0) == 255d)
      assert(result.get(1) == null)
    }

    it("Passed RS_Values with raster and serialized point array") {
      // https://issues.apache.org/jira/browse/SEDONA-266
      // A shuffle changes the internal type for the geometry array (point in this case) from GenericArrayData to UnsafeArrayData.
      // UnsafeArrayData.array() throws UnsupportedOperationException.
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val points = sparkSession.createDataFrame(Seq(("POINT (-13077301.685 4002565.802)",1), ("POINT (0 0)",2)))
        .toDF("point", "id")
        .withColumn("point", expr("ST_GeomFromText(point)"))
        .groupBy().agg(collect_list("point").alias("point"))

      val result = df.crossJoin(points).selectExpr("RS_Values(RS_FromGeoTiff(content), point)").first().getList[Any](0)

      assert(result.size() == 2)
      assert(result.get(0) == 255d)
      assert(result.get(1) == null)
    }

    it("Passed RS_AsGeoTiff") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/*")
      val resultRaw = df.selectExpr("RS_FromGeoTiff(content) as raster").first().get(0)
      val resultLoaded = df.selectExpr("RS_FromGeoTiff(content) as raster")
        .selectExpr("RS_AsGeoTiff(raster) as geotiff")
        .selectExpr("RS_FromGeoTiff(geotiff) as raster_new").first().get(0)
      assert(resultLoaded != null)
      assert(resultLoaded.isInstanceOf[GridCoverage2D])
      assertEquals(resultRaw.asInstanceOf[GridCoverage2D].getEnvelope.toString, resultLoaded.asInstanceOf[GridCoverage2D].getEnvelope.toString)
    }

    it("Passed RS_AsGeoTiff with different compression types") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val resultRaw = df.selectExpr("RS_FromGeoTiff(content) as raster").first().get(0)
      val resultLoadedDf = df.selectExpr("RS_FromGeoTiff(content) as raster")
        .withColumn("geotiff", expr("RS_AsGeoTiff(raster, 'LZW', 1)"))
        .withColumn("geotiff2", expr("RS_AsGeoTiff(raster, 'Deflate', 0.5)"))
        .withColumn("raster_new", expr("RS_FromGeoTiff(geotiff)"))
      val resultLoaded = resultLoadedDf.first().getAs[GridCoverage2D]("raster_new")
      val writtenBinary1 = resultLoadedDf.first().getAs[Array[Byte]]("geotiff")
      val writtenBinary2 = resultLoadedDf.first().getAs[Array[Byte]]("geotiff2")
      assertEquals(resultRaw.asInstanceOf[GridCoverage2D].getEnvelope.toString, resultLoaded.getEnvelope.toString)
      assert(writtenBinary1.length > writtenBinary2.length)
    }

    it("Passed RS_AsArcGrid") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster_asc/*")
      val resultRaw = df.selectExpr("RS_FromArcInfoAsciiGrid(content) as raster").first().get(0)
      val resultLoaded = df.selectExpr("RS_FromArcInfoAsciiGrid(content) as raster")
        .selectExpr("RS_AsArcGrid(raster) as arcgrid")
        .selectExpr("RS_FromArcInfoAsciiGrid(arcgrid) as raster_new").first().get(0)
      assert(resultLoaded != null)
      assert(resultLoaded.isInstanceOf[GridCoverage2D])
      assertEquals(resultRaw.asInstanceOf[GridCoverage2D].getEnvelope.toString, resultLoaded.asInstanceOf[GridCoverage2D].getEnvelope.toString)
    }

    it("Passed RS_AsArcGrid with different bands") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster_geotiff_color/*").selectExpr("RS_FromGeoTiff(content) as raster")
      val rasterDf = df.selectExpr("RS_AsArcGrid(raster, 0) as arc", "RS_AsArcGrid(raster, 1) as arc2")
      val binaryDf = rasterDf.selectExpr("RS_FromArcInfoAsciiGrid(arc) as raster", "RS_FromArcInfoAsciiGrid(arc2) as raster2")
      assertEquals(rasterDf.count(), binaryDf.count())
    }

    it("Passed RS_UpperLeftX"){
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_UpperLeftX(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = -1.3095817809482181E7
      assertEquals(expected, result, 1e-12)
    }

    it("Passed RS_UpperLeftY") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_UpperLeftY(RS_FromGeoTiff(content))").first().getDouble(0)
      val expected: Double = 4021262.7487925636
      assertEquals(expected, result, 1e-8)
    }

    it("Passed RS_Metadata") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_Metadata(RS_FromGeoTiff(content))").first().getSeq(0)
      assertEquals(10, result.length)
      assertEquals(512.0, result(2), 0.001)
      assertEquals(517.0, result(3), 0.001)
      assertEquals(1.0, result(9), 0.001)
    }

    it("Passed RS_MakeEmptyRaster") {
      val widthInPixel = 10
      val heightInPixel = 10
      val upperLeftX = 0.0
      val upperLeftY = 0.0
      val cellSize = 1.0
      val numBands = 2
      // Test without skewX, skewY, srid
      var result = sparkSession.sql(s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize))").first().getSeq(0)
      assertEquals(numBands, result(9), 0.001)

      // Test with integer type input
      result = sparkSession.sql(s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, ${upperLeftX.toInt}, ${upperLeftY.toInt}, ${cellSize.toInt}))").first().getSeq(0)
      assertEquals(numBands, result(9), 0.001)

      // Test with skewX, skewY, srid
      val skewX = 0.0
      val skewY = 0.0
      val srid = 0
      result = sparkSession.sql(s"SELECT RS_Metadata(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize, -$cellSize, $skewX, $skewY, $srid))").first().getSeq(0)
      assertEquals(numBands, result(9), 0.001)
    }

    it("Passed RS_BandAsArray") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val metadata = df.selectExpr("RS_Metadata(RS_FromGeoTiff(content))").first().getSeq(0)
      val width = metadata(2).asInstanceOf[Double].toInt
      val height = metadata(3).asInstanceOf[Double].toInt
      val result = df.selectExpr("RS_BandAsArray(RS_FromGeoTiff(content), 1)").first().getSeq(0)
      assertEquals(width * height, result.length)
      val resultOutOfBound = df.selectExpr("RS_BandAsArray(RS_FromGeoTiff(content), 100)").first().isNullAt(0)
      assertEquals(true, resultOutOfBound)
    }

    it("Passed RS_AddBandFromArray") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster", "RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2) as band")
      val bandNewExpected:Seq[Double] = df.selectExpr("band").first().getSeq(0)
      val bandNewActual:Seq[Double] = df.selectExpr("RS_BandAsArray(RS_AddBandFromArray(raster, band, 1), 1)").first().getSeq(0)
      for (i <- bandNewExpected.indices) {
        // The band value needs to be mod 256 because the ColorModel will mod 256.
        assertEquals(bandNewExpected(i)%256, bandNewActual(i), 0.001)
      }
      // Test with out of bound band index. It should fail.
      intercept[Exception] {
        df.selectExpr("RS_BandAsArray(RS_AddBandFromArray(raster, band, 100), 1)").collect()
      }
    }

    it("Passed RS_Intersects") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
        .selectExpr("path", "RS_FromGeoTiff(content) as raster")

      // query window without SRID
      assert(df.selectExpr("RS_Intersects(raster, ST_Point(-13076178,4003651))").first().getBoolean(0))
      assert(!df.selectExpr("RS_Intersects(raster, ST_Point(-13055247,3979620))").first().getBoolean(0))

      // query window and raster are in the same CRS
      assert(df.selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(-13067806,4009116), 3857))").first().getBoolean(0))
      assert(!df.selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(-13057457,4027023), 3857))").first().getBoolean(0))

      // query window and raster not in the same CRS
      assert(df.selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(33.81798,-117.47993), 4326))").first().getBoolean(0))
      assert(!df.selectExpr("RS_Intersects(raster, ST_SetSRID(ST_Point(33.97896,-117.27868), 4326))").first().getBoolean(0))
    }

    it("Passed RS_AddBandFromArray collect generated raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster", "RS_BandAsArray(RS_FromGeoTiff(content), 1) as band")
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

    it("Passed RS_ConvexHull with raster") {
      val df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      val result = df.selectExpr("RS_ConvexHull(RS_FromGeoTiff(content))").first().getAs[Geometry](0);
      val coordinates = result.getCoordinates;

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

    it("Passed RS_PixelAsPoint with raster") {
      val widthInPixel = 5
      val heightInPixel = 10
      val upperLeftX = 123.19
      val upperLeftY = -12
      val cellSize = 4
      val numBands = 2
      val result = sparkSession.sql(s"SELECT RS_PixelAsPoint(RS_MakeEmptyRaster($numBands, $widthInPixel, $heightInPixel, $upperLeftX, $upperLeftY, $cellSize), 2, 1)").first().getAs[Geometry](0);
      val expectedX = 127.19
      val expectedY = -12
      val actualCoordinates = result.getCoordinate;
      assertEquals(expectedX, actualCoordinates.x, 1e-5)
      assertEquals(expectedY, actualCoordinates.y, 1e-5)
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

    it("Passed RS_WorldToRasterCoord with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("ST_AsText(RS_WorldToRasterCoord(raster, -13095817.809482181, 4021262.7487925636))").first().getString(0);
      val expected = "POINT (1 1)"
      assertEquals(expected, result )
    }

    it("Passed RS_WorldToRasterCoordX with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_WorldToRasterCoordX(raster, -13095817.809482181, 4021262.7487925636)").first().getInt(0);
      val expected = 1
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoordY with raster") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_WorldToRasterCoordY(raster, -13095817.809482181, 4021262.7487925636)").first().getInt(0);
      val expected = 1
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoord with raster - geom parameter") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("ST_AsText(RS_WorldToRasterCoord(raster, ST_GeomFromText('POINT (-13095817.809482181 4021262.7487925636)')))").first().getString(0);
      val expected = "POINT (1 1)"
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoordX with raster - geom parameter") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_WorldToRasterCoordX(raster, ST_GeomFromText('POINT (-13095817.809482181 4021262.7487925636)'))").first().getInt(0);
      val expected = 1
      assertEquals(expected, result)
    }

    it("Passed RS_WorldToRasterCoordY with raster - geom parameter") {
      var df = sparkSession.read.format("binaryFile").load(resourceFolder + "raster/test1.tiff")
      df = df.selectExpr("RS_FromGeoTiff(content) as raster")
      val result = df.selectExpr("RS_WorldToRasterCoordY(raster, ST_GeomFromText('POINT (-13095817.809482181 4021262.7487925636)'))").first().getInt(0);
      val expected = 1
      assertEquals(expected, result)
    }

    it("Passed RS_Contains") {
      assert(sparkSession.sql("SELECT RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((5 5, 5 10, 10 10, 10 5, 5 5))'))").first().getBoolean(0))
      assert(!sparkSession.sql("SELECT RS_Contains(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((2 2, 2 25, 20 25, 20 2, 2 2))'))").first().getBoolean(0))
    }

    it("Passed RS_Within") {
      assert(sparkSession.sql("SELECT RS_WITHIN(RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1), ST_GeomFromWKT('POLYGON ((0 0, 0 50, 100 50, 100 0, 0 0))'))").first().getBoolean(0))
      assert(!sparkSession.sql("SELECT RS_WITHIN(RS_MakeEmptyRaster(1, 100, 100, 0, 50, 1), ST_GeomFromWKT('POLYGON ((2 2, 2 25, 20 25, 20 2, 2 2))'))").first().getBoolean(0))
    }
  }
}
