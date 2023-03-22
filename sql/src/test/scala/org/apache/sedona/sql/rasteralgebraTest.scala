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
import org.locationtech.jts.geom.Geometry
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
      expectedDF.show()
      inputDf.show()
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
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
      expectedDF.show()
      inputDf.show()
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
      inputDf.show()
      expectedDF.show()
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
  }

  describe("Should pass all transformation tests") {
    it("Passed RS_Append for new data length") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/test3.tif")
      df = df.selectExpr(" image.data as data", "image.nBands as nBands")
      val rowFirst = df.first()
      val nBands = rowFirst.getAs[Int](1)
      val lengthInitial = rowFirst.getAs[mutable.WrappedArray[Double]](0).length
      val lengthBand = lengthInitial / nBands

      df = df.selectExpr("data", "nBands", "RS_GetBand(data, 1, nBands) as band1", "RS_GetBand(data, 2, nBands) as band2")
      df = df.selectExpr("data", "nBands", "RS_NormalizedDifference(band2, band1) as normalizedDifference")
      df = df.selectExpr("RS_Append(data, normalizedDifference, nBands) as targetData")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0).length == lengthInitial + lengthBand)
    }

    it("Passed RS_Append for new band elements") {
      var df = sparkSession.read.format("geotiff").option("dropInvalid", true).load(resourceFolder + "raster/test3.tif")
      df = df.selectExpr(" image.data as data", "image.nBands as nBands")
      var rowFirst = df.first()
      val nBands = rowFirst.getAs[Int](1)
      val lengthInitial = rowFirst.getAs[mutable.WrappedArray[Double]](0).length
      val lengthBand = lengthInitial / nBands

      df = df.selectExpr("data", "nBands", "RS_GetBand(data, 1, nBands) as band1", "RS_GetBand(data, 2, nBands) as band2")
      df = df.selectExpr("data", "nBands", "RS_NormalizedDifference(band2, band1) as normalizedDifference")
      df = df.selectExpr("RS_Append(data, normalizedDifference, nBands) as targetData")

      rowFirst = df.first()
      assert((rowFirst.getAs[mutable.WrappedArray[Double]](0)(lengthInitial) == 0.13) &&
        (rowFirst.getAs[mutable.WrappedArray[Double]](0)(lengthInitial + lengthBand - 1) == 0.03))
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
  }
}
