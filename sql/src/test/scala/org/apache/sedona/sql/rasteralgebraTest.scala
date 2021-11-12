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

import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import scala.collection.mutable


class rasteralgebraTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen{

  import sparkSession.implicits._

  describe("should pass all the arithmetic operations on bands") {
    it("Passed RS_AddBands") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(400.0, 900.0, 1400.0))).toDF("sumOfBands")
      inputDf = inputDf.selectExpr("RS_AddBands(Band1,Band2) as sumOfBands")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_SubtractBands") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(0.0, 100.0, 200.0))).toDF("differenceOfBands")
      inputDf = inputDf.selectExpr("RS_SubtractBands(Band1,Band2) as differenceOfBands")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_MultiplyBands") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 500.0, 800.0))).toDF("Band1", "Band2")
      val expectedDF = Seq((Seq(40000.0, 200000.0, 480000.0))).toDF("MultiplicationOfBands")
      inputDf = inputDf.selectExpr("RS_MultiplyBands(Band1,Band2) as multiplicationOfBands")
      assert(inputDf.first().getAs[mutable.WrappedArray[Double]](0) == expectedDF.first().getAs[mutable.WrappedArray[Double]](0))
    }

    it("Passed RS_DivideBands") {
      var inputDf = Seq((Seq(200.0, 400.0, 600.0), Seq(200.0, 200.0, 500.0)), ((Seq(0.4, 0.26, 0.27), Seq(0.3, 0.32, 0.43)))).toDF("Band1", "Band2")
      val expectedList = List(List(1.0, 2.0, 1.2), List(1.33, 0.81, 0.63))
      val inputList = inputDf.selectExpr("RS_DivideBands(Band1,Band2) as divisionOfBands").as[List[Double]].collect().toList
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
}
