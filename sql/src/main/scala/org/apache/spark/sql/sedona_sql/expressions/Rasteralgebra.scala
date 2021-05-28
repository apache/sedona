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

package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._



// Calculate Normalized Difference between two bands
case class RS_NormalizedDifference(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    val ndvi = normalizeddifference(band1,band2)
    ndvi
  }
  private def normalizeddifference(band1: Array[Byte], band2: Array[Byte]): Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for (i <- 0 until band1.length) {
      if (band1(i).toDouble ==  0) {
        band1(i) = -1
      }
      if (band2(i).toDouble == 0) {
        band2(i) = -1
      }

      result(i) =(((band2(i).toDouble - band1(i).toDouble) / (band2(i).toDouble + band1(i).toDouble)*100).round/100.toDouble).toByte
    }

    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate mean value for a particular band
case class RS_Mean(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val mean = calculateMean(band)
    mean
  }

  private def calculateMean(band:Array[Byte]):Double = {

    ((band.map(x => x.toDouble).toList.sum/band.length)*100).round/100.toDouble
  }


  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate mode of a particular band
case class RS_Mode(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
   val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val mode = calculateMode(band)
    mode
  }

  private def calculateMode(band:Array[Byte]):Array[Byte] = {

    val grouped = band.toList.groupBy(x => x).mapValues(_.size)
    val modeValue = grouped.maxBy(_._2)._2
    val modes = grouped.filter(_._2 == modeValue).map(_._1)
    modes.toArray

  }
  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}
//
// fetch a particular region from a raster image given particular indexes(Array[minx...maxX][minY...maxY])
case class RS_FetchRegion(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 3)
   val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val coordinates =  inputExpressions(1).eval(inputRow).asInstanceOf[GenericArrayData].toIntArray()
    val dim = inputExpressions(2).eval(inputRow).asInstanceOf[GenericArrayData].toIntArray()
    regionEnclosed(band, coordinates,dim)

  }

  private def regionEnclosed(Band: Array[Byte], coordinates: Array[Int], dim: Array[Int]):Array[Byte] = {

    val result1D = new Array[Byte]((coordinates(2) - coordinates(0) + 1) * (coordinates(3) - coordinates(1) + 1))

    var k = 0
    for(i<-coordinates(0) until coordinates(2) + 1) {
      for(j<-coordinates(1) until coordinates(3) + 1) {
        result1D(k) = Band(((i - 0) * dim(0)) + j)
        k+=1
      }
    }
    result1D

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}
//
// Mark all the band values with 1 which are greater than a particular threshold
case class RS_GreaterThan(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    findGreaterThan(band, target)

  }

 private def findGreaterThan(band: Array[Byte], target: Double):Array[Byte] = {

   val result = new Array[Byte](band.length)
   for(i<-0 until band.length) {
     if(band(i).toDouble>target) {
       result(i) = 1.toByte
     }
     else {
       result(i) = 0.toByte
     }
   }
   result
 }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}
//
// Mark all the band values with 1 which are greater than or equal to a particular threshold
case class RS_GreaterThanEqual(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    findGreaterThanEqual(band, target)

  }

  private def findGreaterThanEqual(band: Array[Byte], target: Double):Array[Byte] = {

    val result = new Array[Byte](band.length)
    for(i<-0 until band.length) {
      if(band(i).toDouble>=target) {
        result(i) = 1.toByte
      }
      else {
        result(i) = 0.toByte
      }
    }
    result
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Mark all the band values with 1 which are less than a particular threshold
case class RS_LessThan(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    findLessThan(band, target)

  }

  private def findLessThan(band: Array[Byte], target: Double):Array[Byte] = {

    val result = new Array[Byte](band.length)
    for(i<-0 until band.length) {
      if(band(i).toDouble<target) {
        result(i) = 1.toByte
      }
      else {
        result(i) = 0.toByte
      }
    }
    result
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Mark all the band values with 1 which are less than or equal to a particular threshold
case class RS_LessThanEqual(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    findLessThanEqual(band, target)

  }

  private def findLessThanEqual(band: Array[Byte], target: Double):Array[Byte] = {

    val result = new Array[Byte](band.length)
    for(i<-0 until band.length) {
      if(band(i).toDouble<=target) {
        result(i) = 1.toByte
      }
      else {
        result(i) = 0.toByte
      }
    }
    result
  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Count number of occurences of a particular value in a band
case class RS_Count(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    findCount(band, target)

  }

  private def findCount(band: Array[Byte], target: Double):Int = {

    var result = 0
    for(i<-0 until band.length) {
      if(band(i).toDouble==target) {
        result+=1
      }

    }
    result
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions
}

// Multiply a factor to all values of a band
case class RS_MultiplyFactor(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
   multiply(band, target)

  }

  private def multiply(band: Array[Byte], target: Int):Array[Byte] = {

    var result = new Array[Byte](band.length)
    for(i<-0 until band.length) {

      result(i) = (band(i).toDouble*target).toByte

      }
    result

    }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}
//
// Add two bands
case class RS_AddBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    addBands(band1, band2)

  }

  private def addBands(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      result(i) = (band1(i).toDouble + band2(i).toDouble).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Subtract two bands
case class RS_SubtractBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    subtractBands(band1, band2)

  }

  private def subtractBands(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      result(i) = (band2(i).toDouble - band1(i).toDouble).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Multiple two bands
case class RS_MultiplyBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    multiplyBands(band1, band2)

  }

  private def multiplyBands(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      result(i) = (band1(i).toDouble * band2(i).toDouble).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Divide two bands
case class RS_DivideBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    divideBands(band1, band2)

  }

  private def divideBands(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      result(i) = (((band1(i).toDouble/band2(i).toDouble)*100).round/(100.toDouble)).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}
//
// Modulo of a band
case class RS_Modulo(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]


    val dividend = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble


    modulo(band, dividend)

  }

  private def modulo(band: Array[Byte], dividend:Double):Array[Byte] = {

    val result = new Array[Byte](band.length)
    for(i<-0 until band.length) {
      result(i) = (band(i).toDouble % dividend).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Square root of values in a band
case class RS_SquareRoot(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]

   squareRoot(band)

  }

  private def squareRoot(band: Array[Byte]):Array[Byte] = {

    var result = new Array[Byte](band.length)

    result = band.map(x=>((Math.sqrt(x)*100).round/100.toDouble).toByte)
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Bitwise AND between two bands
case class RS_BitwiseAnd(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]

    assert(band1.length == band2.length)

    bitwiseAnd(band1, band2)

  }

  private def bitwiseAnd(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      result(i) = (band1(i).toInt & band2(i).toInt).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Bitwise OR between two bands
case class RS_BitwiseOr(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    bitwiseOr(band1, band2)

  }

  private def bitwiseOr(band1: Array[Byte], band2: Array[Byte]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = (band1(i).toInt | band2(i).toInt).toByte
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// if a value in band1 and band2 are different,value from band1 ins returned else return 0
case class RS_LogicalDifference(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    logicalDifference(band1, band2)

  }

  private def logicalDifference(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      if(band1(i).toDouble != band2(i).toDouble)
        {
          result(i) = band1(i)
        }
      else
        {
          result(i) = 0.toByte
        }
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// If a value in band 1 is not equal to 0, band1 is returned else value from band2 is returned
case class RS_LogicalOver(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    logicalOver(band1, band2)

  }

  private def logicalOver(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      if(band1(i).toDouble != 0.0)
      {
        result(i) = band1(i)
      }
      else
      {
        result(i) = band2(i)
      }
    }
    result

  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate logical AND between two bands where values for two bands can be either 1.0 or 0.0
case class RS_LogicalAND(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    new GenericArrayData(logicalAND(band1, band2))

  }

  private def logicalAND(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      if(band1(i).toDouble == 1.0 && band2(i).toDouble == 1.0) {
        result(i) = 1.toByte
      }
      else {
        result(i) = 0.toByte
      }
    }
      result
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate logical OR between two bands where values for two bands can be either 1.0 or 0.0
case class RS_LogicalOR(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
    assert(band1.length == band2.length)

    new GenericArrayData(logicalOR(band1, band2))

  }

  private def logicalOR(band1: Array[Byte], band2: Array[Byte]):Array[Byte] = {

    val result = new Array[Byte](band1.length)
    for(i<-0 until band1.length) {
      if(band1(i).toDouble == 0.0 && band2(i).toDouble == 0.0) {
        result(i) = 0.toByte
      }
      else {
        result(i) = 1.toByte
      }
    }
    result
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions
}



