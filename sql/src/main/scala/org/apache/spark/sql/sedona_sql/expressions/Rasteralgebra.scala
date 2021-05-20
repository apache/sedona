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
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.coverage.grid.io.GridFormatFinder
import org.geotools.coverage.grid.{GridCoordinates2D, GridCoverage2D}
import org.geotools.util.factory.Hints
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import java.io.IOException


// Calculate Normalized Difference between two bands
case class rs_NormalizedDifference(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val ndvi = normalizeddifference(band1, band2)

    new GenericArrayData(ndvi)
  }
  private def normalizeddifference(band1: Array[Double], band2: Array[Double]): Array[Double] = {

    val result = new Array[Double](band1.length)
    for (i <- 0 until band1.length) {
      if (band1(i) == 0) {
        band1(i) = -1
      }
      if (band2(i) == 0) {
        band2(i) = -1
      }

      result(i) = (band2(i) - band1(i)) / (band2(i) + band1(i))
    }

    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Calculate mean value for a particular band
case class rs_Mean(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val mean = calculateMean(band1)
    mean
  }

  private def calculateMean(band:Array[Double]):Double = {

    band.toList.sum/band.length
  }


  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate mode of a particular band
case class rs_Mode(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val mean = calculateMode(band1)
    mean
  }

  private def calculateMode(band:Array[Double]):Array[Double] = {

    val grouped = band.toList.groupBy(x => x).mapValues(_.size)
    val modeValue = grouped.maxBy(_._2)._2
    val modes = grouped.filter(_._2 == modeValue).map(_._1)
    modes.toArray

  }
  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate a eucledian distance between two pixels
case class rs_EucledianDistance(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length > 1 && inputExpressions.length <=5)
    val imageURL = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val x1 = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    val y1 = inputExpressions(2).eval(inputRow).asInstanceOf[Int]
    val x2 = inputExpressions(3).eval(inputRow).asInstanceOf[Int]
    val y2 = inputExpressions(4).eval(inputRow).asInstanceOf[Int]
    findDistanceBetweenTwo(imageURL, x1, y1, x2, y2)

  }

  private def findDistanceBetweenTwo(url: String, x1: Int, y1: Int, x2: Int, y2: Int):Double = {

    val format = GridFormatFinder.findFormat(url)
    val hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
    val reader = format.getReader(url, hints)
    var coverage:GridCoverage2D = null

    try coverage = reader.read(null)
    catch {
      case giveUp: IOException =>
        throw new RuntimeException(giveUp)
    }
    reader.dispose()
    val coordinate2D_source = new GridCoordinates2D(x1, y1)
    val coordinates2D_target = new GridCoordinates2D(x2, y2)
    val result_source = coverage.getGridGeometry.gridToWorld(coordinate2D_source)
    val result_target = coverage.getGridGeometry.gridToWorld(coordinates2D_target)
    val factory = new GeometryFactory
    val point1 = factory.createPoint((new Coordinate(result_source.getOrdinate(0), result_source.getOrdinate(1))))
    val point2 = factory.createPoint((new Coordinate(result_target.getOrdinate(0), result_target.getOrdinate(1))))

    point1.distance(point2)

  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// fetch a particular region from a raster image
case class rs_FetchRegion(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 5)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val lowX = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    val lowY = inputExpressions(2).eval(inputRow).asInstanceOf[Int]
    val highX = inputExpressions(3).eval(inputRow).asInstanceOf[Int]
    val highY = inputExpressions(4).eval(inputRow).asInstanceOf[Int]

    regionEnclosed(band, lowX, lowY, highX, highY)

  }

  private def regionEnclosed(Band: Array[Double], lowX: Int, lowY: Int, highX: Int, highY: Int):Array[Double] = {

    val rows, cols = Math.sqrt(Band.length).toInt
    val result2D = Array.ofDim[Int](rows, cols)
    val result1D = new Array[Double]((highX - lowX + 1) * (highY - lowY + 1))

    var i = 0
    while ( {
      i < rows
    }) {
      System.arraycopy(Band, i*cols, result2D(i), 0, cols)
      i += 1
    }

    val k = 0
    for(i<-lowX until highX +1) {
      for(j<-lowY until highY + 1) {
        result1D(k) = result2D(i)(j)
      }
    }

    result1D

  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// Fetch all the band values greater than a particular value
case class rs_GreaterThan(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    findGreaterThan(band, target)

  }

 private def findGreaterThan(band: Array[Double], target: Int):Array[Double] = {

   val result = new Array[Double](band.length)
   for(i<-0 until band.length) {
     if(band(i)>target) {
       result(i) = 1
     }
     else {
       result(i) = 0
     }
   }
   result
 }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// fetch all the band values greater than equal to particular value
case class rs_GreaterThanEqual(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    findGreaterThanEqual(band, target)

  }

  private def findGreaterThanEqual(band: Array[Double], target: Int):Array[Double] = {

    val result = new Array[Double](band.length)
    for(i<-0 until band.length) {
      if(band(i)>=target) {
        result(i) = 1
      }
      else {
        result(i) = 0
      }
    }
    result
  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Fetch band values less than particular value
case class rs_LessThan(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    findLessThan(band, target)

  }

  private def findLessThan(band: Array[Double], target: Int):Array[Double] = {

    val result = new Array[Double](band.length)
    for(i<-0 until band.length) {
      if(band(i)<target) {
        result(i) = 1
      }
      else {
        result(i) = 0
      }
    }
    result
  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Fetch band values less than equal to particular value
case class rs_LessThanEqual(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    findLessThanEqual(band, target)

  }

  private def findLessThanEqual(band: Array[Double], target: Int):Array[Double] = {

    val result = new Array[Double](band.length)
    for(i<-0 until band.length) {
      if(band(i)<=target) {
        result(i) = 1
      }
      else {
        result(i) = 0
      }
    }
    result
  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Count number of occurences of a particular value in a band
case class rs_Count(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    findCount(band, target)

  }

  private def findCount(band: Array[Double], target: Int):Int = {

    var result = 0
    for(i<-0 until band.length) {
      if(band(i)==target) {
        result+=1
      }

    }
    result
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions
}

// Multiply a factor to all values of a band
case class rs_Multiply(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val target = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    multiply(band, target)

  }

  private def multiply(band: Array[Double], target: Int):Array[Double] = {

    var result = new Array[Double](band.length)
    for(i<-0 until band.length) {

      result(i) = band(i)*target

      }
    result

    }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Add two bands
case class rs_AddBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[UnsafeArrayData].toDoubleArray()
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[UnsafeArrayData].toDoubleArray()
    assert(band1.length == band2.length)

    addBands(band1, band2)

  }

  private def addBands(band1: Array[Double], band2: Array[Double]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = band1(i) + band2(i)
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Subtract two bands
case class rs_SubtractBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    assert(band1.length == band2.length)

    subtractBands(band1, band2)

  }

  private def subtractBands(band1: Array[Double], band2: Array[Double]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = band2(i) - band1(i)
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Multiple two bands
case class rs_MultiplyBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    assert(band1.length == band2.length)

    multiplyBands(band1, band2)

  }

  private def multiplyBands(band1: Array[Double], band2: Array[Double]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = band1(i) * band2(i)
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Divide two bands
case class rs_DivideBands(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    assert(band1.length == band2.length)

    divideBands(band1, band2)

  }

  private def divideBands(band1: Array[Double], band2: Array[Double]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = band1(i) / band2(i)
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Modulo of a band
case class rs_Modulo(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val dividend = inputExpressions(1).eval(inputRow).asInstanceOf[Double]


    modulo(band1, dividend)

  }

  private def modulo(band: Array[Double], dividend:Double):Array[Double] = {

    val result = new Array[Double](band.length)
    for(i<-0 until band.length) {
      result(i) = band(i) % dividend
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Square root of values in a band
case class rs_SquareRoot(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    squareRoot(band)

  }

  private def squareRoot(band: Array[Double]):Array[Double] = {

    val result = new Array[Double](band.length)
    for(i<-0 until band.length) {
      result(i) = Math.sqrt(band(i))
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Bitwise AND between two bands
case class rs_BitwiseAnd(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    assert(band1.length == band2.length)

    bitwiseAnd(band1, band2)

  }

  private def bitwiseAnd(band1: Array[Double], band2: Array[Double]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = band1(i).toInt & band2(i).toInt
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Bitwise OR between two bands
case class rs_BitwiseOr(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(1).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    assert(band1.length == band2.length)

    bitwiseOr(band1, band2)

  }

  private def bitwiseOr(band1: Array[Double], band2: Array[Double]):Array[Double] = {

    val result = new Array[Double](band1.length)
    for(i<-0 until band1.length) {
      result(i) = band1(i).toInt | band2(i).toInt
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Negate operator on a band
case class rs_Negate(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()

    negate(band)

  }

  private def negate(band: Array[Double]):Array[Double] = {

    val result = new Array[Double](band.length)
    for(i<-0 until band.length) {
      result(i) = ~band(i).toInt
    }
    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}