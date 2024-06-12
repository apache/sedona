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
package org.apache.spark.sql.sedona_sql.expressions.raster

import org.apache.sedona.common.raster.MapAlgebra
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

/// Calculate Normalized Difference between two bands
case class RS_NormalizedDifference(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.normalizedDifference _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Calculate mean value for a particular band
case class RS_Mean(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.mean _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Calculate mode of a particular band
case class RS_Mode(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.mode _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// fetch a particular region from a raster image given particular indexes(Array[minx...maxX][minY...maxY])
case class RS_FetchRegion(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.fetchRegion _) {

  override def eval(inputRow: InternalRow): Any = {
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData].toDoubleArray()
    val coordinates = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData].toIntArray()
    val dim = inputExpressions(2).eval(inputRow).asInstanceOf[ArrayData].toIntArray()
    new GenericArrayData(MapAlgebra.fetchRegion(band, coordinates, dim))

  }
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Mark all the band values with 1 which are greater than a particular threshold
case class RS_GreaterThan(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.greaterThan _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Mark all the band values with 1 which are greater than or equal to a particular threshold
case class RS_GreaterThanEqual(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.greaterThanEqual _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Mark all the band values with 1 which are less than a particular threshold
case class RS_LessThan(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.lessThan _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Mark all the band values with 1 which are less than or equal to a particular threshold
case class RS_LessThanEqual(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.lessThanEqual _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Count number of occurrences of a particular value in a band
case class RS_CountValue(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.countValue _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Multiply a factor to all values of a band
case class RS_MultiplyFactor(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.multiplyFactor _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Add two bands
case class RS_Add(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.add _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Subtract two bands
case class RS_Subtract(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.subtract _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Multiple two bands
case class RS_Multiply(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.multiply _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Divide two bands
case class RS_Divide(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.divide _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Modulo of a band
case class RS_Modulo(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.modulo _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Square root of values in a band
case class RS_SquareRoot(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.squareRoot _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Bitwise AND between two bands
case class RS_BitwiseAnd(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.bitwiseAnd _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// Bitwise OR between two bands
case class RS_BitwiseOr(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.bitwiseOr _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// if a value in band1 and band2 are different,value from band1 ins returned else return 0
case class RS_LogicalDifference(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.logicalDifference _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

// If a value in band 1 is not equal to 0, band1 is returned else value from band2 is returned
case class RS_LogicalOver(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.logicalOver _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Normalize(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.normalize _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_AddBandFromArray(inputExpressions: Seq[Expression])
    extends InferredExpression(
      nullTolerantInferrableFunction3(MapAlgebra.addBandFromArray),
      nullTolerantInferrableFunction4(MapAlgebra.addBandFromArray),
      inferrableFunction2(MapAlgebra.addBandFromArray)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_BandAsArray(inputExpressions: Seq[Expression])
    extends InferredExpression(MapAlgebra.bandAsArray _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_MapAlgebra(inputExpressions: Seq[Expression])
    extends InferredExpression(
      nullTolerantInferrableFunction3(MapAlgebra.mapAlgebra),
      nullTolerantInferrableFunction4(MapAlgebra.mapAlgebra),
      nullTolerantInferrableFunction5(MapAlgebra.mapAlgebra)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
