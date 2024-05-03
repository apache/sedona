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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.{RasterEnhancer, RasterInputExpressionEnhancer}
import org.apache.spark.sql.types.{ArrayType, DataTypes, UserDefinedType}

import scala.reflect.runtime.universe.{Type, typeOf}
import org.geotools.coverage.grid.GridCoverage2D

object InferrableRasterTypes {
  implicit val gridCoverage2DInstance: InferrableType[GridCoverage2D] =
    new InferrableType[GridCoverage2D] {}
  implicit val gridCoverage2DArrayInstance: InferrableType[Array[GridCoverage2D]] =
    new InferrableType[Array[GridCoverage2D]] {}

  def isRasterType(t: Type): Boolean = t =:= typeOf[GridCoverage2D]
  def isRasterArrayType(t: Type): Boolean = t =:= typeOf[Array[GridCoverage2D]]

  val rasterUDT: UserDefinedType[_] = RasterUDT
  val rasterUDTArray: ArrayType = DataTypes.createArrayType(RasterUDT)

  def rasterExtractor(expr: Expression)(input: InternalRow): Any = expr.toRaster(input)

  def rasterSerializer(output: Any): Any =
    if (output != null) {
      output.asInstanceOf[GridCoverage2D].serialize
    } else {
      null
    }

  def rasterArraySerializer(output: Any): Any =
    if (output != null) {
      val rasters = output.asInstanceOf[Array[GridCoverage2D]]
      val serialized = rasters.map { raster =>
        val serialized = raster.serialize
        raster.dispose(true)
        serialized
      }
      ArrayData.toArrayData(serialized)
    } else {
      null
    }
}
