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

import org.apache.sedona.common.raster.RasterAccessors
import org.apache.sedona.common.utils.RasterUtils
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.geotools.coverage.GridSampleDimension
import org.geotools.coverage.grid.GridCoverage2D

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


//trait TraitRSAggregateExec{
//
//  val initialRaster: GridCoverage2D = {
//    GridCoverage2D
//  }
//
//  val serde = ExpressionEncoder[GridCoverage2D]
//
////  def zero: GridCoverage2D = initialRaster
//
////  def bufferEncoder: ExpressionEncoder[GridCoverage2D] = serde
//
//  def outputEncoder: ExpressionEncoder[GridCoverage2D] = serde
//
//  def finish(out: GridCoverage2D): GridCoverage2D = out
//}

class BandData(var band: Either[Array[Double], Array[Int]], var index: Int, var gridSampleDimension: GridSampleDimension, var isIntegral: Boolean)

class RS_Union_Aggr extends Aggregator[(GridCoverage2D, Int), ArrayBuffer[BandData], GridCoverage2D]  {

  var width: Int = -1

  var height: Int = -1

  def zero: ArrayBuffer[BandData] = ArrayBuffer[BandData]()

  def checkRasterShape(raster: GridCoverage2D): Boolean = {
    // first iteration
    if (width == -1 && height == -1) {
      width = RasterAccessors.getWidth(raster)
      height = RasterAccessors.getHeight(raster)
      true
    }

    val widthNewRaster = RasterAccessors.getWidth(raster)
    val heightNewRaster = RasterAccessors.getHeight(raster)

    if (width == widthNewRaster && height == heightNewRaster)
      true
    else
      false
  }

  def reduce(buffer: ArrayBuffer[BandData], input: (GridCoverage2D, Int)): ArrayBuffer[BandData] = {
    val raster = input._1
    if (!checkRasterShape(raster)) {
      // TODO figure out a better error message
      throw new IllegalArgumentException(("Please provide rasters of same shape."))
    }

    val rasterData = RasterUtils.getRaster(raster.getRenderedImage)
    val isIntegral = RasterUtils.isDataTypeIntegral(rasterData.getDataBuffer.getDataType)
    var band: Either[Array[Double], Array[Int]] = null

    if (isIntegral)
      band = Right(rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Int]]))
    else
      band = Left(rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Double]]))

    val bandData = new BandData(band, input._2, raster.getSampleDimension(0), isIntegral)
    buffer += bandData
  }

  def merge(buffer1: ArrayBuffer[BandData], buffer2: ArrayBuffer[BandData]): ArrayBuffer[BandData] = {
    ArrayBuffer.concat(buffer1, buffer2)
  }

  def finish(merged: ArrayBuffer[BandData]): GridCoverage2D = {
    // create a raster
    val sortedMerged = merged.sortBy(_.index)


  }

  val serde = ExpressionEncoder[GridCoverage2D]

  val bufferSerde = ExpressionEncoder[ArrayBuffer[BandData]]

  def outputEncoder: ExpressionEncoder[GridCoverage2D] = serde

  def bufferEncoder: Encoder[ArrayBuffer[BandData]] = bufferSerde
}
