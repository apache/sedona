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

import org.apache.sedona.common.raster.{RasterAccessors, RasterBandAccessors}
import org.apache.sedona.common.utils.RasterUtils
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.geotools.coverage.GridSampleDimension
import org.geotools.coverage.grid.GridCoverage2D

import java.awt.image.WritableRaster
import javax.media.jai.RasterFactory
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class BandData(var bandInt: Array[Int], var bandDouble: Array[Double], var index: Int, var isIntegral: Boolean)

/**
 * Return a raster containing bands at given indexes from all rasters in a given column
 */
class RS_Union_Aggr extends Aggregator[(GridCoverage2D, Int), ArrayBuffer[BandData], GridCoverage2D]  {

  var width: Int = -1

  var height: Int = -1

  var referenceRaster: GridCoverage2D = _

  var gridSampleDimension: mutable.Map[Int, GridSampleDimension] = new mutable.HashMap()

  def zero: ArrayBuffer[BandData] = ArrayBuffer[BandData]()

  /**
   * Valid raster shape to be the same in the given column
   */
  def checkRasterShape(raster: GridCoverage2D): Boolean = {
    // first iteration
    if (width == -1 && height == -1) {
      width = RasterAccessors.getWidth(raster)
      height = RasterAccessors.getHeight(raster)
      referenceRaster = raster
      true
    } else {
      val widthNewRaster = RasterAccessors.getWidth(raster)
      val heightNewRaster = RasterAccessors.getHeight(raster)

      if (width == widthNewRaster && height == heightNewRaster)
        true
      else
        false
    }
  }

  def reduce(buffer: ArrayBuffer[BandData], input: (GridCoverage2D, Int)): ArrayBuffer[BandData] = {
    val raster = input._1
    if (!checkRasterShape(raster)) {
      throw new IllegalArgumentException("Rasters provides should be of the same shape.")
    }
    if (gridSampleDimension.contains(input._2)) {
      throw new IllegalArgumentException("Indexes shouldn't be repeated. Index should be in an arithmetic sequence.")
    }

    val rasterData = RasterUtils.getRaster(raster.getRenderedImage)
    val isIntegral = RasterUtils.isDataTypeIntegral(rasterData.getDataBuffer.getDataType)

    val bandData = if (isIntegral) {
      val band = rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Int]])
      BandData(band, null, input._2, isIntegral)
    } else {
      val band = rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Double]])
      BandData(null, band, input._2, isIntegral)
    }
    gridSampleDimension = gridSampleDimension + (input._2 -> raster.getSampleDimension(0))

    buffer += bandData
  }

  def merge(buffer1: ArrayBuffer[BandData], buffer2: ArrayBuffer[BandData]): ArrayBuffer[BandData] = {
    ArrayBuffer.concat(buffer1, buffer2)
  }

  def finish(merged: ArrayBuffer[BandData]): GridCoverage2D = {
    val sortedMerged = merged.sortBy(_.index)
    val numBands = sortedMerged.length
    val rasterData = RasterUtils.getRaster(referenceRaster.getRenderedImage)
    val dataTypeCode = rasterData.getDataBuffer.getDataType
    val resultRaster: WritableRaster = RasterFactory.createBandedRaster(dataTypeCode, width, height, numBands, null)
    val gridSampleDimensions: Array[GridSampleDimension] = new Array[GridSampleDimension](numBands)
    var indexCheck = 1

    for (bandData: BandData <- sortedMerged) {
      if (bandData.index != indexCheck) {
        throw new IllegalArgumentException("Indexes should be in a valid arithmetic sequence.")
      }
      indexCheck += 1
      gridSampleDimensions(bandData.index - 1) = gridSampleDimension(bandData.index)
      if(RasterUtils.isDataTypeIntegral(dataTypeCode))
        resultRaster.setSamples(0, 0, width, height, (bandData.index - 1), bandData.bandInt)
      else
        resultRaster.setSamples(0, 0, width, height, bandData.index - 1, bandData.bandDouble)

    }
    val noDataValue = RasterBandAccessors.getBandNoDataValue(referenceRaster)
    RasterUtils.clone(resultRaster, referenceRaster.getGridGeometry, gridSampleDimensions, referenceRaster, noDataValue, true)
  }

  val serde = ExpressionEncoder[GridCoverage2D]

  val bufferSerde = ExpressionEncoder[ArrayBuffer[BandData]]

  def outputEncoder: ExpressionEncoder[GridCoverage2D] = serde

  def bufferEncoder: Encoder[ArrayBuffer[BandData]] = bufferSerde
}
