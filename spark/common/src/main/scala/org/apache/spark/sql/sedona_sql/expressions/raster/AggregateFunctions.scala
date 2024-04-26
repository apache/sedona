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

import org.apache.sedona.common.raster.serde.Serde
import org.apache.sedona.common.raster.{RasterAccessors, RasterBandAccessors}
import org.apache.sedona.common.utils.RasterUtils
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.geotools.coverage.GridSampleDimension
import org.geotools.coverage.grid.GridCoverage2D

import java.awt.image.WritableRaster
import javax.media.jai.RasterFactory
import scala.collection.mutable.ArrayBuffer

case class BandData(
                     var bandsData: Array[Array[Double]],
                     var index: Int,
                     var serializedRaster: Array[Byte],
                     var serializedSampleDimensions: Array[Array[Byte]]
                   )


/**
 * Return a raster containing bands at given indexes from all rasters in a given column
 */
class RS_Union_Aggr extends Aggregator[(GridCoverage2D, Int), ArrayBuffer[BandData], GridCoverage2D]  {

  def zero: ArrayBuffer[BandData] = ArrayBuffer[BandData]()

  def reduce(buffer: ArrayBuffer[BandData], input: (GridCoverage2D, Int)): ArrayBuffer[BandData] = {
      val raster = input._1
      val renderedImage = raster.getRenderedImage
      val numBands = renderedImage.getSampleModel.getNumBands
      val width = renderedImage.getWidth
      val height = renderedImage.getHeight

      // First check if this is the first raster to set dimensions or validate against existing dimensions
      if (buffer.nonEmpty) {
        val referenceRaster = Serde.deserialize(buffer.head.serializedRaster)
        val refWidth = RasterAccessors.getWidth(referenceRaster)
        val refHeight = RasterAccessors.getHeight(referenceRaster)
        if (width != refWidth || height != refHeight) {
          throw new IllegalArgumentException("All rasters must have the same dimensions")
        }
      }

      // Extract data for each band
      val rasterData = renderedImage.getData
      val bandsData = Array.ofDim[Double](numBands, width * height)
      val serializedSampleDimensions = new Array[Array[Byte]](numBands)

      for (band <- 0 until numBands) {
        bandsData(band) = rasterData.getSamples(0, 0, width, height, band, new Array[Double](width * height))
        serializedSampleDimensions(band) = Serde.serializeGridSampleDimension(raster.getSampleDimension(band))
      }

      buffer += BandData(bandsData, input._2, Serde.serialize(raster), serializedSampleDimensions)
      buffer
    }


  def merge(buffer1: ArrayBuffer[BandData], buffer2: ArrayBuffer[BandData]): ArrayBuffer[BandData] = {
    val combined = ArrayBuffer.concat(buffer1, buffer2)
    if (combined.map(_.index).distinct.length != combined.length) {
      throw new IllegalArgumentException("Indexes shouldn't be repeated.")
    }
    combined
  }


  def finish(merged: ArrayBuffer[BandData]): GridCoverage2D = {
    val sortedMerged = merged.sortBy(_.index)
    if (sortedMerged.zipWithIndex.exists { case (band, idx) =>
      if (idx > 0) (band.index - sortedMerged(idx - 1).index) != (sortedMerged(1).index - sortedMerged(0).index)
      else false
    }) {
      throw new IllegalArgumentException("Index should be in an arithmetic sequence.")
    }

    val numBands = sortedMerged.length
    val referenceRaster = Serde.deserialize(sortedMerged.head.serializedRaster)
    val width = RasterAccessors.getWidth(referenceRaster)
    val height = RasterAccessors.getHeight(referenceRaster)
    val dataTypeCode = RasterUtils.getRaster(referenceRaster.getRenderedImage).getDataBuffer.getDataType
    val resultRaster: WritableRaster = RasterFactory.createBandedRaster(dataTypeCode, width, height, numBands, null)
    val gridSampleDimensions = sortedMerged.flatMap(_.serializedSampleDimensions.map(Serde.deserializeGridSampleDimension)).toArray

    var currentBand = 0
    sortedMerged.foreach { bandData =>
      bandData.bandsData.foreach { band =>
        resultRaster.setSamples(0, 0, width, height, currentBand, band)
        currentBand += 1
      }
    }
//    RasterUtils.create(resultRaster, referenceRaster.getGridGeometry, gridSampleDimensions)
    val noDataValue = RasterBandAccessors.getBandNoDataValue(referenceRaster)
    RasterUtils.clone(resultRaster, referenceRaster.getGridGeometry, gridSampleDimensions, referenceRaster, noDataValue, true)
  }

  val serde = ExpressionEncoder[GridCoverage2D]

  val bufferSerde = ExpressionEncoder[ArrayBuffer[BandData]]

  def outputEncoder: ExpressionEncoder[GridCoverage2D] = serde

  def bufferEncoder: Encoder[ArrayBuffer[BandData]] = bufferSerde
}
