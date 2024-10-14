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
import org.apache.sedona.sql.utils.RasterSerializer
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.geotools.coverage.grid.GridCoverage2D

import java.awt.image.WritableRaster
import javax.media.jai.RasterFactory
import scala.collection.mutable.ArrayBuffer

case class BandData(index: Int, width: Int, height: Int, serializedRaster: Array[Byte])

/**
 * Return a raster containing bands at given indexes from all rasters in a given column
 */
class RS_Union_Aggr
    extends Aggregator[(GridCoverage2D, Int), ArrayBuffer[BandData], GridCoverage2D] {

  def zero: ArrayBuffer[BandData] = ArrayBuffer[BandData]()

  def reduce(
      buffer: ArrayBuffer[BandData],
      input: (GridCoverage2D, Int)): ArrayBuffer[BandData] = {
    val (raster, index) = input
    val renderedImage = raster.getRenderedImage
    val width = renderedImage.getWidth
    val height = renderedImage.getHeight
    val serializedRaster = RasterSerializer.serialize(raster)
    raster.dispose(true)

    // First check if this is the first raster to set dimensions or validate against existing dimensions
    if (buffer.nonEmpty) {
      val refWidth = buffer.head.width
      val refHeight = buffer.head.height
      if (width != refWidth || height != refHeight) {
        throw new IllegalArgumentException("All rasters must have the same dimensions")
      }
    }

    buffer += BandData(index, width, height, serializedRaster)
    buffer
  }

  def merge(
      buffer1: ArrayBuffer[BandData],
      buffer2: ArrayBuffer[BandData]): ArrayBuffer[BandData] = {
    if (buffer1.nonEmpty && buffer2.nonEmpty) {
      if (buffer1.head.width != buffer2.head.width || buffer1.head.height != buffer2.head.height) {
        throw new IllegalArgumentException("All rasters must have the same dimensions")
      }
    }
    val combined = ArrayBuffer.concat(buffer1, buffer2)
    if (combined.map(_.index).distinct.length != combined.length) {
      throw new IllegalArgumentException("Indexes shouldn't be repeated.")
    }
    combined
  }

  def finish(merged: ArrayBuffer[BandData]): GridCoverage2D = {
    val sortedMerged = merged.sortBy(_.index)
    if (sortedMerged.zipWithIndex.exists { case (band, idx) =>
        if (idx > 0)
          (band.index - sortedMerged(idx - 1).index) != (sortedMerged(1).index - sortedMerged(
            0).index)
        else false
      }) {
      throw new IllegalArgumentException("Index should be in an arithmetic sequence.")
    }

    val rasters = sortedMerged.map(d => RasterSerializer.deserialize(d.serializedRaster))
    try {
      val gridSampleDimensions = rasters.flatMap(_.getSampleDimensions).toArray
      val totalBands = rasters.map(_.getNumSampleDimensions).sum
      val referenceRaster = rasters.head
      val width = RasterAccessors.getWidth(referenceRaster)
      val height = RasterAccessors.getHeight(referenceRaster)
      val dataTypeCode =
        RasterUtils.getRaster(referenceRaster.getRenderedImage).getDataBuffer.getDataType
      val resultRaster: WritableRaster =
        RasterFactory.createBandedRaster(dataTypeCode, width, height, totalBands, null)

      var currentBand = 0
      rasters.foreach { raster =>
        var bandIndex = 0
        while (bandIndex < raster.getNumSampleDimensions) {
          if (RasterUtils.isDataTypeIntegral(dataTypeCode)) {
            val band = RasterUtils
              .getRaster(raster.getRenderedImage)
              .getSamples(0, 0, width, height, bandIndex, new Array[Int](width * height))
            resultRaster.setSamples(0, 0, width, height, currentBand, band)
          } else {
            val band = RasterUtils
              .getRaster(raster.getRenderedImage)
              .getSamples(0, 0, width, height, bandIndex, new Array[Double](width * height))
            resultRaster.setSamples(0, 0, width, height, currentBand, band)
          }
          currentBand += 1
          bandIndex += 1
        }
      }

      val noDataValue = RasterBandAccessors.getBandNoDataValue(referenceRaster)
      RasterUtils.clone(
        resultRaster,
        referenceRaster.getGridGeometry,
        gridSampleDimensions,
        referenceRaster,
        noDataValue,
        false)
    } finally {
      rasters.foreach(_.dispose(true))
    }
  }

  val serde = ExpressionEncoder[GridCoverage2D]

  val bufferSerde = ExpressionEncoder[ArrayBuffer[BandData]]

  def outputEncoder: ExpressionEncoder[GridCoverage2D] = serde

  def bufferEncoder: Encoder[ArrayBuffer[BandData]] = bufferSerde
}
