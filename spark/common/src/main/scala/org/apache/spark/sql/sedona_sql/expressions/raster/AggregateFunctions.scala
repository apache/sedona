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
                     var bandInt: Array[Int],
                     var bandDouble: Array[Double],
                     var index: Int,
                     var isIntegral: Boolean,
                     var serializedRaster: Array[Byte],
                     var serializedSampleDimension: Array[Byte]
                   )


/**
 * Return a raster containing bands at given indexes from all rasters in a given column
 */
class RS_Union_Aggr extends Aggregator[(GridCoverage2D, Int), ArrayBuffer[BandData], GridCoverage2D]  {

  def zero: ArrayBuffer[BandData] = ArrayBuffer[BandData]()

  def reduce(buffer: ArrayBuffer[BandData], input: (GridCoverage2D, Int)): ArrayBuffer[BandData] = {
    val raster = input._1
    val rasterData = RasterUtils.getRaster(raster.getRenderedImage)
    val isIntegral = RasterUtils.isDataTypeIntegral(rasterData.getDataBuffer.getDataType)

    // Serializing GridSampleDimension
    val serializedBytes = Serde.serializeGridSampleDimension(raster.getSampleDimension(0))

    // Check and set dimensions based on the first raster in the buffer
    if (buffer.isEmpty) {
      val width = RasterAccessors.getWidth(raster)
      val height = RasterAccessors.getHeight(raster)
      val referenceSerializedRaster = Serde.serialize(raster)

      buffer += BandData(
        if (isIntegral) rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Int]]) else null,
        if (!isIntegral) rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Double]]) else null,
        input._2,
        isIntegral,
        referenceSerializedRaster,
        serializedBytes
      )
    } else {
      val referenceRaster = Serde.deserialize(buffer.head.serializedRaster)
      val width = RasterAccessors.getWidth(referenceRaster)
      val height = RasterAccessors.getHeight(referenceRaster)

      if (width != RasterAccessors.getWidth(raster) || height != RasterAccessors.getHeight(raster)) {
        throw new IllegalArgumentException("All rasters must have the same dimensions")
      }

      buffer += BandData(
        if (isIntegral) rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Int]]) else null,
        if (!isIntegral) rasterData.getSamples(0, 0, width, height, 0, null.asInstanceOf[Array[Double]]) else null,
        input._2,
        isIntegral,
        Serde.serialize(raster),
        serializedBytes
      )
    }

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
    val gridSampleDimensions: Array[GridSampleDimension] = new Array[GridSampleDimension](numBands)

    for ((bandData, idx) <- sortedMerged.zipWithIndex) {
      // Deserializing GridSampleDimension
      gridSampleDimensions(idx) =  Serde.deserializeGridSampleDimension(bandData.serializedSampleDimension)

      if(bandData.isIntegral)
        resultRaster.setSamples(0, 0, width, height, idx, bandData.bandInt)
      else
        resultRaster.setSamples(0, 0, width, height, idx, bandData.bandDouble)
    }

    val noDataValue = RasterBandAccessors.getBandNoDataValue(referenceRaster)
    RasterUtils.clone(resultRaster, referenceRaster.getGridGeometry, gridSampleDimensions, referenceRaster, noDataValue, true)
  }

  val serde = ExpressionEncoder[GridCoverage2D]

  val bufferSerde = ExpressionEncoder[ArrayBuffer[BandData]]

  def outputEncoder: ExpressionEncoder[GridCoverage2D] = serde

  def bufferEncoder: Encoder[ArrayBuffer[BandData]] = bufferSerde
}
