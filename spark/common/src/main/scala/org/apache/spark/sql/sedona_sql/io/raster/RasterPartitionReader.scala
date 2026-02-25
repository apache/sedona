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
package org.apache.spark.sql.sedona_sql.io.raster

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.sedona.common.raster.RasterConstructors
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStream
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.io.raster.RasterPartitionReader.rasterToInternalRows
import org.apache.spark.sql.sedona_sql.io.raster.RasterTable.{MAX_AUTO_TILE_SIZE, RASTER, RASTER_NAME, TILE_X, TILE_Y}
import org.apache.spark.sql.types.StructType
import org.geotools.coverage.grid.GridCoverage2D

import java.net.URI
import scala.collection.JavaConverters._

class RasterPartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    dataSchema: StructType,
    rasterOptions: RasterOptions)
    extends PartitionReader[InternalRow] {

  // Track the current file index we're processing
  private var currentFileIndex = 0

  // Current raster being processed
  private var currentRaster: GridCoverage2D = _

  // Current image input stream (must be kept open while the raster is in use)
  private var currentImageStream: HadoopImageInputStream = _

  // Current row
  private var currentRow: InternalRow = _

  // Iterator for the current file's tiles
  private var currentIterator: Iterator[InternalRow] = Iterator.empty

  override def next(): Boolean = {
    // If current iterator has more elements, return true
    if (currentIterator.hasNext) {
      currentRow = currentIterator.next()
      return true
    }

    // If current iterator is exhausted, but we have more files, load the next file
    if (currentFileIndex < partitionedFiles.length) {
      loadNextFile()
      if (currentIterator.hasNext) {
        currentRow = currentIterator.next()
        return true
      }
    }

    // No more data
    false
  }

  override def get(): InternalRow = {
    currentRow
  }

  override def close(): Unit = {
    if (currentRaster != null) {
      currentRaster.dispose(true)
      currentRaster = null
    }
    if (currentImageStream != null) {
      currentImageStream.close()
      currentImageStream = null
    }
  }

  private def loadNextFile(): Unit = {
    // Clean up previous raster and stream if exists
    if (currentRaster != null) {
      currentRaster.dispose(true)
      currentRaster = null
    }
    if (currentImageStream != null) {
      currentImageStream.close()
      currentImageStream = null
    }

    if (currentFileIndex >= partitionedFiles.length) {
      currentIterator = Iterator.empty
      return
    }

    val partition = partitionedFiles(currentFileIndex)
    val path = new Path(new URI(partition.filePath.toString()))

    try {
      // Open a stream-based reader instead of materializing the entire file as byte[].
      // This avoids the 2 GB byte[] limit and reduces memory pressure for large files.
      currentImageStream = new HadoopImageInputStream(path, configuration)

      // Create in-db GridCoverage2D from GeoTiff stream. The RenderedImage is lazy -
      // pixel data will only be decoded when accessed via image.getData(Rectangle).
      currentRaster = RasterConstructors.fromGeoTiff(currentImageStream)
      currentIterator = rasterToInternalRows(currentRaster, dataSchema, rasterOptions, path)
      currentFileIndex += 1
    } catch {
      case e: Exception =>
        if (currentRaster != null) {
          currentRaster.dispose(true)
          currentRaster = null
        }
        if (currentImageStream != null) {
          currentImageStream.close()
          currentImageStream = null
        }
        throw e
    }
  }
}

object RasterPartitionReader {
  def rasterToInternalRows(
      currentRaster: GridCoverage2D,
      dataSchema: StructType,
      rasterOptions: RasterOptions,
      path: Path): Iterator[InternalRow] = {
    val retile = rasterOptions.retile
    val tileWidth = rasterOptions.tileWidth
    val tileHeight = rasterOptions.tileHeight
    val padWithNoData = rasterOptions.padWithNoData

    val writer = new UnsafeRowWriter(dataSchema.length)
    writer.resetRowWriter()

    // Extract the file name from the path
    val fileName = path.getName

    if (retile) {
      val (tw, th) = (tileWidth, tileHeight) match {
        case (Some(tw), Some(th)) => (tw, th)
        case (None, None) =>
          // Use the internal tile size of the input raster
          val tw = currentRaster.getRenderedImage.getTileWidth
          val th = currentRaster.getRenderedImage.getTileHeight
          val tileSizeError = {
            """To resolve this issue, you can try one of the following methods:
            |  1. Disable retile by setting `.option("retile", "false")`.
            |  2. Explicitly set `tileWidth` and `tileHeight`.
            |  3. Convert the raster to a Cloud Optimized GeoTIFF (COG) using tools like `gdal_translate`.
            |""".stripMargin
          }
          if (tw >= MAX_AUTO_TILE_SIZE || th >= MAX_AUTO_TILE_SIZE) {
            throw new IllegalArgumentException(
              s"Internal tile size of $path is too large ($tw x $th). " + tileSizeError)
          }
          if (tw == 0 || th == 0) {
            throw new IllegalArgumentException(
              s"Internal tile size of $path contains zero ($tw x $th). " + tileSizeError)
          }
          if (tw.toDouble / th > 10.0 || th.toDouble / tw > 10.0) {
            throw new IllegalArgumentException(
              s"Internal tile shape of $path is too thin ($tw x $th). " + tileSizeError)
          }
          (tw, th)
        case _ =>
          throw new IllegalArgumentException("Both tileWidth and tileHeight must be set")
      }

      val iter =
        RasterConstructors.generateTiles(currentRaster, null, tw, th, padWithNoData, Double.NaN)
      iter.asScala.map { tile =>
        val tileRaster = tile.getCoverage
        writer.reset()
        writeRaster(writer, dataSchema, tileRaster, tile.getTileX, tile.getTileY, fileName)
        tileRaster.dispose(true)
        writer.getRow
      }
    } else {
      writeRaster(writer, dataSchema, currentRaster, 0, 0, fileName)
      Iterator.single(writer.getRow)
    }
  }

  private def writeRaster(
      writer: UnsafeRowWriter,
      dataSchema: StructType,
      raster: GridCoverage2D,
      x: Int,
      y: Int,
      fileName: String): Unit = {
    dataSchema.fieldNames.zipWithIndex.foreach {
      case (RASTER, i) => writer.write(i, RasterUDT.serialize(raster))
      case (TILE_X, i) => writer.write(i, x)
      case (TILE_Y, i) => writer.write(i, y)
      case (RASTER_NAME, i) =>
        if (fileName != null)
          writer.write(i, org.apache.spark.unsafe.types.UTF8String.fromString(fileName))
        else writer.setNullAt(i)
      case (other, _) =>
        throw new IllegalArgumentException(s"Unsupported field name: $other")
    }
  }
}
