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
package org.apache.sedona.sql.datasources.geopackage.transform

import org.apache.sedona.sql.datasources.geopackage.model.ImageFileFormat.ImageFileFormat
import org.apache.sedona.sql.datasources.geopackage.model.{ImageFileFormat, TileMetadata, TileRowMetadata}
import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory}
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.geometry.GeneralEnvelope

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

object Image {

  private val WEBP_HEX = "52494646"
  private val PNG_HEX = "89504E470D0A1A0A"
  private val TIFF_HEX = Seq("49492A00", "4D4D002A")
  private val JPEG_HEX = Seq("FFD8FFE0", "FFD8FFE1", "FFD8FFE8")

  def readImageFile(
      byteArray: Array[Byte],
      tileMetadata: TileMetadata,
      tileRowMetadata: TileRowMetadata): GridCoverage2D = {
    val format = detectFileFormat(byteArray)

    format match {
      case ImageFileFormat.JPEG | ImageFileFormat.PNG =>
        val image = readImageFromBinary(byteArray)
        val gridCoverage = createGridCoverage2D(image, tileMetadata, tileRowMetadata)
        gridCoverage
      case ImageFileFormat.WEBP =>
        throw new UnsupportedOperationException("WebP format is not supported")
      case ImageFileFormat.TIFF =>
        val reader = new GeoTiffReader(new ByteArrayInputStream(byteArray))
        reader.read(null)
      case _ =>
        null
    }
  }

  def detectFileFormat(byteArray: Array[Byte]): ImageFileFormat = {
    val magicBytes = byteArray.take(12)
    val magicHex = bytesToHex(magicBytes)

    if (magicHex.startsWith(WEBP_HEX)) {
      val webpCheck = new String(byteArray.slice(8, 12))
      if (webpCheck == "WEBP") {
        return ImageFileFormat.WEBP
      }
    }

    if (magicHex.startsWith(PNG_HEX)) {
      return ImageFileFormat.PNG
    }

    if (JPEG_HEX.exists(magicHex.startsWith)) {
      return ImageFileFormat.JPEG
    }

    if (TIFF_HEX.exists(magicHex.startsWith)) {
      return ImageFileFormat.TIFF
    }

    ImageFileFormat.UNKNOWN
  }

  def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map("%02X".format(_)).mkString
  }

  def createGridCoverage2D(
      image: BufferedImage,
      tileMetadata: TileMetadata,
      tileRowMetadata: TileRowMetadata): GridCoverage2D = {
    val envelope = tileMetadata.getEnvelope(tileRowMetadata)

    val genevelope = new GeneralEnvelope(
      Array(envelope.minX, envelope.minY),
      Array(envelope.maxX, envelope.maxY))

    genevelope.setCoordinateReferenceSystem(tileMetadata.getSRID())
    val coverageFactory = new GridCoverageFactory()

    coverageFactory.create("coverage", image, genevelope)
  }

  def readImageFromBinary(imageBinary: Array[Byte]): BufferedImage = {
    val inputStream = new ByteArrayInputStream(imageBinary)
    ImageIO.read(inputStream)
  }
}
