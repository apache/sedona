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
package org.apache.spark.sql.sedona_sql.io.sedonainfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.sedona.common.raster.RasterAccessors
import org.apache.sedona.common.raster.RasterBandAccessors
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStream
import org.apache.sedona.common.utils.RasterUtils
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.referencing.crs.DefaultEngineeringCRS

import scala.collection.mutable
import scala.util.Try

/**
 * Extracts metadata from GeoTIFF files without decoding pixel data. Uses GeoTools GeoTiffReader
 * which lazily decodes the RenderedImage.
 */
object GeoTiffMetadataExtractor extends RasterFileMetadataExtractor {

  override def driver: String = "GTiff"

  override def canHandle(path: Path): Boolean = {
    val name = path.getName.toLowerCase
    name.endsWith(".tif") || name.endsWith(".tiff")
  }

  override def extract(
      path: Path,
      fileSize: Long,
      configuration: Configuration): RasterFileMetadata = {
    val imageStream = new HadoopImageInputStream(path, configuration)
    var reader: GeoTiffReader = null
    var raster: GridCoverage2D = null
    try {
      reader = new GeoTiffReader(
        imageStream,
        new org.geotools.util.factory.Hints(
          org.geotools.util.factory.Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER,
          java.lang.Boolean.TRUE))
      raster = reader.read(null)

      val width = RasterAccessors.getWidth(raster)
      val height = RasterAccessors.getHeight(raster)
      val numBands = RasterAccessors.numBands(raster)
      val srid = RasterAccessors.srid(raster)

      val crsStr = Try {
        val crs = raster.getCoordinateReferenceSystem
        if (crs == null || crs.isInstanceOf[DefaultEngineeringCRS]) null
        else crs.toWKT
      }.getOrElse(null)

      val affine = RasterUtils.getGDALAffineTransform(raster)
      val env = raster.getEnvelope2D

      val image = raster.getRenderedImage
      val tileWidth = image.getTileWidth
      val tileHeight = image.getTileHeight
      val isTiled = tileWidth < width || tileHeight < height

      val bands = extractBands(raster, numBands, tileWidth, tileHeight)
      val overviews = extractOverviews(reader, width, height)
      val metadata = extractMetadata(reader)
      val compression = extractCompression(reader)

      RasterFileMetadata(
        path = path.toString,
        driver = driver,
        fileSize = fileSize,
        width = width,
        height = height,
        numBands = numBands,
        srid = srid,
        crs = crsStr,
        upperLeftX = affine.getTranslateX,
        upperLeftY = affine.getTranslateY,
        scaleX = affine.getScaleX,
        scaleY = affine.getScaleY,
        skewX = affine.getShearX,
        skewY = affine.getShearY,
        envelopeMinX = env.getMinX,
        envelopeMinY = env.getMinY,
        envelopeMaxX = env.getMaxX,
        envelopeMaxY = env.getMaxY,
        bands = bands,
        overviews = overviews,
        metadata = metadata,
        isTiled = isTiled,
        compression = compression)
    } finally {
      if (raster != null) raster.dispose(true)
      if (reader != null) reader.dispose()
      imageStream.close()
    }
  }

  private def extractBands(
      raster: GridCoverage2D,
      numBands: Int,
      tileWidth: Int,
      tileHeight: Int): Seq[BandMetadata] = {
    (1 to numBands).map { i =>
      val dataType = Try(RasterBandAccessors.getBandType(raster, i)).getOrElse(null)
      val noDataValue = Try(RasterBandAccessors.getBandNoDataValue(raster, i)).getOrElse(null)
      val description = Try {
        val desc = raster.getSampleDimension(i - 1).getDescription
        if (desc != null) desc.toString(java.util.Locale.ROOT) else null
      }.getOrElse(null)
      val unit = Try {
        val units = raster.getSampleDimension(i - 1).getUnits
        if (units != null) units.toString else null
      }.getOrElse(null)

      BandMetadata(
        band = i,
        dataType = dataType,
        colorInterpretation = description,
        noDataValue = noDataValue,
        blockWidth = tileWidth,
        blockHeight = tileHeight,
        description = description,
        unit = unit)
    }
  }

  private def extractOverviews(
      reader: GeoTiffReader,
      fullWidth: Int,
      fullHeight: Int): Seq[OverviewMetadata] = {
    try {
      // Use DatasetLayout to get actual internal overview count (not synthetic tile-based levels)
      val layout = reader.getDatasetLayout
      if (layout == null) return Seq.empty

      val numOverviews = layout.getNumInternalOverviews
      if (numOverviews <= 0) return Seq.empty

      val resolutionLevels = reader.getResolutionLevels
      if (resolutionLevels == null || resolutionLevels.length <= 1) return Seq.empty

      // Only report the actual internal overviews, not synthetic resolution levels
      val count = Math.min(numOverviews, resolutionLevels.length - 1)
      val fullResX = resolutionLevels(0)(0)
      val fullResY = resolutionLevels(0)(1)
      (1 to count).map { level =>
        val overviewResX = resolutionLevels(level)(0)
        val overviewResY = resolutionLevels(level)(1)
        OverviewMetadata(
          level = level,
          width = Math.round(fullWidth.toDouble * fullResX / overviewResX).toInt,
          height = Math.round(fullHeight.toDouble * fullResY / overviewResY).toInt)
      }
    } catch {
      case _: Exception => Seq.empty
    }
  }

  private def extractMetadata(reader: GeoTiffReader): Map[String, String] = {
    try {
      val metadata = reader.getMetadata
      if (metadata == null) return Map.empty

      val rootNode = metadata.getRootNode
      if (rootNode == null) return Map.empty

      val map = new mutable.LinkedHashMap[String, String]()
      extractMetadataFromNode(rootNode, "", map)
      map.toMap
    } catch {
      case _: Exception => Map.empty
    }
  }

  private def extractMetadataFromNode(
      node: org.w3c.dom.Node,
      prefix: String,
      map: mutable.LinkedHashMap[String, String]): Unit = {
    if (node == null) return

    val attrs = node.getAttributes
    if (attrs != null) {
      val nameAttr = attrs.getNamedItem("name")
      val valueAttr = attrs.getNamedItem("value")
      if (nameAttr != null && valueAttr != null) {
        val key =
          if (prefix.nonEmpty) s"$prefix.${nameAttr.getNodeValue}"
          else nameAttr.getNodeValue
        map.put(key, valueAttr.getNodeValue)
      }
    }

    val children = node.getChildNodes
    if (children != null) {
      val childPrefix = if (prefix.nonEmpty && node.getNodeName != "#document") {
        s"$prefix.${node.getNodeName}"
      } else if (node.getNodeName != "#document") {
        node.getNodeName
      } else {
        prefix
      }
      for (i <- 0 until children.getLength) {
        extractMetadataFromNode(children.item(i), childPrefix, map)
      }
    }
  }

  private def extractCompression(reader: GeoTiffReader): String = {
    try {
      val metadata = reader.getMetadata
      if (metadata == null) return null

      val rootNode = metadata.getRootNode
      if (rootNode == null) return null

      findCompressionInNode(rootNode)
    } catch {
      case _: Exception => null
    }
  }

  private def findCompressionInNode(node: org.w3c.dom.Node): String = {
    if (node == null) return null

    val attrs = node.getAttributes
    if (attrs != null) {
      val nameAttr = attrs.getNamedItem("name")
      val valueAttr = attrs.getNamedItem("value")
      if (nameAttr != null && valueAttr != null &&
        nameAttr.getNodeValue.equalsIgnoreCase("Compression")) {
        return valueAttr.getNodeValue
      }
    }

    val children = node.getChildNodes
    if (children != null) {
      for (i <- 0 until children.getLength) {
        val result = findCompressionInNode(children.item(i))
        if (result != null) return result
      }
    }
    null
  }
}
