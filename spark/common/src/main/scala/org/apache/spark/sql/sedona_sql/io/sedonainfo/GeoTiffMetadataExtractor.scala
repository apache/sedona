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
      configuration: Configuration,
      requiredFields: Set[String] = Set.empty): RasterFileMetadata = {
    val imageStream = new HadoopImageInputStream(path, configuration)
    var reader: GeoTiffReader = null
    var raster: GridCoverage2D = null
    try {
      reader = new GeoTiffReader(
        imageStream,
        new org.geotools.util.factory.Hints(
          org.geotools.util.factory.Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER,
          java.lang.Boolean.TRUE))

      val needAll = requiredFields.isEmpty
      def need(field: String): Boolean = needAll || requiredFields.contains(field)

      // Extract TIFF IIO metadata BEFORE read() which may alter stream state
      val isTiled = if (need("isTiled")) hasTiffTag(reader, TAG_TILE_WIDTH) else false
      val photometric =
        if (need("bands")) extractPhotometricInterpretation(reader) else -1
      val tiffMetadata =
        if (need("metadata")) extractMetadata(reader) else Map.empty[String, String]
      val compression = if (need("compression")) extractCompression(reader) else null

      raster = reader.read(null)

      val width = RasterAccessors.getWidth(raster)
      val height = RasterAccessors.getHeight(raster)
      val numBands = RasterAccessors.numBands(raster)
      val srid = RasterAccessors.srid(raster)

      val crsStr = if (need("crs")) {
        Try {
          val crs = raster.getCoordinateReferenceSystem
          if (crs == null || crs.isInstanceOf[DefaultEngineeringCRS]) null
          else crs.toWKT
        }.getOrElse(null)
      } else null

      val affine = RasterUtils.getGDALAffineTransform(raster)
      val env = raster.getEnvelope2D

      val image = raster.getRenderedImage
      val tileWidth = image.getTileWidth
      val tileHeight = image.getTileHeight

      val bands =
        if (need("bands")) extractBands(raster, numBands, tileWidth, tileHeight, photometric)
        else Seq.empty
      val overviews =
        if (need("overviews")) extractOverviews(reader, width, height) else Seq.empty

      RasterFileMetadata(
        path = path.toString,
        driver = driver,
        fileSize = fileSize,
        width = width,
        height = height,
        numBands = numBands,
        srid = srid,
        crs = crsStr,
        geoTransform = GeoTransformMetadata(
          upperLeftX = affine.getTranslateX,
          upperLeftY = affine.getTranslateY,
          scaleX = affine.getScaleX,
          scaleY = affine.getScaleY,
          skewX = affine.getShearX,
          skewY = affine.getShearY),
        cornerCoordinates = CornerCoordinatesMetadata(
          minX = env.getMinX,
          minY = env.getMinY,
          maxX = env.getMaxX,
          maxY = env.getMaxY),
        bands = bands,
        overviews = overviews,
        metadata = tiffMetadata,
        isTiled = isTiled,
        compression = compression)
    } finally {
      if (raster != null) raster.dispose(true)
      if (reader != null) reader.dispose()
      imageStream.close()
    }
  }

  // TIFF tag constants
  private val TAG_TILE_WIDTH = 322

  // TIFF Photometric Interpretation values
  private val PHOTOMETRIC_MIN_IS_WHITE = 0
  private val PHOTOMETRIC_MIN_IS_BLACK = 1
  private val PHOTOMETRIC_RGB = 2
  private val PHOTOMETRIC_PALETTE = 3

  private def extractBands(
      raster: GridCoverage2D,
      numBands: Int,
      tileWidth: Int,
      tileHeight: Int,
      photometric: Int): Seq[BandMetadata] = {
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
      val colorInterp = resolveColorInterpretation(photometric, i, numBands)

      BandMetadata(
        band = i,
        dataType = dataType,
        colorInterpretation = colorInterp,
        noDataValue = noDataValue,
        blockWidth = tileWidth,
        blockHeight = tileHeight,
        description = description,
        unit = unit)
    }
  }

  /**
   * Derive per-band color interpretation from the TIFF Photometric Interpretation tag.
   */
  private def resolveColorInterpretation(photometric: Int, band: Int, numBands: Int): String = {
    photometric match {
      case PHOTOMETRIC_MIN_IS_WHITE | PHOTOMETRIC_MIN_IS_BLACK =>
        if (numBands == 1) "Gray"
        else if (band <= numBands) s"Gray${band}" // multi-band grayscale
        else "Undefined"
      case PHOTOMETRIC_RGB =>
        band match {
          case 1 => "Red"
          case 2 => "Green"
          case 3 => "Blue"
          case 4 => "Alpha"
          case _ => "Undefined"
        }
      case PHOTOMETRIC_PALETTE => "Palette"
      case _ => "Undefined"
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

  /**
   * Extract compression type from TIFF tag 259. Returns the human-readable description (e.g.,
   * "LZW", "Deflate") from the TIFFShort description attribute if available, otherwise returns
   * the raw numeric value.
   */
  private def extractCompression(reader: GeoTiffReader): String = {
    try {
      val metadata = reader.getMetadata
      if (metadata == null) return null
      val rootNode = metadata.getRootNode
      if (rootNode == null) return null

      // Tag 259 = Compression
      val desc = findTiffFieldDescription(rootNode, 259)
      if (desc != null) return desc
      findTiffFieldValue(rootNode, 259)
    } catch {
      case _: Exception => null
    }
  }

  /**
   * Check if a TIFF tag exists in the IIO metadata tree by its tag number. Uses the "number"
   * attribute of TIFFField elements.
   */
  private def hasTiffTag(reader: GeoTiffReader, tagNumber: Int): Boolean = {
    try {
      val metadata = reader.getMetadata
      if (metadata == null) return false
      val rootNode = metadata.getRootNode
      if (rootNode == null) return false
      findTiffFieldByNumber(rootNode, tagNumber)
    } catch {
      case _: Exception => false
    }
  }

  private def findTiffFieldByNumber(node: org.w3c.dom.Node, tagNumber: Int): Boolean = {
    if (node == null) return false

    if (node.getNodeName == "TIFFField") {
      val attrs = node.getAttributes
      if (attrs != null) {
        val numAttr = attrs.getNamedItem("number")
        if (numAttr != null && Try(numAttr.getNodeValue.toInt).getOrElse(-1) == tagNumber) {
          return true
        }
      }
    }

    val children = node.getChildNodes
    if (children != null) {
      for (i <- 0 until children.getLength) {
        if (findTiffFieldByNumber(children.item(i), tagNumber)) return true
      }
    }
    false
  }

  /**
   * Extract the TIFF Photometric Interpretation tag value (tag 262). Returns -1 if not found.
   */
  private def extractPhotometricInterpretation(reader: GeoTiffReader): Int = {
    try {
      val metadata = reader.getMetadata
      if (metadata == null) return -1
      val rootNode = metadata.getRootNode
      if (rootNode == null) return -1
      val value = findTiffFieldValue(rootNode, 262) // PhotometricInterpretation
      if (value != null) value.toInt else -1
    } catch {
      case _: Exception => -1
    }
  }

  /**
   * Find the value of a TIFF field by tag number in the IIO metadata tree. Looks for TIFFField
   * elements with matching "number" attribute and extracts the value.
   */
  private def findTiffFieldValue(node: org.w3c.dom.Node, tagNumber: Int): String = {
    if (node == null) return null

    if (node.getNodeName == "TIFFField") {
      val attrs = node.getAttributes
      if (attrs != null) {
        val numAttr = attrs.getNamedItem("number")
        if (numAttr != null && Try(numAttr.getNodeValue.toInt).getOrElse(-1) == tagNumber) {
          return extractValueFromTiffField(node)
        }
      }
    }

    val children = node.getChildNodes
    if (children != null) {
      for (i <- 0 until children.getLength) {
        val result = findTiffFieldValue(children.item(i), tagNumber)
        if (result != null) return result
      }
    }
    null
  }

  /**
   * Extract the value from a TIFFField node. Handles TIFFShorts, TIFFLongs, TIFFAscii, etc.
   * Returns the "value" attribute from the first leaf element (e.g., TIFFShort, TIFFLong).
   */
  private def extractValueFromTiffField(fieldNode: org.w3c.dom.Node): String = {
    val children = fieldNode.getChildNodes
    if (children == null) return null
    for (i <- 0 until children.getLength) {
      val child = children.item(i)
      val grandchildren = child.getChildNodes
      if (grandchildren != null) {
        for (j <- 0 until grandchildren.getLength) {
          val gc = grandchildren.item(j)
          val attrs = gc.getAttributes
          if (attrs != null) {
            val valueAttr = attrs.getNamedItem("value")
            if (valueAttr != null) return valueAttr.getNodeValue
          }
        }
      }
    }
    null
  }

  /**
   * Find the human-readable "description" attribute of a TIFF field by tag number. For example,
   * tag 259 (Compression) has description="LZW" on the TIFFShort element.
   */
  private def findTiffFieldDescription(node: org.w3c.dom.Node, tagNumber: Int): String = {
    if (node == null) return null

    if (node.getNodeName == "TIFFField") {
      val attrs = node.getAttributes
      if (attrs != null) {
        val numAttr = attrs.getNamedItem("number")
        if (numAttr != null && Try(numAttr.getNodeValue.toInt).getOrElse(-1) == tagNumber) {
          return extractDescriptionFromTiffField(node)
        }
      }
    }

    val children = node.getChildNodes
    if (children != null) {
      for (i <- 0 until children.getLength) {
        val result = findTiffFieldDescription(children.item(i), tagNumber)
        if (result != null) return result
      }
    }
    null
  }

  private def extractDescriptionFromTiffField(fieldNode: org.w3c.dom.Node): String = {
    val children = fieldNode.getChildNodes
    if (children == null) return null
    for (i <- 0 until children.getLength) {
      val child = children.item(i)
      val grandchildren = child.getChildNodes
      if (grandchildren != null) {
        for (j <- 0 until grandchildren.getLength) {
          val gc = grandchildren.item(j)
          val attrs = gc.getAttributes
          if (attrs != null) {
            val descAttr = attrs.getNamedItem("description")
            if (descAttr != null) return descAttr.getNodeValue
          }
        }
      }
    }
    null
  }
}
