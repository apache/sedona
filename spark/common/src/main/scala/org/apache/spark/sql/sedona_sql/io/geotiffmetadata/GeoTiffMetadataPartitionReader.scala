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
package org.apache.spark.sql.sedona_sql.io.geotiffmetadata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.sedona.common.raster.RasterAccessors
import org.apache.sedona.common.raster.RasterBandAccessors
import org.apache.sedona.common.raster.inputstream.HadoopImageInputStream
import org.apache.sedona.common.utils.RasterUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.referencing.crs.DefaultEngineeringCRS

import java.net.URI
import scala.collection.mutable
import scala.util.Try

class GeoTiffMetadataPartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    readDataSchema: StructType)
    extends PartitionReader[InternalRow] {

  private var currentFileIndex = 0
  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (currentFileIndex < partitionedFiles.length) {
      currentRow = readFileMetadata(partitionedFiles(currentFileIndex))
      currentFileIndex += 1
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}

  private def readFileMetadata(partition: PartitionedFile): InternalRow = {
    val path = new Path(new URI(partition.filePath.toString()))
    val imageStream = new HadoopImageInputStream(path, configuration)
    var reader: GeoTiffReader = null
    var raster: GridCoverage2D = null
    try {
      reader = new GeoTiffReader(
        imageStream,
        new org.geotools.util.factory.Hints(
          org.geotools.util.factory.Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER,
          java.lang.Boolean.TRUE))

      // Extract TIFF IIO metadata BEFORE read() to avoid stream state issues
      val isTiled = GeoTiffMetadataPartitionReader.hasTiffTag(reader, 322)
      val photometric = GeoTiffMetadataPartitionReader.extractPhotometricInterpretation(reader)
      val tiffMetadata = GeoTiffMetadataPartitionReader.extractMetadata(reader)
      val compression = GeoTiffMetadataPartitionReader.extractCompression(reader)

      raster = reader.read(null)

      lazy val width = RasterAccessors.getWidth(raster)
      lazy val height = RasterAccessors.getHeight(raster)
      lazy val numBands = RasterAccessors.numBands(raster)
      lazy val srid = RasterAccessors.srid(raster)

      lazy val crsStr = Try {
        val crs = raster.getCoordinateReferenceSystem
        if (crs == null || crs.isInstanceOf[DefaultEngineeringCRS]) null
        else crs.toWKT
      }.getOrElse(null)

      lazy val affine = RasterUtils.getGDALAffineTransform(raster)
      lazy val geoTransformRow = new GenericInternalRow(
        Array[Any](
          affine.getTranslateX,
          affine.getTranslateY,
          affine.getScaleX,
          affine.getScaleY,
          affine.getShearX,
          affine.getShearY))

      lazy val env = raster.getEnvelope2D
      lazy val cornerCoordinatesRow = new GenericInternalRow(
        Array[Any](env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))

      lazy val image = raster.getRenderedImage
      lazy val tileWidth = image.getTileWidth
      lazy val tileHeight = image.getTileHeight

      lazy val bandsArray = GeoTiffMetadataPartitionReader
        .buildBandsArray(raster, numBands, tileWidth, tileHeight, photometric)
      lazy val overviewsArray =
        GeoTiffMetadataPartitionReader.buildOverviewsArray(reader, width, height)
      lazy val metadataMap = GeoTiffMetadataPartitionReader.buildMetadataMap(tiffMetadata)

      val fields = readDataSchema.fieldNames.map {
        case "path" => UTF8String.fromString(path.toString)
        case "driver" => UTF8String.fromString("GTiff")
        case "fileSize" => partition.fileSize: Any
        case "width" => width: Any
        case "height" => height: Any
        case "numBands" => numBands: Any
        case "srid" => srid: Any
        case "crs" =>
          if (crsStr != null) UTF8String.fromString(crsStr) else null
        case "geoTransform" => geoTransformRow
        case "cornerCoordinates" => cornerCoordinatesRow
        case "bands" => bandsArray
        case "overviews" => overviewsArray
        case "metadata" => metadataMap
        case "isTiled" => isTiled: Any
        case "compression" =>
          if (compression != null) UTF8String.fromString(compression) else null
        case other =>
          throw new IllegalArgumentException(s"Unsupported field name: $other")
      }

      new GenericInternalRow(fields)
    } finally {
      if (raster != null) raster.dispose(true)
      if (reader != null) reader.dispose()
      imageStream.close()
    }
  }
}

object GeoTiffMetadataPartitionReader {

  // TIFF Photometric Interpretation values
  private val PHOTOMETRIC_MIN_IS_WHITE = 0
  private val PHOTOMETRIC_MIN_IS_BLACK = 1
  private val PHOTOMETRIC_RGB = 2
  private val PHOTOMETRIC_PALETTE = 3

  def buildBandsArray(
      raster: GridCoverage2D,
      numBands: Int,
      tileWidth: Int,
      tileHeight: Int,
      photometric: Int): GenericArrayData = {
    val bands = (1 to numBands).map { i =>
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

      new GenericInternalRow(
        Array[Any](
          i,
          if (dataType != null) UTF8String.fromString(dataType) else null,
          if (colorInterp != null) UTF8String.fromString(colorInterp) else null,
          if (noDataValue != null) noDataValue.doubleValue() else null,
          tileWidth,
          tileHeight,
          if (description != null) UTF8String.fromString(description) else null,
          if (unit != null) UTF8String.fromString(unit) else null))
    }.toArray
    new GenericArrayData(bands)
  }

  private def resolveColorInterpretation(photometric: Int, band: Int, numBands: Int): String = {
    photometric match {
      case PHOTOMETRIC_MIN_IS_WHITE | PHOTOMETRIC_MIN_IS_BLACK =>
        if (numBands == 1) "Gray"
        else s"Gray$band"
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

  def buildOverviewsArray(
      reader: GeoTiffReader,
      fullWidth: Int,
      fullHeight: Int): GenericArrayData = {
    try {
      val layout = reader.getDatasetLayout
      if (layout == null) return new GenericArrayData(Array.empty[InternalRow])

      val numOverviews = layout.getNumInternalOverviews
      if (numOverviews <= 0) return new GenericArrayData(Array.empty[InternalRow])

      val resolutionLevels = reader.getResolutionLevels
      if (resolutionLevels == null || resolutionLevels.length <= 1)
        return new GenericArrayData(Array.empty[InternalRow])

      val count = Math.min(numOverviews, resolutionLevels.length - 1)
      val fullResX = resolutionLevels(0)(0)
      val fullResY = resolutionLevels(0)(1)
      val overviews = (1 to count).map { level =>
        val ovrResX = resolutionLevels(level)(0)
        val ovrResY = resolutionLevels(level)(1)
        new GenericInternalRow(
          Array[Any](
            level,
            Math.round(fullWidth.toDouble * fullResX / ovrResX).toInt,
            Math.round(fullHeight.toDouble * fullResY / ovrResY).toInt))
      }.toArray
      new GenericArrayData(overviews)
    } catch {
      case _: Exception => new GenericArrayData(Array.empty[InternalRow])
    }
  }

  def buildMetadataMap(
      metadata: Map[String, String]): org.apache.spark.sql.catalyst.util.MapData = {
    if (metadata.isEmpty) return null
    // Build key/value arrays from a single traversal to guarantee index alignment
    val entries = metadata.toSeq
    val keys = new Array[Any](entries.size)
    val values = new Array[Any](entries.size)
    var i = 0
    while (i < entries.size) {
      val (k, v) = entries(i)
      keys(i) = UTF8String.fromString(k)
      values(i) = UTF8String.fromString(v)
      i += 1
    }
    org.apache.spark.sql.catalyst.util.ArrayBasedMapData(keys, values)
  }

  // ---- TIFF IIO metadata helpers ----

  def hasTiffTag(reader: GeoTiffReader, tagNumber: Int): Boolean = {
    try {
      val md = reader.getMetadata
      if (md == null) return false
      val root = md.getRootNode
      if (root == null) return false
      findTiffFieldByNumber(root, tagNumber)
    } catch { case _: Exception => false }
  }

  def extractPhotometricInterpretation(reader: GeoTiffReader): Int = {
    try {
      val md = reader.getMetadata
      if (md == null) return -1
      val root = md.getRootNode
      if (root == null) return -1
      val v = findTiffFieldValue(root, 262)
      if (v != null) v.toInt else -1
    } catch { case _: Exception => -1 }
  }

  def extractCompression(reader: GeoTiffReader): String = {
    try {
      val md = reader.getMetadata
      if (md == null) return null
      val root = md.getRootNode
      if (root == null) return null
      val desc = findTiffFieldDescription(root, 259)
      if (desc != null) desc else findTiffFieldValue(root, 259)
    } catch { case _: Exception => null }
  }

  def extractMetadata(reader: GeoTiffReader): Map[String, String] = {
    try {
      val md = reader.getMetadata
      if (md == null) return Map.empty
      val root = md.getRootNode
      if (root == null) return Map.empty
      val map = new mutable.LinkedHashMap[String, String]()
      extractMetadataFromNode(root, "", map)
      map.toMap
    } catch { case _: Exception => Map.empty }
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
      } else prefix
      for (i <- 0 until children.getLength)
        extractMetadataFromNode(children.item(i), childPrefix, map)
    }
  }

  private def findTiffFieldByNumber(node: org.w3c.dom.Node, tagNumber: Int): Boolean = {
    if (node == null) return false
    if (node.getNodeName == "TIFFField") {
      val attrs = node.getAttributes
      if (attrs != null) {
        val numAttr = attrs.getNamedItem("number")
        if (numAttr != null && Try(numAttr.getNodeValue.toInt).getOrElse(-1) == tagNumber)
          return true
      }
    }
    val children = node.getChildNodes
    if (children != null)
      for (i <- 0 until children.getLength)
        if (findTiffFieldByNumber(children.item(i), tagNumber)) return true
    false
  }

  private def findTiffFieldValue(node: org.w3c.dom.Node, tagNumber: Int): String = {
    if (node == null) return null
    if (node.getNodeName == "TIFFField") {
      val attrs = node.getAttributes
      if (attrs != null) {
        val numAttr = attrs.getNamedItem("number")
        if (numAttr != null && Try(numAttr.getNodeValue.toInt).getOrElse(-1) == tagNumber)
          return extractLeafAttr(node, "value")
      }
    }
    val children = node.getChildNodes
    if (children != null)
      for (i <- 0 until children.getLength) {
        val r = findTiffFieldValue(children.item(i), tagNumber)
        if (r != null) return r
      }
    null
  }

  private def findTiffFieldDescription(node: org.w3c.dom.Node, tagNumber: Int): String = {
    if (node == null) return null
    if (node.getNodeName == "TIFFField") {
      val attrs = node.getAttributes
      if (attrs != null) {
        val numAttr = attrs.getNamedItem("number")
        if (numAttr != null && Try(numAttr.getNodeValue.toInt).getOrElse(-1) == tagNumber)
          return extractLeafAttr(node, "description")
      }
    }
    val children = node.getChildNodes
    if (children != null)
      for (i <- 0 until children.getLength) {
        val r = findTiffFieldDescription(children.item(i), tagNumber)
        if (r != null) return r
      }
    null
  }

  private def extractLeafAttr(fieldNode: org.w3c.dom.Node, attrName: String): String = {
    val children = fieldNode.getChildNodes
    if (children == null) return null
    for (i <- 0 until children.getLength) {
      val gc = children.item(i).getChildNodes
      if (gc != null)
        for (j <- 0 until gc.getLength) {
          val attrs = gc.item(j).getAttributes
          if (attrs != null) {
            val a = attrs.getNamedItem(attrName)
            if (a != null) return a.getNodeValue
          }
        }
    }
    null
  }
}
