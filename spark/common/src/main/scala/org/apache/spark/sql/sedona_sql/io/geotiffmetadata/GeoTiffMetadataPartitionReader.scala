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
import org.apache.spark.sql.catalyst.util.ArrayData
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
  private var hasNext_ = false

  override def next(): Boolean = {
    if (currentFileIndex < partitionedFiles.length) {
      currentRow = readFileMetadata(partitionedFiles(currentFileIndex))
      currentFileIndex += 1
      hasNext_ = true
      true
    } else {
      hasNext_ = false
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
      raster = reader.read(null)

      // Lazily compute metadata values only when requested
      lazy val filePath = path.toString
      lazy val fileSize = partition.fileSize
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
      lazy val isTiled = tileWidth < width || tileHeight < height

      lazy val bandsArray = buildBandsArray(raster, numBands, tileWidth, tileHeight)
      lazy val overviewsArray = buildOverviewsArray(reader, width, height)
      lazy val metadataMap = buildMetadataMap(reader)
      lazy val compression = extractCompression(reader)

      // Build row matching readDataSchema field order
      val fields = readDataSchema.fieldNames.map {
        case "path" => UTF8String.fromString(filePath)
        case "driver" => UTF8String.fromString("GTiff")
        case "fileSize" => fileSize: Any
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

  private def buildBandsArray(
      raster: GridCoverage2D,
      numBands: Int,
      tileWidth: Int,
      tileHeight: Int): ArrayData = {
    val bands = (1 to numBands).map { i =>
      val dataType = Try(RasterBandAccessors.getBandType(raster, i)).getOrElse(null)
      val noDataValue = Try(RasterBandAccessors.getBandNoDataValue(raster, i)).getOrElse(null)
      val description = Try {
        val desc = raster.getSampleDimension(i - 1).getDescription
        if (desc != null) desc.toString(java.util.Locale.ROOT) else null
      }.getOrElse(null)

      // Color interpretation is typically stored in the band description
      val colorInterp = description

      // Unit type from sample dimension
      val unit = Try {
        val units = raster.getSampleDimension(i - 1).getUnits
        if (units != null) units.toString else null
      }.getOrElse(null)

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

  private def buildOverviewsArray(
      reader: GeoTiffReader,
      fullWidth: Int,
      fullHeight: Int): ArrayData = {
    try {
      val resolutionLevels = reader.getResolutionLevels
      if (resolutionLevels == null || resolutionLevels.length <= 1) {
        new GenericArrayData(Array.empty[InternalRow])
      } else {
        // Level 0 is full resolution; levels 1+ are overviews
        val fullResX = resolutionLevels(0)(0)
        val fullResY = resolutionLevels(0)(1)
        val overviews = (1 until resolutionLevels.length).map { level =>
          val overviewResX = resolutionLevels(level)(0)
          val overviewResY = resolutionLevels(level)(1)
          val overviewWidth = Math.round(fullWidth.toDouble * fullResX / overviewResX).toInt
          val overviewHeight = Math.round(fullHeight.toDouble * fullResY / overviewResY).toInt
          new GenericInternalRow(Array[Any](level, overviewWidth, overviewHeight))
        }.toArray
        new GenericArrayData(overviews)
      }
    } catch {
      case _: Exception => new GenericArrayData(Array.empty[InternalRow])
    }
  }

  private def buildMetadataMap(
      reader: GeoTiffReader): org.apache.spark.sql.catalyst.util.MapData = {
    try {
      val metadata = reader.getMetadata
      if (metadata == null) return null

      val rootNode = metadata.getRootNode
      if (rootNode == null) return null

      val map = new mutable.LinkedHashMap[UTF8String, UTF8String]()
      extractMetadataFromNode(rootNode, "", map)

      if (map.isEmpty) return null

      org.apache.spark.sql.catalyst.util.ArrayBasedMapData(map.keys.toArray, map.values.toArray)
    } catch {
      case _: Exception => null
    }
  }

  private def extractMetadataFromNode(
      node: org.w3c.dom.Node,
      prefix: String,
      map: mutable.LinkedHashMap[UTF8String, UTF8String]): Unit = {
    if (node == null) return

    // Extract attributes
    val attrs = node.getAttributes
    if (attrs != null) {
      val nameAttr = attrs.getNamedItem("name")
      val valueAttr = attrs.getNamedItem("value")
      if (nameAttr != null && valueAttr != null) {
        val key =
          if (prefix.nonEmpty) s"$prefix.${nameAttr.getNodeValue}"
          else nameAttr.getNodeValue
        map.put(UTF8String.fromString(key), UTF8String.fromString(valueAttr.getNodeValue))
      }
    }

    // Recurse into children
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
