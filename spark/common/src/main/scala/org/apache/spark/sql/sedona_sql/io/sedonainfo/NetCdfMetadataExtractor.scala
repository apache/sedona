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
import ucar.nc2.NetcdfFiles

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Extracts metadata from NetCDF files without reading data arrays. Only coordinate variable
 * arrays (lat/lon) are read to compute spatial extent.
 */
object NetCdfMetadataExtractor extends RasterFileMetadataExtractor {

  override def driver: String = "NetCDF"

  override def canHandle(path: Path): Boolean = {
    val name = path.getName.toLowerCase
    name.endsWith(".nc") || name.endsWith(".nc4") || name.endsWith(".netcdf")
  }

  override def extract(
      path: Path,
      fileSize: Long,
      configuration: Configuration,
      requiredFields: Set[String] = Set.empty): RasterFileMetadata = {
    // Read file bytes via Hadoop FS, then open in memory
    val fs = path.getFileSystem(configuration)
    val status = fs.getFileStatus(path)
    val stream = fs.open(path)
    val bytes =
      try {
        val buf = new Array[Byte](status.getLen.toInt)
        org.apache.commons.io.IOUtils.readFully(stream, buf)
        buf
      } finally {
        stream.close()
      }

    val ncFile = NetcdfFiles.openInMemory("", bytes)
    try {
      extractFromNetcdf(path.toString, fileSize, ncFile)
    } finally {
      ncFile.close()
    }
  }

  private def extractFromNetcdf(
      filePath: String,
      fileSize: Long,
      ncFile: ucar.nc2.NetcdfFile): RasterFileMetadata = {

    // Find record variables (variables with >= 2 dimensions)
    val allVars = ncFile.getVariables.asScala.toSeq
    val recordVars = allVars.filter(_.getDimensions.size() >= 2)

    // Find lat/lon coordinate variables from the first record variable
    val (width, height, geoTransform, cornerCoords) = if (recordVars.nonEmpty) {
      extractSpatialInfo(ncFile, recordVars.head)
    } else {
      (0, 0, GeoTransformMetadata(0, 0, 0, 0, 0, 0), CornerCoordinatesMetadata(0, 0, 0, 0))
    }

    // Each record variable is one band
    val bands = recordVars.zipWithIndex.map { case (v, idx) =>
      val noData = Try {
        val attr = v.findAttribute("missing_value")
        if (attr != null) attr.getNumericValue.doubleValue() else null
      }.getOrElse(null).asInstanceOf[java.lang.Double]

      val unit = Try {
        val attr = v.findAttribute("units")
        if (attr != null) attr.getStringValue else null
      }.getOrElse(null)

      val longName = Try {
        val attr = v.findAttribute("long_name")
        if (attr != null) attr.getStringValue else null
      }.getOrElse(null)

      val dims =
        v.getDimensions.asScala.map(d => s"${d.getShortName}=${d.getLength}").mkString(",")

      BandMetadata(
        band = idx + 1,
        dataType = v.getDataType.toString,
        colorInterpretation = "Undefined",
        noDataValue = noData,
        blockWidth = width,
        blockHeight = height,
        description = s"${v.getShortName}($dims)",
        unit = unit)
    }

    // Global attributes as metadata map
    val globalAttrs = ncFile.getGlobalAttributes.asScala.flatMap { attr =>
      Try {
        val value =
          if (attr.isString) attr.getStringValue
          else if (attr.getNumericValue != null) attr.getNumericValue.toString
          else null
        if (value != null) Some(attr.getShortName -> value) else None
      }.getOrElse(None)
    }.toMap

    // Add dimension info to metadata
    val dimInfo = ncFile.getDimensions.asScala
      .map(d => s"${d.getShortName}=${d.getLength}")
      .mkString(",")
    val metadata = globalAttrs + ("dimensions" -> dimInfo)

    // Add variable list to metadata
    val varList = recordVars.map(_.getShortName).mkString(",")
    val metadataWithVars = metadata + ("variables" -> varList)

    // CRS: check for crs_wkt or spatial_ref global attribute
    val crs = Try {
      val crsAttr = ncFile.findGlobalAttribute("crs_wkt")
      if (crsAttr != null) crsAttr.getStringValue
      else {
        val spatialRef = ncFile.findGlobalAttribute("spatial_ref")
        if (spatialRef != null) spatialRef.getStringValue else null
      }
    }.getOrElse(null)

    RasterFileMetadata(
      path = filePath,
      driver = driver,
      fileSize = fileSize,
      width = width,
      height = height,
      numBands = recordVars.size,
      srid = 0,
      crs = crs,
      geoTransform = geoTransform,
      cornerCoordinates = cornerCoords,
      bands = bands,
      overviews = Seq.empty,
      metadata = metadataWithVars,
      isTiled = false,
      compression = null)
  }

  /**
   * Extract spatial extent from coordinate variables of a record variable. Assumes last 2
   * dimensions are Y and X (same convention as NetCdfReader).
   */
  private def extractSpatialInfo(ncFile: ucar.nc2.NetcdfFile, recordVar: ucar.nc2.Variable)
      : (Int, Int, GeoTransformMetadata, CornerCoordinatesMetadata) = {
    val dims = recordVar.getDimensions.asScala.toSeq
    val numDims = dims.size
    val latDimName = dims(numDims - 2).getShortName
    val lonDimName = dims(numDims - 1).getShortName

    val lonVar = findVariable(ncFile, lonDimName)
    val latVar = findVariable(ncFile, latDimName)

    if (lonVar == null || latVar == null) {
      return (0, 0, GeoTransformMetadata(0, 0, 0, 0, 0, 0), CornerCoordinatesMetadata(0, 0, 0, 0))
    }

    val lonData = lonVar.read()
    val latData = latVar.read()
    val width = lonData.getShape()(0)
    val height = latData.getShape()(0)

    val lonFirst = lonData.getDouble(0)
    val lonLast = lonData.getDouble(width - 1)
    val latFirst = latData.getDouble(0)
    val latLast = latData.getDouble(height - 1)

    val lonIncreasing = lonFirst < lonLast
    val latIncreasing = latFirst < latLast

    val minX = if (lonIncreasing) lonFirst else lonLast
    val maxX = if (lonIncreasing) lonLast else lonFirst
    val minY = if (latIncreasing) latFirst else latLast
    val maxY = if (latIncreasing) latLast else latFirst

    val scaleX = Math.abs(lonLast - lonFirst) / (width - 1)
    val scaleY = -(Math.abs(latLast - latFirst) / (height - 1))

    val geoTransform = GeoTransformMetadata(
      upperLeftX = minX,
      upperLeftY = maxY,
      scaleX = scaleX,
      scaleY = scaleY,
      skewX = 0.0,
      skewY = 0.0)

    // Envelope covers the full extent including half-pixel borders
    val halfPixelX = scaleX / 2
    val halfPixelY = Math.abs(scaleY) / 2
    val cornerCoords = CornerCoordinatesMetadata(
      minX = minX - halfPixelX,
      minY = minY - halfPixelY,
      maxX = maxX + halfPixelX,
      maxY = maxY + halfPixelY)

    (width, height, geoTransform, cornerCoords)
  }

  private def findVariable(ncFile: ucar.nc2.NetcdfFile, name: String): ucar.nc2.Variable = {
    // Search recursively through groups
    findVariableInGroup(name, ncFile.getRootGroup)
  }

  private def findVariableInGroup(name: String, group: ucar.nc2.Group): ucar.nc2.Variable = {
    val v = group.findVariableLocal(name)
    if (v != null) return v
    for (g <- group.getGroups.asScala) {
      val found = findVariableInGroup(name, g)
      if (found != null) return found
    }
    null
  }
}
