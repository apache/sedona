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
package org.apache.spark.sql.sedona_sql.io.netcdfmetadata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import ucar.nc2.Attribute
import ucar.nc2.Group
import ucar.nc2.NetcdfFile
import ucar.nc2.NetcdfFiles
import ucar.nc2.Variable

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class NetCdfMetadataPartitionReader(
    configuration: Configuration,
    partitionedFiles: Array[PartitionedFile],
    readDataSchema: StructType)
    extends PartitionReader[InternalRow] {

  import NetCdfMetadataPartitionReader._

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
    val requested = readDataSchema.fieldNames.toSet
    val needOpen = requested.exists(f => HEADER_FIELDS.contains(f) || COORD_FIELDS.contains(f))
    val path = new Path(new URI(partition.filePath.toString()))

    // Skip all I/O if only cheap fields (path, driver, fileSize) are requested
    val info = if (needOpen) {
      val raf = new HadoopRandomAccessFile(
        path,
        configuration,
        partition.fileSize,
        partition.modificationTime)
      val ncFile =
        try {
          NetcdfFiles.open(raf, path.toString, null, null)
        } catch {
          case e: Throwable =>
            Try(raf.close())
            throw e
        }
      // Closing the NetcdfFile also closes the RandomAccessFile it was opened with
      try extractInfo(ncFile, requested)
      finally ncFile.close()
    } else {
      NetCdfFileInfo()
    }

    buildRow(path, partition, info)
  }

  private def extractInfo(ncFile: NetcdfFile, requested: Set[String]): NetCdfFileInfo = {
    val allVariables = collectVariables(ncFile.getRootGroup)
    // Convention shared with RS_FromNetCDF: the first variable with >= 2 dimensions defines
    // the grid, and its trailing two dimensions are (y, x).
    val firstRecordVar = allVariables.find(_.getRank >= 2)

    val needGridSize = requested.contains("width") || requested.contains("height")
    val needExtent = requested.exists(COORD_FIELDS.contains)
    val needCrs = requested.contains("crs") || requested.contains("srid")

    // Grid size comes from dimension lengths — no data arrays are read for this.
    val gridDims =
      if (needGridSize || needExtent) firstRecordVar.map { v =>
        val dims = v.getDimensions.asScala
        (dims(v.getRank - 2), dims(v.getRank - 1))
      }
      else None

    // Spatial extent needs the 1-D coordinate variable arrays — the only data read performed.
    val extent =
      if (needExtent) {
        gridDims.flatMap { case (latDim, lonDim) =>
          computeExtent(ncFile, latDim.getShortName, lonDim.getShortName)
        }
      } else None

    val crsWkt = if (needCrs) findCrsWkt(ncFile, firstRecordVar) else null
    val srid =
      if (requested.contains("srid") && crsWkt != null) lookupSrid(crsWkt)
      else None

    NetCdfFileInfo(
      format = if (requested.contains("format")) ncFile.getFileTypeId else null,
      width = gridDims.map(_._2.getLength),
      height = gridDims.map(_._1.getLength),
      srid = srid,
      crs = crsWkt,
      extent = extent,
      dimensions =
        if (requested.contains("dimensions")) collectDimensions(ncFile.getRootGroup) else Nil,
      variables = if (requested.contains("variables")) allVariables.map(toVariableInfo) else Nil,
      globalAttributes =
        if (requested.contains("globalAttributes"))
          attributesToMap(ncFile.getRootGroup.attributes())
        else Map.empty)
  }

  /** All variables in the file, root group first, then nested groups depth-first. */
  private def collectVariables(group: Group): List[Variable] = {
    group.getVariables.asScala.toList ++
      group.getGroups.asScala.toList.flatMap(collectVariables)
  }

  /** All dimensions in the file; dimensions in nested groups are prefixed with the group path. */
  private def collectDimensions(group: Group): List[DimensionInfo] = {
    val prefix = if (group.isRoot) "" else group.getFullName + "/"
    group.getDimensions.asScala.toList.map { d =>
      DimensionInfo(prefix + d.getShortName, d.getLength, d.isUnlimited)
    } ++ group.getGroups.asScala.toList.flatMap(collectDimensions)
  }

  private def toVariableInfo(v: Variable): VariableInfo = {
    VariableInfo(
      name = v.getFullName,
      dataType = Option(v.getDataType).map(_.toString).orNull,
      // Anonymous dimensions (HDF5 datasets without dimension scales) have a null short name;
      // fall back to the length, matching the CDL convention, so the array stays non-null.
      dimensions = v.getDimensions.asScala.toList.map(d =>
        Option(d.getShortName).getOrElse(d.getLength.toString)),
      shape = v.getShape.toList,
      units = attrString(v, "units"),
      longName = attrString(v, "long_name"),
      standardName = attrString(v, "standard_name"),
      // CF convention: _FillValue takes precedence over missing_value
      noDataValue = attrDouble(v, "_FillValue").orElse(attrDouble(v, "missing_value")),
      isCoordinate = v.isCoordinateVariable,
      attributes = attributesToMap(v.attributes()))
  }

  private def attrString(v: Variable, name: String): String =
    v.attributes().findAttributeString(name, null)

  private def attrDouble(v: Variable, name: String): Option[Double] =
    Option(v.attributes().findAttribute(name))
      .flatMap(a => Option(numericAttrValue(a, 0)))
      .map(_.doubleValue())

  /**
   * Numeric value of an attribute element, widened when the attribute is unsigned. cdm-core
   * returns the raw signed primitive for unsigned types, so e.g. a `ubyte` 255 would otherwise
   * surface as -1.
   */
  private def numericAttrValue(attr: Attribute, index: Int): Number = {
    val n = attr.getNumericValue(index)
    if (n == null) null
    else if (attr.getDataType.isUnsigned) ucar.ma2.DataType.widenNumberIfNegative(n)
    else n
  }

  private def attributesToMap(attributes: ucar.nc2.AttributeContainer): Map[String, String] = {
    val map = mutable.LinkedHashMap[String, String]()
    attributes.asScala.foreach { attr =>
      val value = attributeValueToString(attr)
      if (value != null) map.put(attr.getShortName, value)
    }
    map.toMap
  }

  private def attributeValueToString(attr: Attribute): String = {
    Try {
      if (attr.isString) {
        if (attr.getLength <= 1) attr.getStringValue
        else (0 until attr.getLength).map(attr.getStringValue).mkString(",")
      } else if (attr.getLength == 1) {
        Option(numericAttrValue(attr, 0)).map(_.toString).orNull
      } else {
        (0 until attr.getLength)
          .map(i => Option(numericAttrValue(attr, i)).map(_.toString).getOrElse(""))
          .mkString(",")
      }
    }.getOrElse(null)
  }

  /**
   * Compute the spatial extent from the 1-D coordinate variables named after the grid's (y, x)
   * dimensions. Coordinate values are pixel centers (CF convention). For an evenly spaced grid,
   * the GDAL-style geoTransform is emitted and the corner coordinates are extended by half a
   * pixel on each side, matching `RS_FromNetCDF`. For an unevenly spaced (but monotonic) grid, an
   * affine transform would misrepresent the geometry, so geoTransform is omitted and the corner
   * coordinates cover the coordinate centers only.
   */
  private def computeExtent(
      ncFile: NetcdfFile,
      latDimName: String,
      lonDimName: String): Option[GridExtent] = {
    val lonVar = findVariable(ncFile.getRootGroup, lonDimName)
    val latVar = findVariable(ncFile.getRootGroup, latDimName)
    // Require rank-1 numeric coordinate variables; a char/String label variable sharing a grid
    // dimension name would otherwise throw ForbiddenConversionException on read.
    if (lonVar == null || latVar == null || lonVar.getRank != 1 || latVar.getRank != 1 ||
      !lonVar.getDataType.isNumeric || !latVar.getDataType.isNumeric) {
      return None
    }

    val lonValues = read1D(lonVar)
    val latValues = read1D(latVar)
    if (lonValues.length < 2 || latValues.length < 2) return None
    // A non-finite coordinate makes the extent undefined; a null extent is the honest answer
    // and also keeps NaN/Inf out of the min/max fallback below.
    if (!lonValues.forall(isFinite) || !latValues.forall(isFinite)) return None

    val lonSpacing = regularSpacing(lonValues)
    val latSpacing = regularSpacing(latValues)

    if (lonSpacing.isDefined && latSpacing.isDefined) {
      // Regular grid: spacing is uniform, so head/last are the extreme coordinate centers
      val minLon = math.min(lonValues.head, lonValues.last)
      val maxLon = math.max(lonValues.head, lonValues.last)
      val minLat = math.min(latValues.head, latValues.last)
      val maxLat = math.max(latValues.head, latValues.last)
      val scaleX = math.abs(lonSpacing.get)
      val scaleY = -math.abs(latSpacing.get) // north-up: rows go from max to min latitude
      val upperLeftX = minLon - scaleX / 2
      val upperLeftY = maxLat + math.abs(scaleY) / 2
      Some(
        GridExtent(
          geoTransform = Some(Array(upperLeftX, upperLeftY, scaleX, scaleY, 0.0, 0.0)),
          cornerCoordinates =
            Array(upperLeftX, minLat - math.abs(scaleY) / 2, maxLon + scaleX / 2, upperLeftY)))
    } else {
      // Irregular or non-monotonic coordinates: no affine transform; extent of centers only
      Some(
        GridExtent(
          geoTransform = None,
          cornerCoordinates = Array(lonValues.min, latValues.min, lonValues.max, latValues.max)))
    }
  }

  private def isFinite(v: Double): Boolean = !v.isNaN && !v.isInfinite

  /** Read a 1-D coordinate variable into a double array. */
  private def read1D(v: Variable): Array[Double] = {
    val data = v.read()
    val n = data.getSize.toInt
    Array.tabulate(n)(i => data.getDouble(i))
  }

  /**
   * Mean spacing if `values` describes an evenly spaced grid that an affine transform can
   * represent faithfully; None otherwise.
   *
   * Each value is compared against its position on the best-fit line (`head + i * mean`) rather
   * than against its neighbor's difference. Differencing neighbors amplifies float32 coordinate
   * quantization (~1 ulp of the value) by `1/spacing`, which can misclassify a genuinely regular
   * float32 grid as irregular for fine, non-dyadic spacings at large coordinate magnitudes.
   * Absolute positions do not accumulate that error, so a per-value bound of half a pixel — the
   * point at which an affine-mapped center would fall into the wrong cell — cleanly separates
   * regular grids from irregular ones (Gaussian, stretched), which deviate by many pixels.
   */
  private def regularSpacing(values: Array[Double]): Option[Double] = {
    val n = values.length
    val mean = (values.last - values.head) / (n - 1)
    if (mean == 0 || mean.isNaN || mean.isInfinite) return None
    val maxDeviation = math.abs(mean) * MAX_FIT_DEVIATION_PIXELS
    var i = 0
    while (i < n) {
      if (math.abs(values(i) - (values.head + i * mean)) > maxDeviation) return None
      i += 1
    }
    Some(mean)
  }

  private def findVariable(group: Group, shortName: String): Variable = {
    val v = group.findVariableLocal(shortName)
    if (v != null) return v
    val it = group.getGroups.asScala.iterator
    while (it.hasNext) {
      val found = findVariable(it.next(), shortName)
      if (found != null) return found
    }
    null
  }

  /**
   * Find a CRS in WKT form. The CF `grid_mapping` variable of the grid-defining variable takes
   * precedence (`crs_wkt` per CF, `spatial_ref` as written by GDAL); global attributes with the
   * same names are the fallback.
   */
  private def findCrsWkt(ncFile: NetcdfFile, firstRecordVar: Option[Variable]): String = {
    val globalAttrs = ncFile.getRootGroup.attributes()
    firstRecordVar
      .flatMap { rv =>
        Option(rv.attributes().findAttributeString("grid_mapping", null)).flatMap { gmName =>
          Option(findVariable(ncFile.getRootGroup, gmName)).flatMap { gmVar =>
            Option(gmVar.attributes().findAttributeString("crs_wkt", null))
              .orElse(Option(gmVar.attributes().findAttributeString("spatial_ref", null)))
          }
        }
      }
      .orElse(Option(globalAttrs.findAttributeString("crs_wkt", null)))
      .orElse(Option(globalAttrs.findAttributeString("spatial_ref", null)))
      .orNull
  }

  private def lookupSrid(crsWkt: String): Option[Int] = {
    Try {
      val crs = org.geotools.referencing.CRS.parseWKT(crsWkt)
      Option(org.geotools.referencing.CRS.lookupEpsgCode(crs, true)).map(_.intValue())
    }.toOption.flatten
  }

  private def buildRow(
      path: Path,
      partition: PartitionedFile,
      info: NetCdfFileInfo): InternalRow = {
    val fields = readDataSchema.fieldNames.map {
      case "path" => UTF8String.fromString(path.toString)
      case "driver" => UTF8String.fromString("NetCDF")
      case "fileSize" => partition.fileSize: Any
      case "format" =>
        if (info.format != null) UTF8String.fromString(info.format) else null
      case "width" => info.width.map(w => w: Any).orNull
      case "height" => info.height.map(h => h: Any).orNull
      case "srid" => info.srid.map(s => s: Any).orNull
      case "crs" =>
        if (info.crs != null) UTF8String.fromString(info.crs) else null
      case "geoTransform" =>
        info.extent
          .flatMap(_.geoTransform)
          .map(gt => new GenericInternalRow(gt.map(v => v: Any)))
          .orNull
      case "cornerCoordinates" =>
        info.extent
          .map(e => new GenericInternalRow(e.cornerCoordinates.map(v => v: Any)))
          .orNull
      case "dimensions" => buildDimensionsArray(info.dimensions)
      case "variables" => buildVariablesArray(info.variables)
      case "globalAttributes" => buildStringMap(info.globalAttributes)
      case other =>
        throw new IllegalArgumentException(s"Unsupported field name: $other")
    }

    new GenericInternalRow(fields)
  }

  private def buildDimensionsArray(dimensions: Seq[DimensionInfo]): GenericArrayData = {
    new GenericArrayData(dimensions.map { d =>
      new GenericInternalRow(Array[Any](UTF8String.fromString(d.name), d.length, d.isUnlimited))
    }.toArray)
  }

  private def buildVariablesArray(variables: Seq[VariableInfo]): GenericArrayData = {
    new GenericArrayData(variables.map { v =>
      new GenericInternalRow(
        Array[Any](
          UTF8String.fromString(v.name),
          if (v.dataType != null) UTF8String.fromString(v.dataType) else null,
          new GenericArrayData(v.dimensions.map(UTF8String.fromString).toArray[Any]),
          new GenericArrayData(v.shape.map(s => s: Any).toArray),
          if (v.units != null) UTF8String.fromString(v.units) else null,
          if (v.longName != null) UTF8String.fromString(v.longName) else null,
          if (v.standardName != null) UTF8String.fromString(v.standardName) else null,
          v.noDataValue.map(d => d: Any).orNull,
          v.isCoordinate,
          buildStringMap(v.attributes)))
    }.toArray)
  }

  private def buildStringMap(m: Map[String, String]): MapData = {
    // Build key/value arrays from a single traversal to guarantee index alignment
    val entries = m.toSeq
    val keys = new Array[Any](entries.size)
    val values = new Array[Any](entries.size)
    var i = 0
    while (i < entries.size) {
      val (k, v) = entries(i)
      keys(i) = UTF8String.fromString(k)
      values(i) = UTF8String.fromString(v)
      i += 1
    }
    ArrayBasedMapData(keys, values)
  }
}

object NetCdfMetadataPartitionReader {

  /**
   * Maximum allowed deviation of any coordinate from the best-fit affine line, in pixels, for the
   * grid to be treated as regular. Half a pixel is the point beyond which an affine-mapped cell
   * center would land in the wrong cell.
   */
  private val MAX_FIT_DEVIATION_PIXELS = 0.5

  // Fields that require opening the file and parsing its header
  private val HEADER_FIELDS =
    Set("format", "width", "height", "srid", "crs", "dimensions", "variables", "globalAttributes")

  // Fields that additionally require reading the 1-D coordinate variable arrays
  private val COORD_FIELDS = Set("geoTransform", "cornerCoordinates")

  private[netcdfmetadata] case class DimensionInfo(
      name: String,
      length: Int,
      isUnlimited: Boolean)

  private[netcdfmetadata] case class VariableInfo(
      name: String,
      dataType: String,
      dimensions: Seq[String],
      shape: Seq[Int],
      units: String,
      longName: String,
      standardName: String,
      noDataValue: Option[Double],
      isCoordinate: Boolean,
      attributes: Map[String, String])

  /**
   * geoTransform is (upperLeftX, upperLeftY, scaleX, scaleY, skewX, skewY); corners are (minX,
   * minY, maxX, maxY).
   */
  private[netcdfmetadata] case class GridExtent(
      geoTransform: Option[Array[Double]],
      cornerCoordinates: Array[Double])

  private[netcdfmetadata] case class NetCdfFileInfo(
      format: String = null,
      width: Option[Int] = None,
      height: Option[Int] = None,
      srid: Option[Int] = None,
      crs: String = null,
      extent: Option[GridExtent] = None,
      dimensions: Seq[DimensionInfo] = Nil,
      variables: Seq[VariableInfo] = Nil,
      globalAttributes: Map[String, String] = Map.empty)
}
