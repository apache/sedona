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
import org.datasyslab.proj4sedona.core.Proj
import ucar.nc2.Attribute
import ucar.nc2.Dimension
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

    val needGridSize = requested.contains("width") || requested.contains("height")
    val needExtent = requested.exists(COORD_FIELDS.contains)
    val needCrs = requested.contains("crs") || requested.contains("srid")

    // The grid-defining variable anchors width/height, the spatial extent, and the CRS.
    // Grid size comes from dimension lengths — no data arrays are read for this.
    val gridVar =
      if (needGridSize || needExtent || needCrs) selectGridVariable(allVariables) else None
    val gridDims = gridVar.map(trailingDims)

    // Spatial extent needs the 1-D coordinate variable arrays — the only data read performed.
    val extent =
      if (needExtent) {
        gridVar.flatMap { v =>
          val (latDim, lonDim) = trailingDims(v)
          computeExtent(v, latDim, lonDim)
        }
      } else None

    val (crsWkt, srid) =
      if (needCrs) resolveCrs(ncFile, gridVar, requested.contains("srid"))
      else (null, None)

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
   * Select the variable that defines the raster grid: its trailing two dimensions are interpreted
   * as (y, x), the convention shared with `RS_FromNetCDF`. Rank >= 2 variables referenced by a CF
   * `bounds` or `climatology` attribute are cell-boundary variables (e.g. `lat_bnds(lat, nv)`),
   * not data, and are skipped. Among the remaining candidates, the first one whose trailing
   * dimensions have resolvable 1-D numeric coordinate variables is preferred, so ancillary rank-2
   * variables that precede the data in the file cannot hijack the grid.
   */
  private def selectGridVariable(allVariables: Seq[Variable]): Option[Variable] = {
    val boundsNames = allVariables.flatMap { v =>
      Seq("bounds", "climatology").flatMap(a => Option(attrString(v, a)))
    }.toSet
    val candidates =
      allVariables.filter(v => v.getRank >= 2 && !boundsNames.contains(v.getShortName))
    candidates
      .find { v =>
        val (latDim, lonDim) = trailingDims(v)
        findCoordinateVariable(v, latDim).isDefined && findCoordinateVariable(v, lonDim).isDefined
      }
      .orElse(candidates.headOption)
      .orElse(allVariables.find(_.getRank >= 2))
  }

  private def trailingDims(v: Variable): (Dimension, Dimension) = {
    val dims = v.getDimensions.asScala
    (dims(v.getRank - 2), dims(v.getRank - 1))
  }

  /**
   * Find the 1-D numeric coordinate variable for `dim`, searching the data variable's group and
   * then its ancestors (NUG scoping), so an identically named variable in an unrelated group is
   * never picked up. The nearest name match wins; if that variable is not a valid coordinate
   * (wrong rank, non-numeric — e.g. a char/String label sharing the dimension name), the
   * reference is treated as unresolved rather than continuing past the shadowing name.
   */
  private def findCoordinateVariable(dataVar: Variable, dim: Dimension): Option[Variable] = {
    val name = dim.getShortName
    if (name == null) return None
    var group = dataVar.getParentGroup
    while (group != null) {
      val v = group.findVariableLocal(name)
      if (v != null) {
        return if (v.getRank == 1 && v.getDataType != null && v.getDataType.isNumeric &&
          v.getDimension(0).getShortName == name) Some(v)
        else None
      }
      group = group.getParentGroup
    }
    None
  }

  /**
   * Compute the spatial extent from the 1-D coordinate variables of the grid's (y, x) dimensions.
   * Coordinate values are pixel centers (CF convention). For an evenly spaced grid, the
   * GDAL-style geoTransform is emitted and the corner coordinates are extended by half a pixel on
   * each side, matching `RS_FromNetCDF`. For an unevenly spaced (but monotonic) grid, an affine
   * transform would misrepresent the geometry, so geoTransform is omitted and the corner
   * coordinates cover the coordinate centers only.
   */
  private def computeExtent(
      gridVar: Variable,
      latDim: Dimension,
      lonDim: Dimension): Option[GridExtent] = {
    val lonVarOpt = findCoordinateVariable(gridVar, lonDim)
    val latVarOpt = findCoordinateVariable(gridVar, latDim)
    if (lonVarOpt.isEmpty || latVarOpt.isEmpty) return None
    val lonVar = lonVarOpt.get
    val latVar = latVarOpt.get

    val lonValues = read1D(lonVar)
    val latValues = read1D(latVar)
    if (lonValues.length < 2 || latValues.length < 2) return None
    // A non-finite coordinate makes the extent undefined; a null extent is the honest answer
    // and also keeps NaN/Inf out of the min/max fallback below.
    if (!lonValues.forall(isFinite) || !latValues.forall(isFinite)) return None

    val lonSpacing = regularSpacing(lonValues, storedAsFloat32(lonVar))
    val latSpacing = regularSpacing(latValues, storedAsFloat32(latVar))

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

  private def storedAsFloat32(v: Variable): Boolean = v.getDataType == ucar.ma2.DataType.FLOAT

  /**
   * Read a 1-D coordinate variable into decoded double values. CF packing attributes are applied:
   * `_Unsigned` (classic-format unsigned integers stored in signed types) widens the raw value,
   * then `scale_factor` and `add_offset` unpack it. Without this, transforms and extents would be
   * reported in raw storage units.
   */
  private def read1D(v: Variable): Array[Double] = {
    val data = v.read()
    val n = data.getSize.toInt
    val dataType = v.getDataType
    val unsignedIntegral = dataType.isIntegral &&
      (dataType.isUnsigned || "true".equalsIgnoreCase(attrString(v, "_Unsigned")))
    val scale = attrDouble(v, "scale_factor").getOrElse(1.0)
    val offset = attrDouble(v, "add_offset").getOrElse(0.0)
    Array.tabulate(n) { i =>
      val raw =
        if (unsignedIntegral) {
          val bits = dataType.getSize * 8
          val value = data.getLong(i)
          if (bits >= 64) value.toDouble else (value & ((1L << bits) - 1)).toDouble
        } else {
          data.getDouble(i)
        }
      raw * scale + offset
    }
  }

  /**
   * Mean spacing if `values` describes an evenly spaced, strictly monotonic grid that an affine
   * transform can represent faithfully; None otherwise. Three criteria are combined:
   *
   *   - strict monotonicity: every step has the sign of the mean spacing;
   *   - each step within a tolerance of the mean: 0.1% relative, floored at a few ulps of the
   *     stored coordinate magnitude — float32 storage quantizes each value to ~1 ulp of its
   *     magnitude, and differencing neighbors amplifies that noise by 1/spacing, so a fine
   *     non-dyadic float32 grid at large coordinates would otherwise misclassify as irregular;
   *   - every value within half a pixel of the line `head + i * mean`, the point beyond which an
   *     affine-mapped cell center lands in the wrong cell.
   *
   * The step check catches locally uneven spacing that stays within half a pixel of the line
   * (e.g. 0, 1.4, 2); the line check catches slow drift that per-step checks miss.
   */
  private def regularSpacing(values: Array[Double], storedAsFloat32: Boolean): Option[Double] = {
    val n = values.length
    val mean = (values.last - values.head) / (n - 1)
    if (mean == 0 || mean.isNaN || mean.isInfinite) return None
    var maxAbs = 0.0
    var i = 0
    while (i < n) {
      maxAbs = math.max(maxAbs, math.abs(values(i)))
      i += 1
    }
    val quantum = if (storedAsFloat32) Math.ulp(maxAbs.toFloat).toDouble else Math.ulp(maxAbs)
    val stepTolerance = math.max(math.abs(mean) * RELATIVE_STEP_TOLERANCE, 4 * quantum)
    val fitTolerance = math.abs(mean) * MAX_FIT_DEVIATION_PIXELS
    i = 0
    while (i < n - 1) {
      val step = values(i + 1) - values(i)
      if (step * mean <= 0) return None
      if (math.abs(step - mean) > stepTolerance) return None
      if (math.abs(values(i + 1) - (values.head + (i + 1) * mean)) > fitTolerance) return None
      i += 1
    }
    Some(mean)
  }

  /** Resolve a by-name reference (e.g. a grid mapping) from a variable's group upward. */
  private def findReferencedVariable(from: Variable, shortName: String): Option[Variable] = {
    var group = from.getParentGroup
    while (group != null) {
      val v = group.findVariableLocal(shortName)
      if (v != null) return Some(v)
      group = group.getParentGroup
    }
    None
  }

  /**
   * Resolve the CRS: the faithful WKT as declared by the file, plus the derived EPSG code.
   *
   * The CF `grid_mapping` variable of the grid-defining variable takes precedence (`crs_wkt` per
   * CF, `spatial_ref` as written by GDAL); global attributes with the same names are the
   * fallback. The WKT is reported verbatim — downstream conversion for GeoTools-based raster
   * functions is `RS_SetCRS`'s job, which accepts WKT1 and WKT2. When the file carries no WKT, a
   * `latitude_longitude` grid mapping with no explicit ellipsoid/datum parameters is assumed to
   * be WGS 84 (EPSG:4326); parameter-defined projected mappings are not translated.
   */
  private def resolveCrs(
      ncFile: NetcdfFile,
      gridVar: Option[Variable],
      needSrid: Boolean): (String, Option[Int]) = {
    val gmVar = gridVar.flatMap { gv =>
      Option(attrString(gv, "grid_mapping"))
        .map(gridMappingVariableName)
        .flatMap(name => findReferencedVariable(gv, name))
    }
    val globalAttrs = ncFile.getRootGroup.attributes()
    val wkt = gmVar
      .flatMap(v => Option(attrString(v, "crs_wkt")).orElse(Option(attrString(v, "spatial_ref"))))
      .orElse(Option(globalAttrs.findAttributeString("crs_wkt", null)))
      .orElse(Option(globalAttrs.findAttributeString("spatial_ref", null)))
      .orNull
    val srid =
      if (!needSrid) None
      else if (wkt != null) lookupSrid(wkt)
      else gmVar.flatMap(latitudeLongitudeSrid)
    (wkt, srid)
  }

  /**
   * Name of the grid mapping variable in a CF `grid_mapping` attribute. Handles the extended form
   * `"crsVar: coord1 coord2 ..."` by taking the first mapping name.
   */
  private def gridMappingVariableName(attrValue: String): String =
    attrValue.trim.split("\\s+")(0).stripSuffix(":")

  /**
   * Minimal CF grid-mapping support for files with no CRS WKT: a `latitude_longitude` mapping
   * that carries no explicit ellipsoid/datum parameters is assumed WGS 84. Conservative on
   * purpose — any explicit datum description disables the assumption rather than being checked
   * for WGS 84 equivalence.
   */
  private def latitudeLongitudeSrid(gmVar: Variable): Option[Int] = {
    val name = attrString(gmVar, "grid_mapping_name")
    val hasDatumOverride =
      DATUM_OVERRIDE_ATTRS.exists(a => gmVar.attributes().findAttribute(a) != null)
    if (name == "latitude_longitude" && !hasDatumOverride) Some(4326) else None
  }

  /**
   * EPSG code for a CRS WKT via proj4sedona (pure Java — keeps this data source free of the
   * optional LGPL GeoTools runtime). Resolves the WKT's EPSG authority identifier, including
   * structural matches for common authority-less WKT; null when no EPSG identity is found.
   */
  private def lookupSrid(crsWkt: String): Option[Int] = {
    Try {
      val authority = new Proj(crsWkt).toAuthority
      if (authority != null && authority.length == 2 && "EPSG".equalsIgnoreCase(authority(0))) {
        Try(authority(1).trim.toInt).toOption
      } else None
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

  /** Maximum step-to-step deviation from the mean spacing, relative to the mean. */
  private val RELATIVE_STEP_TOLERANCE = 1e-3

  /**
   * Attributes on a `latitude_longitude` grid mapping variable that describe a datum other than
   * (or in addition to) the WGS 84 default, disabling the EPSG:4326 assumption.
   */
  private val DATUM_OVERRIDE_ATTRS = Set(
    "semi_major_axis",
    "semi_minor_axis",
    "inverse_flattening",
    "earth_radius",
    "longitude_of_prime_meridian",
    "towgs84",
    "reference_ellipsoid_name",
    "horizontal_datum_name",
    "geographic_crs_name",
    "prime_meridian_name")

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
