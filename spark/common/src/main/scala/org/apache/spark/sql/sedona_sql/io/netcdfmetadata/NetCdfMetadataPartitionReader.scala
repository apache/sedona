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
          computeExtent(v, latDim, lonDim, allVariables)
        }
      } else None

    val (crsWkt, srid) =
      if (needCrs) resolveCrs(ncFile, gridVar, requested.contains("srid"), allVariables)
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
      // Dimension names are qualified with their declaring group's path so they join back to
      // the `dimensions` inventory (e.g. sub/temp reports [sub/lat, sub/lon], not [lat, lon])
      dimensions = v.getDimensions.asScala.toList.map(qualifiedDimensionName(v, _)),
      shape = v.getShape.toList,
      units = attrString(v, "units"),
      longName = attrString(v, "long_name"),
      standardName = attrString(v, "standard_name"),
      // CF convention: _FillValue takes precedence over missing_value; the owning variable's
      // _Unsigned semantics apply to the stored value (e.g. byte -1 with _Unsigned means 255)
      noDataValue = attrDouble(v, "_FillValue")
        .orElse(attrDouble(v, "missing_value"))
        .map(widenIfUnsignedVariable(v, _)),
      isCoordinate = v.isCoordinateVariable,
      attributes = attributesToMap(v.attributes()))
  }

  /**
   * Dimension name qualified with the path of its declaring group, matching the naming used by
   * the `dimensions` inventory. The declaring group is found by walking the variable's group
   * chain upward and matching the dimension by identity. Anonymous dimensions (null short name,
   * e.g. HDF5 datasets without dimension scales) fall back to the length, matching the CDL
   * convention, so the array stays non-null.
   */
  private def qualifiedDimensionName(v: Variable, d: Dimension): String = {
    val base = Option(d.getShortName).getOrElse(d.getLength.toString)
    var group = v.getParentGroup
    while (group != null) {
      if (group.getDimensions.asScala.exists(_ eq d)) {
        return if (group.isRoot) base else group.getFullName + "/" + base
      }
      group = group.getParentGroup
    }
    base
  }

  /** Whether the variable stores unsigned integers (unsigned type, or classic `_Unsigned`). */
  private def isUnsignedIntegral(v: Variable): Boolean = {
    val dataType = v.getDataType
    dataType != null && dataType.isIntegral &&
    (dataType.isUnsigned || "true".equalsIgnoreCase(attrString(v, "_Unsigned")))
  }

  /**
   * Reinterpret a signed stored value as unsigned when the owning variable declares unsigned
   * semantics; attribute values (`_FillValue`, `missing_value`) are stored in the variable's
   * type, so they widen the same way as data.
   */
  private def widenIfUnsignedVariable(v: Variable, value: Double): Double = {
    if (value < 0 && isUnsignedIntegral(v)) {
      value + math.pow(2, v.getDataType.getSize * 8)
    } else {
      value
    }
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
   * as (y, x), the convention shared with `RS_FromNetCDF`. Rank >= 2 variables referenced by CF
   * metadata attributes — cell boundaries (`bounds`, `climatology`), auxiliary coordinates
   * (`coordinates`), ancillary/quality variables (`ancillary_variables`), cell measures
   * (`cell_measures`), and formula terms (`formula_terms`) — describe other variables, not data,
   * and are skipped. Among the remaining candidates, the first one whose trailing dimensions have
   * resolvable 1-D numeric coordinate variables is preferred, so ancillary rank-2 variables that
   * precede the data in the file cannot hijack the grid.
   */
  private def selectGridVariable(allVariables: Seq[Variable]): Option[Variable] = {
    // Resolve every CF metadata reference from its owning variable and exclude the referenced
    // variables by identity. Excluding by bare name would let a path reference such as
    // "/aux/temperature" shadow an unrelated root variable that shares its basename.
    val referenced = for {
      owner <- allVariables
      attr <- METADATA_REFERENCE_ATTRS
      value <- Option(attrString(owner, attr)).toList
      token <- referencedTokens(value)
      target <- findReferencedVariable(owner, token).toList
    } yield target
    val candidates =
      allVariables.filter(v => v.getRank >= 2 && !referenced.exists(_ eq v))
    candidates
      .find { v =>
        val (latDim, lonDim) = trailingDims(v)
        findCoordinateVariable(v, latDim, allVariables).isDefined &&
        findCoordinateVariable(v, lonDim, allVariables).isDefined
      }
      .orElse(candidates.headOption)
      .orElse(allVariables.find(_.getRank >= 2))
  }

  /**
   * Variable-name tokens in a CF reference attribute value. Handles blank-separated lists
   * (`ancillary_variables`) and keyed forms like `"area: cell_area"` (keys end with `:` and are
   * dropped). Tokens may be plain names or group paths; both resolve via
   * [[findReferencedVariable]].
   */
  private def referencedTokens(attrValue: String): Seq[String] =
    attrValue.trim.split("\\s+").toSeq.filter(t => t.nonEmpty && !t.endsWith(":"))

  private def trailingDims(v: Variable): (Dimension, Dimension) = {
    val dims = v.getDimensions.asScala
    (dims(v.getRank - 2), dims(v.getRank - 1))
  }

  /**
   * Find the 1-D numeric coordinate variable for `dim`. Search order follows CF/NUG scoping:
   * first the data variable's group and its ancestors by name (proximity), then a lateral search
   * anywhere in the file matched strictly by dimension identity — a NUG coordinate variable may
   * legally live outside the data variable's lineage (e.g. a sibling group) as long as it
   * references the very same dimension. Identity matching (`eq`) guarantees an identically named
   * dimension declared in an unrelated group is never picked up.
   */
  private def findCoordinateVariable(
      dataVar: Variable,
      dim: Dimension,
      allVariables: Seq[Variable]): Option[Variable] = {
    val name = dim.getShortName
    if (name == null) return None

    def isCoordinateFor(v: Variable): Boolean =
      v.getRank == 1 && v.getDataType != null && v.getDataType.isNumeric &&
        v.getShortName == name && (v.getDimension(0) eq dim)

    // Proximity: the data variable's group, then its ancestors
    var group = dataVar.getParentGroup
    while (group != null) {
      val v = group.findVariableLocal(name)
      if (v != null && isCoordinateFor(v)) return Some(v)
      group = group.getParentGroup
    }
    // Lateral: anywhere in the file, by dimension identity
    allVariables.find(isCoordinateFor)
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
      lonDim: Dimension,
      allVariables: Seq[Variable]): Option[GridExtent] = {
    val lonVarOpt = findCoordinateVariable(gridVar, lonDim, allVariables)
    val latVarOpt = findCoordinateVariable(gridVar, latDim, allVariables)
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
    val unsignedIntegral = isUnsignedIntegral(v)
    val bits = v.getDataType.getSize * 8
    val scale = attrDouble(v, "scale_factor").getOrElse(1.0)
    val offset = attrDouble(v, "add_offset").getOrElse(0.0)
    Array.tabulate(n) { i =>
      val raw =
        if (unsignedIntegral) {
          val value = data.getLong(i)
          if (bits >= 64) {
            // 64-bit unsigned: reinterpret the raw signed long (values >= 2^63 are negative)
            if (value >= 0) value.toDouble else value.toDouble + math.pow(2, 64)
          } else {
            (value & ((1L << bits) - 1)).toDouble
          }
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

  /**
   * Resolve a variable reference (e.g. a grid mapping). CF allows plain names (resolved by
   * proximity: the referencing variable's group, then its ancestors), absolute paths (`/crs`),
   * and relative paths (`sub/crs`, `../crs`).
   */
  private def findReferencedVariable(from: Variable, reference: String): Option[Variable] = {
    if (reference.contains("/")) {
      val startGroup =
        if (reference.startsWith("/")) rootGroupOf(from) else from.getParentGroup
      resolvePath(startGroup, reference.split("/").filter(_.nonEmpty))
    } else {
      var group = from.getParentGroup
      while (group != null) {
        val v = group.findVariableLocal(reference)
        if (v != null) return Some(v)
        group = group.getParentGroup
      }
      None
    }
  }

  private def rootGroupOf(v: Variable): Group = {
    var group = v.getParentGroup
    while (group.getParentGroup != null) {
      group = group.getParentGroup
    }
    group
  }

  /** Walk `segments` from `start`: all but the last are group names (`..` steps up). */
  private def resolvePath(start: Group, segments: Array[String]): Option[Variable] = {
    if (start == null || segments.isEmpty) return None
    var group = start
    var i = 0
    while (i < segments.length - 1) {
      group = segments(i) match {
        case "." => group
        case ".." => group.getParentGroup
        case name => group.getGroups.asScala.find(_.getShortName == name).orNull
      }
      if (group == null) return None
      i += 1
    }
    Option(group.findVariableLocal(segments.last))
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
      needSrid: Boolean,
      allVariables: Seq[Variable]): (String, Option[Int]) = {
    val gmVar = gridVar.flatMap { gv =>
      val (latDim, lonDim) = trailingDims(gv)
      // The actual coordinate variables of the grid, for identity matching of the extended
      // grid_mapping form; the dimension names are the fallback when none resolve
      val gridCoordVars =
        Seq(
          findCoordinateVariable(gv, latDim, allVariables),
          findCoordinateVariable(gv, lonDim, allVariables)).flatten
      val gridDimNames = Set(latDim.getShortName, lonDim.getShortName).filter(_ != null)
      Option(attrString(gv, "grid_mapping"))
        .flatMap(selectGridMappingName(gv, _, gridCoordVars, gridDimNames))
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
   * Name of the grid mapping variable in a CF `grid_mapping` attribute that applies to the grid's
   * coordinates. The simple form (`"crs"`) names one mapping that applies to the whole variable.
   * The extended form (`"crsA: coord1 coord2 ..."`) explicitly scopes each mapping to its listed
   * coordinates, so a clause — including a single one — is selected only when its resolved
   * coordinate set covers both of the grid's (y, x) coordinate variables. References are resolved
   * relative to the data variable and matched by identity, so a clause referencing identically
   * named coordinates in another group (e.g. `/geo/x` vs the root `x`) is never confused with the
   * grid's own, and a mapping tied only to auxiliary coordinates never labels the x/y extent.
   * When the grid's coordinate variables cannot be resolved, clause coordinate basenames must
   * cover the grid dimension names instead. When no clause covers both axes, the association is
   * ambiguous and no CRS is reported.
   */
  private def selectGridMappingName(
      dataVar: Variable,
      attrValue: String,
      gridCoordVars: Seq[Variable],
      gridDimNames: Set[String]): Option[String] = {
    val tokens = attrValue.trim.split("\\s+").toSeq.filter(_.nonEmpty)
    if (tokens.isEmpty) return None
    if (!tokens.exists(_.endsWith(":"))) return Some(tokens.head)

    // Extended form: group tokens into (mappingName, coordinateReferences) clauses
    val clauses = mutable.ListBuffer[(String, mutable.ListBuffer[String])]()
    tokens.foreach { token =>
      if (token.endsWith(":")) {
        clauses += ((token.stripSuffix(":"), mutable.ListBuffer[String]()))
      } else if (clauses.nonEmpty) {
        clauses.last._2 += token
      }
    }

    // Every expanded clause — including a single one — explicitly scopes its mapping to the
    // listed coordinates, so a clause applies to the grid only when its resolved coordinates
    // cover BOTH grid axes. A "geographic: lat lon" clause tied to 2-D auxiliary coordinates
    // says nothing about the 1-D x/y coordinates the extent came from; selecting it would
    // label a projected extent with a geographic CRS.
    if (gridCoordVars.size == 2) {
      // A clause coordinate reference matches a grid coordinate when it resolves (by scope
      // for plain names, by path otherwise) to that variable. A plain name that is not
      // visible in the data variable's scope falls back to the same CF-aware lateral
      // resolution the grid coordinates themselves were found with — a sibling-group
      // coordinate (e.g. /coords/x) is legitimately referenced by its plain name.
      def matches(reference: String, gridCoord: Variable): Boolean = {
        findReferencedVariable(dataVar, reference) match {
          case Some(resolved) => resolved eq gridCoord
          case None => !reference.contains("/") && gridCoord.getShortName == reference
        }
      }
      clauses
        .find(clause => gridCoordVars.forall(gv => clause._2.exists(matches(_, gv))))
        .map(_._1)
    } else if (gridDimNames.size == 2) {
      // Grid coordinates unresolvable (extent is null anyway): best-effort basename coverage
      clauses
        .find(clause =>
          gridDimNames.forall(n =>
            clause._2.exists(t => t.substring(t.lastIndexOf('/') + 1) == n)))
        .map(_._1)
    } else {
      None
    }
  }

  /**
   * Minimal CF grid-mapping support for files with no CRS WKT: EPSG:4326 is reported only when a
   * `latitude_longitude` grid mapping positively identifies the WGS 84 datum by *name*
   * (`horizontal_datum_name` or `geographic_crs_name`) with a Greenwich prime meridian. Ellipsoid
   * parameters can only veto, never qualify: many datums (e.g. CR05) use the WGS 84 ellipsoid, so
   * the Earth figure does not identify the datum. Any present identifier that disagrees with WGS
   * 84 disables the inference.
   */
  private def latitudeLongitudeSrid(gmVar: Variable): Option[Int] = {
    if (attrString(gmVar, "grid_mapping_name") != "latitude_longitude") return None

    def normalized(attr: String): Option[String] =
      Option(attrString(gmVar, attr)).map(_.toLowerCase.replaceAll("[^a-z0-9]", ""))

    val datumName = normalized("horizontal_datum_name")
    val crsName = normalized("geographic_crs_name")
    val nameIdentifiesWgs84 =
      datumName.exists(WGS84_DATUM_NAMES.contains) || crsName.exists(WGS84_DATUM_NAMES.contains)
    val nameContradicts =
      datumName.exists(!WGS84_DATUM_NAMES.contains(_)) ||
        crsName.exists(!WGS84_DATUM_NAMES.contains(_)) ||
        normalized("reference_ellipsoid_name").exists(!WGS84_ELLIPSOID_NAMES.contains(_)) ||
        normalized("prime_meridian_name").exists(_ != "greenwich")

    // Ellipsoid parameters, when present, must be consistent with the WGS 84 ellipsoid
    val paramsContradict =
      attrDouble(gmVar, "semi_major_axis").exists(a => math.abs(a - 6378137.0) >= 1e-3) ||
        attrDouble(gmVar, "inverse_flattening").exists(f =>
          math.abs(f - 298.257223563) >= 1e-6) ||
        attrDouble(gmVar, "semi_minor_axis").exists(b => math.abs(b - 6356752.314245) >= 1e-3)

    val greenwich = attrDouble(gmVar, "longitude_of_prime_meridian").forall(_ == 0.0)

    if (nameIdentifiesWgs84 && !nameContradicts && !paramsContradict && greenwich) Some(4326)
    else None
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
   * CF attributes whose values reference other variables (cell boundaries, auxiliary coordinates,
   * ancillary data, cell measures, formula terms); referenced variables describe metadata, not
   * raster data, and are excluded from grid selection.
   */
  private val METADATA_REFERENCE_ATTRS = Seq(
    "bounds",
    "climatology",
    "coordinates",
    "ancillary_variables",
    "cell_measures",
    "formula_terms",
    "grid_mapping")

  /**
   * Normalized datum/CRS name values that identify WGS 84, including the EPSG:6326 ensemble name
   * ("World Geodetic System 1984 ensemble").
   */
  private val WGS84_DATUM_NAMES =
    Set("wgs84", "wgs1984", "worldgeodeticsystem1984", "worldgeodeticsystem1984ensemble")

  /** Normalized `reference_ellipsoid_name` values consistent with the WGS 84 ellipsoid. */
  private val WGS84_ELLIPSOID_NAMES = Set("wgs84", "wgs1984", "worldgeodeticsystem1984")

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
