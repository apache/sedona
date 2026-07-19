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
package org.apache.sedona.common.raster.netcdf;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.*;
import org.apache.sedona.common.raster.RasterConstructors;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.coverage.grid.GridCoverage2D;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.*;

public class NetCdfReader {
  /*
  ==================================== Sedona NetCDF Public APIS ====================================
   */

  /**
   * Return a string depicting information about record variables in the provided netcdf file along
   * with its dimensions
   *
   * @param netcdfFile NetCDF file to retrieve info of
   * @return a string containing information about all record variables present within the file
   */
  public static String getRecordInfo(NetcdfFile netcdfFile) {
    ImmutableList<Variable> recordVariables = getRecordVariables(netcdfFile);
    if (Objects.isNull(recordVariables) || recordVariables.isEmpty()) return "";
    int numRecordVariables = recordVariables.size();
    StringBuilder recordInfo = new StringBuilder();
    for (int i = 0; i < numRecordVariables; i++) {
      Variable variable = recordVariables.get(i);
      recordInfo.append(variable.getNameAndDimensions(false)); // make strict true to get short name
      if (i != numRecordVariables - 1) recordInfo.append("\n\n");
    }
    return recordInfo.toString();
  }

  /**
   * Return a raster containing the record variable data, assuming the last two dimensions of the
   * variable as Y and X respectively
   *
   * @param netcdfFile NetCDF file to read
   * @param variableShortName short name of the variable to read
   * @return GridCoverage2D object
   * @throws FactoryException
   * @throws IOException
   */
  public static GridCoverage2D getRaster(NetcdfFile netcdfFile, String variableShortName)
      throws FactoryException, IOException {
    Variable recordVariable = getRecordVariableFromShortName(netcdfFile, variableShortName);
    ImmutableList<Dimension> variableDimensions = recordVariable.getDimensions();
    int numDimensions = variableDimensions.size();
    if (numDimensions < 2) {
      throw new IllegalArgumentException(NetCdfConstants.INSUFFICIENT_DIMENSIONS_VARIABLE);
    }
    // Assume ... Y, X dimension list in the variable
    Dimension latDimension = variableDimensions.get(numDimensions - 2);
    Dimension lonDimension = variableDimensions.get(numDimensions - 1);

    Variable[] coordVariablePair =
        getCoordVariables(
            netcdfFile,
            recordVariable,
            latDimension,
            lonDimension,
            latDimension.getShortName(),
            lonDimension.getShortName());
    Variable latVariable = coordVariablePair[1];
    Variable lonVariable = coordVariablePair[0];
    return NetCdfReader.getRasterHelper(
        netcdfFile, recordVariable, lonVariable, latVariable, false);
  }

  /**
   * Return a raster for the record variable using explicitly named Y and X coordinate variables.
   * Each named coordinate must be attached to one of the record variable's dimensions; the axes are
   * derived from the coordinates' actual dimensions (preferring the trailing defaults when a name
   * is ambiguous), the raster plane is extracted with stride-aware permuted indexing, and every
   * remaining dimension becomes a band dimension in row-major order. A dimension repeated in the
   * variable's shape resolves to its trailing occurrence or is rejected as ambiguous.
   *
   * @param netcdfFile NetCDF file to read
   * @param variableShortName short name of the variable to read
   * @param latDimShortName name of the coordinate variable serving the raster's Y axis
   * @param lonDimShortName name of the coordinate variable serving the raster's X axis
   * @return GridCoverage2D object
   * @throws FactoryException
   * @throws IOException
   */
  public static GridCoverage2D getRaster(
      NetcdfFile netcdfFile,
      String variableShortName,
      String latDimShortName,
      String lonDimShortName)
      throws FactoryException, IOException {
    Variable recordVariable = getRecordVariableFromShortName(netcdfFile, variableShortName);
    ImmutableList<Dimension> variableDimensions = recordVariable.getDimensions();
    int numDimensions = variableDimensions.size();
    if (numDimensions < 2) {
      throw new IllegalArgumentException(NetCdfConstants.INSUFFICIENT_DIMENSIONS_VARIABLE);
    }
    // Prefer coordinates serving the default trailing (Y, X) axes; when a named coordinate is
    // attached to another dimension of the record variable, the axes are derived from that
    // coordinate's dimension and the raster is extracted with permuted strides, every
    // remaining dimension becoming a band dimension. Coordinates over dimensions the record
    // variable does not have are rejected.
    Dimension latDimension = variableDimensions.get(numDimensions - 2);
    Dimension lonDimension = variableDimensions.get(numDimensions - 1);

    Variable latVariable =
        findCoordVariableWithFallback(netcdfFile, recordVariable, latDimension, latDimShortName);
    Variable lonVariable =
        findCoordVariableWithFallback(netcdfFile, recordVariable, lonDimension, lonDimShortName);
    ensureCoordVar(lonVariable, latVariable);
    return NetCdfReader.getRasterHelper(netcdfFile, recordVariable, lonVariable, latVariable, true);
  }

  /**
   * Return the properties map of the raster which contains metadata information about a netCDF file
   *
   * @param raster GridCoverage2D raster created from a NetCDF file
   * @return a map of containing metadata
   */
  public static Map getRasterMetadata(GridCoverage2D raster) {
    return raster.getProperties();
  }

  /*
  ==================================== Sedona NetCDF Helper APIS ====================================
   */

  private static Variable getRecordVariableFromShortName(
      NetcdfFile netcdfFile, String variableShortName) {
    Variable recordVariable;
    if (variableShortName != null && variableShortName.contains("/")) {
      // Full-path names such as "sub/temp" (as reported by the netcdf.metadata data source
      // for variables in nested groups) resolve through the file's full-name lookup.
      recordVariable = netcdfFile.findVariable(variableShortName);
    } else {
      recordVariable = findVariableRecursive(variableShortName, netcdfFile.getRootGroup());
    }
    ensureRecordVar(recordVariable);
    return recordVariable;
  }

  private static Variable[] getCoordVariables(
      NetcdfFile netcdfFile,
      Variable recordVariable,
      Dimension latDim,
      Dimension lonDim,
      String latVariableShortName,
      String lonVariableShortName) {
    Variable lonVariable =
        findCoordVariable(netcdfFile, recordVariable, lonDim, lonVariableShortName);
    Variable latVariable =
        findCoordVariable(netcdfFile, recordVariable, latDim, latVariableShortName);
    ensureCoordVar(lonVariable, latVariable);
    return new Variable[] {lonVariable, latVariable};
  }

  /**
   * Resolve the coordinate variable for a dimension of the record variable. Full-path names
   * ("sub/lat") resolve directly. Plain names search the record variable's group and its ancestors
   * first, then laterally across the whole file. In every case — including path-resolved candidates
   * — the variable must be rank-1, numeric, and attached to the expected dimension (matched by
   * identity), so a same-named variable over a different axis (e.g. a local {@code lat(time)} next
   * to the true {@code lat(lat)}) or an unrelated group's dimension is never selected.
   */
  private static Variable findCoordVariable(
      NetcdfFile netcdfFile, Variable recordVariable, Dimension expectedDim, String name) {
    if (name == null) return null;
    if (name.contains("/")) {
      Variable v = netcdfFile.findVariable(name);
      return isCoordinateFor(v, expectedDim) ? v : null;
    }
    for (Group group = recordVariable.getParentGroup();
        group != null;
        group = group.getParentGroup()) {
      Variable v = group.findVariableLocal(name);
      if (isCoordinateFor(v, expectedDim)) return v;
    }
    for (Variable v : netcdfFile.getVariables()) {
      if (name.equals(v.getShortName()) && isCoordinateFor(v, expectedDim)) {
        return v;
      }
    }
    return null;
  }

  /**
   * Whether a variable can serve as the coordinate variable for the expected dimension: rank-1,
   * numeric, and its sole dimension is that exact dimension (identity).
   */
  private static boolean isCoordinateFor(Variable v, Dimension expectedDim) {
    if (v == null || v.getRank() != 1 || v.getDataType() == null || !v.getDataType().isNumeric()) {
      return false;
    }
    return v.getDimension(0) == expectedDim;
  }

  /**
   * Resolve an explicitly named coordinate: prefer a candidate serving the default trailing axis,
   * then fall back to a candidate attached to any of the record variable's dimensions (identity),
   * which selects a permuted axis handled by the stride-aware extraction.
   */
  private static Variable findCoordVariableWithFallback(
      NetcdfFile netcdfFile, Variable recordVariable, Dimension preferredDim, String name) {
    Variable v = findCoordVariable(netcdfFile, recordVariable, preferredDim, name);
    if (v != null) return v;
    return findCoordVariableAnyAxis(netcdfFile, recordVariable, name);
  }

  /** Same search order as {@link #findCoordVariable}, accepting any record dimension. */
  private static Variable findCoordVariableAnyAxis(
      NetcdfFile netcdfFile, Variable recordVariable, String name) {
    if (name == null) return null;
    if (name.contains("/")) {
      Variable v = netcdfFile.findVariable(name);
      return isCoordinateForAnyAxis(v, recordVariable) ? v : null;
    }
    for (Group group = recordVariable.getParentGroup();
        group != null;
        group = group.getParentGroup()) {
      Variable v = group.findVariableLocal(name);
      if (isCoordinateForAnyAxis(v, recordVariable)) return v;
    }
    for (Variable v : netcdfFile.getVariables()) {
      if (name.equals(v.getShortName()) && isCoordinateForAnyAxis(v, recordVariable)) {
        return v;
      }
    }
    return null;
  }

  private static boolean isCoordinateForAnyAxis(Variable v, Variable recordVariable) {
    if (v == null || v.getRank() != 1 || v.getDataType() == null || !v.getDataType().isNumeric()) {
      return false;
    }
    Dimension dim = v.getDimension(0);
    for (Dimension recordDim : recordVariable.getDimensions()) {
      if (recordDim == dim) return true;
    }
    return false;
  }

  /**
   * Index of the axis served by {@code dim} within the record variable's dimensions, matched by
   * identity. NetCDF permits a dimension to occur more than once in a variable's shape; when it
   * does, the occurrence at {@code preferredIndex} (the default trailing axis) is used if it
   * matches, otherwise the intended occurrence is ambiguous and -1 is returned.
   */
  private static int axisIndexOf(Variable recordVariable, Dimension dim, int preferredIndex) {
    List<Dimension> dims = recordVariable.getDimensions();
    int found = -1;
    int occurrences = 0;
    for (int i = 0; i < dims.size(); i++) {
      if (dims.get(i) == dim) {
        occurrences++;
        if (found == -1) found = i;
      }
    }
    if (occurrences <= 1) return found;
    return dims.get(preferredIndex) == dim ? preferredIndex : -1;
  }

  private static ImmutableList<Variable> getRecordVariables(NetcdfFile netcdfFile) {
    ImmutableList<Variable> variables = netcdfFile.getVariables();
    List<Variable> recordVariables = null;
    for (Variable variable : variables) {
      ImmutableList<Dimension> variableDimensions = variable.getDimensions();
      if (variableDimensions.size() < 2)
        continue; // record variables have least 2 dimensions (lat and lon)
      if (Objects.isNull(recordVariables)) recordVariables = new ArrayList<>();
      // ASSUMPTION: A record is not restricted to having an unlimited dimension
      recordVariables.add(variable);
    }
    if (Objects.isNull(recordVariables)) return null;
    return ImmutableList.copyOf(recordVariables);
  }

  private static GridCoverage2D getRasterHelper(
      NetcdfFile netcdfFile,
      Variable recordVariable,
      Variable lonVariable,
      Variable latVariable,
      boolean findCoordIndices)
      throws IOException, FactoryException {
    // assert(netcdfFile.getFileTypeVersion().equalsIgnoreCase("1")); //CDF - 1
    ImmutableList<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();

    Array lonData, latData;
    double minX = 0, maxY = 0, translateX, translateY, skewX = 0, skewY = 0;
    int width, height;
    boolean lonIncreasing;
    boolean latIncreasing;
    HashMap<String, List<String>> varMetadata = new HashMap<>();

    // ========================= Set Raster geo-reference (width, height, scaleX, scaleY
    // =========================
    lonData = lonVariable.read();
    ValueDecoder lonDecoder = new ValueDecoder(lonVariable);
    width = lonData.getShape()[0];
    double firstVal, lastVal;
    firstVal = lonDecoder.get(lonData, 0);
    lastVal = lonDecoder.get(lonData, width - 1);
    lonIncreasing = firstVal < lastVal;
    minX = lonIncreasing ? firstVal : lastVal;
    translateX = Math.abs(lastVal - firstVal) / (width - 1);

    latData = latVariable.read();
    ValueDecoder latDecoder = new ValueDecoder(latVariable);
    height = latData.getShape()[0];
    firstVal = latDecoder.get(latData, 0);
    lastVal = latDecoder.get(latData, height - 1);
    latIncreasing = firstVal < lastVal;
    maxY = latIncreasing ? lastVal : firstVal;
    translateY = Math.abs(lastVal - firstVal) / (height - 1);
    // ======================================================================================================

    List<Dimension> recordDimensions = recordVariable.getDimensions();
    int numRecordDimensions = recordDimensions.size();

    // ==================== Derive the spatial axis indices ====================
    int lonIndex = numRecordDimensions - 1;
    int latIndex = numRecordDimensions - 2;
    if (findCoordIndices) {
      // Derive the axis indices from the coordinate variables' actual dimension identities,
      // never from name matching, so a coordinate whose name differs from (or collides with)
      // a dimension name still slices the correct axis. Repeated dimensions resolve to their
      // default trailing occurrence, or are rejected as ambiguous.
      latIndex = axisIndexOf(recordVariable, latVariable.getDimension(0), numRecordDimensions - 2);
      lonIndex = axisIndexOf(recordVariable, lonVariable.getDimension(0), numRecordDimensions - 1);
      if (latIndex == -1 || lonIndex == -1 || latIndex == lonIndex) {
        throw new IllegalArgumentException(NetCdfConstants.COORD_IDX_NOT_FOUND);
      }
    }

    // read recordVariable data
    Array recordData = recordVariable.read();
    ValueDecoder recordDecoder = new ValueDecoder(recordVariable);
    RawValueValidity recordValidity = new RawValueValidity(recordVariable, recordDecoder);
    Double noDataValue = recordValidity.outputNoData(recordData);
    int[] shape = recordData.getIndex().getShape();
    // Row-major strides of the source layout, so the raster plane can be extracted for any
    // spatial axis positions — not only trailing (y, x)
    long[] sourceStrides = new long[numRecordDimensions];
    long strideAccumulator = 1;
    for (int i = numRecordDimensions - 1; i >= 0; i--) {
      sourceStrides[i] = strideAccumulator;
      strideAccumulator *= shape[i];
    }
    // Every non-spatial dimension is a band dimension; bands iterate over them in row-major
    // order (last band dimension fastest), preserving the historical band order for trailing
    // spatial axes
    int[] bandDims = new int[numRecordDimensions - 2];
    int numBands = 1;
    {
      int b = 0;
      for (int i = 0; i < numRecordDimensions; i++) {
        if (i != latIndex && i != lonIndex) {
          bandDims[b++] = i;
          numBands *= shape[i];
        }
      }
    }
    int numValuesPerBand = width * height;
    ValueDecoder[] dimensionDecoders = new ValueDecoder[numRecordDimensions];
    Array[] dimensionData = new Array[numRecordDimensions];
    for (int i = 0; i < numRecordDimensions; i++) {
      if (i == lonIndex) {
        dimensionDecoders[i] = lonDecoder;
        dimensionData[i] = lonData;
      } else if (i == latIndex) {
        dimensionDecoders[i] = latDecoder;
        dimensionData[i] = latData;
      } else {
        Dimension recordDimension = recordDimensions.get(i);
        String dimensionShortName = recordDimension.getShortName();
        // Resolve within the record variable's scope, validated against this exact dimension,
        // so a same-named variable elsewhere (e.g. a root `time` next to /sub/time) or over a
        // different axis is never bound
        Variable recordDimVar =
            findCoordVariable(netcdfFile, recordVariable, recordDimension, dimensionShortName);
        if (Objects.isNull(recordDimVar))
          throw new IllegalArgumentException(
              String.format(NetCdfConstants.COORD_VARIABLE_NOT_FOUND, dimensionShortName));
        AttributeContainer recordDimAttrs = recordDimVar.attributes();
        varMetadata.computeIfAbsent(dimensionShortName, k -> new ArrayList<>());
        for (Attribute attr : recordDimAttrs) {
          varMetadata.get(dimensionShortName).add(attr.getStringValue());
        }
        dimensionDecoders[i] = new ValueDecoder(recordDimVar);
        dimensionData[i] = recordDimVar.read();
      }
    }
    double[][] allBandValues = new double[numBands][numValuesPerBand];
    List<List<String>> bandMetaData = new ArrayList<>();
    int[] bandCounter = new int[bandDims.length];
    for (int currBandNum = 0; currBandNum < numBands; currBandNum++) {
      if (bandDims.length > 0) {
        // Band metadata in reversed band-dimension order (preserves the historical output)
        List<String> currBandMetadata = new ArrayList<>();
        for (int m = bandDims.length - 1; m >= 0; m--) {
          int dimIdx = bandDims[m];
          double data = dimensionDecoders[dimIdx].get(dimensionData[dimIdx], bandCounter[m]);
          String metadata = recordDimensions.get(dimIdx).getShortName() + " : " + data;
          currBandMetadata.add(metadata);
        }
        bandMetaData.add(currBandMetadata);
      }
      long bandOffset = 0;
      for (int m = 0; m < bandDims.length; m++) {
        bandOffset += (long) bandCounter[m] * sourceStrides[bandDims[m]];
      }
      // Stride-aware extraction of the (y, x) plane: for trailing spatial axes this reduces
      // exactly to the contiguous-plane arithmetic; for permuted axes it walks the source
      // with the axis strides. The transform is normalized to west-to-east, north-up order, so
      // preserve each coordinate variable's direction when selecting the source element.
      for (int j = height - 1; j >= 0; j--) {
        for (int i = 0; i < width; i++) {
          int sourceY = latIncreasing ? height - 1 - j : j;
          int sourceX = lonIncreasing ? i : width - 1 - i;
          long sourceIndex =
              bandOffset
                  + (long) sourceY * sourceStrides[latIndex]
                  + (long) sourceX * sourceStrides[lonIndex];
          double currRecord =
              recordValidity.isInvalid(recordData, (int) sourceIndex)
                  ? noDataValue
                  : recordDecoder.get(recordData, (int) sourceIndex);
          allBandValues[currBandNum][j * width + i] = currRecord;
        }
      }
      // Advance the band odometer (row-major: last band dimension fastest)
      for (int m = bandDims.length - 1; m >= 0; m--) {
        bandCounter[m]++;
        if (bandCounter[m] < shape[bandDims[m]]) break;
        bandCounter[m] = 0;
      }
    }
    Map<String, List<String>> rasterProperties =
        getRasterProperties(globalAttributes, varMetadata, bandMetaData);
    return RasterConstructors.makeNonEmptyRaster(
        numBands,
        width,
        height,
        minX,
        maxY,
        translateX,
        -translateY,
        skewX,
        skewY,
        0,
        allBandValues,
        rasterProperties,
        noDataValue,
        PixelInCell.CELL_CENTER);
  }

  private static Variable findVariableRecursive(String shortName, Group currGroup) {
    if (!Objects.isNull(currGroup.findVariableLocal(shortName))) {
      return currGroup.findVariableLocal(shortName);
    } else {
      for (Group group : currGroup.getGroups()) {
        Variable ret = findVariableRecursive(shortName, group);
        if (!Objects.isNull(ret)) return ret;
      }
      return null;
    }
  }

  private static Map<String, List<String>> getRasterProperties(
      ImmutableList<Attribute> globalAttrs,
      Map<String, List<String>> varAttrs,
      List<List<String>> bandAttrs) {
    Map<String, List<String>> res = new HashMap<>(varAttrs);
    if (!Objects.isNull(globalAttrs) && !globalAttrs.isEmpty()) {
      List<String> globalAttrString = new ArrayList<>();
      for (Attribute globalAttr : globalAttrs) {
        globalAttrString.add(globalAttr.getStringValue());
      }
      res.put(NetCdfConstants.GLOBAL_ATTR_PREFIX, globalAttrString);
    }
    for (int band = 0; band < bandAttrs.size(); band++) {
      int currBandNum = band + 1;
      res.put(NetCdfConstants.BAND_ATTR_PREFIX + currBandNum, bandAttrs.get(band));
    }
    return res;
  }

  private static Double getElement(Array array, int index) {
    double res;
    switch (array.getDataType()) {
      case INT:
      case UINT:
        res = array.getInt(index);
        break;
      case LONG:
      case ULONG:
        res = array.getLong(index);
        break;
      case FLOAT:
        res = array.getFloat(index);
        break;
      case DOUBLE:
        res = array.getDouble(index);
        break;
      case BYTE:
      case UBYTE:
        res = array.getByte(index);
        break;
      case SHORT:
      case USHORT:
        res = array.getShort(index);
        break;
      default:
        throw new IllegalArgumentException("Error casting dimension data to double");
    }
    return res;
  }

  /** Decode values with the unsigned and CF packing attributes of their owning variable. */
  private static final class ValueDecoder {
    private final DataType dataType;
    private final boolean unsigned;
    private final int bitWidth;
    private final double unsignedRange;
    private final double scale;
    private final double offset;

    private ValueDecoder(Variable var) {
      dataType = var.getDataType();
      unsigned =
          dataType.isIntegral()
              && (dataType.isUnsigned()
                  || "true"
                      .equalsIgnoreCase(var.attributes().findAttributeString("_Unsigned", null)));
      bitWidth = dataType.getSize() * 8;
      unsignedRange = unsigned ? Math.pow(2, bitWidth) : 0;
      scale = getNumericAttr(var, NetCdfConstants.SCALE_FACTOR, 1.0);
      offset = getNumericAttr(var, NetCdfConstants.ADD_OFFSET, 0.0);
    }

    private double get(Array data, int index) {
      return decodeWidened(getWidened(data, index));
    }

    private double getWidened(Array data, int index) {
      if (!isIntegral()) return getElement(data, index);
      long value = getIntegral(data, index);
      double widened = value;
      if (unsigned && bitWidth == Long.SIZE && value < 0) widened += unsignedRange;
      return widened;
    }

    private long getIntegral(Array data, int index) {
      return normalizeIntegral(data.getLong(index));
    }

    private long normalizeIntegral(long value) {
      if (!unsigned || bitWidth == Long.SIZE) return value;
      return value & ((1L << bitWidth) - 1);
    }

    private int compareIntegral(long left, long right) {
      return unsigned ? Long.compareUnsigned(left, right) : Long.compare(left, right);
    }

    private double decodeWidened(double value) {
      return value * scale + offset;
    }

    private boolean isPacked() {
      return scale != 1.0 || offset != 0.0;
    }

    private boolean isIntegral() {
      return dataType.isIntegral();
    }

    private boolean is64BitIntegral() {
      return isIntegral() && bitWidth == Long.SIZE;
    }
  }

  /** Evaluate CF missing-value and validity attributes in the packed/raw value domain. */
  private static final class RawValueValidity {
    private final ValueDecoder decoder;
    private final List<Long> integralMissingValues = new ArrayList<>();
    private final List<Double> floatingMissingValues = new ArrayList<>();
    private final Long integralValidMin;
    private final Long integralValidMax;
    private final Double floatingValidMin;
    private final Double floatingValidMax;

    private RawValueValidity(Variable var, ValueDecoder decoder) {
      this.decoder = decoder;
      Attribute fillValue = var.findAttribute("_FillValue");
      Attribute missingValue = var.findAttribute(NetCdfConstants.MISSING_VALUE);
      Attribute validRange = var.findAttribute(NetCdfConstants.VALID_RANGE);
      Attribute validMin = var.findAttribute(NetCdfConstants.VALID_MIN);
      Attribute validMax = var.findAttribute(NetCdfConstants.VALID_MAX);

      if (decoder.isIntegral()) {
        addIntegralValues(fillValue, decoder, integralMissingValues);
        addIntegralValues(missingValue, decoder, integralMissingValues);
        Long min = null;
        Long max = null;
        if (validRange != null && validRange.getLength() >= 2) {
          Long first = integralValue(validRange, 0, decoder);
          Long second = integralValue(validRange, 1, decoder);
          if (first != null && second != null) {
            min = decoder.compareIntegral(first, second) <= 0 ? first : second;
            max = decoder.compareIntegral(first, second) <= 0 ? second : first;
          }
        } else {
          min = integralValue(validMin, 0, decoder);
          max = integralValue(validMax, 0, decoder);
        }
        integralValidMin = min;
        integralValidMax = max;
        floatingValidMin = null;
        floatingValidMax = null;
      } else {
        addFloatingValues(fillValue, floatingMissingValues);
        addFloatingValues(missingValue, floatingMissingValues);
        Double min = null;
        Double max = null;
        if (validRange != null && validRange.getLength() >= 2) {
          Double first = floatingValue(validRange, 0);
          Double second = floatingValue(validRange, 1);
          if (first != null && second != null) {
            min = Math.min(first, second);
            max = Math.max(first, second);
          }
        } else {
          min = floatingValue(validMin, 0);
          max = floatingValue(validMax, 0);
        }
        integralValidMin = null;
        integralValidMax = null;
        floatingValidMin = min;
        floatingValidMax = max;
      }
    }

    private boolean isInvalid(Array data, int index) {
      if (decoder.isIntegral()) {
        long value = decoder.getIntegral(data, index);
        for (long missing : integralMissingValues) {
          if (value == missing) return true;
        }
        if (integralValidMin != null && decoder.compareIntegral(value, integralValidMin) < 0)
          return true;
        return integralValidMax != null && decoder.compareIntegral(value, integralValidMax) > 0;
      }

      double value = getElement(data, index);
      for (double missing : floatingMissingValues) {
        if (sameValue(value, missing)) return true;
      }
      if (floatingValidMin != null && !(value >= floatingValidMin)) return true;
      return floatingValidMax != null && !(value <= floatingValidMax);
    }

    private Double outputNoData(Array data) {
      if (!hasValidityRules()) return null;
      if (!requiresSyntheticNoData()) return firstMissingValue();

      // Pick a finite double absent from every valid decoded sample. The N+1 candidate values and
      // at most N input samples guarantee a free slot without retaining every decoded value.
      long sizeLong = data.getSize();
      if (sizeLong > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("NetCDF variable contains too many values");
      }
      int size = (int) sizeLong;
      BitSet occupiedCandidates = new BitSet();
      for (int i = 0; i < size; i++) {
        if (isInvalid(data, i)) continue;
        double value = decoder.get(data, i);
        if (value >= 0 && value <= size) {
          int candidate = (int) value;
          if (value == candidate) occupiedCandidates.set(candidate);
        }
      }
      int freeCandidate = occupiedCandidates.nextClearBit(0);
      return (double) freeCandidate;
    }

    private boolean hasValidityRules() {
      if (decoder.isIntegral()) {
        return !integralMissingValues.isEmpty()
            || integralValidMin != null
            || integralValidMax != null;
      }
      return !floatingMissingValues.isEmpty()
          || floatingValidMin != null
          || floatingValidMax != null;
    }

    private boolean requiresSyntheticNoData() {
      if (decoder.isPacked() || decoder.is64BitIntegral()) return true;
      if (decoder.isIntegral()) return integralMissingValues.isEmpty();
      return floatingMissingValues.isEmpty() || !Double.isFinite(floatingMissingValues.get(0));
    }

    private double firstMissingValue() {
      return decoder.isIntegral()
          ? integralMissingValues.get(0).doubleValue()
          : floatingMissingValues.get(0);
    }

    private static void addIntegralValues(
        Attribute attr, ValueDecoder decoder, List<Long> destination) {
      if (attr == null) return;
      for (int i = 0; i < attr.getLength(); i++) {
        Long value = integralValue(attr, i, decoder);
        if (value != null) destination.add(value);
      }
    }

    private static Long integralValue(Attribute attr, int index, ValueDecoder decoder) {
      if (attr == null || index >= attr.getLength()) return null;
      Number value = attr.getNumericValue(index);
      return value == null ? null : decoder.normalizeIntegral(value.longValue());
    }

    private static void addFloatingValues(Attribute attr, List<Double> destination) {
      if (attr == null) return;
      for (int i = 0; i < attr.getLength(); i++) {
        Double value = floatingValue(attr, i);
        if (value != null) destination.add(value);
      }
    }

    private static Double floatingValue(Attribute attr, int index) {
      if (attr == null || index >= attr.getLength()) return null;
      Number value = attr.getNumericValue(index);
      return value == null ? null : value.doubleValue();
    }

    private static boolean sameValue(double left, double right) {
      return left == right || (Double.isNaN(left) && Double.isNaN(right));
    }
  }

  private static double getNumericAttr(Variable var, String name, double defaultValue) {
    Attribute attr = var.attributes().findAttribute(name);
    if (attr == null) return defaultValue;
    Number numericValue = attr.getNumericValue();
    return numericValue == null ? defaultValue : numericValue.doubleValue();
  }

  private static void ensureCoordVar(Variable lonVar, Variable latVar)
      throws NullPointerException, IllegalArgumentException {
    if (latVar == null) {
      throw new NullPointerException(NetCdfConstants.INVALID_LAT_NAME);
    }

    if (lonVar == null) {
      throw new NullPointerException(NetCdfConstants.INVALID_LON_NAME);
    }

    if (latVar.getShape().length > 1) {
      throw new IllegalArgumentException(NetCdfConstants.INVALID_LAT_SHAPE);
    }

    if (lonVar.getShape().length > 1) {
      throw new IllegalArgumentException(NetCdfConstants.INVALID_LON_SHAPE);
    }
  }

  private static void ensureRecordVar(Variable recordVar) throws NullPointerException {
    if (recordVar == null) {
      throw new IllegalArgumentException(NetCdfConstants.INVALID_RECORD_NAME);
    }
  }
}
