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
   * @param netcdfFile NetCDF file to read
   * @param variableShortName short name of the variable to read
   * @param latDimShortName name of the coordinate variable serving the record variable's Y
   *     (second-to-last) dimension; it must be attached to exactly that dimension
   * @param lonDimShortName name of the coordinate variable serving the record variable's X (last)
   *     dimension; it must be attached to exactly that dimension
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
    // The named coordinate variables must serve the record variable's trailing (Y, X)
    // dimensions — the raster layout assumes them — so each is validated against its intended
    // axis; non-spatial, duplicate, or swapped coordinates are rejected.
    Dimension latDimension = variableDimensions.get(numDimensions - 2);
    Dimension lonDimension = variableDimensions.get(numDimensions - 1);

    Variable[] coordVariablePair =
        getCoordVariables(
            netcdfFile,
            recordVariable,
            latDimension,
            lonDimension,
            latDimShortName,
            lonDimShortName);
    Variable latVariable = coordVariablePair[1];
    Variable lonVariable = coordVariablePair[0];
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

  /** Index of the given dimension within the record variable's dimensions, by identity. */
  private static int dimensionIndexOf(Variable recordVariable, Dimension dim) {
    List<Dimension> dims = recordVariable.getDimensions();
    for (int i = 0; i < dims.size(); i++) {
      if (dims.get(i) == dim) return i;
    }
    return -1;
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
    boolean isIncreasing = false;
    HashMap<String, List<String>> varMetadata = new HashMap<>();

    // ========================= Set Raster geo-reference (width, height, scaleX, scaleY
    // =========================
    lonData = lonVariable.read();
    width = lonData.getShape()[0];
    double firstVal, lastVal;
    firstVal = getDecodedElement(lonVariable, lonData, 0);
    lastVal = getDecodedElement(lonVariable, lonData, width - 1);
    isIncreasing = firstVal < lastVal;
    minX = isIncreasing ? firstVal : lastVal;
    translateX = Math.abs(lastVal - firstVal) / (width - 1);

    latData = latVariable.read();
    height = latData.getShape()[0];
    firstVal = getDecodedElement(latVariable, latData, 0);
    lastVal = getDecodedElement(latVariable, latData, height - 1);
    isIncreasing = firstVal < lastVal;
    maxY = isIncreasing ? lastVal : firstVal;
    translateY = Math.abs(lastVal - firstVal) / (height - 1);
    // ======================================================================================================

    List<Dimension> recordDimensions = recordVariable.getDimensions();
    int numRecordDimensions = recordDimensions.size();

    // ==================== Re-check existence of dimensions if necessary ====================
    int lonIndex = numRecordDimensions - 1;
    int latIndex = numRecordDimensions - 2;
    if (findCoordIndices) {
      // Derive the axis indices from the coordinate variables' actual dimension identities,
      // never from name matching, so a coordinate whose name differs from (or collides with)
      // a dimension name still slices the correct axis
      latIndex = dimensionIndexOf(recordVariable, latVariable.getDimension(0));
      lonIndex = dimensionIndexOf(recordVariable, lonVariable.getDimension(0));
      if (latIndex == -1 || lonIndex == -1 || latIndex == lonIndex) {
        throw new IllegalArgumentException(NetCdfConstants.COORD_IDX_NOT_FOUND);
      }
    }

    // read recordVariable data
    Array recordData = recordVariable.read();

    // Set noDataValue if available, null otherwise
    Double noDataValue = null;
    if (recordVariable.attributes().hasAttribute(NetCdfConstants.MISSING_VALUE)) {
      Attribute noDataAttribute = recordVariable.findAttribute(NetCdfConstants.MISSING_VALUE);
      if (!Objects.isNull(noDataAttribute)) noDataValue = getAttrDoubleValue(noDataAttribute);
    }
    int[] strides = recordData.getIndex().getShape();
    int[] pointers = recordData.getIndex().getCurrentCounter();
    int numBands = 1;
    int numValuesPerBand = 1;
    for (int i = 0; i < numRecordDimensions; i++) {
      // Compare by axis index, not by name: a band dimension sharing a coordinate variable's
      // name must still count toward the bands
      if (i == latIndex || i == lonIndex) {
        numValuesPerBand *= strides[i];
      } else {
        numBands *= strides[i];
      }
    }
    Array[] dimensionVars = new Array[numRecordDimensions];
    double scaleFactor = 1, offset = 0;
    for (int i = 0; i < numRecordDimensions; i++) {
      if (i == lonIndex) dimensionVars[i] = lonData;
      else if (i == latIndex) dimensionVars[i] = latData;
      else {
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
          if (attr.getShortName().equalsIgnoreCase(NetCdfConstants.SCALE_FACTOR)) {
            Array values = attr.getValues();
            if (!Objects.isNull(values)) scaleFactor = getElement(attr.getValues(), 0);
          }
          if (attr.getShortName().equalsIgnoreCase(NetCdfConstants.ADD_OFFSET)) {
            Array values = attr.getValues();
            if (!Objects.isNull(values)) offset = getElement(attr.getValues(), 0);
          }
        }
        dimensionVars[i] = recordDimVar.read();
      }
    }
    double[][] allBandValues = new double[numBands][numValuesPerBand];
    // check for offset in the record variable
    List<List<String>> bandMetaData = new ArrayList<>();
    int currBandNum = 0;
    while (currBandNum < numBands) {
      if (dimensionVars.length > 2) {
        // start from the bottom 3rd dimension going up to form metadata
        List<String> currBandMetadata = new ArrayList<>();
        for (int metadataDim = dimensionVars.length - 3; metadataDim >= 0; metadataDim--) {
          double data = getElement(dimensionVars[metadataDim], pointers[metadataDim]);
          String metadata = recordDimensions.get(metadataDim).getShortName() + " : " + data;
          currBandMetadata.add(metadata);
        }
        bandMetaData.add(currBandMetadata);
      }
      // int dataIndex = currBandNum;
      for (int j = height - 1; j >= 0; j--) {
        for (int i = 0; i < width; i++) {
          int index = (j * width + i);
          int dataIndex = currBandNum * (width * height) + ((height - 1 - j) * width + i);
          double currRecord = scaleFactor * getElement(recordData, dataIndex) + offset;
          allBandValues[currBandNum][index] = currRecord;
        }
      }
      boolean addCarry = true;
      for (int currDim = dimensionVars.length - 3; currDim >= 0; currDim--) {
        int maxIndex = strides[currDim];
        if (addCarry) {
          pointers[currDim]++;
          addCarry = false;
        }
        if (pointers[currDim] == maxIndex) {
          pointers[currDim] = 0;
          addCarry = true;
        }
      }
      currBandNum++;
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

  /**
   * Read a coordinate element decoded per CF packing conventions: unsigned integer storage
   * (unsigned data types, or classic {@code _Unsigned="true"}) is widened, then {@code
   * scale_factor} and {@code add_offset} are applied. Without this, packed coordinate variables
   * would georeference the raster in raw storage units.
   */
  private static double getDecodedElement(Variable var, Array data, int index) {
    double raw = getElement(data, index);
    DataType dataType = var.getDataType();
    boolean unsigned =
        dataType.isIntegral()
            && (dataType.isUnsigned()
                || "true"
                    .equalsIgnoreCase(var.attributes().findAttributeString("_Unsigned", null)));
    if (unsigned && raw < 0) {
      raw += Math.pow(2, dataType.getSize() * 8);
    }
    double scale = getNumericAttr(var, NetCdfConstants.SCALE_FACTOR, 1.0);
    double offset = getNumericAttr(var, NetCdfConstants.ADD_OFFSET, 0.0);
    return raw * scale + offset;
  }

  private static double getNumericAttr(Variable var, String name, double defaultValue) {
    Attribute attr = var.attributes().findAttribute(name);
    if (attr == null) return defaultValue;
    Number numericValue = attr.getNumericValue();
    return numericValue == null ? defaultValue : numericValue.doubleValue();
  }

  private static Double getAttrDoubleValue(Attribute attr) {
    Number numericValue = attr.getNumericValue();
    if (!Objects.isNull(numericValue)) {
      return numericValue.doubleValue();
    } else {
      throw new IllegalArgumentException(NetCdfConstants.NON_NUMERIC_VALUE);
    }
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
