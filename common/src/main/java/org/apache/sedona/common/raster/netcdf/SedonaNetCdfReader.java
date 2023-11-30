package org.apache.sedona.common.raster.netcdf;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.sedona.common.raster.RasterConstructors;
import org.geotools.coverage.grid.GridCoverage2D;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.datum.PixelInCell;
import ucar.ma2.Array;
import ucar.nc2.*;

import java.io.IOException;
import java.util.*;

public class SedonaNetCdfReader {
    /*
    ==================================== Sedona NetCDF Public APIS ====================================
     */

    /**
     * Return a string depicting information about record variables in the provided netcdf file along with its dimensions
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
            recordInfo.append(variable.getNameAndDimensions(false)); //make strict true to get short name
            if (i != numRecordVariables - 1) recordInfo.append("\n\n");
        }
        return recordInfo.toString();
    }

    /**
     * Return a raster containing the record variable data, assuming the last two dimensions of the variable as Y and X respectively
     * @param netcdfFile NetCDF file to read
     * @param variableShortName short name of the variable to read
     * @return GridCoverage2D object
     * @throws FactoryException
     * @throws IOException
     */
    public static GridCoverage2D getRaster(NetcdfFile netcdfFile, String variableShortName) throws FactoryException, IOException {
        Variable recordVariable = getRecordVariableFromShortName(netcdfFile, variableShortName);
        ImmutableList<Dimension> variableDimensions = recordVariable.getDimensions();
        int numDimensions = variableDimensions.size();
        if (numDimensions < 2) {
            throw new IllegalArgumentException(SedonaNetCdfConstants.INSUFFICIENT_DIMENSIONS_VARIABLE);
        }
        //Assume ... Y, X dimension list in the variable
        String latDimensionShortName = variableDimensions.get(numDimensions - 2).getShortName();
        String lonDimensionShortName = variableDimensions.get(numDimensions - 1).getShortName();

        ImmutablePair<Variable, Variable> coordVariablePair = getCoordVariables(netcdfFile, latDimensionShortName, lonDimensionShortName);
        Variable latVariable = coordVariablePair.right;
        Variable lonVariable = coordVariablePair.left;
        return SedonaNetCdfReader.getRasterHelper(netcdfFile, recordVariable, lonVariable, latVariable, false);
    }

    /**
     *
     * @param netcdfFile NetCDF file to read
     * @param variableShortName short name of the variable to read
     * @param latDimShortName short name of the variable that serves as the lat dimension of the record variable
     * @param lonDimShortName short name of the variable that serves as the lon dimension of the record variable
     * @return GridCoverage2D object
     * @throws FactoryException
     * @throws IOException
     */
    public static GridCoverage2D getRaster(NetcdfFile netcdfFile, String variableShortName, String latDimShortName, String lonDimShortName) throws FactoryException, IOException {
        Variable recordVariable = getRecordVariableFromShortName(netcdfFile, variableShortName);

        ImmutablePair<Variable, Variable> coordVariablePair = getCoordVariables(netcdfFile, latDimShortName, lonDimShortName);
        Variable latVariable = coordVariablePair.right;
        Variable lonVariable = coordVariablePair.left;
        return SedonaNetCdfReader.getRasterHelper(netcdfFile, recordVariable, lonVariable, latVariable, true);
    }

    /**
     * Return the properties map of the raster which contains metadata information about a netCDF file
     * @param raster GridCoverage2D raster created from a NetCDF file
     * @return a map of containing metadata
     */
    public static Map getRasterMetadata(GridCoverage2D raster) {
        return raster.getProperties();
    }

     /*
    ==================================== Sedona NetCDF Helper APIS ====================================
     */

    private static Variable getRecordVariableFromShortName(NetcdfFile netcdfFile, String variableShortName) {
        Variable recordVariable = findVariableRecursive(variableShortName, netcdfFile.getRootGroup());
        ensureRecordVar(recordVariable);
        return recordVariable;
    }

    private static ImmutablePair<Variable, Variable> getCoordVariables(NetcdfFile netcdfFile, String latVariableShortName, String lonVariableShortName) {
        Group rootGroup = netcdfFile.getRootGroup();

        //TODO: optimize to get all variables in 1 tree traversal?
        Variable lonVariable = findVariableRecursive(lonVariableShortName, rootGroup);
        Variable latVariable = findVariableRecursive(latVariableShortName, rootGroup);
        ensureCoordVar(lonVariable, latVariable);
        return new ImmutablePair<>(lonVariable, latVariable);
    }

    private static ImmutableList<Variable> getRecordVariables(NetcdfFile netcdfFile) {
        ImmutableList<Variable> variables = netcdfFile.getVariables();
        List<Variable> recordVariables = null;
        for (Variable variable: variables) {
            ImmutableList<Dimension> variableDimensions = variable.getDimensions();
            if (variableDimensions.size() < 2) continue; //record variables have least 2 dimensions (lat and lon)
            if (Objects.isNull(recordVariables)) recordVariables = new ArrayList<>();
            //ASSUMPTION: A record is not restricted to having an unlimited dimension
            recordVariables.add(variable);
        }
        if (Objects.isNull(recordVariables)) return null;
        return ImmutableList.copyOf(recordVariables);
    }
    private static GridCoverage2D getRasterHelper(NetcdfFile netcdfFile, Variable recordVariable, Variable lonVariable, Variable latVariable, boolean findCoordIndices) throws IOException, FactoryException {
        //assert(netcdfFile.getFileTypeVersion().equalsIgnoreCase("1")); //CDF - 1
        ImmutableList<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();
        Group rootGroup = netcdfFile.getRootGroup();

        Array lonData, latData;
        double minX = 0, maxY = 0, translateX, translateY, skewX = 0, skewY = 0;
        int width, height;
        HashMap<String, List<String>> varMetadata = new HashMap<>();


        //========================= Set Raster geo-reference (width, height, scaleX, scaleY =========================
        lonData = lonVariable.read();
        width = lonData.getShape()[0];
        double firstVal, lastVal;
        firstVal = getElement(lonData, 0);
        lastVal = getElement(lonData, width - 1);
        translateX = Math.abs(lastVal - firstVal) / (width - 1);

        latData = latVariable.read();
        height = latData.getShape()[0];
        firstVal = getElement(latData, 0);
        lastVal = getElement(latData, height - 1);
        translateY = Math.abs(lastVal - firstVal) / (height - 1);
        //======================================================================================================

        String lonVariableName = lonVariable.getShortName();
        String latVariableName = latVariable.getShortName();

        List<Dimension> recordDimensions = recordVariable.getDimensions();
        int numRecordDimensions = recordDimensions.size();

        //==================== Re-check existence of dimensions if necessary ====================
        int lonIndex = numRecordDimensions - 1;
        int latIndex = numRecordDimensions - 2;
        if (findCoordIndices) {
            lonIndex = recordVariable.findDimensionIndex(lonVariableName);
            latIndex = recordVariable.findDimensionIndex(latVariableName);
            if (latIndex == -1 || lonIndex == -1) {
                throw new IllegalArgumentException(SedonaNetCdfConstants.COORD_IDX_NOT_FOUND);
            }
        }

        //read recordVariable data
        Array recordData = recordVariable.read();

        // Set noDataValue if available, null otherwise
        Double noDataValue = null;
        if (recordVariable.attributes().hasAttribute(SedonaNetCdfConstants.MISSING_VALUE)) {
            Attribute noDataAttribute = recordVariable.findAttribute(SedonaNetCdfConstants.MISSING_VALUE);
            if (!Objects.isNull(noDataAttribute))
                noDataValue = getAttrDoubleValue(noDataAttribute);
        }
        int[] strides = recordData.getIndex().getShape();
        int[] pointers = recordData.getIndex().getCurrentCounter();
        int numBands = 1;
        int numValuesPerBand = 1;
        for (int i = 0; i < numRecordDimensions; i++) {
            Dimension recordDimension = recordDimensions.get(i);
            String dimensionShortName = recordDimension.getShortName();
            //TODO: Maybe replace this string comparison with index comparison since we already have lon and lat dimension indices stored?
            if (dimensionShortName.equalsIgnoreCase(latVariableName) || dimensionShortName.equalsIgnoreCase(lonVariableName)) {
                numValuesPerBand *= strides[i];
            }else {
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
                //TODO: This leads to tree traversal for dimensions, can we do this in one go?
                Variable recordDimVar = findVariableRecursive(dimensionShortName, rootGroup);//rootGroup.findVariableLocal(dimensionShortName);
                if (Objects.isNull(recordDimVar)) throw new IllegalArgumentException(String.format(SedonaNetCdfConstants.COORD_VARIABLE_NOT_FOUND, dimensionShortName));
                AttributeContainer recordDimAttrs = recordDimVar.attributes();
                varMetadata.computeIfAbsent(dimensionShortName, k -> new ArrayList<>());
                for (Attribute attr: recordDimAttrs) {
                    varMetadata.get(dimensionShortName).add(attr.getStringValue());
                    if (attr.getShortName().equalsIgnoreCase(SedonaNetCdfConstants.SCALE_FACTOR)) {
                        Array values = attr.getValues();
                        if (!Objects.isNull(values))
                            scaleFactor = getElement(attr.getValues(), 0);
                    }
                    if (attr.getShortName().equalsIgnoreCase(SedonaNetCdfConstants.ADD_OFFSET)) {
                        Array values = attr.getValues();
                        if (!Objects.isNull(values))
                            offset = getElement(attr.getValues(), 0);
                    }
                }
                dimensionVars[i] = recordDimVar.read();

            }
        }
        double[][] allBandValues = new double[numBands][numValuesPerBand];
        //check for offset in the record variable
        List<List<String>> bandMetaData = new ArrayList<>();
        int currBandNum = 0;
        while (currBandNum < numBands) {
            if (dimensionVars.length > 2) {
                //start from the bottom 3rd dimension going up to form metadata
                List<String> currBandMetadata = new ArrayList<>();
                for (int metadataDim = dimensionVars.length - 3; metadataDim >= 0; metadataDim--) {
                    double data = getElement(dimensionVars[metadataDim], pointers[metadataDim]);
                    String metadata = recordDimensions.get(metadataDim).getShortName() + " : " + data;
                    currBandMetadata.add(metadata);
                }
                bandMetaData.add(currBandMetadata);
            }
            //int dataIndex = currBandNum;
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
        Map<String, List<String>> rasterProperties = getRasterProperties(globalAttributes, varMetadata, bandMetaData);
        return RasterConstructors.makeNonEmptyRaster(numBands, width, height, minX, maxY, translateX, -translateY, skewX, skewY, 0, allBandValues, rasterProperties, noDataValue, PixelInCell.CELL_CENTER);
    }

    private static Variable findVariableRecursive(String shortName, Group currGroup) {
        if (!Objects.isNull(currGroup.findVariableLocal(shortName))) {
            return currGroup.findVariableLocal(shortName);
        }else {
            for (Group group: currGroup.getGroups()) {
                Variable ret = findVariableRecursive(shortName, group);
                if (!Objects.isNull(ret)) return ret;
            }
            return null;
        }
    }

    private static Map<String, List<String>> getRasterProperties(ImmutableList<Attribute> globalAttrs, Map<String, List<String>> varAttrs, List<List<String>> bandAttrs) {
        Map<String, List<String>> res = new HashMap<>(varAttrs);
        if (!Objects.isNull(globalAttrs) && !globalAttrs.isEmpty()) {
            List<String> globalAttrString = new ArrayList<>();
            for (Attribute globalAttr: globalAttrs) {
                globalAttrString.add(globalAttr.getStringValue());
            }
            res.put(SedonaNetCdfConstants.GLOBAL_ATTR_PREFIX, globalAttrString);
        }
        for(int band = 0; band < bandAttrs.size(); band++) {
            int currBandNum = band + 1;
            res.put(SedonaNetCdfConstants.BAND_ATTR_PREFIX + currBandNum, bandAttrs.get(band));
        }
        return res;
    }

    private static Double getElement(Array array, int index) {
        double res;
        switch (array.getDataType()) {
            case INT:
                res = array.getInt(index);
                break;
            case LONG:
                res = array.getLong(index);
                break;
            case FLOAT:
                res = array.getFloat(index);
                break;
            case DOUBLE:
                res = array.getDouble(index);
                break;
            case BYTE:
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

    private static Double getAttrDoubleValue(Attribute attr) {
        Number numericValue = attr.getNumericValue();
        if (!Objects.isNull(numericValue)) {
            return (double) numericValue;
        }else {
            throw new IllegalArgumentException(SedonaNetCdfConstants.NON_NUMERIC_VALUE);
        }
    }


    private static void ensureCoordVar(Variable lonVar, Variable latVar) throws NullPointerException, IllegalArgumentException {
        if (latVar == null) {
            throw new NullPointerException(SedonaNetCdfConstants.INVALID_LAT_NAME);
        }

        if (lonVar == null) {
            throw new NullPointerException(SedonaNetCdfConstants.INVALID_LON_NAME);
        }

        if (latVar.getShape().length > 1) {
            throw new IllegalArgumentException(SedonaNetCdfConstants.INVALID_LAT_SHAPE);
        }

        if (lonVar.getShape().length > 1) {
            throw new IllegalArgumentException(SedonaNetCdfConstants.INVALID_LON_SHAPE);
        }
    }

    private static void ensureRecordVar(Variable recordVar) throws NullPointerException {
        if (recordVar == null) {
            throw new IllegalArgumentException(SedonaNetCdfConstants.INVALID_RECORD_NAME);
        }
    }


}
