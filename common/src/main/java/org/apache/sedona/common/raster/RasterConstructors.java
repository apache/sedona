/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.raster;

import com.google.common.collect.ImmutableList;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.sedona.common.raster.inputstream.ByteArrayImageInputStream;
import org.apache.sedona.common.raster.netcdf.NetCDFConstants;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.io.netcdf.NetCDFReader;
import org.geotools.data.DataSourceException;
import org.geotools.gce.arcgrid.ArcGridReader;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.w3c.dom.Attr;
import sun.nio.ch.Net;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.*;

import javax.media.jai.RasterFactory;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.*;

import static org.ejml.UtilEjml.assertTrue;

public class RasterConstructors
{
    public static GridCoverage2D fromArcInfoAsciiGrid(byte[] bytes) throws IOException {
        ArcGridReader reader = new ArcGridReader(new ByteArrayImageInputStream(bytes), new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
        return reader.read(null);
    }

    public static GridCoverage2D fromGeoTiff(byte[] bytes) throws IOException {
        GeoTiffReader geoTiffReader = new GeoTiffReader(new ByteArrayImageInputStream(bytes), new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
        return geoTiffReader.read(null);
    }

    public static GridCoverage2D fromNetCDF(byte[] bytes, String lonDimensionName, String latDimensionName, String variableName, boolean addMetadata) throws IOException, FactoryException {
       NetcdfFile netcdfFile = NetcdfFiles.openInMemory("", bytes);
       GridCoverage2D raster = readNetCDFClassic(netcdfFile, lonDimensionName, latDimensionName, variableName);
        //NetCDFReader netCDFReader = new NetCDFReader(new ByteArrayImageInputStream(bytes), new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
        //String[] names = netCDFReader.getGridCoverageNames();
       // return netCDFReader.read(names[0], null);
        return raster;
    }

    private static GridCoverage2D readNetCDFClassic(NetcdfFile netcdfFile, String lonVariableName, String latVariableName, String variableName) throws IOException, FactoryException {
        //assert(netcdfFile.getFileTypeVersion().equalsIgnoreCase("1")); //CDF - 1
        ImmutableList<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();
        Group rootGroup = netcdfFile.getRootGroup();
        Variable lonVar = findVariableRecursive(lonVariableName, rootGroup);//rootGroup.findVariableLocal(lonVariableName);
        Variable latVar = findVariableRecursive(latVariableName, rootGroup);//rootGroup.findVariableLocal(latVariableName);

//        Dimension lonDimension = findDimensionRecursive(lonVariableName, rootGroup);//rootGroup.findDimension(lonVariableName);
//        Dimension latDimension = findDimensionRecursive(latVariableName, rootGroup);//rootGroup.findDimension(latVariableName);

        Variable recordVariable = findVariableRecursive(variableName, rootGroup);//rootGroup.findVariableLocal(variableName);
        ensureCoordVar(lonVar, latVar);
        ensureRecordVar(recordVariable);
        AttributeContainer lonVarAttr = lonVar.attributes();
        AttributeContainer latVarAttr = latVar.attributes();
        Array lonData = null, latData = null;
        boolean readLon = false, readLat = false;
        double minX = 0, maxX = 0, minY = 0, maxY = 0, translateX = 0, translateY = 0, skewX = 0, skewY = 0;
        int width = 0, height = 0;
        HashMap<String, List<String>> varMetadata = new HashMap<>();

        //check min val lon attr
        if (lonVarAttr.hasAttribute(NetCDFConstants.VALID_MIN)) {
            minX = new Double(lonVarAttr.findAttribute(NetCDFConstants.VALID_MIN).getStringValue());
        }else readLon = true;

        //check max val lon attr
        if (lonVarAttr.hasAttribute(NetCDFConstants.VALID_MAX)) {
            maxX = new Double(lonVarAttr.findAttribute(NetCDFConstants.VALID_MAX).getStringValue());
        }else readLon = true;

        //check min val lat attr
        if (latVarAttr.hasAttribute(NetCDFConstants.VALID_MIN)) {
            minY = new Double(latVarAttr.findAttribute(NetCDFConstants.VALID_MIN).getStringValue());
        }else readLat = true;

        //check max val lat attr
        if (latVarAttr.hasAttribute(NetCDFConstants.VALID_MAX)) {
            maxY = new Double(latVarAttr.findAttribute(NetCDFConstants.VALID_MAX).getStringValue());
        }else readLat = true;

        //one of the metadata is missing, need to read data
        if (readLon) {
            lonData = lonVar.read();
            width = lonData.getShape()[0];
            boolean isIncreasing = false;
            double firstVal, lastVal;
            firstVal = getElement(lonData, 0);
            lastVal = getElement(lonData, width - 1);
            isIncreasing = firstVal < lastVal;
            minX = isIncreasing ? firstVal : lastVal;
            maxX = isIncreasing ? lastVal : firstVal;
            translateX = Math.abs(lastVal - firstVal) / (width - 1);
        }

        if (readLat) {
            latData = latVar.read();
            height = latData.getShape()[0];
            boolean isIncreasing = false;
            double firstVal, lastVal;
            firstVal = getElement(latData, 0);
            lastVal = getElement(latData, height - 1);
            isIncreasing = firstVal < lastVal;
            minY = isIncreasing ? firstVal : lastVal;
            maxY = isIncreasing ? lastVal : firstVal;
            translateY = Math.abs(lastVal - firstVal) / (height - 1);
        }

        if (!readLat && !readLon) {
            width = (int) (maxX - minX);
            height = (int) (maxY - minY);
        }

        int lonIndex = recordVariable.findDimensionIndex(lonVariableName);
        int latIndex = recordVariable.findDimensionIndex(latVariableName);
        if (latIndex == -1 || lonIndex == -1) {
            throw new IllegalArgumentException("Given record variable does not contain one of the latitude or longitude dimensions as the participating dimensions");
        }

        List<Dimension> recordDimensions = recordVariable.getDimensions();
        int numRecordDimensions = recordDimensions.size();
        Array recordData = recordVariable.read();
        Double noDataValue = null;
        if (recordVariable.attributes().hasAttribute(NetCDFConstants.MISSING_VALUE)) {
            noDataValue = getAttrDoubleValue(recordVariable.findAttribute(NetCDFConstants.MISSING_VALUE));
        }
        int[] strides = recordData.getIndex().getShape();
        int[] pointers = recordData.getIndex().getCurrentCounter();
        int numBands = 1;
        int numValuesPerBand = 1;
        for (int i = 0; i < numRecordDimensions; i++) {
            Dimension recordDimension = recordDimensions.get(i);
            String dimensionShortName = recordDimension.getShortName();
            if (dimensionShortName.equalsIgnoreCase(latVariableName) || dimensionShortName.equalsIgnoreCase(lonVariableName)) {
                numValuesPerBand *= strides[i];
            }else {
                numBands *= strides[i];
            }
        }
        Array[] dimensionVars = new Array[numRecordDimensions];
        double scaleFactor = 1d, offset = 0d;
        for (int i = 0; i < numRecordDimensions; i++) {
            if (i == lonIndex) dimensionVars[i] = lonData;
            else if (i == latIndex) dimensionVars[i] = latData;
            else {
                Dimension recordDimension = recordDimensions.get(i);
                String dimensionShortName = recordDimension.getShortName();
                Variable recordDimVar = findVariableRecursive(dimensionShortName, rootGroup);//rootGroup.findVariableLocal(dimensionShortName);
                if (Objects.isNull(recordDimVar)) throw new IllegalArgumentException(String.format("Corresponding coordinate variable not found for dimension %s", dimensionShortName));
                AttributeContainer recordDimAttrs = recordDimVar.attributes();
                varMetadata.computeIfAbsent(dimensionShortName, k -> new ArrayList<>());
                for (Attribute attr: recordDimAttrs) {
                    varMetadata.get(dimensionShortName).add(attr.getStringValue());
                    if (attr.getShortName().equalsIgnoreCase(NetCDFConstants.SCALE_FACTOR)) {
                        scaleFactor = getElement(attr.getValues(), 0);
                    }
                    if (attr.getShortName().equalsIgnoreCase(NetCDFConstants.ADD_OFFSET)) {
                        offset = getElement(attr.getValues(), 0);
                    }
                }
                dimensionVars[i] = recordDimVar.read();

            }
        }
        //double[] allBandValues = new double[numBands * width * height];
        double[][] allBandValues = new double[numBands][numValuesPerBand];
        //check for offset in the record variable
       // if (recordDim)

        //String[][] bandMetaData = new String[numValuesPerBand][numBands];
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

    private static Dimension findDimensionRecursive(String shortName, Group currGroup) {
        if (!Objects.isNull(currGroup.findDimensionLocal(shortName))) {
            return currGroup.findDimensionLocal(shortName);
        }else {
            for (Group group: currGroup.getGroups()) {
                Dimension ret = findDimensionRecursive(shortName, group);
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
            res.put(NetCDFConstants.GLOBAL_ATTR_PREFIX, globalAttrString);
        }
        for(int band = 0; band < bandAttrs.size(); band++) {
            int currBandNum = band + 1;
            res.put(NetCDFConstants.BAND_ATTR_PREFIX + currBandNum, bandAttrs.get(band));
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
                res = array.getShort(index);
                break;
            case USHORT:
                res = array.getShort(index);
                break;
            default:
                throw new IllegalArgumentException("Error casting dimension data to double");
        }
        return res;
    }

    private static Double getAttrDoubleValue(Attribute attr) {
        try {
            return (double) attr.getNumericValue();
        }catch (NullPointerException e) {
            throw new IllegalArgumentException("An attribute expected to have numeric values does not have numeric value");
        }
    }

    private static boolean allDimensionsRead(int[] currPointers, int[] shape) {
        for (int i = 0; i < currPointers.length; i++) {
            if (currPointers[i] < shape[i] - 1) return false;
        }
        return true;
    }

    private static boolean isRecordDimension(int rDimIdx, int latDimIdx, int lonDimIdx) {
        return !(rDimIdx == latDimIdx || rDimIdx == lonDimIdx);
    }

    private static void ensureCoordVar(Variable lonVar, Variable latVar) throws NullPointerException, IllegalArgumentException {
        if (latVar == null) {
            throw new NullPointerException("Provided longitude variable short name is invalid");
        }

        if (lonVar == null) {
            throw new NullPointerException("Provided latitude variable short name is invalid");
        }

        if (latVar.getShape().length > 1) {
            throw new IllegalArgumentException("Shape of latitude variable is supposed to be 1");
        }

        if (lonVar.getShape().length > 1) {
            throw new IllegalArgumentException("Shape of longitude variable is supposed to be 1");
        }
    }

    private static void ensureRecordVar(Variable recordVar) throws NullPointerException {
        if (recordVar == null) {
            throw new NullPointerException("Provided record variable short name is invalid");
        }
    }


    /**
     * Convenience function setting DOUBLE as datatype for the bands
     * Create a new empty raster with the given number of empty bands.
     * The bounding envelope is defined by the upper left corner and the scale.
     * The math formula of the envelope is: minX = upperLeftX = lowerLeftX, minY (lowerLeftY) = upperLeftY - height * pixelSize
     * <ul>
     *   <li>The raster is defined by the width and height
     *   <li>The upper left corner is defined by the upperLeftX and upperLeftY
     *   <li>The scale is defined by pixelSize. The scaleX is equal to pixelSize and scaleY is equal to -pixelSize
     *   <li>skewX and skewY are zero, which means no shear or rotation.
     *   <li>SRID is default to 0 which means the default CRS (Generic 2D)
     * </ul>
     * @param numBand the number of bands
     * @param widthInPixel the width of the raster, in pixel
     * @param heightInPixel the height of the raster, in pixel
     * @param upperLeftX the upper left corner of the raster. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster. Note that: the minY of the envelope is equal to the upperLeftY - height * pixelSize
     * @param pixelSize the size of the pixel in the unit of the CRS
     * @return the new empty raster
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSize)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize, 0, 0, 0);
    }

    /**
     * Convenience function allowing explicitly setting the datatype for all the bands
     * @param numBand
     * @param dataType
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX
     * @param upperLeftY
     * @param pixelSize
     * @return
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, String dataType, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double pixelSize)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize, 0, 0, 0);
    }


    /**
     * Convenience function for creating a raster with data type DOUBLE for all the bands
     * @param numBand
     * @param widthInPixel
     * @param heightInPixel
     * @param upperLeftX
     * @param upperLeftY
     * @param scaleX
     * @param scaleY
     * @param skewX
     * @param skewY
     * @param srid
     * @return
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid)
            throws FactoryException
    {
        return makeEmptyRaster(numBand, "d", widthInPixel, heightInPixel, upperLeftX, upperLeftY, scaleX, scaleY, skewX, skewY, srid);
    }

    /**
     * Create a new empty raster with the given number of empty bands
     * @param numBand the number of bands
     * @param bandDataType the data type of the raster, one of D | B | I | F | S | US
     * @param widthInPixel the width of the raster, in pixel
     * @param heightInPixel the height of the raster, in pixel
     * @param upperLeftX the upper left corner of the raster, in the CRS unit. Note that: the minX of the envelope is equal to the upperLeftX
     * @param upperLeftY the upper left corner of the raster, in the CRS unit. Note that: the minY of the envelope is equal to the upperLeftY + height * scaleY
     * @param scaleX the scale of the raster (pixel size on X), in the CRS unit
     * @param scaleY the scale of the raster (pixel size on Y), in the CRS unit
     * @param skewX the skew of the raster on X, in the CRS unit
     * @param skewY the skew of the raster on Y, in the CRS unit
     * @param srid the srid of the CRS. 0 means the default CRS (Cartesian 2D)
     * @return the new empty raster
     * @throws FactoryException
     */
    public static GridCoverage2D makeEmptyRaster(int numBand, String bandDataType, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid)
            throws FactoryException
    {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            // Create the CRS from the srid
            // Longitude first, Latitude second
            crs = CRS.decode("EPSG:" + srid, true);
        }

        // Create a new empty raster
        WritableRaster raster = RasterFactory.createBandedRaster(RasterUtils.getDataTypeCode(bandDataType), widthInPixel, heightInPixel, numBand, null);
        MathTransform transform = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        GridGeometry2D gridGeometry = new GridGeometry2D(
                new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
                PixelInCell.CELL_CORNER,
                transform, crs, null);
        return RasterUtils.create(raster, gridGeometry, null, null);
    }

    public static GridCoverage2D makeNonEmptyRaster(int numBand, int widthInPixel, int heightInPixel, double upperLeftX, double upperLeftY, double scaleX, double scaleY, double skewX, double skewY, int srid, double[][] data, Map<String, List<String>> properties, Double noDataValue, PixelInCell anchor) throws FactoryException {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        } else {
            // Create the CRS from the srid
            // Longitude first, Latitude second
            crs = CRS.decode("EPSG:" + srid, true);
        }

        // Create a new empty raster
        WritableRaster raster = RasterFactory.createBandedRaster(5, widthInPixel, heightInPixel, numBand, null); //create a raster with double values
        for (int i = 0; i < numBand; i++) {
            raster.setSamples(0, 0, widthInPixel, heightInPixel, i, data[i]);
        }
        //raster.setPixels(0, 0, widthInPixel, heightInPixel, data);
        MathTransform transform = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        GridGeometry2D gridGeometry = new GridGeometry2D(
                new GridEnvelope2D(0, 0, widthInPixel, heightInPixel),
                anchor,
                transform, crs, null);
        return RasterUtils.create(raster, gridGeometry, null, noDataValue, properties);
    }
}
