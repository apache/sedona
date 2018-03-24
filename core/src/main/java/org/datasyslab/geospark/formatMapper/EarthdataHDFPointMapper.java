/*
 * FILE: EarthdataHDFPointMapper
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.sernetcdf.SerNetCDFUtils;
import ucar.ma2.Array;
import ucar.nc2.dataset.NetcdfDataset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class EarthdataHDFPointMapper.
 */
public class EarthdataHDFPointMapper
        implements FlatMapFunction<Iterator<String>, Geometry>
{

    /**
     * The offset.
     */
    private int offset = 0;

    /**
     * The increment.
     */
    private int increment = 1;

    /**
     * The root group name.
     */
    private String rootGroupName = "MOD_Swath_LST";

    /**
     * The geolocation field.
     */
    private String geolocationField = "Geolocation_Fields";

    /**
     * The longitude name.
     */
    private String longitudeName = "Longitude";

    /**
     * The latitude name.
     */
    private String latitudeName = "Latitude";

    /**
     * The data field name.
     */
    private String dataFieldName = "Data_Fields";

    /**
     * The data variable name.
     */
    private String dataVariableName = "LST";

    /**
     * The data variable list.
     */
    private String[] dataVariableList;

    /**
     * The longitude path.
     */
    private String longitudePath = "";

    /**
     * The latitude path.
     */
    private String latitudePath = "";

    /**
     * The data path.
     */
    private String dataPath = "";

    /**
     * The data path list.
     */
    private String[] dataPathList;

    /**
     * The switch coordinate XY. By default, longitude is X, latitude is Y
     */
    private boolean switchCoordinateXY = false;

    /**
     * The url prefix.
     */
    private String urlPrefix = "";

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     * @param switchCoordinateXY the switch coordinate XY
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName, boolean switchCoordinateXY)
    {
        this.offset = offset;
        this.increment = increment;
        this.rootGroupName = rootGroupName;
        this.dataVariableList = dataVariableList;
        this.dataVariableName = dataVariableName;
        this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
        this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
        this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
        this.dataPathList = new String[dataVariableList.length];
        for (int i = 0; i < dataVariableList.length; i++) {
            dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
        }
        this.switchCoordinateXY = switchCoordinateXY;
    }

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName)
    {
        this.offset = offset;
        this.increment = increment;
        this.rootGroupName = rootGroupName;
        this.dataVariableList = dataVariableList;
        this.dataVariableName = dataVariableName;
        this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
        this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
        this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
        this.dataPathList = new String[dataVariableList.length];
        for (int i = 0; i < dataVariableList.length; i++) {
            dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
        }
    }

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     * @param switchCoordinateXY the switch coordinate XY
     * @param urlPrefix the url prefix
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName,
            boolean switchCoordinateXY, String urlPrefix)
    {
        this.offset = offset;
        this.increment = increment;
        this.rootGroupName = rootGroupName;
        this.dataVariableList = dataVariableList;
        this.dataVariableName = dataVariableName;
        this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
        this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
        this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
        this.dataPathList = new String[dataVariableList.length];
        for (int i = 0; i < dataVariableList.length; i++) {
            dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
        }
        this.switchCoordinateXY = switchCoordinateXY;
        this.urlPrefix = urlPrefix;
    }

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     * @param urlPrefix the url prefix
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName,
            String urlPrefix)
    {
        this.offset = offset;
        this.increment = increment;
        this.rootGroupName = rootGroupName;
        this.dataVariableList = dataVariableList;
        this.dataVariableName = dataVariableName;
        this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
        this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
        this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
        this.dataPathList = new String[dataVariableList.length];
        for (int i = 0; i < dataVariableList.length; i++) {
            dataPathList[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
        }
        this.urlPrefix = urlPrefix;
    }

    @Override
    public Iterator<Geometry> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<Geometry> hdfData = new ArrayList<Geometry>();
        while (stringIterator.hasNext()) {
            String hdfAddress = stringIterator.next();
            NetcdfDataset netcdfSet = SerNetCDFUtils.loadNetCDFDataSet(urlPrefix + hdfAddress);
            Array longitudeArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.longitudePath);
            Array latitudeArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.latitudePath);
            Array dataArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.dataPath);
            Array[] dataArrayList = new Array[this.dataVariableList.length];
            for (int i = 0; i < this.dataVariableList.length; i++) {
                dataArrayList[i] = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, dataPathList[i]);
            }
            int[] geolocationShape = longitudeArray.getShape();
            GeometryFactory geometryFactory = new GeometryFactory();
            for (int j = 0; j < geolocationShape[0]; j++) {
                for (int i = 0; i < geolocationShape[1]; i++) {
                    // We probably need to switch longitude and latitude if needed.
                    Coordinate coordinate = null;
                    if (switchCoordinateXY) {
                        coordinate = new Coordinate(SerNetCDFUtils.getDataSym(longitudeArray, j, i),
                                SerNetCDFUtils.getDataSym(latitudeArray, j, i), SerNetCDFUtils.getDataAsym(dataArray, j, i, offset, increment));
                    }
                    else {
                        coordinate = new Coordinate(SerNetCDFUtils.getDataSym(latitudeArray, j, i),
                                SerNetCDFUtils.getDataSym(longitudeArray, j, i), SerNetCDFUtils.getDataAsym(dataArray, j, i, offset, increment));
                    }
                    Point observation = geometryFactory.createPoint(coordinate);
                    String userData = "";

                    for (int k = 0; k < dataVariableList.length - 1; k++) {
                        userData += SerNetCDFUtils.getDataAsym(dataArrayList[k], j, i, offset, increment) + " ";
                    }
                    userData += SerNetCDFUtils.getDataAsym(dataArrayList[dataVariableList.length - 1], j, i, offset, increment);

                    observation.setUserData(userData);
                    hdfData.add(observation);
                }
            }
        }
        return hdfData.iterator();
    }
}
