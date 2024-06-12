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
package org.apache.sedona.core.formatMapper.netcdfParser;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;

/** The Class SerNetCDFUtils. */
public class SerNetCDFUtils implements Serializable {

  /**
   * Extracts a variable's data from a NetCDF.
   *
   * <p>If the variable is not found then an Array with the element 0.0 is returned with shape
   * (1,1). Note that all dimensions of size 1 are eliminated. For example the array shape (21, 5,
   * 1, 2) is reduced to (21, 5, 2) since the 3rd dimension only ranges over a single value.
   *
   * @param netcdfFile The NetCDF file.
   * @param variable The variable whose data we want to extract.
   * @return 2D Data array.
   */
  public static Array getNetCDF2DArray(NetcdfDataset netcdfFile, String variable) {
    Variable netcdfVal = netcdfFile.findVariable(variable);
    Array searchVariable = null;
    try {
      searchVariable = netcdfVal.read();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return searchVariable;
  }

  /**
   * Gets the data from asymmetric mapping. One geolocation maps more than one observation. Or, one
   * observation maps more than one geolocation.
   *
   * @param array the array
   * @param i the i
   * @param j the j
   * @param offset the offset
   * @param increment the increment
   * @return the data asym
   */
  public static Double getDataAsym(Array array, int i, int j, int offset, int increment) {
    Index dataIndex = Index.factory(array.getShape());
    int[] location = {offset + i * increment, offset + j * increment};
    dataIndex.set(location);
    switch (array.getDataType()) {
      case INT:
        return new Double((Integer) array.getObject(dataIndex));
      case SHORT:
        return new Double((Short) array.getObject(dataIndex));
      case FLOAT:
        return new Double((Float) array.getObject(dataIndex));
      case DOUBLE:
        return new Double((Double) array.getObject(dataIndex));
      case LONG:
        return new Double((Long) array.getObject(dataIndex));
      default:
        return (Double) array.getObject(dataIndex);
    }
  }

  /**
   * Gets the data from symmetric mapping. One geolocation maps one observation.
   *
   * @param array the array
   * @param i the i
   * @param j the j
   * @return the data sym
   */
  public static Double getDataSym(Array array, int i, int j) {
    Index dataIndex = Index.factory(array.getShape());
    int[] location = {i, j};
    dataIndex.set(location);
    switch (array.getDataType()) {
      case INT:
        return new Double((Integer) array.getObject(dataIndex));
      case SHORT:
        return new Double((Short) array.getObject(dataIndex));
      case FLOAT:
        return new Double((Float) array.getObject(dataIndex));
      case DOUBLE:
        return new Double((Double) array.getObject(dataIndex));
      case LONG:
        return new Double((Long) array.getObject(dataIndex));
      default:
        return (Double) array.getObject(dataIndex);
    }
  }

  /**
   * Loads a NetCDF Dataset from a URL.
   *
   * @param url the url
   * @return the netcdf dataset
   */
  public static NetcdfDataset loadNetCDFDataSet(String url) {
    NetcdfDataset dataset = null;
    try {
      dataset = NetcdfDatasets.openDataset(url);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return dataset;
  }

  /**
   * Loads a NetCDF Dataset from HDFS.
   *
   * @param dfsUri HDFS URI(eg. hdfs://master:9000/)
   * @param location File path on HDFS
   * @param bufferSize The size of the buffer to be used
   * @return the netcdf dataset
   */
  public static NetcdfDataset loadDFSNetCDFDataSet(
      String dfsUri, String location, Integer bufferSize) {
    NetcdfDataset dataset = null;
    try {
      HDFSRandomAccessFile raf = new HDFSRandomAccessFile(dfsUri, location, bufferSize);
      dataset = new NetcdfDataset(NetcdfFile.open(raf, location, null, null));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return dataset;
  }

  /**
   * Gets the size of a dimension of a NetCDF file.
   *
   * @param netcdfFile the netcdf file
   * @param rowDim the row dim
   * @return the dimension size
   */
  public static Integer getDimensionSize(NetcdfDataset netcdfFile, String rowDim) {
    int dimSize = -1;
    Iterator<Dimension> it = netcdfFile.getDimensions().iterator();
    while (it.hasNext()) {
      Dimension d = it.next();
      if (d.getShortName().equals(rowDim)) {
        dimSize = d.getLength();
      }
    }
    if (dimSize < 0) {
      throw new IllegalStateException("Dimension does not exist!!!");
    }
    return dimSize;
  }
}
