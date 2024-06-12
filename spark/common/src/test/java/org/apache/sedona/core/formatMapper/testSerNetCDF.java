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
package org.apache.sedona.core.formatMapper;

import java.util.List;
import org.apache.sedona.core.formatMapper.netcdfParser.SerNetCDFUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import ucar.ma2.Array;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * The following tests currently have no effect as we plan to implement all raster formats in
 * DataFrame styles and deprecate all Netcdf support
 */
public class testSerNetCDF {

  static String filename = "";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    //		filename = System.getProperty("user.dir")+"/src/test/resources/" +
    // "MYD11_L2.A2017091.0155.006.2017094143610.hdf";
  }

  @After
  public void tearDown() throws Exception {}

  @Ignore
  public void test() {
    NetcdfDataset ncfile = null;
    int offset = 2;
    int increment = 5;
    double scaleFactor = 0.02;

    String swathName = "MOD_Swath_LST";
    String geolocationFieldName = "Geolocation_Fields";
    String dataFieldName = "Data_Fields";
    String dataVariableName = "LST";
    ncfile = SerNetCDFUtils.loadNetCDFDataSet(filename);

    List<Variable> variables = ncfile.getVariables();
    Array longitude =
        SerNetCDFUtils.getNetCDF2DArray(
            ncfile, swathName + "/" + geolocationFieldName + "/Longitude");
    Array latitude =
        SerNetCDFUtils.getNetCDF2DArray(
            ncfile, swathName + "/" + geolocationFieldName + "/Latitude");
    Array dataVariable =
        SerNetCDFUtils.getNetCDF2DArray(
            ncfile, swathName + "/" + dataFieldName + "/" + dataVariableName);
    int[] geolocationShape = longitude.getShape();

    for (int j = 0; j < geolocationShape[0]; j++) {
      for (int i = 0; i < geolocationShape[1]; i++) {
        // We probably need to switch longitude and latitude if needed.
        double lng = SerNetCDFUtils.getDataSym(longitude, j, i);
        double lat = SerNetCDFUtils.getDataSym(latitude, j, i);
        String userData =
            String.valueOf(SerNetCDFUtils.getDataAsym(dataVariable, j, i, offset, increment));
      }
    }
  }
}
