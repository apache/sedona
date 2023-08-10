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
package org.apache.sedona.common.raster;

import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;

public class RasterBandAccessors {

    public static Double getBandNoDataValue(GridCoverage2D raster, int band) {
        if (band > RasterAccessors.numBands(raster)) {
            throw new IllegalArgumentException("Provided band index is not present in the raster");
        }
        GridSampleDimension bandSampleDimension = raster.getSampleDimension(band - 1);
        if (bandSampleDimension.getNoDataValues() == null) return null;
        return raster.getSampleDimension(band - 1).getNoDataValues()[0];
    }

    public static Double getBandNoDataValue(GridCoverage2D raster) {
        return getBandNoDataValue(raster, 1);
    }

    public static String getBandType(GridCoverage2D raster, int band) {
        if (band > RasterAccessors.numBands(raster)) {
            throw new IllegalArgumentException("Provided band index is not present in the raster");
        }
        GridSampleDimension bandSampleDimension = raster.getSampleDimension(band - 1);
        return bandSampleDimension.getSampleDimensionType().name();
    }

    public static String getBandType(GridCoverage2D raster){
        return getBandType(raster, 1);
    }
}
