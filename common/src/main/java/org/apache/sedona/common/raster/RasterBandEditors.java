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

import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.metadata.spatial.PixelOrientation;

public class RasterBandEditors {
    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, int bandIndex, double noDataValue) {
        ensureBand(raster, bandIndex);
        Double rasterNoData = RasterBandAccessors.getBandNoDataValue(raster, bandIndex);
        if ( !(rasterNoData == null) && rasterNoData == noDataValue) {
            return raster;
        }
        GridSampleDimension[] bands = raster.getSampleDimensions();
        bands[bandIndex - 1] = RasterUtils.createSampleDimensionWithNoDataValue(bands[bandIndex - 1], noDataValue);

        int width = RasterAccessors.getWidth(raster), height = RasterAccessors.getHeight(raster);
        AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
        GridGeometry2D gridGeometry2D = new GridGeometry2D(
                new GridEnvelope2D(0, 0, width, height),
                PixelOrientation.UPPER_LEFT,
                affine, raster.getCoordinateReferenceSystem2D(), null
        );

        return RasterUtils.create(raster.getRenderedImage(), gridGeometry2D, bands, null);
    }

    public static GridCoverage2D setBandNoDataValue(GridCoverage2D raster, double noDataValue) {
        return setBandNoDataValue(raster, 1, noDataValue);
    }

    private static void ensureBand(GridCoverage2D raster, int band) throws IllegalArgumentException {
        if (band > RasterAccessors.numBands(raster)) {
            throw new IllegalArgumentException(String.format("Provided band index %d is not present in the raster", band));
        }
    }
}
