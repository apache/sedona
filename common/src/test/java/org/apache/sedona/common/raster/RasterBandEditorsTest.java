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

import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class RasterBandEditorsTest extends RasterTestBase{

    @Test
    public void testSetBandNoDataValueWithRaster() throws IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(raster, 1,3);
        double actual = RasterBandAccessors.getBandNoDataValue(grid);
        double expected = 3;
        assertEquals(expected, actual, 0.1d);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(grid, 1)));

        grid = RasterBandEditors.setBandNoDataValue(raster, -999);
        actual = RasterBandAccessors.getBandNoDataValue(grid);
        expected = -999;
        assertEquals(expected, actual, 0.1d);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(grid, 1)));
    }

    @Test
    public void testSetBandNoDataValueWithEmptyRaster() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 4326);
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(emptyRaster, 1, 999);
        double actual = RasterBandAccessors.getBandNoDataValue(grid);
        double expected = 999;
        assertEquals(expected, actual, 0.1d);

        grid = RasterBandEditors.setBandNoDataValue(emptyRaster, -444.444);
        actual = RasterBandAccessors.getBandNoDataValue(emptyRaster);
        expected = -444.444;
        assertEquals(expected, actual, 0.0001d);
    }

    @Test
    public void testSetBandNoDataValueWithEmptyRasterMultipleBand() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 0);
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(emptyRaster, -9999);
        grid = RasterBandEditors.setBandNoDataValue(grid, 2, 444);
        assertEquals(-9999, (double) RasterBandAccessors.getBandNoDataValue(grid), 0.1d);
        assertEquals(444, (double) RasterBandAccessors.getBandNoDataValue(grid, 2), 0.1d);
    }
}
