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

import static org.junit.Assert.*;

public class RasterBandAccessorsTest extends RasterTestBase {

    @Test
    public void testBandNoDataValueCustomBand() throws FactoryException {
        int width = 5, height = 10;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, width, height, 53, 51, 1, 1, 0, 0, 4326);
        double[] values = new double[width * height];
        for (int i = 0; i < values.length; i++) {
            values[i] = i + 1;
        }
        emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 2, 1d);
        assertNotNull(RasterBandAccessors.getBandNoDataValue(emptyRaster, 2));
        assertEquals(1, RasterBandAccessors.getBandNoDataValue(emptyRaster, 2), 1e-9);
        assertNull(RasterBandAccessors.getBandNoDataValue(emptyRaster));
    }

    @Test
    public void testBandNoDataValueDefaultBand() throws FactoryException {
        int width = 5, height = 10;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, width, height, 53, 51, 1, 1, 0, 0, 4326);
        double[] values = new double[width * height];
        for (int i = 0; i < values.length; i++) {
            values[i] = i + 1;
        }
        emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 1d);
        assertNotNull(RasterBandAccessors.getBandNoDataValue(emptyRaster));
        assertEquals(1, RasterBandAccessors.getBandNoDataValue(emptyRaster), 1e-9);
    }

    @Test
    public void testBandNoDataValueDefaultNoData() throws FactoryException {
        int width = 5, height = 10;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1,"I", width, height, 53, 51, 1, 1, 0, 0, 0);
        double[] values = new double[width * height];
        for (int i = 0; i < values.length; i++) {
            values[i] = i + 1;
        }
        assertNull(RasterBandAccessors.getBandNoDataValue(emptyRaster, 1));
    }

    @Test
    public void testBandNoDataValueIllegalBand() throws FactoryException, IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> RasterBandAccessors.getBandNoDataValue(raster, 2));
        assertEquals("Provided band index is not present in the raster", exception.getMessage());
    }

    @Test
    public void testBandPixelType() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, "I", 2, 2, 53, 51, 1, 1, 0, 0, 0);
        double[] values = new double[]{1.2, 1.1, 32.2, 43.2};
        emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values, 1, 0.0);

    }

    @Test
    public void testBandPixelTypeDefaultBand() {

    }

    @Test
    public void testBandPixelTypeIllegalBand() {

    }



}
