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

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RasterOutputTests extends RasterTestBase {

    @Test
    public void testAsBase64() throws IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        String resultRaw = RasterOutputs.asBase64(raster);
        assertTrue(resultRaw.contains("iVBORw0KGgoAAAANSUhEUgAAAgAAAAIFCAAAAACB3hqVAACAAElEQVR42uy9K5fkONil27Bh0oQBAwY1NDQ0NDU0NBQVFBQUFRQUFBQ1NDQ0NTT0PFtR1d/MmR9w1lnrVF8qKysjwtZ72/u9+Z9//v9f/6/96rvB9GPfz8M0jcMS/LJOxm7B2udcXHb5NGGb48rfeDM7s9jZzNO6uiX5aN28zPO62HBE/0Q7LXaa1vh5de/X8PN5r/MW3GSWeZqtd8lmH806O+esNUtI6xiPdfz/wCGZZU12Wv1qbVrn1YRgC3dicsq1zGF/nmePMT5P9ot3LnubSk4peu/K5cKz5BRnO2fr/HDXupdc8l7TlU5X9uPZzqf92mJMewmRbz1PiqkkZ8N9P257/OPXlHnP"));
    }

    @Test
    public void testAsGeoTiff() throws IOException {
        GridCoverage2D rasterOg = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        GridCoverage2D rasterTest = RasterConstructors.fromGeoTiff(RasterOutputs.asGeoTiff(rasterFromGeoTiff(resourceFolder + "raster/test1.tiff")));
        assert(rasterTest != null);
        assertEquals(rasterTest.getEnvelope().toString(), rasterOg.getEnvelope().toString());
    }

    @Test
    public void testAsGeoTiffWithCompressionTypes() throws IOException {
        GridCoverage2D rasterOg = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        byte[] rasterBytes1 = RasterOutputs.asGeoTiff(rasterOg, "LZW", 1.0);
        byte[] rasterBytes2 = RasterOutputs.asGeoTiff(rasterOg, "Deflate", 0.5);
        GridCoverage2D rasterNew = RasterConstructors.fromGeoTiff(rasterBytes1);
        assertEquals(rasterOg.getEnvelope().toString(), rasterNew.getEnvelope().toString());
        assert(rasterBytes1.length > rasterBytes2.length);
    }
}
