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
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RasterEditorsTest extends RasterTestBase {
    @Test
    public void testSetGeoReferenceWithRaster() throws IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        GridCoverage2D actualGrid = RasterEditors.setGeoReference(raster, -13095817, 4021262, 72, -72, 0, 0);
        String actual = RasterAccessors.getGeoReference(actualGrid);
        String expected = "72.000000 \n0.000000 \n0.000000 \n-72.000000 \n-13095817.000000 \n4021262.000000";
        assertEquals(expected, actual);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(actualGrid, 1)));

        actualGrid = RasterEditors.setGeoReference(raster, "56 1 1 -56 23 34");
        actual = RasterAccessors.getGeoReference(actualGrid);
        expected = "56.000000 \n1.000000 \n1.000000 \n-56.000000 \n23.000000 \n34.000000";
        assertEquals(expected, actual);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(actualGrid, 1)));

        actualGrid = RasterEditors.setGeoReference(raster, "56 1 1 -56 23 34", "esri");
        actual = RasterAccessors.getGeoReference(actualGrid);
        expected = "56.000000 \n1.000000 \n1.000000 \n-56.000000 \n-5.000000 \n62.000000";
        assertEquals(expected, actual);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(actualGrid, 1)));
    }

    @Test
    public void testSetGeoReferenceWithEmptyRaster() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 20, 20, 0, 0, 8);
        GridCoverage2D actualGrid = RasterEditors.setGeoReference(emptyRaster, 10, -10, 10, -10, 10, 10);
        String actual = RasterAccessors.getGeoReference(actualGrid);
        String expected = "10.000000 \n10.000000 \n10.000000 \n-10.000000 \n10.000000 \n-10.000000";
        assertEquals(expected, actual);

        actualGrid = RasterEditors.setGeoReference(emptyRaster, "10 3 3 -10 20 -12");
        actual = RasterAccessors.getGeoReference(actualGrid);
        expected = "10.000000 \n3.000000 \n3.000000 \n-10.000000 \n20.000000 \n-12.000000";
        assertEquals(expected, actual);

        actualGrid = RasterEditors.setGeoReference(emptyRaster, "10 3 3 -10 20 -12", "ESRI");
        actual = RasterAccessors.getGeoReference(actualGrid);
        expected = "10.000000 \n3.000000 \n3.000000 \n-10.000000 \n15.000000 \n-7.000000";
        assertEquals(expected, actual);
    }

    @Test
    public void testSetGeoReferenceWithEmptyRasterSRID() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 4326);
        GridCoverage2D actualGrid = RasterEditors.setGeoReference(emptyRaster, 10, -10, 10, -10, 10, 10);
        String actual = RasterAccessors.getGeoReference(actualGrid);
        String expected = "10.000000 \n10.000000 \n10.000000 \n-10.000000 \n10.000000 \n-10.000000";
        assertEquals(expected, actual);

        actualGrid = RasterEditors.setGeoReference(emptyRaster, "10 3 3 -10 20 -12");
        actual = RasterAccessors.getGeoReference(actualGrid);
        expected = "10.000000 \n3.000000 \n3.000000 \n-10.000000 \n20.000000 \n-12.000000";
        assertEquals(expected, actual);

        actualGrid = RasterEditors.setGeoReference(emptyRaster, "10 3 3 -10 20 -12", "ESRI");
        actual = RasterAccessors.getGeoReference(actualGrid);
        expected = "10.000000 \n3.000000 \n3.000000 \n-10.000000 \n15.000000 \n-7.000000";
        assertEquals(expected, actual);
    }


    @Test
    public void testResample() throws FactoryException, TransformException {
        double[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 3, 3, 0, 0, 2, -2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);

        //test with height and width
        GridCoverage2D newRaster = RasterEditors.resample(raster, 5, 5, 0, 0, false, "nearestneighbor");
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|4.0  4.0  5.0  6.0  6.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {0, 0, 5, 5, 1.2, -1.2, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        //test with scaleX and scaleY
        newRaster = RasterEditors.resample(raster, 1.2, -1.2, 1, -1, true, null);
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "|  1.0    1.0    2.0    3.0    3.0    NaN|\n" +
                "|  1.0    1.0    2.0    3.0    3.0    NaN|\n" +
                "|  4.0    4.0    5.0    6.0    6.0    NaN|\n" +
                "|  7.0    7.0    8.0    9.0    9.0    NaN|\n" +
                "|  7.0    7.0    8.0    9.0    9.0    NaN|\n" +
                "|  NaN    NaN    NaN    NaN    NaN    NaN|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[]{-0.2, 0.2, 6, 6, 1.2, -1.2, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }

    @Test
    public void testResampleResizeFlavor() throws FactoryException, TransformException {
        double[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 3, 3, 0, 0, 2, -2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);
        GridCoverage2D newRaster = RasterEditors.resample(raster, 5, 5, false, "nearestneighbor");
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|4.0  4.0  5.0  6.0  6.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {0, 0, 5, 5, 1.2, -1.2, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        //check with scaleX and scaleY
        newRaster = RasterEditors.resample(raster, 1.2, -1.2, true, "bilinear");
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|4.0  4.0  5.0  6.0  6.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[]{0, 0, 5, 5, 1.2, -1.2, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }


    @Test
    public void testResampleRefRaster() throws FactoryException, TransformException {
        double[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 3, 3, 0, 0, 2, -2, 0, 0, 0);
        GridCoverage2D refRaster = RasterConstructors.makeEmptyRaster(2, "d", 5, 5, 1, -1, 1.2, -1.2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);

        //test with height and width
        GridCoverage2D newRaster = RasterEditors.resample(raster, refRaster, false, null);
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|1.0  1.0  2.0  3.0  3.0|\n" +
                "|4.0  4.0  5.0  6.0  6.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n" +
                "|7.0  7.0  8.0  9.0  9.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {-0.2, 0.2, 5, 5, 1.24, -1.24, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        //test with scaleX and scaleY
        newRaster = RasterEditors.resample(raster, refRaster, true, null);
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "|  1.0    1.0    2.0    3.0    3.0    NaN|\n" +
                "|  1.0    1.0    2.0    3.0    3.0    NaN|\n" +
                "|  4.0    4.0    5.0    6.0    6.0    NaN|\n" +
                "|  7.0    7.0    8.0    9.0    9.0    NaN|\n" +
                "|  7.0    7.0    8.0    9.0    9.0    NaN|\n" +
                "|  NaN    NaN    NaN    NaN    NaN    NaN|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[]{-0.2, 0.2, 6, 6, 1.2, -1.2, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }

    @Test
    public void testResampleDiffAlgorithms() throws FactoryException, TransformException {
        /*
        Even though we cannot match interpolation with that of PostGIS for other algorithms, this is a sanity test case to detect potentially invalid changes to the function
         */
        double[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 3, 3, 0, 0, 2, -2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);

        //test bilinear
        GridCoverage2D newRaster = RasterEditors.resample(raster, 5, 5, 0, 0, false, "bilinear");
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "|       NaN         NaN         NaN         NaN         NaN|\n" +
                "|       NaN    2.600000    3.200000    3.800000    4.200000|\n" +
                "|       NaN    4.400000    5.000000    5.600000    6.000000|\n" +
                "|       NaN    6.200000    6.800000    7.400000    7.800000|\n" +
                "|       NaN    7.400000    8.000000    8.600000    9.000000|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {0, 0, 5, 5, 1.2, -1.2, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }



        //test bicubic
        newRaster = RasterEditors.resample(raster, 5, 5, 0, 0, false, "bicubic");
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "|       NaN         NaN         NaN         NaN         NaN|\n" +
                "|       NaN    2.305379    2.979034    3.648548    4.042909|\n" +
                "|       NaN    4.326345    5.000000    5.669513    6.063874|\n" +
                "|       NaN    6.334885    7.008540    7.678053    8.072415|\n" +
                "|       NaN    7.517968    8.191623    8.861137    9.255498|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }

    @Test
    public void testResampleRefRasterDiffSRID() throws FactoryException {
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 3, 3, 0, 0, 2, -2, 0, 0, 0);
        GridCoverage2D refRaster = RasterConstructors.makeEmptyRaster(2, "d", 5, 5, 1, -1, 1.2, -1.2, 0, 0, 4326);
        assertThrows("Provided input raster and reference raster have different SRIDs", IllegalArgumentException.class, () -> RasterEditors.resample(raster, refRaster, false, null));
    }

}
