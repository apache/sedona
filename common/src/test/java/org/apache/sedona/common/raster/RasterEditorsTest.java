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
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.image.DataBuffer;
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
        double[] values = {1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 4, 3, 0, 0, 2, -2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);

        //test with height and width
        GridCoverage2D newRaster = RasterEditors.resample(raster, 6, 5, 1, -1, false, "nearestneighbor");
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "| 1.0   1.0   2.0   3.0   3.0   5.0|\n" +
                "| 1.0   1.0   2.0   3.0   3.0   5.0|\n" +
                "| 4.0   4.0   5.0   6.0   6.0   9.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {-0.33333333333333326, 0.19999999999999996, 6, 5, 1.388888888888889, -1.24, 0, 0, 0, 1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        //test with scaleX and scaleY
        newRaster = RasterEditors.resample(raster, 1.2, -1.4, 1, -1, true, null);
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|\n" +
                "| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|\n" +
                "| 4.0   4.0   5.0   6.0   6.0   9.0   9.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[]{-0.20000000298023224, 0.4000000059604645, 7.0, 5.0, 1.2, -1.4, 0.0, 0.0, 0.0, 1.0};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }

    @Test
    public void testResampleNoDataValueHandled() throws FactoryException, TransformException {
        double[] values1 = {1,99,3,4,99,6,7,99,9,10,99,12};
        double[] values2 = {10,11,-9999,13,14,15,-9999,17,18,19,-9999,21};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 4, 3, 0, 0, 2, -2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values1, 1, 99.0);
        raster = MapAlgebra.addBandFromArray(raster, values2, 2, -9999.0);

        GridCoverage2D newRaster = RasterEditors.resample(raster, 6, 5, 1, -1, false, "bilinear");

        String res1 = RasterOutputs.asMatrix(newRaster, 1);
        String res2 = RasterOutputs.asMatrix(newRaster, 2);
        String expectedRes1 = "|99.000000  99.000000  99.000000  99.000000  99.000000  99.000000|\n" +
                "|99.000000   3.569688  99.000000   4.439826   4.551458   4.825000|\n" +
                "|99.000000  99.000000   5.974132   6.590451   6.605208  99.000000|\n" +
                "|99.000000  99.000000   8.260383   8.064202   8.748022  99.000000|\n" +
                "|99.000000   9.375000   9.913194  99.000000  99.000000  12.000000|\n";
        String expectedRes2 = "|-9999.000000  -9999.000000  -9999.000000  -9999.000000  -9999.000000  -9999.000000|\n" +
                "|-9999.000000     11.695000     12.482500  -9999.000000  -9999.000000     14.320000|\n" +
                "|-9999.000000     14.175000     14.876389  -9999.000000  -9999.000000     16.800000|\n" +
                "|-9999.000000     16.655001     17.270278  -9999.000000  -9999.000000     19.280001|\n" +
                "|-9999.000000     18.375000     18.930555  -9999.000000  -9999.000000     21.000000|\n";

        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {-0.33333298563957214, 0.20000000298023224, 6.0, 5.0, 1.3888888309399288, -1.2400000005960465, 0.0, 0.0, 0.0, 2.0};
        assertEquals(expectedRes1, res1);
        assertEquals(expectedRes2, res2);
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        newRaster = RasterEditors.resample(raster, 6, 5, 1, -1, false, "bicubic");

        res1 = RasterOutputs.asMatrix(newRaster, 1);
        res2 = RasterOutputs.asMatrix(newRaster, 2);
        expectedRes1 = "|99.000000  99.000000  99.000000  99.000000  99.000000  99.000000|\n" +
                "|99.000000   3.257045  99.000000   4.211917   4.254402   4.550135|\n" +
                "|99.000000  99.000000   5.885300   6.683639   6.667900  99.000000|\n" +
                "|99.000000  99.000000   8.418216   8.162022   8.936398  99.000000|\n" +
                "|99.000000   9.593572  10.217107  99.000000  99.000000  12.530972|\n";
        expectedRes2 = "|-9999.000000  -9999.000000  -9999.000000  -9999.000000  -9999.000000  -9999.000000|\n" +
                "|-9999.000000     11.252455     12.144701  -9999.000000  -9999.000000     13.989338|\n" +
                "|-9999.000000     14.089166     14.862710  -9999.000000  -9999.000000     16.841017|\n" +
                "|-9999.000000     16.901485     17.557348  -9999.000000  -9999.000000     19.668177|\n" +
                "|-9999.000000     18.642647     19.225651  -9999.000000  -9999.000000     21.418527|\n";

        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[] {-0.33333298563957214, 0.20000000298023224, 6.0, 5.0, 1.3888888309399288, -1.2400000005960465, 0.0, 0.0, 0.0, 2.0};
        assertEquals(expectedRes1, res1);
        assertEquals(expectedRes2, res2);
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }

    @Test
    public void testResampleResizeFlavor() throws FactoryException, TransformException {
        double[] values = {1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 4, 3, 0, 0, 2, -2, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);
        GridCoverage2D newRaster = RasterEditors.resample(raster, 6, 5, false, "nearestneighbor");
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "| 1.0   2.0   2.0   3.0   5.0   5.0|\n" +
                "| 1.0   2.0   2.0   3.0   5.0   5.0|\n" +
                "| 4.0   5.0   5.0   6.0   9.0   9.0|\n" +
                "| 7.0   8.0   8.0   9.0  10.0  10.0|\n" +
                "| 7.0   8.0   8.0   9.0  10.0  10.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {0,0,6,5,1.3333333333333333,-1.2,0,0,0,1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        //check with scaleX and scaleY
        newRaster = RasterEditors.resample(raster, 1.2, -1.4, true, null);
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "|  1.0    1.0    2.0    3.0    3.0    5.0    5.0|\n" +
                "|  4.0    4.0    5.0    6.0    6.0    9.0    9.0|\n" +
                "|  4.0    4.0    5.0    6.0    6.0    9.0    9.0|\n" +
                "|  7.0    7.0    8.0    9.0    9.0   10.0   10.0|\n" +
                "|  NaN    NaN    NaN    NaN    NaN    NaN    NaN|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[]{0,0,7,5,1.2,-1.4,0,0,0,1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }
    }


    @Test
    public void testResampleRefRaster() throws FactoryException, TransformException {
        double[] values = {1, 2, 3, 5, 4, 5, 6, 9, 7, 8, 9, 10};
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, "d", 4, 3, 0, 0, 2, -2, 0, 0, 0);
        GridCoverage2D refRaster = RasterConstructors.makeEmptyRaster(2, "d", 6, 5, 1, -1, 1.2, -1.4, 0, 0, 0);
        raster = MapAlgebra.addBandFromArray(raster, values, 1, null);

        //test with height and width
        GridCoverage2D newRaster = RasterEditors.resample(raster, refRaster, false, null);
        String res = RasterOutputs.asMatrix(newRaster);
        String expectedRes = "| 1.0   1.0   2.0   3.0   3.0   5.0|\n" +
                "| 1.0   1.0   2.0   3.0   3.0   5.0|\n" +
                "| 4.0   4.0   5.0   6.0   6.0   9.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        double[] metadata = RasterAccessors.metadata(newRaster);
        double[] expectedMetadata = {-0.33333333333333326,0.19999999999999996,6,5,1.388888888888889,-1.24,0,0,0,1};
        //verify correct raster geometry
        for (int i = 0; i < metadata.length; i++) {
            assertEquals(expectedMetadata[i], metadata[i], 1e-6);
        }

        //test with scaleX and scaleY
        newRaster = RasterEditors.resample(raster, refRaster, true, null);
        res = RasterOutputs.asMatrix(newRaster);
        expectedRes = "| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|\n" +
                "| 1.0   1.0   2.0   3.0   3.0   5.0   5.0|\n" +
                "| 4.0   4.0   5.0   6.0   6.0   9.0   9.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|\n" +
                "| 7.0   7.0   8.0   9.0   9.0  10.0  10.0|\n";
        //verify correct interpolation
        assertEquals(expectedRes, res);
        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[]{-0.20000000298023224, 0.4000000059604645, 7.0, 5.0, 1.2, -1.4, 0.0, 0.0, 0.0, 1.0};
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

    @Test
    public void testNormalizeAll() throws FactoryException {
        GridCoverage2D raster1 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);
        GridCoverage2D raster2 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);
        GridCoverage2D raster3 = RasterConstructors.makeEmptyRaster(2, "I", 4, 4, 0, 0, 1);
        GridCoverage2D raster4 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);
        GridCoverage2D raster5 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);

        for (int band = 1; band <= 2; band++) {
            double[] bandValues1 = new double[4 * 4];
            double[] bandValues2 = new double[4 * 4];
            double[] bandValues3 = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            double[] bandValues4 = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,0};
            double[] bandValues5 = new double[4 * 4];
            for (int i = 0; i < bandValues1.length; i++) {
                bandValues1[i] = (i) * band;
                bandValues2[i] = (1) * (band-1);
                bandValues5[i] = i + ((band-1)*15);
            }
            raster1 = MapAlgebra.addBandFromArray(raster1, bandValues1, band);
            raster2 = MapAlgebra.addBandFromArray(raster2, bandValues2, band);
            raster3 = MapAlgebra.addBandFromArray(raster3, bandValues3, band);
            raster4 = MapAlgebra.addBandFromArray(raster4, bandValues4, band);
            raster4 = RasterBandEditors.setBandNoDataValue(raster4, band, 0.0);
            raster5 = MapAlgebra.addBandFromArray(raster5, bandValues5, band);
        }
        raster3 = RasterBandEditors.setBandNoDataValue(raster3, 1, 16.0);
        raster3 = RasterBandEditors.setBandNoDataValue(raster3, 2, 1.0);

        GridCoverage2D normalizedRaster1 = RasterEditors.normalizeAll(raster1, 0, 255, false, -9999.0);
        GridCoverage2D normalizedRaster2 = RasterEditors.normalizeAll(raster1, 256d, 511d, false, -9999.0);
        GridCoverage2D normalizedRaster3 = RasterEditors.normalizeAll(raster2);
        GridCoverage2D normalizedRaster4 = RasterEditors.normalizeAll(raster3, 0, 255, true, 95.0);
        GridCoverage2D normalizedRaster5 = RasterEditors.normalizeAll(raster4, 0, 255, true, 255.0);
        GridCoverage2D normalizedRaster6 = RasterEditors.normalizeAll(raster5, 0.0, 255.0, -9999.0, 0.0, 30.0);
        GridCoverage2D normalizedRaster7 = RasterEditors.normalizeAll(raster5, 0, 255, false, -9999.0);
        GridCoverage2D normalizedRaster8 = RasterEditors.normalizeAll(raster3, 0, 255);
        GridCoverage2D normalizedRaster10 = RasterEditors.normalizeAll(raster3, 0, 255, false);

        double[] expected1 = {0.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 255.0};
        double[] expected2 = {256.0, 273.0, 290.0, 307.0, 324.0, 341.0, 358.0, 375.0, 392.0, 409.0, 426.0, 443.0, 460.0, 477.0, 494.0, 511.0};
        double[] expected3 = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        double[] expected4 = {0.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 95.0};
        double[] expected5 = {95.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 255.0};
        double[] expected6 = {0.0, 18.214285714285715, 36.42857142857143, 54.642857142857146, 72.85714285714286, 91.07142857142857, 109.28571428571429, 127.5, 145.71428571428572, 163.92857142857142, 182.14285714285714, 200.35714285714286, 218.57142857142858, 236.78571428571428, 255.0, 255.0};
        double[] expected7 = {0.0, 16.0, 33.0, 50.0, 67.0, 84.0, 101.0, 118.0, 135.0, 152.0, 169.0, 186.0, 203.0, 220.0, 237.0, 255.0};
        double[] expected8 = {255.0, 16.0, 33.0, 50.0, 67.0, 84.0, 101.0, 118.0, 135.0, 152.0, 169.0, 186.0, 203.0, 220.0, 237.0, 254.0};
        double[] expected9 = {0.0, 18.0, 36.0, 54.0, 72.0, 90.0, 108.0, 127.0, 145.0, 163.0, 181.0, 199.0, 217.0, 235.0, 254.0, 255.0};
        double[] expected10 = {255.0, 0.0, 18.0, 36.0, 54.0, 72.0, 90.0, 108.0, 127.0, 145.0, 163.0, 181.0, 199.0, 217.0, 235.0, 254.0};

        // Step 3: Validate the results for each band
        for (int band = 1; band <= 2; band++) {
            double[] normalizedBand1 = MapAlgebra.bandAsArray(normalizedRaster1, band);
            double[] normalizedBand2 = MapAlgebra.bandAsArray(normalizedRaster2, band);
            double[] normalizedBand5 = MapAlgebra.bandAsArray(normalizedRaster5, band);
            double[] normalizedBand6 = MapAlgebra.bandAsArray(normalizedRaster6, band);
            double[] normalizedBand7 = MapAlgebra.bandAsArray(normalizedRaster7, band);
            double normalizedMin6 = Arrays.stream(normalizedBand6).min().getAsDouble();
            double normalizedMax6 = Arrays.stream(normalizedBand6).max().getAsDouble();

            assertEquals(Arrays.toString(expected1), Arrays.toString(normalizedBand1));
            assertEquals(Arrays.toString(expected2), Arrays.toString(normalizedBand2));
            assertEquals(Arrays.toString(expected6), Arrays.toString(normalizedBand5));
            assertEquals(Arrays.toString(expected1), Arrays.toString(normalizedBand7));

            assertEquals(0+((band-1)*127.5), normalizedMin6, 0.01d);
            assertEquals(127.5+((band-1)*127.5), normalizedMax6, 0.01d);
        }

        assertEquals(95.0, RasterUtils.getNoDataValue(normalizedRaster4.getSampleDimension(0)), 0.01d);
        assertEquals(95.0, RasterUtils.getNoDataValue(normalizedRaster4.getSampleDimension(1)), 0.01d);

        assertEquals(Arrays.toString(expected3), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster3, 1)));
        assertEquals(Arrays.toString(expected4), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster4, 1)));
        assertEquals(Arrays.toString(expected5), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster4, 2)));
        assertEquals(Arrays.toString(expected7), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster8, 1)));
        assertEquals(Arrays.toString(expected8), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster8, 2)));
        assertEquals(Arrays.toString(expected9), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster10, 1)));
        assertEquals(Arrays.toString(expected10), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster10, 2)));
    }

    @Test
    public void testNormalizeAll2() throws FactoryException {
        String[] pixelTypes = {"B", "I", "S", "US", "F", "D"}; // Byte, Integer, Short, Unsigned Short, Float, Double
        for (String pixelType : pixelTypes) {
            testNormalizeAll2(10, 10, pixelType);
        }
    }

    private void testNormalizeAll2(int width, int height, String pixelType) throws FactoryException {
        // Create an empty raster with the specified pixel type
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, pixelType, width, height, 10, 20, 1);

        // Fill raster
        double[] bandValues = new double[width * height];
        for (int i = 0; i < bandValues.length; i++) {
            bandValues[i] = i;
        }
        raster = MapAlgebra.addBandFromArray(raster, bandValues, 1);

        GridCoverage2D normalizedRaster = RasterEditors.normalizeAll(raster, 0, 255);

        // Check the normalized values and data type
        double[] normalizedBandValues = MapAlgebra.bandAsArray(normalizedRaster, 1);
        for (int i = 0; i < bandValues.length; i++) {
            double expected = (bandValues[i] - 0) * (254 - 0) / (99 - 0);
            double actual = normalizedBandValues[i];
            switch (normalizedRaster.getRenderedImage().getSampleModel().getDataType()) {
                case DataBuffer.TYPE_BYTE:
                case DataBuffer.TYPE_SHORT:
                case DataBuffer.TYPE_USHORT:
                case DataBuffer.TYPE_INT:
                    assertEquals((int) expected, (int) actual);
                    break;
                default:
                    assertEquals(expected, actual, 0.01);
            }
        }

        // Assert the data type remains as expected
        int resultDataType = normalizedRaster.getRenderedImage().getSampleModel().getDataType();
        int expectedDataType = RasterUtils.getDataTypeCode(pixelType);
        assertEquals(expectedDataType, resultDataType);
    }

}
