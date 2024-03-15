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
        GridCoverage2D raster1 = RasterConstructors.makeEmptyRaster(1, "d", 4, 3, 0, 0, 2, -2, 0, 0, 0);
        raster1 = MapAlgebra.addBandFromArray(raster1, values1, 1, 99.0);
        raster1 = MapAlgebra.addBandFromArray(raster1, values2, 2, -9999.0);

        values1 = new double[] {1,2,3,4,5,6,7,8,9,10,99,12,13,14,15,16,17,18,19,20,21,22,23,24,25};
        values2 = new double[] {10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,-9999};
        GridCoverage2D raster2 = RasterConstructors.makeEmptyRaster(1, "d", 5, 5, 0, 0, 2, -2, 0, 0, 0);
        raster2 = MapAlgebra.addBandFromArray(raster2, values1, 1, 99.0);
        raster2 = MapAlgebra.addBandFromArray(raster2, values2, 2, -9999.0);

        GridCoverage2D newRaster = RasterEditors.resample(raster1, 6, 5, 1, -1, false, "bilinear");

        String res1 = RasterOutputs.asMatrix(newRaster, 1);
        String res2 = RasterOutputs.asMatrix(newRaster, 2);
        String expectedRes1 = "|99.000000  99.000000  99.000000  99.000000  99.000000  99.000000|\n" +
                "|99.000000   3.838750  99.000000   4.479375   4.400208   4.495000|\n" +
                "|99.000000  99.000000   5.985764   6.593403   6.169791  99.000000|\n" +
                "|99.000000  99.000000   8.250487   7.955348   8.473751  99.000000|\n" +
                "|99.000000   9.375000   9.895833  99.000000  99.000000  12.000000|\n";
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

        newRaster = RasterEditors.resample(raster1, 6, 5, 1, -1, false, "bicubic");

        res1 = RasterOutputs.asMatrix(newRaster, 1);
        res2 = RasterOutputs.asMatrix(newRaster, 2);
        expectedRes1 = "|99.000000  99.000000  99.000000  99.000000  99.000000  99.000000|\n" +
                "|99.000000   3.539351  99.000000   4.273053   4.102367   4.205373|\n" +
                "|99.000000  99.000000   5.866029   6.737416   6.227383  99.000000|\n" +
                "|99.000000  99.000000   8.386340   8.053590   8.636799  99.000000|\n" +
                "|99.000000   9.564744  10.207731  99.000000  99.000000  12.610778|\n";
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

        newRaster = RasterEditors.resample(raster2, 3, 3, 1, -1, false, "bilinear");

        res1 = RasterOutputs.asMatrix(newRaster, 1);
        res2 = RasterOutputs.asMatrix(newRaster, 2);
        expectedRes1 = "|99.000000  99.000000  99.000000|\n" +
                "|99.000000   9.500002  11.555557|\n" +
                "|99.000000  19.777779  21.833334|\n";
        expectedRes2 = "|-9999.000000  -9999.000000  -9999.000000|\n" +
                "|-9999.000000     18.500002     20.555557|\n" +
                "|-9999.000000     28.777779     29.718364|\n";

        metadata = RasterAccessors.metadata(newRaster);
        expectedMetadata = new double[] {-2.3333330154418945, 2.3333330154418945, 3.0, 3.0, 4.111111005147298, -4.111111005147298, 0.0, 0.0, 0.0, 2.0};
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

}
