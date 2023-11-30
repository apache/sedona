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

import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class RasterBandEditorsTest extends RasterTestBase{

    @Test
    public void testSetBandNoDataValueWithRaster() throws IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(raster, 1,3d);
        double actual = RasterBandAccessors.getBandNoDataValue(grid);
        double expected = 3;
        assertEquals(expected, actual, 0.1d);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(grid, 1)));

        grid = RasterBandEditors.setBandNoDataValue(raster, -999d);
        actual = RasterBandAccessors.getBandNoDataValue(grid);
        expected = -999;
        assertEquals(expected, actual, 0.1d);
        assert(Arrays.equals(MapAlgebra.bandAsArray(raster, 1), MapAlgebra.bandAsArray(grid, 1)));
    }

    @Test
    public void testSetBandNoDataValueWithNull() throws IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(raster, 1,null);
        String actual = Arrays.toString(grid.getSampleDimensions());
        String expected = "[RenderedSampleDimension[\"PALETTE_INDEX\"]]";
        assertEquals(expected, actual);
    }

    @Test
    public void testSetBandNoDataValueWithEmptyRaster() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 4326);
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(emptyRaster, 1, 999d);
        double actual = RasterBandAccessors.getBandNoDataValue(grid);
        double expected = 999;
        assertEquals(expected, actual, 0.1d);

        grid = RasterBandEditors.setBandNoDataValue(emptyRaster, -444.444);
        actual = RasterBandAccessors.getBandNoDataValue(grid);
        expected = -444.444;
        assertEquals(expected, actual, 0.0001d);
    }

    @Test
    public void testSetBandNoDataValueWithEmptyRasterMultipleBand() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 20, 20, 0, 0, 8, 8, 0.1, 0.1, 0);
        GridCoverage2D grid = RasterBandEditors.setBandNoDataValue(emptyRaster, -9999d);
        grid = RasterBandEditors.setBandNoDataValue(grid, 2, 444d);
        assertEquals(-9999, (double) RasterBandAccessors.getBandNoDataValue(grid), 0.1d);
        assertEquals(444, (double) RasterBandAccessors.getBandNoDataValue(grid, 2), 0.1d);
    }

    @Test
    public void testClip() throws IOException, FactoryException, TransformException, ParseException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster_geotiff_color/FAA_UTM18N_NAD83.tif");
        String polygon = "POLYGON ((236722 4204770, 243900 4204770, 243900 4197590, 221170 4197590, 236722 4204770))";
        Geometry geom = Constructors.geomFromWKT(polygon, RasterAccessors.srid(raster));

        GridCoverage2D clippedRaster = RasterBandEditors.clip(raster, 1, geom, 200, false);
        double[] clippedMetadata = Arrays.stream(RasterAccessors.metadata(clippedRaster), 0, 9).toArray();
        double[] originalMetadata = Arrays.stream(RasterAccessors.metadata(raster), 0, 9).toArray();
        assertArrayEquals(originalMetadata, clippedMetadata, 0.01d);

        String actual = String.valueOf(clippedRaster.getSampleDimensions()[0]);
        String expected = "RenderedSampleDimension(\"RED_BAND\":[200.0 ... 200.0])\n  ‣ Category(\"No data\":[200...200])\n";
        assertEquals(expected, actual);

        List<Geometry> points = new ArrayList<>();
        points.add(Constructors.geomFromWKT("POINT(223802 4.21769e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(224759 4.20453e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(237201 4.20429e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(237919 4.20357e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(254668 4.21769e+06)", 26918));
        Double[] actualValues = PixelFunctions.values(clippedRaster, points, 1).toArray(new Double[0]);
        Double[] expectedValues = new Double[] {null, null, 0.0, 0.0, null};
        assertTrue(Arrays.equals(expectedValues, actualValues));

        GridCoverage2D croppedRaster = RasterBandEditors.clip(raster, 1, geom, 200, true);
        points = new ArrayList<>();
        points.add(Constructors.geomFromWKT("POINT(236842 4.20465e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(236961 4.20453e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(237201 4.20429e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(237919 4.20357e+06)", 26918));
        points.add(Constructors.geomFromWKT("POINT(223802 4.20465e+06)", 26918));
        actualValues = PixelFunctions.values(croppedRaster, points, 1).toArray(new Double[0]);
        expectedValues = new Double[] {0.0, 0.0, 0.0, 0.0, null};
        assertTrue(Arrays.equals(expectedValues, actualValues));
    }

    @Test
    public void testAddBandWithEmptyRaster() throws FactoryException {
        double[][] rasterData1 = new double[][] {
                {13, 80, 49, 15, 4, 46, 47, 94, 58, 37, 6, 22, 98, 26, 78, 66, 86, 79, 5, 65, 7, 12, 89, 67},
                {37, 4, 5, 15, 60, 83, 24, 19, 23, 87, 98, 89, 59, 71, 42, 46, 0, 80, 27, 73, 66, 100, 78, 64},
                {73, 39, 50, 13, 45, 21, 87, 38, 63, 22, 44, 6, 8, 24, 19, 10, 89, 3, 48, 28, 0, 71, 59, 11}
        };
        GridCoverage2D toRaster = RasterConstructors.makeNonEmptyRaster(3, "i", 4, 6, 1, -1, 1, 1, 0, 0, 0, rasterData1);

        // fromRaster's data type is Double to test the preservation of data type
        double[][] rasterData2 = new double[][] {
                {35, 68, 56, 87, 49, 20, 73, 90, 45, 96, 52, 98, 2, 82, 88, 74, 77, 60, 5, 61, 81, 32, 9, 15},
                {55, 49, 72, 10, 63, 94, 100, 83, 61, 47, 20, 15, 34, 46, 52, 11, 23, 98, 70, 67, 18, 39, 53, 91}
        };
        GridCoverage2D fromRaster = RasterConstructors.makeNonEmptyRaster(2, "d", 4, 6, 10, -10, 1, -1, 0, 0, 0, rasterData2);

        // test 4 parameter variant
        testAddBand4Param(fromRaster, toRaster);

        // test 3 parameter variant
        testAddBand3Param(fromRaster, toRaster);

        // test 2 parameter variant
        testAddBand2Param(fromRaster, toRaster);
    }

    public static void testAddBand4Param(GridCoverage2D fromRaster, GridCoverage2D toRaster) throws FactoryException {
        GridCoverage2D actualRaster = RasterBandEditors.addBand(toRaster, fromRaster, 1, 4);

        // test numBands
        int actualNumBands = RasterAccessors.numBands(actualRaster);
        int expectedNumBands = 4;
        assertEquals(expectedNumBands, actualNumBands);

        // test data type preservation
        String actualDataType = RasterBandAccessors.getBandType(actualRaster);
        String expectedDataType = "SIGNED_32BITS";
        assertEquals(expectedDataType, actualDataType);

        // test new band values in the resultant raster
        double[] actualBandValues = MapAlgebra.bandAsArray(actualRaster, 4);
        double[] expectedBandValues = MapAlgebra.bandAsArray(fromRaster, 1);
        assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

        // test preservation of original raster
        // remove last index as that's number of bands and they wouldn't be equal
        double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(actualRaster), 0, 9).toArray();
        double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(toRaster), 0, 9).toArray();
        assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
    }

    public static void testAddBand3Param(GridCoverage2D fromRaster, GridCoverage2D toRaster) throws FactoryException {
        GridCoverage2D actualRaster = RasterBandEditors.addBand(toRaster, fromRaster, 2);

        // test numBands
        int actualNumBands = RasterAccessors.numBands(actualRaster);
        int expectedNumBands = 4;
        assertEquals(expectedNumBands, actualNumBands);

        // test data type preservation
        String actualDataType = RasterBandAccessors.getBandType(actualRaster);
        String expectedDataType = "SIGNED_32BITS";
        assertEquals(expectedDataType, actualDataType);

        // test new band values in the resultant raster
        double[] actualBandValues = MapAlgebra.bandAsArray(actualRaster, 4);
        double[] expectedBandValues = MapAlgebra.bandAsArray(fromRaster, 2);
        assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

        // test preservation of original raster
        // remove last index as that's number of bands and they wouldn't be equal
        double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(actualRaster), 0, 9).toArray();
        double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(toRaster), 0, 9).toArray();
        assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
    }

    public static void testAddBand2Param(GridCoverage2D fromRaster, GridCoverage2D toRaster) throws FactoryException {
        GridCoverage2D actualRaster = RasterBandEditors.addBand(toRaster, fromRaster);

        // test numBands
        int actualNumBands = RasterAccessors.numBands(actualRaster);
        int expectedNumBands = 4;
        assertEquals(expectedNumBands, actualNumBands);

        // test data type preservation
        String actualDataType = RasterBandAccessors.getBandType(actualRaster);
        String expectedDataType = "SIGNED_32BITS";
        assertEquals(expectedDataType, actualDataType);

        // test new band values in the resultant raster
        double[] actualBandValues = MapAlgebra.bandAsArray(actualRaster, 4);
        double[] expectedBandValues = MapAlgebra.bandAsArray(fromRaster, 1);
        assertArrayEquals(expectedBandValues, actualBandValues, 0.1d);

        // test preservation of original raster
        // remove last index as that's number of bands and they wouldn't be equal
        double[] actualMetadata = Arrays.stream(RasterAccessors.metadata(actualRaster), 0, 9).toArray();
        double[] expectedMetadata = Arrays.stream(RasterAccessors.metadata(toRaster), 0, 9).toArray();
        assertArrayEquals(expectedMetadata, actualMetadata, 0.1d);
    }

}
