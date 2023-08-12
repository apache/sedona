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
import org.junit.Assert;
import org.junit.Test;
import org.opengis.referencing.FactoryException;

import java.awt.image.DataBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MapAlgebraTest extends RasterTestBase
{
    @Test
    public void testAddBandAsArrayAppend()
            throws FactoryException
    {
        GridCoverage2D raster = createEmptyRaster(1);
        double[] band1 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = i;
        }
        double[] band2 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        double[] band3 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band2.length; i++) {
            band2[i] = i * 2;
            band3[i] = i * 3;
        }
        // Replace the first band
        GridCoverage2D rasterWithBand1 = MapAlgebra.addBandFromArray(raster, band1, 1);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));

        //replace the first band with a customNoDataValue
        rasterWithBand1 = MapAlgebra.addBandFromArray(rasterWithBand1, band1, 1, -999d);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));
        assertEquals(-999, RasterUtils.getNoDataValue(rasterWithBand1.getSampleDimension(0)), 1e-9);

        //replace first band with a different customNoDataValue
        rasterWithBand1 = MapAlgebra.addBandFromArray(rasterWithBand1, band1, 1, -9999d);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));
        assertEquals(-9999, RasterUtils.getNoDataValue(rasterWithBand1.getSampleDimension(0)), 1e-9);

        //remove noDataValue from the first band
        rasterWithBand1 = MapAlgebra.addBandFromArray(rasterWithBand1, band1, 1, null);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));
        assertTrue(Double.isNaN(RasterUtils.getNoDataValue(rasterWithBand1.getSampleDimension(0))));

        // Append a new band with default noDataValue
        GridCoverage2D rasterWithBand2 = MapAlgebra.addBandFromArray(rasterWithBand1, band2);
        assertEquals(2, RasterAccessors.numBands(rasterWithBand2));
        assertEquals(raster.getEnvelope(), rasterWithBand2.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand2.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand2));
        assertTrue(Double.isNaN(RasterUtils.getNoDataValue(rasterWithBand2.getSampleDimension(1))));

        // Append a new band with custom noDataValue
        GridCoverage2D rasterWithBand3 = MapAlgebra.addBandFromArray(rasterWithBand2, band3, 3, 2d);
        assertEquals(3, RasterAccessors.numBands(rasterWithBand3));
        assertEquals(raster.getEnvelope(), rasterWithBand3.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand3.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand3));
        assertEquals(2, RasterUtils.getNoDataValue(rasterWithBand3.getSampleDimension(2)), 1e-9);

        // Check the value of the first band when use the raster with only one band
        double[] firstBand = MapAlgebra.bandAsArray(rasterWithBand1, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the first band when use the raster with two bands

        //Check the value of the first band when use the raster with three bands
        firstBand = MapAlgebra.bandAsArray(rasterWithBand3, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the second band
        double[] secondBand = MapAlgebra.bandAsArray(rasterWithBand2, 2);
        for (int i = 0; i < secondBand.length; i++) {
            assertEquals(i * 2, secondBand[i], 0.1);
        }

        // Check the value of the third band
        double[] thirdBand = MapAlgebra.bandAsArray(rasterWithBand3, 3);
        for (int i = 0; i < secondBand.length; i++) {
            assertEquals(i * 3, thirdBand[i], 0.1);
        }
    }

    @Test
    public void testAddBandAsArrayReplace()
            throws FactoryException
    {
        GridCoverage2D raster = createEmptyRaster(2);
        double[] band1 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = i;
        }
        double[] band2 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band2.length; i++) {
            band2[i] = i * 2;
        }
        // Replace the first band
        GridCoverage2D rasterWithBand1 = MapAlgebra.addBandFromArray(raster, band1, 1);
        assertEquals(2, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));

        // Replace the second band
        GridCoverage2D rasterWithBand2 = MapAlgebra.addBandFromArray(rasterWithBand1, band2, 2);
        assertEquals(2, RasterAccessors.numBands(rasterWithBand2));
        assertEquals(raster.getEnvelope(), rasterWithBand2.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand2.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand2));

        // Check the value of the first band when use the raster with only one band
        double[] firstBand = MapAlgebra.bandAsArray(rasterWithBand1, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the first band when use the raster with two bands
        firstBand = MapAlgebra.bandAsArray(rasterWithBand2, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the second band
        double[] secondBand = MapAlgebra.bandAsArray(rasterWithBand2, 2);
        for (int i = 0; i < secondBand.length; i++) {
            assertEquals(i * 2, secondBand[i], 0.1);
        }
    }

    @Test
    public void testBandAsArray()
            throws FactoryException
    {
        int widthInPixel = 10;
        int heightInPixel = 10;
        double upperLeftX = 0;
        double upperLeftY = 0;
        double cellSize = 1;
        int numbBands = 1;
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(numbBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, cellSize);
        // Out of bound index should return null
        double[] band = MapAlgebra.bandAsArray(raster, 0);
        assertNull(band);
        band = MapAlgebra.bandAsArray(raster, 1);
        assertEquals(widthInPixel * heightInPixel, band.length);
        for (int i = 0; i < band.length; i++) {
            // The default value is 0.0
            assertEquals(0.0, band[i], 0.1);
        }
        // Now set the value of the first band and check again
        for (int i = 0; i < band.length; i++) {
            band[i] = i * 0.1;
        }
        double[] bandNew = MapAlgebra.bandAsArray(MapAlgebra.addBandFromArray(raster, band, 1), 1);
        assertEquals(band.length, bandNew.length);
        for (int i = 0; i < band.length; i++) {
            assertEquals(band[i], bandNew[i], 1e-9);
        }
    }

    @Test
    public void testMapAlgebra() throws FactoryException {
        Random random = new Random();
        String[] pixelTypes = {null, "b", "i", "s", "us", "f", "d"};
        for (String pixelType : pixelTypes) {
            int width = random.nextInt(100) + 10;
            int height = random.nextInt(100) + 10;
            testMapAlgebra(width, height, pixelType, null);
            testMapAlgebra(width, height, pixelType, 100.0);
        }
    }

    private void testMapAlgebra(int width, int height, String pixelType, Double noDataValue) throws FactoryException {
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, "b", width, height, 10, 20, 1);
        double[] band1 = new double[width * height];
        double[] band2 = new double[width * height];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = Math.random() * 10;
            band2[i] = Math.random() * 10;
        }
        raster = MapAlgebra.addBandFromArray(raster, band1, 1);
        raster = MapAlgebra.addBandFromArray(raster, band2, 2);
        GridCoverage2D result = MapAlgebra.mapAlgebra(raster, pixelType, "out = (rast[0] + rast[1]) * 0.4;", noDataValue);
        double actualNoDataValue = RasterUtils.getNoDataValue(result.getSampleDimension(0));
        if (noDataValue != null) {
            Assert.assertEquals(noDataValue, actualNoDataValue, 1e-9);
        } else {
            Assert.assertTrue(Double.isNaN(actualNoDataValue));
        }

        int resultDataType = result.getRenderedImage().getSampleModel().getDataType();
        int expectedDataType;
        if (pixelType != null) {
            expectedDataType = RasterUtils.getDataTypeCode(pixelType);
        } else {
            expectedDataType = raster.getRenderedImage().getSampleModel().getDataType();
        }
        Assert.assertEquals(expectedDataType, resultDataType);

        Assert.assertEquals(raster.getGridGeometry().getGridToCRS2D(), result.getGridGeometry().getGridToCRS2D());
        band1 = MapAlgebra.bandAsArray(raster, 1);
        band2 = MapAlgebra.bandAsArray(raster, 2);
        double[] bandResult = MapAlgebra.bandAsArray(result, 1);
        Assert.assertEquals(band1.length, bandResult.length);
        for (int i = 0; i < band1.length; i++) {
            double expected = (band1[i] + band2[i]) * 0.4;
            double actual = bandResult[i];
            switch (resultDataType) {
                case DataBuffer.TYPE_BYTE:
                case DataBuffer.TYPE_SHORT:
                case DataBuffer.TYPE_USHORT:
                case DataBuffer.TYPE_INT:
                    Assert.assertEquals((int) expected, (int) actual);
                    break;
                default:
                    Assert.assertEquals(expected, actual, 1e-3);
            }
        }
    }
}
