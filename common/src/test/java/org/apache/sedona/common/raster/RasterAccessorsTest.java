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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;

import static org.junit.Assert.assertEquals;

public class RasterAccessorsTest extends RasterTestBase
{
    @Test
    public void envelope() throws FactoryException
    {
        Geometry envelope = RasterAccessors.envelope(oneBandRaster);
        assertEquals(3600.0d, envelope.getArea(), 0.1d);
        assertEquals(378922.0d + 30.0d, envelope.getCentroid().getX(), 0.1d);
        assertEquals(4072345.0d + 30.0d, envelope.getCentroid().getY(), 0.1d);
        assertEquals(4326, RasterAccessors.envelope(multiBandRaster).getSRID());
    }

    @Test
    public void testEnvelopeUsingSkewedRaster() throws FactoryException {
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 100, 100, 5, 4, 3, -2, 0.1, 0.15, 3857);
        Geometry envelope = RasterAccessors.envelope(raster);
        Envelope env = envelope.getEnvelopeInternal();
        // The expected values were obtained by running the following query in PostGIS:
        // SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(100, 100, 5, 4, 3, -2, 0.1, 0.15, 3857)));
        assertEquals(5, env.getMinX(), 1e-9);
        assertEquals(315, env.getMaxX(), 1e-9);
        assertEquals(-196, env.getMinY(), 1e-9);
        assertEquals(19, env.getMaxY(), 1e-9);

        raster = RasterConstructors.makeEmptyRaster(1, 800, 700, 5, 4, 0.3, -0.2, -0.1, -0.15, 3857);
        envelope = RasterAccessors.envelope(raster);
        env = envelope.getEnvelopeInternal();
        // The expected values were obtained by running the following query in PostGIS:
        // SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(800, 700, 5, 4, 0.3, -0.2, -0.1, -0.15, 3857)));
        assertEquals(-65, env.getMinX(), 1e-9);
        assertEquals(245, env.getMaxX(), 1e-9);
        assertEquals(-256, env.getMinY(), 1e-9);
        assertEquals(4, env.getMaxY(), 1e-9);
    }

    @Test
    public void testNumBands() {
        assertEquals(1, RasterAccessors.numBands(oneBandRaster));
        assertEquals(4, RasterAccessors.numBands(multiBandRaster));
    }
    @Test
    public void testWidthAndHeight() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 10, 20, 0, 0, 8);
        assertEquals(20, RasterAccessors.getHeight(emptyRaster));
        assertEquals(10, RasterAccessors.getWidth(emptyRaster));
    }

    @Test
    public void testSrid() throws FactoryException {
        assertEquals(0, RasterAccessors.srid(oneBandRaster));
        assertEquals(4326, RasterAccessors.srid(multiBandRaster));
    }

    @Test
    public void testUpperLeftX() throws FactoryException {
        GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(1, 3, 4, 1,2, 5);
        double upperLeftX = RasterAccessors.getUpperLeftX(gridCoverage2D);
        assertEquals(1, upperLeftX, 0.1d);

        gridCoverage2D = RasterConstructors.makeEmptyRaster(10, 7, 8, 5, 6, 9);
        upperLeftX = RasterAccessors.getUpperLeftX(gridCoverage2D);
        assertEquals(5, upperLeftX, 0.1d);
    }

    @Test
    public void testUpperLeftY() throws FactoryException {
        GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(1, 3, 4, 1,2, 5);
        double upperLeftY = RasterAccessors.getUpperLeftY(gridCoverage2D);
        assertEquals(2, upperLeftY, 0.1d);

        gridCoverage2D = RasterConstructors.makeEmptyRaster(10, 7, 8, 5, 6, 9);
        upperLeftY = RasterAccessors.getUpperLeftY(gridCoverage2D);
        assertEquals(6, upperLeftY, 0.1d);
    }

    @Test
    public void testScaleX() throws UnsupportedOperationException, FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 10, 15, 0, 0, 1, -2, 0, 0, 0);
        assertEquals(1, RasterAccessors.getScaleX(emptyRaster), 1e-9);
    }

    @Test
    public void testScaleY() throws UnsupportedOperationException, FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 10, 15, 0, 0, 1, -2, 0, 0, 0);
        assertEquals(-2, RasterAccessors.getScaleY(emptyRaster), 1e-9);
    }

    @Test
    public void testMetaData()
            throws FactoryException
    {
        double upperLeftX = 1;
        double upperLeftY = 2;
        int widthInPixel = 3;
        int heightInPixel = 4;
        double pixelSize = 5;
        int numBands = 1;

        GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
        double[] metadata = RasterAccessors.metadata(gridCoverage2D);
        assertEquals(upperLeftX, metadata[0], 1e-9);
        assertEquals(upperLeftY, metadata[1], 1e-9);
        assertEquals(widthInPixel, metadata[2], 1e-9);
        assertEquals(heightInPixel, metadata[3], 1e-9);
        assertEquals(pixelSize, metadata[4], 1e-9);
        assertEquals(-1 * pixelSize, metadata[5], 1e-9);
        assertEquals(0, metadata[6], 1e-9);
        assertEquals(0, metadata[7], 1e-9);
        assertEquals(0, metadata[8], 1e-9);
        assertEquals(numBands, metadata[9], 1e-9);
        assertEquals(10, metadata.length);

        upperLeftX = 5;
        upperLeftY = 6;
        widthInPixel = 7;
        heightInPixel = 8;
        pixelSize = 9;
        numBands = 10;

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);

        metadata = RasterAccessors.metadata(gridCoverage2D);

        assertEquals(upperLeftX, metadata[0], 1e-9);
        assertEquals(upperLeftY, metadata[1], 1e-9);
        assertEquals(widthInPixel, metadata[2], 1e-9);
        assertEquals(heightInPixel, metadata[3], 1e-9);
        assertEquals(pixelSize, metadata[4], 1e-9);
        assertEquals(-1 * pixelSize, metadata[5], 1e-9);
        assertEquals(0, metadata[6], 1e-9);
        assertEquals(0, metadata[7], 1e-9);
        assertEquals(0, metadata[8], 1e-9);
        assertEquals(numBands, metadata[9], 1e-9);

        assertEquals(10, metadata.length);
    }

    @Test
    public void testMetaDataUsingSkewedRaster() throws FactoryException {
        int widthInPixel = 3;
        int heightInPixel = 4;
        double upperLeftX = 100.0;
        double upperLeftY = 200.0;
        double scaleX = 2.0;
        double scaleY = -3.0;
        double skewX = 0.1;
        double skewY = 0.2;
        int numBands = 1;

        GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, scaleX, scaleY, skewX, skewY, 3857);
        double[] metadata = RasterAccessors.metadata(gridCoverage2D);
        assertEquals(upperLeftX, metadata[0], 1e-9);
        assertEquals(upperLeftY, metadata[1], 1e-9);
        assertEquals(widthInPixel, metadata[2], 1e-9);
        assertEquals(heightInPixel, metadata[3], 1e-9);
        assertEquals(scaleX, metadata[4], 1e-9);
        assertEquals(scaleY, metadata[5], 1e-9);
        assertEquals(skewX, metadata[6], 1e-9);
        assertEquals(skewY, metadata[7], 1e-9);
        assertEquals(3857, metadata[8], 1e-9);
        assertEquals(numBands, metadata[9], 1e-9);
        assertEquals(10, metadata.length);
    }
}
