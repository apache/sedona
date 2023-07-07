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
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.awt.image.DataBuffer;

public class RasterPredicatesTest extends RasterTestBase {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    @Test
    public void testIntersectsNoCrs() {
        Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, null);
        boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertTrue(result);
        queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
        result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertFalse(result);
    }

    @Test
    public void testIntersectsQueryWindowNoCrs() {
        Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, "EPSG:3857");
        boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertTrue(result);
        queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
        result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertFalse(result);
    }

    @Test
    public void testIntersectsRasterNoCrs() {
        Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
        queryWindow.setSRID(3857);
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, null);
        boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertTrue(result);
        queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
        queryWindow.setSRID(3857);
        result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertFalse(result);
    }

    @Test
    public void testIntersectsSameCrs() {
        Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
        queryWindow.setSRID(3857);
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, "EPSG:3857");
        boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertTrue(result);
        queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(10, 20, 10, 20));
        queryWindow.setSRID(3857);
        result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertTrue(result);
        queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
        queryWindow.setSRID(3857);
        result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertFalse(result);
    }

    @Test
    public void testIntersectsWithTransformations() {
        Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
        queryWindow.setSRID(4326);
        GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, "EPSG:3857");
        boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertTrue(result);
        queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(10, 20, 10, 20));
        queryWindow.setSRID(4326);
        result = RasterPredicates.rsIntersects(raster, queryWindow);
        Assert.assertFalse(result);
    }
}
