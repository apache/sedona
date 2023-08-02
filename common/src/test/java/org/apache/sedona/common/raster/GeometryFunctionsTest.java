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

import org.apache.sedona.common.Functions;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;

public class GeometryFunctionsTest extends RasterTestBase {

    @Test
    public void testConvexHull() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 156, -132, 5);
        Geometry convexHull = GeometryFunctions.convexHull(emptyRaster);
        String expected = "POLYGON ((156 -132, 181 -132, 181 -182, 156 -182, 156 -132))";
        String actual = Functions.asWKT(convexHull);
        assertEquals(expected, actual);
    }

    @Test
    public void testConvexHullSkewed() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 156, -132, 5, 10, 3, 5, 0);
        Geometry convexHull = GeometryFunctions.convexHull(emptyRaster);
        String expected = "POLYGON ((156 -132, 181 -107, 211 -7, 186 -32, 156 -132))";
        String actual = Functions.asWKT(convexHull);
        assertEquals(expected, actual);
    }

    @Test
    public void testConvexHullFromRasterFile() throws FactoryException, TransformException, IOException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");//RasterConstructors.makeEmptyRaster(1, 5, 10, 156, -132, 5, 10, 3, 5, 0);
        Geometry convexHull = GeometryFunctions.convexHull(raster);
        Coordinate[] coordinates = convexHull.getCoordinates();
        Coordinate expectedCoordOne = new Coordinate(-13095817.809482181, 4021262.7487925636);
        Coordinate expectedCoordTwo = new Coordinate(-13058785.559768861, 4021262.7487925636);
        Coordinate expectedCoordThree = new Coordinate(-13058785.559768861, 3983868.8560156375);
        Coordinate expectedCoordFour = new Coordinate(-13095817.809482181, 3983868.8560156375);
        assertEquals(expectedCoordOne.getX(), coordinates[0].getX(), 0.5d);
        assertEquals(expectedCoordOne.getY(), coordinates[0].getY(), 0.5d);

        assertEquals(expectedCoordTwo.getX(), coordinates[1].getX(), 0.5d);
        assertEquals(expectedCoordTwo.getY(), coordinates[1].getY(), 0.5d);

        assertEquals(expectedCoordThree.getX(), coordinates[2].getX(), 0.5d);
        assertEquals(expectedCoordThree.getY(), coordinates[2].getY(), 0.5d);

        assertEquals(expectedCoordFour.getX(), coordinates[3].getX(), 0.5d);
        assertEquals(expectedCoordFour.getY(), coordinates[3].getY(), 0.5d);

        assertEquals(expectedCoordOne.getX(), coordinates[4].getX(), 0.5d);
        assertEquals(expectedCoordOne.getY(), coordinates[4].getY(), 0.5d);
    }
}
