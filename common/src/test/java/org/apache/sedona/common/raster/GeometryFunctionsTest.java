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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.sedona.common.Functions;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

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
    GridCoverage2D emptyRaster =
        RasterConstructors.makeEmptyRaster(1, 5, 10, 156, -132, 5, 10, 3, 5, 0);
    Geometry convexHull = GeometryFunctions.convexHull(emptyRaster);
    String expected = "POLYGON ((156 -132, 181 -107, 211 -7, 186 -32, 156 -132))";
    String actual = Functions.asWKT(convexHull);
    assertEquals(expected, actual);
  }

  @Test
  public void testConvexHullFromRasterFile()
      throws FactoryException, TransformException, IOException {
    GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
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

  @Test
  public void testMinConvexHull() throws FactoryException, TransformException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 =
        new double[] {0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0};
    double[] values2 =
        new double[] {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    Geometry minConvexHull1 = GeometryFunctions.minConvexHull(emptyRaster, 1);
    Geometry minConvexHull2 = GeometryFunctions.minConvexHull(emptyRaster, 2);
    Geometry minConvexHullAll = GeometryFunctions.minConvexHull(emptyRaster);
    String expected1 = "POLYGON ((1 -1, 4 -1, 4 -5, 1 -5, 1 -1))";
    String expected2 = "POLYGON ((0 -1, 4 -1, 4 -5, 0 -5, 0 -1))";
    String expectedAll = "POLYGON ((0 -1, 4 -1, 4 -5, 0 -5, 0 -1))";
    assertEquals(expected1, Functions.asWKT(minConvexHull1));
    assertEquals(expected2, Functions.asWKT(minConvexHull2));
    assertEquals(expectedAll, Functions.asWKT(minConvexHullAll));
  }

  @Test
  public void testMinConvexHullRectangle() throws FactoryException, TransformException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 3, 0, 0, 1, -1, 0, 0, 0);
    double[] values1 = new double[] {0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0};
    double[] values2 = new double[] {0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0};
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values1, 1, 0d);
    emptyRaster = MapAlgebra.addBandFromArray(emptyRaster, values2, 2, 0d);
    Geometry minConvexHull1 = GeometryFunctions.minConvexHull(emptyRaster, 1);
    Geometry minConvexHull2 = GeometryFunctions.minConvexHull(emptyRaster, 2);
    Geometry minConvexHullAll = GeometryFunctions.minConvexHull(emptyRaster);
    String expected1 = "POLYGON ((1 -1, 4 -1, 4 -3, 1 -3, 1 -1))";
    String expected2 = "POLYGON ((0 0, 4 0, 4 -3, 0 -3, 0 0))";
    String expectedAll = "POLYGON ((0 0, 4 0, 4 -3, 0 -3, 0 0))";
    assertEquals(expected1, Functions.asWKT(minConvexHull1));
    assertEquals(expected2, Functions.asWKT(minConvexHull2));
    assertEquals(expectedAll, Functions.asWKT(minConvexHullAll));
  }

  @Test
  public void testMinConvexHullIllegalBand() throws FactoryException, TransformException {
    GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 3, 0, 0, 1, -1, 0, 0, 0);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GeometryFunctions.minConvexHull(emptyRaster, 3));
    assertEquals("Provided band index 3 does not lie in the raster", exception.getMessage());
  }

  @Test
  public void envelope() throws FactoryException {
    Geometry envelope = GeometryFunctions.envelope(oneBandRaster);
    assertEquals(3600.0d, envelope.getArea(), 0.1d);
    assertEquals(378922.0d + 30.0d, envelope.getCentroid().getX(), 0.1d);
    assertEquals(4072345.0d + 30.0d, envelope.getCentroid().getY(), 0.1d);
    assertEquals(4326, GeometryFunctions.envelope(multiBandRaster).getSRID());
  }

  @Test
  public void testEnvelopeUsingSkewedRaster() throws FactoryException {
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, 100, 100, 5, 4, 3, -2, 0.1, 0.15, 3857);
    Geometry envelope = GeometryFunctions.envelope(raster);
    Envelope env = envelope.getEnvelopeInternal();
    // The expected values were obtained by running the following query in PostGIS:
    // SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(100, 100, 5, 4, 3, -2, 0.1, 0.15, 3857)));
    assertEquals(5, env.getMinX(), 1e-9);
    assertEquals(315, env.getMaxX(), 1e-9);
    assertEquals(-196, env.getMinY(), 1e-9);
    assertEquals(19, env.getMaxY(), 1e-9);

    raster = RasterConstructors.makeEmptyRaster(1, 800, 700, 5, 4, 0.3, -0.2, -0.1, -0.15, 3857);
    envelope = GeometryFunctions.envelope(raster);
    env = envelope.getEnvelopeInternal();
    // The expected values were obtained by running the following query in PostGIS:
    // SELECT ST_AsText(ST_Envelope(ST_MakeEmptyRaster(800, 700, 5, 4, 0.3, -0.2, -0.1, -0.15,
    // 3857)));
    assertEquals(-65, env.getMinX(), 1e-9);
    assertEquals(245, env.getMaxX(), 1e-9);
    assertEquals(-256, env.getMinY(), 1e-9);
    assertEquals(4, env.getMaxY(), 1e-9);
  }
}
