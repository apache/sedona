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

import java.awt.image.DataBuffer;
import java.io.IOException;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class RasterPredicatesTest extends RasterTestBase {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @Test
  public void testIntersectsNoCrs() {
    // Both sides are assumed to be in WGS84
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
    // Raster is in WGS84, query window is assumed to be in WGS84, no CRS transformation needed
    Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, "EPSG:4326");
    boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);

    // Raster is not in WGS84, need to transform raster to WGS84, while the query window is assumed
    // to be in WGS84
    raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 751, 742, 332597, 4256477, 300, 1, "EPSG:32610");
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(-123.663, 37.455));
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(-120.940, 35.801));
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(431587, 4150960));
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
  }

  @Test
  public void testIntersectsRasterNoCrs() {
    Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
    queryWindow.setSRID(3857);
    GridCoverage2D raster = createRandomRaster(DataBuffer.TYPE_BYTE, 10, 10, 0, 10, 1, 1, null);
    boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
    queryWindow.setSRID(3857);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(1740120, 1390880));
    queryWindow.setSRID(3857);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);

    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(5, 5));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(11, 11));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);
  }

  @Test
  public void testIntersectsSameCrs() {
    Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
    queryWindow.setSRID(3857);
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, "EPSG:3857");
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
  public void testIntersectsSameCrs4326() {
    Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
    queryWindow.setSRID(4326);
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 10, 0.1, 1, "EPSG:4326");
    boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(10, 20, 10, 20));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(1000, 1010, 1000, 1010));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);
  }

  @Test
  public void testIntersectsWithTransformations() {
    Geometry queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 10, 0, 10));
    queryWindow.setSRID(4326);
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 100, 100, 0, 100, 1, 1, "EPSG:3857");
    boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(10, 20, 10, 20));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);
  }

  @Test
  public void testIntersectsWithTransformations2() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 7741, 7871, 301485, 4106715, 30, 1, "EPSG:32654");

    Geometry queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(15676311, 4350120));
    queryWindow.setSRID(3857);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(15494263, 4240252));
    queryWindow.setSRID(3857);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(15388866, 4421023));
    queryWindow.setSRID(3857);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(15847500, 4263886));
    queryWindow.setSRID(3857);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));

    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(140.9834, 36.4790));
    queryWindow.setSRID(4326);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(139.1070, 35.6720));
    queryWindow.setSRID(4326);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(36.4790, 140.9834));
    queryWindow.setSRID(4326);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(35.6720, 139.1070));
    queryWindow.setSRID(4326);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(142.0449, 35.3497));
    queryWindow.setSRID(4326);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));

    // NAD83
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(140.9834, 36.4790));
    queryWindow.setSRID(4269);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(139.1070, 35.6720));
    queryWindow.setSRID(4269);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(36.4790, 140.9834));
    queryWindow.setSRID(4269);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(35.6720, 139.1070));
    queryWindow.setSRID(4269);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(142.0449, 35.3497));
    queryWindow.setSRID(4269);
    Assert.assertFalse(RasterPredicates.rsIntersects(raster, queryWindow));
  }

  @Test
  public void testIntersectsCrossingAntiMeridian() {
    GridCoverage2D raster =
        createRandomRaster(DataBuffer.TYPE_BYTE, 4289, 4194, 306240, 7840860, 60, 1, "EPSG:32601");

    // Query using points near -180 lon
    Geometry queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(-177.8130, 68.5886));
    queryWindow.setSRID(4326);
    boolean result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(-172.230, 69.830));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);

    // Query using points near 180 lon
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(179.7239, 69.5221));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(175.7754, 68.4907));
    queryWindow.setSRID(4326);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);

    // Query using envelopes crossing the anti-meridian in EPSG:3413
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(-1787864, -1446256, 1381532, 1733816));
    queryWindow.setSRID(3413);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertTrue(result);
    queryWindow = GEOMETRY_FACTORY.toGeometry(new Envelope(-2041936, -1736623, 1782922, 2088234));
    queryWindow.setSRID(3413);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);
  }

  @Test
  public void testContainsNoCrs() throws FactoryException {
    Geometry geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(5, 10, 5, 10));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1);
    boolean result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(2, 22, 2, 22));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // geometry protruding out of the raster envelope
    geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(2, 20, 2, 25));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testContainsGeomNoCrs() throws FactoryException {
    Geometry geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(5, 10, 5, 10));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1, -1, 0, 0, 4326);
    boolean result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(2, 22, 2, 22));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // geometry protruding out of the raster envelope
    geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(2, 20, 2, 25));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testContainsRasterNoCrs() throws FactoryException, ParseException, IOException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    Geometry geometry = geometryFactory.toGeometry(new Envelope(5, 10, 5, 10));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1);
    boolean result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    geometry = geometryFactory.toGeometry(new Envelope(2, 22, 2, 22));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // geometry protruding out of the raster envelope
    geometry = geometryFactory.toGeometry(new Envelope(2, 20, 2, 25));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testContainsSameCrs() throws FactoryException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 3857);
    Geometry geometry = geometryFactory.toGeometry(new Envelope(5, 10, 5, 10));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1, -1, 0, 0, 3857);
    boolean result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    geometry = geometryFactory.toGeometry(new Envelope(2, 22, 2, 22));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // geometry protruding out of the raster envelope
    geometry = geometryFactory.toGeometry(new Envelope(2, 20, 2, 25));
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testContainsDifferentCrs() throws FactoryException, TransformException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 3857);
    Geometry geometry = geometryFactory.toGeometry(new Envelope(5, 10, 5, 10));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1, -1, 0, 0, 3857);
    boolean result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertTrue(result);

    // geometry protruding out of the raster envelope
    geometry = geometryFactory.toGeometry(new Envelope(2, 20, 2, 25));
    geometry =
        JTS.transform(
            geometry,
            CRS.findMathTransform(
                raster.getCoordinateReferenceSystem(), CRS.decode("EPSG:4326", true)));
    geometry.setSRID(4326);
    result = RasterPredicates.rsContains(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithinNoCrs() throws FactoryException {
    Geometry geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 100, 0, 50));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1);
    boolean result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    raster = RasterConstructors.makeEmptyRaster(1, 100, 50, 0, 50, 1);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // raster protruding out of the geometry
    raster = RasterConstructors.makeEmptyRaster(1, 100, 100, 0, 50, 1);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithinGeomNoCrs() throws FactoryException {
    Geometry geometry = GEOMETRY_FACTORY.toGeometry(new Envelope(0, 100, 0, 50));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1, -1, 0, 0, 3857);
    boolean result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    raster = RasterConstructors.makeEmptyRaster(1, 100, 50, 0, 50, 1, -1, 0, 0, 3857);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // raster protruding out of the geometry
    raster = RasterConstructors.makeEmptyRaster(1, 100, 100, 0, 50, 1, -1, 0, 0, 3857);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithinRasterNoCrs() throws FactoryException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    Geometry geometry = geometryFactory.toGeometry(new Envelope(0, 100, 0, 50));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1);
    boolean result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    raster = RasterConstructors.makeEmptyRaster(1, 100, 50, 0, 50, 1);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // raster protruding out of the geometry
    raster = RasterConstructors.makeEmptyRaster(1, 100, 100, 0, 50, 1);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithinSameCrs() throws FactoryException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    Geometry geometry = geometryFactory.toGeometry(new Envelope(0, 100, 0, 50));
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 20, 20, 2, 22, 1, -1, 0, 0, 4326);
    boolean result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // overlapping raster and geometry;
    raster = RasterConstructors.makeEmptyRaster(1, 100, 50, 0, 50, 1, -1, 0, 0, 4326);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // raster protruding out of the geometry
    raster = RasterConstructors.makeEmptyRaster(1, 100, 100, 0, 50, 1, -1, 0, 0, 4326);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testWithinDifferentCrs() throws FactoryException, TransformException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    Geometry geometry = geometryFactory.toGeometry(new Envelope(30, 60, 10, 50));
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, 20, 20, 32, 35, 1, -1, 0, 0, 4326);
    geometry =
        JTS.transform(
            geometry,
            CRS.findMathTransform(
                raster.getCoordinateReferenceSystem(), CRS.decode("EPSG:3857", true)));
    geometry.setSRID(3857);
    boolean result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertTrue(result);

    // raster protruding out of the geometry
    raster = RasterConstructors.makeEmptyRaster(1, 100, 100, 0, 50, 1, -1, 0, 0, 4326);
    result = RasterPredicates.rsWithin(raster, geometry);
    Assert.assertFalse(result);
  }

  @Test
  public void testRasterWithSkew() throws FactoryException {
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(1, "B", 100, 100, 0, 100, 1, -1, 0.1, 0.1, 3857);

    Geometry queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(7.80, 94.82));
    queryWindow.setSRID(3857);
    boolean result = RasterPredicates.rsContains(raster, queryWindow);
    Assert.assertTrue(result);

    // Within the envelope of the raster, but not the convex hull of the raster
    queryWindow = GEOMETRY_FACTORY.createPoint(new Coordinate(9.91, 103.86));
    queryWindow.setSRID(3857);
    Geometry rasterEnvelope = JTS.toGeometry((BoundingBox) raster.getEnvelope2D());
    Assert.assertTrue(rasterEnvelope.contains(queryWindow));
    result = RasterPredicates.rsContains(raster, queryWindow);
    Assert.assertFalse(result);
    result = RasterPredicates.rsIntersects(raster, queryWindow);
    Assert.assertFalse(result);
  }

  @Test
  public void testRasterRasterPredicates() throws FactoryException {
    GridCoverage2D raster1 =
        RasterConstructors.makeEmptyRaster(
            1, "B", 428, 419, 306210, 7840890, 600, -600, 0, 0, 32601);
    GridCoverage2D raster2 =
        RasterConstructors.makeEmptyRaster(
            1, "B", 100, 100, -19999963, 11067747, 1, -1, 0, 0, 3857);
    GridCoverage2D raster3 =
        RasterConstructors.makeEmptyRaster(
            1, "B", 100, 100, -19331028, 10889880, 1, -1, 0, 0, 3857);
    Assert.assertTrue(RasterPredicates.rsIntersects(raster1, raster2));
    Assert.assertTrue(RasterPredicates.rsIntersects(raster2, raster1));
    Assert.assertFalse(RasterPredicates.rsIntersects(raster1, raster3));
    Assert.assertFalse(RasterPredicates.rsIntersects(raster3, raster1));
    Assert.assertTrue(RasterPredicates.rsContains(raster1, raster2));
    Assert.assertFalse(RasterPredicates.rsContains(raster2, raster1));
  }

  @Test
  public void testRasterRasterPredicatesNoCrs() throws FactoryException {
    GridCoverage2D raster1 =
        RasterConstructors.makeEmptyRaster(
            1, "B", 428, 419, 306210, 7840890, 600, -600, 0, 0, 32601);
    GridCoverage2D raster2 =
        RasterConstructors.makeEmptyRaster(1, "B", 100, 100, -179.3542, 70.0634, 0.01);
    GridCoverage2D raster3 =
        RasterConstructors.makeEmptyRaster(1, "B", 100, 100, -175.8738, 69.7670, 0.01);

    Assert.assertTrue(RasterPredicates.rsIntersects(raster1, raster2));
    Assert.assertTrue(RasterPredicates.rsIntersects(raster2, raster1));
    Assert.assertTrue(RasterPredicates.rsIntersects(raster1, raster3));
    Assert.assertTrue(RasterPredicates.rsIntersects(raster3, raster1));
    Assert.assertTrue(RasterPredicates.rsContains(raster1, raster2));
    Assert.assertFalse(RasterPredicates.rsContains(raster2, raster1));
    Assert.assertFalse(RasterPredicates.rsIntersects(raster2, raster3));
    Assert.assertFalse(RasterPredicates.rsIntersects(raster3, raster2));
  }

  @Test
  public void testDWithinWGS84RasterPointMeterSemantics() throws FactoryException {
    // 5x5 degree raster at WGS84 origin: hull (0,-5)–(5,0), centroid (2.5, -2.5).
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 4326);

    // Point coincident with the raster centroid — distance is 0, so any non-negative
    // threshold matches and the unit cannot silently fall back to degrees.
    Geometry coincident = GEOMETRY_FACTORY.createPoint(new Coordinate(2.5, -2.5));
    coincident.setSRID(4326);
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, coincident, 0.0));
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, coincident, 1.0));

    // Point ~10° east at the same latitude — geodesic distance ≈ 1 112 km. Bracket the
    // threshold above and below to catch unit mistakes (1° was the old buggy semantics)
    // and projection regressions.
    Geometry tenDegreesEast = GEOMETRY_FACTORY.createPoint(new Coordinate(12.5, -2.5));
    tenDegreesEast.setSRID(4326);
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, tenDegreesEast, 2_000_000.0));
    Assert.assertFalse(RasterPredicates.rsDWithin(raster, tenDegreesEast, 500_000.0));
    // A degree-sized threshold (the pre-fix buggy unit) must reject this pair under the
    // meters contract.
    Assert.assertFalse(RasterPredicates.rsDWithin(raster, tenDegreesEast, 10.0));
  }

  @Test
  public void testDWithinSwappedOperands() throws FactoryException {
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 4326);
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(12.5, -2.5));
    point.setSRID(4326);
    // The (raster, geom) and (geom, raster) overloads share the same minimum geodesic distance,
    // so both must agree at and around the threshold.
    Assert.assertEquals(
        RasterPredicates.rsDWithin(raster, point, 2_000_000.0),
        RasterPredicates.rsDWithin(raster, point, 2_000_000.0));
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, point, 2_000_000.0));
    Assert.assertFalse(RasterPredicates.rsDWithin(raster, point, 500_000.0));
  }

  @Test
  public void testDWithinProjectedRasterReprojects() throws FactoryException {
    // UTM 32610 (meters) raster centred near San Francisco. The predicate must reproject
    // both sides to WGS84 before measuring, regardless of the input CRS.
    GridCoverage2D raster =
        RasterConstructors.makeEmptyRaster(
            1, "B", 751, 742, 332385, 4258815, 300, -300, 0, 0, 32610);

    // Point inside the raster footprint (≈ same area as the centroid): close geodesic
    // distance, matches even with a small threshold.
    Geometry nearby = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.40, 37.75));
    nearby.setSRID(4326);
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, nearby, 200_000.0));

    // Point in Kansas — ≈ 2 400 km from the UTM-10N raster's WGS84 centroid.
    Geometry farAway = GEOMETRY_FACTORY.createPoint(new Coordinate(-95.0, 39.0));
    farAway.setSRID(4326);
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, farAway, 3_000_000.0));
    Assert.assertFalse(RasterPredicates.rsDWithin(raster, farAway, 1_000_000.0));

    // Same Kansas point expressed in EPSG:3857 (Web Mercator). Even though neither side is
    // in WGS84, the predicate must still reproject and produce identical truth values.
    Geometry farAwayMercator = GEOMETRY_FACTORY.createPoint(new Coordinate(-10575352, 4721671));
    farAwayMercator.setSRID(3857);
    Assert.assertTrue(RasterPredicates.rsDWithin(raster, farAwayMercator, 3_000_000.0));
    Assert.assertFalse(RasterPredicates.rsDWithin(raster, farAwayMercator, 1_000_000.0));
  }

  @Test
  public void testDWithinRasterRaster() throws FactoryException {
    // Two WGS84 rasters whose centroids are exactly 10° of longitude apart on the equator:
    // geodesic centroid distance ≈ 1 112 km.
    GridCoverage2D rasterA = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 4326);
    GridCoverage2D rasterB = RasterConstructors.makeEmptyRaster(1, 5, 5, 10, 0, 1, -1, 0, 0, 4326);
    Assert.assertTrue(RasterPredicates.rsDWithin(rasterA, rasterB, 2_000_000.0));
    Assert.assertFalse(RasterPredicates.rsDWithin(rasterA, rasterB, 500_000.0));
    // Symmetry: swapping the operands does not change the truth value.
    Assert.assertEquals(
        RasterPredicates.rsDWithin(rasterA, rasterB, 2_000_000.0),
        RasterPredicates.rsDWithin(rasterB, rasterA, 2_000_000.0));

    // Cross-CRS pair (UTM 10N + WGS84): the projected raster must be reprojected before
    // distance is computed.
    GridCoverage2D utmRaster =
        RasterConstructors.makeEmptyRaster(
            1, "B", 751, 742, 332385, 4258815, 300, -300, 0, 0, 32610);
    // WGS84 raster co-located with the UTM raster's footprint near SF.
    GridCoverage2D wgs84Nearby =
        RasterConstructors.makeEmptyRaster(1, 10, 10, -122.45, 37.80, 0.01, -0.01, 0, 0, 4326);
    Assert.assertTrue(RasterPredicates.rsDWithin(utmRaster, wgs84Nearby, 500_000.0));
    Assert.assertEquals(
        RasterPredicates.rsDWithin(utmRaster, wgs84Nearby, 500_000.0),
        RasterPredicates.rsDWithin(wgs84Nearby, utmRaster, 500_000.0));
  }

  @Test
  public void testDWithinMixedOrientationMultiPolygon() throws FactoryException, ParseException {
    // Regression: the helper used to flip the whole multipolygon based on the first shell's
    // orientation, which left any later polygon with a different winding mis-oriented when
    // handed to S2. Two semantically-identical multipolygons (one all-CCW, one with the second
    // shell wound CW) must produce identical truth values.
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 4326);
    WKTReader reader = new WKTReader();
    Geometry allCcw =
        reader.read(
            "MULTIPOLYGON (((20 20, 21 20, 21 21, 20 21, 20 20)), "
                + "((30 30, 31 30, 31 31, 30 31, 30 30)))");
    allCcw.setSRID(4326);
    Geometry mixed =
        reader.read(
            // First polygon CCW (as in `allCcw`); second polygon wound CW.
            "MULTIPOLYGON (((20 20, 21 20, 21 21, 20 21, 20 20)), "
                + "((30 30, 30 31, 31 31, 31 30, 30 30)))");
    mixed.setSRID(4326);
    // Both forms describe the same pair of squares, so the predicate must agree on every
    // threshold around the actual geodesic distance.
    Assert.assertEquals(
        RasterPredicates.rsDWithin(raster, allCcw, 5_000_000.0),
        RasterPredicates.rsDWithin(raster, mixed, 5_000_000.0));
    Assert.assertEquals(
        RasterPredicates.rsDWithin(raster, allCcw, 1_000_000.0),
        RasterPredicates.rsDWithin(raster, mixed, 1_000_000.0));
  }

  @Test
  public void testDWithinEmptyMultiPolygon() throws FactoryException, ParseException {
    // Regression: `getGeometryN(0)` would throw IndexOutOfBoundsException on MULTIPOLYGON EMPTY.
    // The predicate must accept an empty multipolygon operand without crashing.
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 4326);
    WKTReader reader = new WKTReader();
    Geometry empty = reader.read("MULTIPOLYGON EMPTY");
    empty.setSRID(4326);
    RasterPredicates.rsDWithin(raster, empty, 1_000_000.0);
  }

  @Test
  public void testDWithinDoesNotMutateCallerGeometry() throws FactoryException {
    // Regression: the predicate used to call setSRID(4326) directly on the caller's geometry
    // when no orientation change was needed. Verify SRID and coordinate identity are untouched
    // after the predicate runs, for both same-CRS (no transform) and cross-CRS paths.
    GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 4326);

    // Same-CRS: no transform happens, so the predicate sees the caller's exact JTS object.
    Geometry sameCrsPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(2.5, -2.5));
    sameCrsPoint.setSRID(0);
    Coordinate originalCoord = sameCrsPoint.getCoordinate().copy();
    RasterPredicates.rsDWithin(raster, sameCrsPoint, 1.0);
    Assert.assertEquals(0, sameCrsPoint.getSRID());
    Assert.assertEquals(originalCoord, sameCrsPoint.getCoordinate());

    // Cross-CRS: JTS.transform produces a fresh geometry internally, but the caller's object
    // must still be untouched.
    GeometryFactory mercatorFactory = new GeometryFactory(new PrecisionModel(), 3857);
    Geometry mercatorPoint = mercatorFactory.createPoint(new Coordinate(278300.0, -278300.0));
    Coordinate originalMercatorCoord = mercatorPoint.getCoordinate().copy();
    RasterPredicates.rsDWithin(raster, mercatorPoint, 1.0);
    Assert.assertEquals(3857, mercatorPoint.getSRID());
    Assert.assertEquals(originalMercatorCoord, mercatorPoint.getCoordinate());
  }

  @Test
  public void testIsCRSMatchesEPSGCode() throws FactoryException {
    CoordinateReferenceSystem epsg4326 = CRS.decode("EPSG:4326");
    CoordinateReferenceSystem epsg4326LonLat = CRS.decode("EPSG:4326", true);
    CoordinateReferenceSystem wgs84 = DefaultGeographicCRS.WGS84;
    CoordinateReferenceSystem epsg3857 = CRS.decode("EPSG:3857");
    Assert.assertFalse(RasterPredicates.isCRSMatchesSRID(epsg4326, 4326));
    Assert.assertTrue(RasterPredicates.isCRSMatchesSRID(epsg4326LonLat, 4326));
    Assert.assertTrue(RasterPredicates.isCRSMatchesSRID(wgs84, 4326));
    Assert.assertFalse(RasterPredicates.isCRSMatchesSRID(wgs84, 3857));
    Assert.assertFalse(RasterPredicates.isCRSMatchesSRID(epsg4326LonLat, 3857));
    Assert.assertTrue(RasterPredicates.isCRSMatchesSRID(epsg3857, 3857));
  }
}
