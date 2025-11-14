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
package org.apache.sedona.common.jts2geojson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.wololo.geojson.Point;

public class GeoJSONReaderTest {

  @Test
  public void testPointConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[] coords = new double[] {1.0, 2.0};
    Point point = new Point(coords);
    Geometry geometry = reader.read(point);
    assertEquals(1.0, geometry.getCoordinate().x, 0.0);
    assertEquals(2.0, geometry.getCoordinate().y, 0.0);
  }

  @Test
  public void testMultiPointConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][] coords = new double[][] {{1.0, 2.0}, {3.0, 4.0}};
    org.wololo.geojson.MultiPoint multiPoint = new org.wololo.geojson.MultiPoint(coords);
    Geometry geometry = reader.read(multiPoint);
    assertEquals(1.0, geometry.getCoordinates()[0].x, 0.0);
    assertEquals(2.0, geometry.getCoordinates()[0].y, 0.0);
    assertEquals(3.0, geometry.getCoordinates()[1].x, 0.0);
    assertEquals(4.0, geometry.getCoordinates()[1].y, 0.0);
  }

  @Test
  public void testPointWithZConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[] coords = new double[] {1.0, 2.0, 3.0};
    Point point = new Point(coords);
    Geometry geometry = reader.read(point);
    assertEquals(1.0, geometry.getCoordinate().x, 0.0);
    assertEquals(2.0, geometry.getCoordinate().y, 0.0);
    assertEquals(3.0, geometry.getCoordinate().getZ(), 0.0);
  }

  @Test
  public void testMultiPointWithZConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][] coords = new double[][] {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
    org.wololo.geojson.MultiPoint multiPoint = new org.wololo.geojson.MultiPoint(coords);
    Geometry geometry = reader.read(multiPoint);
    assertEquals(1.0, geometry.getCoordinates()[0].x, 0.0);
    assertEquals(2.0, geometry.getCoordinates()[0].y, 0.0);
    assertEquals(3.0, geometry.getCoordinates()[0].getZ(), 0.0);
    assertEquals(4.0, geometry.getCoordinates()[1].x, 0.0);
    assertEquals(5.0, geometry.getCoordinates()[1].y, 0.0);
    assertEquals(6.0, geometry.getCoordinates()[1].getZ(), 0.0);
  }

  @Test
  public void testPointWithXYZMConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[] coords = new double[] {1.0, 2.0, 3.0, 4.0};
    Point point = new Point(coords);
    Geometry geometry = reader.read(point);

    assertEquals(1.0, geometry.getCoordinate().x, 0.0);
    assertEquals(2.0, geometry.getCoordinate().y, 0.0);
    assertEquals(3.0, geometry.getCoordinate().getZ(), 0.0);
    assertEquals(4.0, geometry.getCoordinate().getM(), 0.0);

    // Verify the coordinate is an XYZM coordinate
    assertTrue(geometry.getCoordinate() instanceof CoordinateXYZM);
  }

  @Test
  public void testMultiPointWithXYZMConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][] coords = new double[][] {{1.0, 2.0, 3.0, 4.0}, {5.0, 6.0, 7.0, 8.0}};
    org.wololo.geojson.MultiPoint multiPoint = new org.wololo.geojson.MultiPoint(coords);
    Geometry geometry = reader.read(multiPoint);

    assertEquals(1.0, geometry.getCoordinates()[0].x, 0.0);
    assertEquals(2.0, geometry.getCoordinates()[0].y, 0.0);
    assertEquals(3.0, geometry.getCoordinates()[0].getZ(), 0.0);
    assertEquals(4.0, geometry.getCoordinates()[0].getM(), 0.0);

    assertEquals(5.0, geometry.getCoordinates()[1].x, 0.0);
    assertEquals(6.0, geometry.getCoordinates()[1].y, 0.0);
    assertEquals(7.0, geometry.getCoordinates()[1].getZ(), 0.0);
    assertEquals(8.0, geometry.getCoordinates()[1].getM(), 0.0);

    // Verify the coordinates are XYZM coordinates
    assertTrue(geometry.getCoordinates()[0] instanceof CoordinateXYZM);
    assertTrue(geometry.getCoordinates()[1] instanceof CoordinateXYZM);
  }

  @Test
  public void testLineStringConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][] coords = new double[][] {{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}};
    org.wololo.geojson.LineString lineString = new org.wololo.geojson.LineString(coords);
    Geometry geometry = reader.read(lineString);

    assertEquals("LineString", geometry.getGeometryType());
    assertEquals(3, geometry.getCoordinates().length);
    assertEquals(1.0, geometry.getCoordinates()[0].x, 0.0);
    assertEquals(2.0, geometry.getCoordinates()[0].y, 0.0);
    assertEquals(3.0, geometry.getCoordinates()[1].x, 0.0);
    assertEquals(4.0, geometry.getCoordinates()[1].y, 0.0);
    assertEquals(5.0, geometry.getCoordinates()[2].x, 0.0);
    assertEquals(6.0, geometry.getCoordinates()[2].y, 0.0);
  }

  @Test
  public void testLineStringWithZConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][] coords = new double[][] {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}};
    org.wololo.geojson.LineString lineString = new org.wololo.geojson.LineString(coords);
    Geometry geometry = reader.read(lineString);

    assertEquals("LineString", geometry.getGeometryType());
    assertEquals(3, geometry.getCoordinates().length);
    assertEquals(1.0, geometry.getCoordinates()[0].x, 0.0);
    assertEquals(2.0, geometry.getCoordinates()[0].y, 0.0);
    assertEquals(3.0, geometry.getCoordinates()[0].getZ(), 0.0);
    assertEquals(4.0, geometry.getCoordinates()[1].x, 0.0);
    assertEquals(5.0, geometry.getCoordinates()[1].y, 0.0);
    assertEquals(6.0, geometry.getCoordinates()[1].getZ(), 0.0);
    assertEquals(7.0, geometry.getCoordinates()[2].x, 0.0);
    assertEquals(8.0, geometry.getCoordinates()[2].y, 0.0);
    assertEquals(9.0, geometry.getCoordinates()[2].getZ(), 0.0);
  }

  @Test
  public void testPolygonConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    // Polygon with one exterior ring
    double[][][] coords =
        new double[][][] {{{0.0, 0.0}, {0.0, 10.0}, {10.0, 10.0}, {10.0, 0.0}, {0.0, 0.0}}};
    org.wololo.geojson.Polygon polygon = new org.wololo.geojson.Polygon(coords);
    Geometry geometry = reader.read(polygon);

    assertEquals("Polygon", geometry.getGeometryType());
    assertEquals(5, geometry.getCoordinates().length);
    assertEquals(0.0, geometry.getCoordinates()[0].x, 0.0);
    assertEquals(0.0, geometry.getCoordinates()[0].y, 0.0);
    assertEquals(0.0, geometry.getCoordinates()[1].x, 0.0);
    assertEquals(10.0, geometry.getCoordinates()[1].y, 0.0);
  }

  @Test
  public void testPolygonWithHoleConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    // Polygon with exterior ring and one hole
    double[][][] coords =
        new double[][][] {
          {{0.0, 0.0}, {0.0, 10.0}, {10.0, 10.0}, {10.0, 0.0}, {0.0, 0.0}},
          {{2.0, 2.0}, {2.0, 8.0}, {8.0, 8.0}, {8.0, 2.0}, {2.0, 2.0}}
        };
    org.wololo.geojson.Polygon polygon = new org.wololo.geojson.Polygon(coords);
    Geometry geometry = reader.read(polygon);

    assertEquals("Polygon", geometry.getGeometryType());
    assertEquals(1, geometry.getNumGeometries());
    assertEquals(1, ((org.locationtech.jts.geom.Polygon) geometry).getNumInteriorRing());

    // Check exterior ring
    assertEquals(
        5,
        ((org.locationtech.jts.geom.Polygon) geometry).getExteriorRing().getCoordinates().length);

    // Check interior ring
    assertEquals(
        5,
        ((org.locationtech.jts.geom.Polygon) geometry).getInteriorRingN(0).getCoordinates().length);
    assertEquals(
        2.0,
        ((org.locationtech.jts.geom.Polygon) geometry).getInteriorRingN(0).getCoordinates()[0].x,
        0.0);
    assertEquals(
        2.0,
        ((org.locationtech.jts.geom.Polygon) geometry).getInteriorRingN(0).getCoordinates()[0].y,
        0.0);
  }

  @Test
  public void testMultiLineStringConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][][] coords =
        new double[][][] {
          {{1.0, 2.0}, {3.0, 4.0}},
          {{5.0, 6.0}, {7.0, 8.0}}
        };
    org.wololo.geojson.MultiLineString multiLineString =
        new org.wololo.geojson.MultiLineString(coords);
    Geometry geometry = reader.read(multiLineString);

    assertEquals("MultiLineString", geometry.getGeometryType());
    assertEquals(2, geometry.getNumGeometries());

    // Check first linestring
    assertEquals(2, geometry.getGeometryN(0).getCoordinates().length);
    assertEquals(1.0, geometry.getGeometryN(0).getCoordinates()[0].x, 0.0);
    assertEquals(2.0, geometry.getGeometryN(0).getCoordinates()[0].y, 0.0);

    // Check second linestring
    assertEquals(2, geometry.getGeometryN(1).getCoordinates().length);
    assertEquals(5.0, geometry.getGeometryN(1).getCoordinates()[0].x, 0.0);
    assertEquals(6.0, geometry.getGeometryN(1).getCoordinates()[0].y, 0.0);
  }

  @Test
  public void testMultiPolygonConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    double[][][][] coords =
        new double[][][][] {
          // First polygon
          {{{0.0, 0.0}, {0.0, 5.0}, {5.0, 5.0}, {5.0, 0.0}, {0.0, 0.0}}},
          // Second polygon
          {{{10.0, 10.0}, {10.0, 15.0}, {15.0, 15.0}, {15.0, 10.0}, {10.0, 10.0}}}
        };
    org.wololo.geojson.MultiPolygon multiPolygon = new org.wololo.geojson.MultiPolygon(coords);
    Geometry geometry = reader.read(multiPolygon);

    assertEquals("MultiPolygon", geometry.getGeometryType());
    assertEquals(2, geometry.getNumGeometries());

    // Check first polygon
    assertEquals(5, geometry.getGeometryN(0).getCoordinates().length);
    assertEquals(0.0, geometry.getGeometryN(0).getCoordinates()[0].x, 0.0);
    assertEquals(0.0, geometry.getGeometryN(0).getCoordinates()[0].y, 0.0);

    // Check second polygon
    assertEquals(5, geometry.getGeometryN(1).getCoordinates().length);
    assertEquals(10.0, geometry.getGeometryN(1).getCoordinates()[0].x, 0.0);
    assertEquals(10.0, geometry.getGeometryN(1).getCoordinates()[0].y, 0.0);
  }

  @Test
  public void testGeometryCollectionConversion() {
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader = new GeoJSONReader();

    // Create a Point
    double[] pointCoords = new double[] {1.0, 2.0};
    org.wololo.geojson.Point point = new org.wololo.geojson.Point(pointCoords);

    // Create a LineString
    double[][] lineCoords = new double[][] {{3.0, 4.0}, {5.0, 6.0}};
    org.wololo.geojson.LineString lineString = new org.wololo.geojson.LineString(lineCoords);

    // Create a GeometryCollection with these geometries
    org.wololo.geojson.Geometry[] geometries =
        new org.wololo.geojson.Geometry[] {point, lineString};
    org.wololo.geojson.GeometryCollection geometryCollection =
        new org.wololo.geojson.GeometryCollection(geometries);

    Geometry geometry = reader.read(geometryCollection);

    assertEquals("GeometryCollection", geometry.getGeometryType());
    assertEquals(2, geometry.getNumGeometries());

    // Check first geometry (point)
    assertEquals("Point", geometry.getGeometryN(0).getGeometryType());
    assertEquals(1.0, geometry.getGeometryN(0).getCoordinate().x, 0.0);
    assertEquals(2.0, geometry.getGeometryN(0).getCoordinate().y, 0.0);

    // Check second geometry (linestring)
    assertEquals("LineString", geometry.getGeometryN(1).getGeometryType());
    assertEquals(3.0, geometry.getGeometryN(1).getCoordinates()[0].x, 0.0);
    assertEquals(4.0, geometry.getGeometryN(1).getCoordinates()[0].y, 0.0);
  }
}
