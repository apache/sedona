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
import static org.junit.Assert.fail;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;

public class GeoJSONWriterTest {

  @Test
  public void testPointConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Point point = factory.createPoint(new Coordinate(1, 1));
    String expected = "{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(point);
    assertEquals("Point should be correctly converted to GeoJSON", expected, json.toString());

    org.locationtech.jts.geom.Geometry geometry = reader.read(json);
    assertEquals(
        "GeoJSON should be correctly converted back to geometry",
        "POINT (1 1)",
        geometry.toString());
  }

  @Test
  public void testLineStringConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader =
        new org.apache.sedona.common.jts2geojson.GeoJSONReader();
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    String expected =
        "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(lineString);
    assertEquals("LineString should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testLineStringWithIdConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    String expected =
        "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(lineString);
    assertEquals(
        "LineString with ID should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testLinearRingConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    LinearRing ring = factory.createLinearRing(coordinates);
    String expected =
        "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(ring);
    assertEquals("LinearRing should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testPolygonConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    Polygon polygon = factory.createPolygon(coordinates);
    String expected =
        "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(polygon);
    assertEquals("Polygon should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiPointConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    MultiPoint multiPoint = factory.createMultiPointFromCoords(lineString.getCoordinates());
    String expected =
        "{\"type\":\"MultiPoint\",\"coordinates\":[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiPoint);
    assertEquals("MultiPoint should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiLineStringConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    MultiLineString multiLineString =
        factory.createMultiLineString(new LineString[] {lineString, lineString});
    String expected =
        "{\"type\":\"MultiLineString\",\"coordinates\":[[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]],[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiLineString);
    assertEquals(
        "MultiLineString should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiPolygonConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1), new Coordinate(1, 2), new Coordinate(2, 2), new Coordinate(1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    Polygon polygon = factory.createPolygon(lineString.getCoordinates());
    MultiPolygon multiPolygon = factory.createMultiPolygon(new Polygon[] {polygon, polygon});
    String expected =
        "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]],[[[1.0,1.0],[1.0,2.0],[2.0,2.0],[1.0,1.0]]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiPolygon);
    assertEquals(
        "MultiPolygon should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testFeatureCollectionConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Point point = factory.createPoint(new Coordinate(1, 1));
    org.wololo.geojson.Geometry json = writer.write(point);
    Feature feature1 = new Feature(json, null);
    Feature feature2 = new Feature(json, null);
    FeatureCollection featureCollection = new FeatureCollection(new Feature[] {feature1, feature2});
    String expected =
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,1.0]},\"properties\":null},{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,1.0]},\"properties\":null}]}";

    // Test
    assertEquals(
        "FeatureCollection should be correctly converted to GeoJSON",
        expected,
        featureCollection.toString());
  }

  // 3D Tests

  @Test
  public void testPointWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONReader reader = new GeoJSONReader();
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Point point = factory.createPoint(new Coordinate(1, 1, 1));
    String expected = "{\"type\":\"Point\",\"coordinates\":[1.0,1.0,1.0]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(point);
    assertEquals("3D Point should be correctly converted to GeoJSON", expected, json.toString());

    org.locationtech.jts.geom.Geometry geometry = reader.read(json);
    assertEquals(
        "GeoJSON should be correctly converted back to geometry",
        "POINT (1 1)",
        geometry.toString());
  }

  @Test
  public void testLineStringWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1, 1),
          new Coordinate(1, 2, 1),
          new Coordinate(2, 2, 2),
          new Coordinate(1, 1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    String expected =
        "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(lineString);
    assertEquals(
        "3D LineString should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testPolygonWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1, 1),
          new Coordinate(1, 2, 1),
          new Coordinate(2, 2, 2),
          new Coordinate(1, 1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    Polygon polygon = factory.createPolygon(lineString.getCoordinates());
    String expected =
        "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(polygon);
    assertEquals("3D Polygon should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiPointWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1, 1),
          new Coordinate(1, 2, 1),
          new Coordinate(2, 2, 2),
          new Coordinate(1, 1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    MultiPoint multiPoint = factory.createMultiPointFromCoords(lineString.getCoordinates());
    String expected =
        "{\"type\":\"MultiPoint\",\"coordinates\":[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiPoint);
    assertEquals(
        "3D MultiPoint should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiLineStringWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1, 1),
          new Coordinate(1, 2, 1),
          new Coordinate(2, 2, 2),
          new Coordinate(1, 1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    MultiLineString multiLineString =
        factory.createMultiLineString(new LineString[] {lineString, lineString});
    String expected =
        "{\"type\":\"MultiLineString\",\"coordinates\":[[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]],[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiLineString);
    assertEquals(
        "3D MultiLineString should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiPolygonWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1, 1, 1),
          new Coordinate(1, 2, 1),
          new Coordinate(2, 2, 2),
          new Coordinate(1, 1, 1)
        };
    LineString lineString = factory.createLineString(coordinates);
    Polygon polygon = factory.createPolygon(lineString.getCoordinates());
    MultiPolygon multiPolygon = factory.createMultiPolygon(new Polygon[] {polygon, polygon});
    String expected =
        "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]]],[[[1.0,1.0,1.0],[1.0,2.0,1.0],[2.0,2.0,2.0],[1.0,1.0,1.0]]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiPolygon);
    assertEquals(
        "3D MultiPolygon should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testFeatureCollectionWithZConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Point point = factory.createPoint(new Coordinate(1, 1, 1));
    org.wololo.geojson.Geometry json = writer.write(point);
    Feature feature1 = new Feature(json, null);
    Feature feature2 = new Feature(json, null);
    FeatureCollection featureCollection = new FeatureCollection(new Feature[] {feature1, feature2});
    String expected =
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,1.0,1.0]},\"properties\":null},{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,1.0,1.0]},\"properties\":null}]}";

    // Test
    assertEquals(
        "3D FeatureCollection should be correctly converted to GeoJSON",
        expected,
        featureCollection.toString());
  }

  // XYZM Tests

  @Test
  public void testPointWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Point point = factory.createPoint(new CoordinateXYZM(1, 1, 1, 2));
    String expected = "{\"type\":\"Point\",\"coordinates\":[1.0,1.0,1.0,2.0]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(point);
    assertEquals("XYZM Point should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testLineStringWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYZM(1, 1, 1, 10),
          new CoordinateXYZM(1, 2, 1, 20),
          new CoordinateXYZM(2, 2, 2, 30),
          new CoordinateXYZM(1, 1, 1, 10)
        };
    LineString lineString = factory.createLineString(coordinates);
    String expected =
        "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0,1.0,10.0],[1.0,2.0,1.0,20.0],[2.0,2.0,2.0,30.0],[1.0,1.0,1.0,10.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(lineString);
    assertEquals(
        "XYZM LineString should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testPolygonWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYZM(1, 1, 1, 10),
          new CoordinateXYZM(1, 2, 1, 20),
          new CoordinateXYZM(2, 2, 2, 30),
          new CoordinateXYZM(1, 1, 1, 10)
        };
    Polygon polygon = factory.createPolygon(coordinates);
    String expected =
        "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0,1.0,10.0],[1.0,2.0,1.0,20.0],[2.0,2.0,2.0,30.0],[1.0,1.0,1.0,10.0]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(polygon);
    assertEquals(
        "XYZM Polygon should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiPointWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYZM(1, 1, 1, 10),
          new CoordinateXYZM(1, 2, 1, 20),
          new CoordinateXYZM(2, 2, 2, 30)
        };
    MultiPoint multiPoint = factory.createMultiPointFromCoords(coordinates);
    String expected =
        "{\"type\":\"MultiPoint\",\"coordinates\":[[1.0,1.0,1.0,10.0],[1.0,2.0,1.0,20.0],[2.0,2.0,2.0,30.0]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiPoint);
    assertEquals(
        "XYZM MultiPoint should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiLineStringWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates1 =
        new Coordinate[] {new CoordinateXYZM(1, 1, 1, 10), new CoordinateXYZM(1, 2, 1, 20)};
    Coordinate[] coordinates2 =
        new Coordinate[] {new CoordinateXYZM(2, 2, 2, 30), new CoordinateXYZM(3, 3, 3, 40)};
    LineString lineString1 = factory.createLineString(coordinates1);
    LineString lineString2 = factory.createLineString(coordinates2);
    MultiLineString multiLineString =
        factory.createMultiLineString(new LineString[] {lineString1, lineString2});
    String expected =
        "{\"type\":\"MultiLineString\",\"coordinates\":[[[1.0,1.0,1.0,10.0],[1.0,2.0,1.0,20.0]],[[2.0,2.0,2.0,30.0],[3.0,3.0,3.0,40.0]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiLineString);
    assertEquals(
        "XYZM MultiLineString should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testMultiPolygonWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYZM(1, 1, 1, 10),
          new CoordinateXYZM(1, 2, 1, 20),
          new CoordinateXYZM(2, 2, 2, 30),
          new CoordinateXYZM(1, 1, 1, 10)
        };
    Polygon polygon = factory.createPolygon(coordinates);
    MultiPolygon multiPolygon = factory.createMultiPolygon(new Polygon[] {polygon, polygon});
    String expected =
        "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[1.0,1.0,1.0,10.0],[1.0,2.0,1.0,20.0],[2.0,2.0,2.0,30.0],[1.0,1.0,1.0,10.0]]],[[[1.0,1.0,1.0,10.0],[1.0,2.0,1.0,20.0],[2.0,2.0,2.0,30.0],[1.0,1.0,1.0,10.0]]]]}";

    // Test
    org.wololo.geojson.Geometry json = writer.write(multiPolygon);
    assertEquals(
        "XYZM MultiPolygon should be correctly converted to GeoJSON", expected, json.toString());
  }

  @Test
  public void testFeatureCollectionWithZMConversion() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data
    Point point = factory.createPoint(new CoordinateXYZM(1, 1, 1, 10));
    org.wololo.geojson.Geometry json = writer.write(point);
    Feature feature1 = new Feature(json, null);
    Feature feature2 = new Feature(json, null);
    FeatureCollection featureCollection = new FeatureCollection(new Feature[] {feature1, feature2});
    String expected =
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,1.0,1.0,10.0]},\"properties\":null},{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1.0,1.0,1.0,10.0]},\"properties\":null}]}";

    // Test
    assertEquals(
        "XYZM FeatureCollection should be correctly converted to GeoJSON",
        expected,
        featureCollection.toString());
  }

  // XYM Tests - These should fail as XYM is not supported

  @Test
  public void testPointWithMConversionFails() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data - XYM coordinate
    Point point = factory.createPoint(new CoordinateXYM(1, 1, 10));

    // Test - should throw UnsupportedOperationException
    try {
      writer.write(point);
      fail("XYM coordinates should not be supported");
    } catch (UnsupportedOperationException e) {
      assertEquals(
          "XYM coordinates are not supported. Please convert to XYZM coordinates by adding a Z value.",
          e.getMessage());
    }
  }

  @Test
  public void testLineStringWithMConversionFails() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer =
        new org.apache.sedona.common.jts2geojson.GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data - XYM coordinates
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYM(1, 1, 10), new CoordinateXYM(1, 2, 20), new CoordinateXYM(2, 2, 30)
        };
    LineString lineString = factory.createLineString(coordinates);

    // Test - should throw UnsupportedOperationException
    try {
      writer.write(lineString);
      fail("XYM coordinates should not be supported");
    } catch (UnsupportedOperationException e) {
      assertEquals(
          "XYM coordinates are not supported. Please convert to XYZM coordinates by adding a Z value.",
          e.getMessage());
    }
  }

  @Test
  public void testPolygonWithMConversionFails() {
    // Setup
    org.apache.sedona.common.jts2geojson.GeoJSONWriter writer = new GeoJSONWriter();
    GeometryFactory factory = new GeometryFactory();

    // Test data - XYM coordinates
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYM(1, 1, 10),
          new CoordinateXYM(1, 2, 20),
          new CoordinateXYM(2, 2, 30),
          new CoordinateXYM(1, 1, 10)
        };
    Polygon polygon = factory.createPolygon(coordinates);

    // Test - should throw UnsupportedOperationException
    try {
      writer.write(polygon);
      fail("XYM coordinates should not be supported");
    } catch (UnsupportedOperationException e) {
      assertEquals(
          "XYM coordinates are not supported. Please convert to XYZM coordinates by adding a Z value.",
          e.getMessage());
    }
  }
}
