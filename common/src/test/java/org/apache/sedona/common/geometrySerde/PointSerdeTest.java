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
package org.apache.sedona.common.geometrySerde;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class PointSerdeTest {
  private static final GeometryFactory gf = new GeometryFactory();

  @Test
  public void testEmptyPoint() {
    byte[] bytes = GeometrySerializer.serialize(gf.createPoint());
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Point);
    Assert.assertTrue(geom.isEmpty());
    Assert.assertEquals(0, geom.getSRID());
  }

  @Test
  public void testEmptyPointWithSRID() {
    Point point = gf.createPoint();
    point.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(point);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Point);
    Assert.assertTrue(geom.isEmpty());
    Assert.assertEquals(4326, geom.getSRID());
  }

  @Test
  public void test2DPoint() {
    Point point = gf.createPoint(new Coordinate(1.0, 2.0));
    point.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(point);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Point);
    Assert.assertFalse(geom.isEmpty());
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(1.0, geom.getCoordinate().x, 1e-6);
    Assert.assertEquals(2.0, geom.getCoordinate().y, 1e-6);
    Assert.assertTrue(Double.isNaN(geom.getCoordinate().getZ()));
    Assert.assertTrue(Double.isNaN(geom.getCoordinate().getM()));
  }

  @Test
  public void testXYZPoint() {
    Point point = gf.createPoint(new Coordinate(1.0, 2.0, 3.0));
    point.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(point);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Point);
    Assert.assertFalse(geom.isEmpty());
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(1.0, geom.getCoordinate().x, 1e-6);
    Assert.assertEquals(2.0, geom.getCoordinate().y, 1e-6);
    Assert.assertEquals(3.0, geom.getCoordinate().getZ(), 1e-6);
    Assert.assertTrue(Double.isNaN(geom.getCoordinate().getM()));
  }

  @Test
  public void testXYMPoint() {
    Point point = gf.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    byte[] bytes = GeometrySerializer.serialize(point);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Point);
    Assert.assertFalse(geom.isEmpty());
    Assert.assertEquals(0, geom.getSRID());
    Assert.assertEquals(1.0, geom.getCoordinate().x, 1e-6);
    Assert.assertEquals(2.0, geom.getCoordinate().y, 1e-6);
    Assert.assertTrue(Double.isNaN(geom.getCoordinate().getZ()));
    Assert.assertEquals(3.0, geom.getCoordinate().getM(), 1e-6);
  }

  @Test
  public void testXYZMPoint() {
    Point point = gf.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    point.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(point);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Point);
    Assert.assertFalse(geom.isEmpty());
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(1.0, geom.getCoordinate().x, 1e-6);
    Assert.assertEquals(2.0, geom.getCoordinate().y, 1e-6);
    Assert.assertEquals(3.0, geom.getCoordinate().getZ(), 1e-6);
    Assert.assertEquals(4.0, geom.getCoordinate().getM(), 1e-6);
  }
}
