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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

public class PolygonSerdeTest {
  private static final GeometryFactory gf = new GeometryFactory();

  @Test
  public void testEmptyPolygon() {
    Polygon polygon = gf.createPolygon();
    polygon.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(polygon);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Polygon);
    Assert.assertTrue(geom.isEmpty());
    Assert.assertEquals(4326, geom.getSRID());
  }

  @Test
  public void testPolygonWithoutHoles() {
    LinearRing shell =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(0, 1),
              new Coordinate(1, 1),
              new Coordinate(1, 0),
              new Coordinate(0, 0)
            });
    Polygon polygon = gf.createPolygon(shell);
    polygon.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(polygon);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Polygon);
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(polygon, geom);
  }

  @Test
  public void testPolygonWithHoles() {
    LinearRing shell =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(0, 1),
              new Coordinate(1, 1),
              new Coordinate(1, 0),
              new Coordinate(0, 0)
            });
    LinearRing hole1 =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.1, 0.1),
              new Coordinate(0.1, 0.2),
              new Coordinate(0.2, 0.2),
              new Coordinate(0.2, 0.1),
              new Coordinate(0.1, 0.1)
            });
    LinearRing hole2 =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.3, 0.3),
              new Coordinate(0.3, 0.4),
              new Coordinate(0.4, 0.4),
              new Coordinate(0.4, 0.3),
              new Coordinate(0.3, 0.3)
            });
    Polygon polygon = gf.createPolygon(shell, new LinearRing[] {hole1, hole2});
    polygon.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(polygon);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof Polygon);
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(polygon, geom);
  }
}
