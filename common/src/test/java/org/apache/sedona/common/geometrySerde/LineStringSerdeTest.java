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
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

public class LineStringSerdeTest {
  private static final GeometryFactory gf = new GeometryFactory();

  @Test
  public void testEmptyLineString() {
    LineString lineString = gf.createLineString();
    lineString.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(lineString);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof LineString);
    Assert.assertTrue(geom.isEmpty());
    Assert.assertEquals(4326, geom.getSRID());
  }

  @Test
  public void testLineString() {
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1.0, 2.0), new Coordinate(3.0, 4.0), new Coordinate(5.0, 6.0),
        };
    LineString lineString = gf.createLineString(coordinates);
    lineString.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(lineString);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof LineString);
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(3, geom.getNumPoints());
    CoordinateSequence coordSeq = ((LineString) geom).getCoordinateSequence();
    Coordinate coord = coordSeq.getCoordinate(0);
    Assert.assertEquals(1.0, coord.x, 1e-6);
    Assert.assertEquals(2.0, coord.y, 1e-6);
    Assert.assertTrue(Double.isNaN(coord.getZ()));
    Assert.assertTrue(Double.isNaN(coord.getM()));
    coord = coordSeq.getCoordinate(1);
    Assert.assertEquals(3.0, coord.x, 1e-6);
    Assert.assertEquals(4.0, coord.y, 1e-6);
    coord = coordSeq.getCoordinate(2);
    Assert.assertEquals(5.0, coord.x, 1e-6);
    Assert.assertEquals(6.0, coord.y, 1e-6);
  }

  @Test
  public void testLineStringXYZ() {
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1.0, 2.0, 3.0), new Coordinate(4.0, 5.0, 6.0),
        };
    LineString lineString = gf.createLineString(coordinates);
    lineString.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(lineString);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof LineString);
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(2, geom.getNumPoints());
    CoordinateSequence coordSeq = ((LineString) geom).getCoordinateSequence();
    Coordinate coord = coordSeq.getCoordinate(0);
    Assert.assertEquals(1.0, coord.x, 1e-6);
    Assert.assertEquals(2.0, coord.y, 1e-6);
    Assert.assertEquals(3.0, coord.getZ(), 1e-6);
    Assert.assertTrue(Double.isNaN(coord.getM()));
    coord = coordSeq.getCoordinate(1);
    Assert.assertEquals(4.0, coord.x, 1e-6);
    Assert.assertEquals(5.0, coord.y, 1e-6);
    Assert.assertEquals(6.0, coord.getZ(), 1e-6);
  }

  @Test
  public void testLineStringXYM() {
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYM(1.0, 2.0, 3.0), new CoordinateXYM(4.0, 5.0, 6.0),
        };
    LineString lineString = gf.createLineString(coordinates);
    lineString.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(lineString);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof LineString);
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(2, geom.getNumPoints());
    CoordinateSequence coordSeq = ((LineString) geom).getCoordinateSequence();
    Coordinate coord = coordSeq.getCoordinate(0);
    Assert.assertEquals(1.0, coord.x, 1e-6);
    Assert.assertEquals(2.0, coord.y, 1e-6);
    Assert.assertTrue(Double.isNaN(coord.getZ()));
    Assert.assertEquals(3.0, coord.getM(), 1e-6);
    coord = coordSeq.getCoordinate(1);
    Assert.assertEquals(4.0, coord.x, 1e-6);
    Assert.assertEquals(5.0, coord.y, 1e-6);
    Assert.assertTrue(Double.isNaN(coord.getZ()));
    Assert.assertEquals(6.0, coord.getM(), 1e-6);
  }

  @Test
  public void testLineStringXYZM() {
    Coordinate[] coordinates =
        new Coordinate[] {
          new CoordinateXYZM(1.0, 2.0, 3.0, 4.0), new CoordinateXYZM(5.0, 6.0, 7.0, 8.0),
        };
    LineString lineString = gf.createLineString(coordinates);
    lineString.setSRID(4326);
    byte[] bytes = GeometrySerializer.serialize(lineString);
    Geometry geom = GeometrySerializer.deserialize(bytes);
    Assert.assertTrue(geom instanceof LineString);
    Assert.assertEquals(4326, geom.getSRID());
    Assert.assertEquals(2, geom.getNumPoints());
    CoordinateSequence coordSeq = ((LineString) geom).getCoordinateSequence();
    Coordinate coord = coordSeq.getCoordinate(0);
    Assert.assertEquals(1.0, coord.x, 1e-6);
    Assert.assertEquals(2.0, coord.y, 1e-6);
    Assert.assertEquals(3.0, coord.getZ(), 1e-6);
    Assert.assertEquals(4.0, coord.getM(), 1e-6);
    coord = coordSeq.getCoordinate(1);
    Assert.assertEquals(5.0, coord.x, 1e-6);
    Assert.assertEquals(6.0, coord.y, 1e-6);
    Assert.assertEquals(7.0, coord.getZ(), 1e-6);
    Assert.assertEquals(8.0, coord.getM(), 1e-6);
  }
}
