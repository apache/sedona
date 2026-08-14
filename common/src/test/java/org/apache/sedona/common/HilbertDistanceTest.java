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
package org.apache.sedona.common;

import static org.apache.sedona.common.utils.HilbertDistance.compute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class HilbertDistanceTest {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @Test
  public void encodesCornerAddressesAndUnsignedMaximum() {
    assertEquals(0L, distance(0.0, 0.0, 2));
    assertEquals(5L, distance(0.0, 1.0, 2));
    assertEquals(10L, distance(1.0, 1.0, 2));
    assertEquals(15L, distance(1.0, 0.0, 2));
    assertEquals(4294967295L, distance(1.0, 0.0, 16));
  }

  @Test
  public void usesTheGeometryEnvelopeMidpointAndTruncatesScaledCoordinates() {
    Geometry line =
        GEOMETRY_FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0.0, 0.0), new Coordinate(2.0, 2.0)});

    assertEquals(2L, compute(line, 0.0, 0.0, 2.0, 2.0, 2));
  }

  @Test
  public void clipsCoordinatesOutsideTheExtent() {
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(2.0, -1.0));

    assertEquals(15L, compute(point, 0.0, 0.0, 1.0, 1.0, 2));
  }

  @Test
  public void mapsZeroWidthAndNanAxesToZero() {
    Geometry zeroWidthPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(5.0, 1.0));
    assertEquals(5L, compute(zeroWidthPoint, 5.0, 0.0, 5.0, 1.0, 2));

    Geometry nanBoundsPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(0.5, 0.5));
    assertEquals(3L, compute(nanBoundsPoint, Double.NaN, 0.0, Double.NaN, 1.0, 2));
  }

  @Test
  public void mapsNonPositiveLevelsToZero() {
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 0.0));

    assertEquals(0L, compute(point, 0.0, 0.0, 1.0, 1.0, 0));
    assertEquals(0L, compute(point, 0.0, 0.0, 1.0, 1.0, -4));
  }

  @Test
  public void rejectsEmptyGeometryBeforeCollapsingNonPositiveLevels() {
    Geometry emptyPoint = GEOMETRY_FACTORY.createPoint();
    try {
      compute(emptyPoint, 0.0, 0.0, 1.0, 1.0, 0);
      fail("Expected empty geometry to be rejected");
    } catch (IllegalArgumentException exception) {
      assertEquals(
          "Hilbert distance cannot be computed for an empty geometry", exception.getMessage());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLevelsAboveSixteen() {
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(0.0, 0.0));
    compute(point, 0.0, 0.0, 1.0, 1.0, 17);
  }

  private static long distance(double x, double y, int level) {
    Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    return compute(point, 0.0, 0.0, 1.0, 1.0, level);
  }
}
