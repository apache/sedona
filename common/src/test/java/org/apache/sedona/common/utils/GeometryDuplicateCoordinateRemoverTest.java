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
package org.apache.sedona.common.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class GeometryDuplicateCoordinateRemoverTest {
  private static final GeometryFactory FACTORY = new GeometryFactory();

  private static class CountingCoordinate extends Coordinate {
    static long comparisons;

    CountingCoordinate(double x, double y) {
      super(x, y);
    }

    @Override
    public double distance(Coordinate other) {
      comparisons++;
      return super.distance(other);
    }
  }

  @Test
  public void narrowPointSetDoesNotCompareEveryPair() {
    for (int size : new int[] {10000, 20000}) {
      Coordinate[] points = new Coordinate[size];
      for (int i = 0; i < size; i++) points[i] = new CountingCoordinate(0, 2.0 * i);
      GeometryDuplicateCoordinateRemover.process(FACTORY.createMultiPoint(), 1);
      CountingCoordinate.comparisons = 0;
      Coordinate[] result =
          GeometryDuplicateCoordinateRemover.removeDuplicatePointsMultiPoint(points, false);
      assertEquals(size, result.length);
      assertTrue(
          "distance comparisons: " + CountingCoordinate.comparisons,
          CountingCoordinate.comparisons <= 4L * size);
    }
  }

  @Test
  public void preservesGreedySelectionAndStrictBoundary() {
    Coordinate[] points = {
      new Coordinate(0, 0), new Coordinate(0.75, 0), new Coordinate(1.5, 0),
      new Coordinate(0, 1), new Coordinate(0, 3), new Coordinate(0.25, 3)
    };
    GeometryDuplicateCoordinateRemover.process(FACTORY.createMultiPoint(), 1);
    assertArrayEquals(
        new Coordinate[] {
          new Coordinate(0, 0), new Coordinate(0, 1), new Coordinate(0, 3), new Coordinate(1.5, 0)
        },
        GeometryDuplicateCoordinateRemover.removeDuplicatePointsMultiPoint(points, false));
  }

  @Test
  public void tinyToleranceKeepsDistantPoints() {
    Coordinate[] points = {new Coordinate(0, 0), new Coordinate(0, 1e-180)};
    GeometryDuplicateCoordinateRemover.process(FACTORY.createMultiPoint(), 1e-200);
    assertArrayEquals(
        points, GeometryDuplicateCoordinateRemover.removeDuplicatePointsMultiPoint(points, false));
  }

  @Test
  public void retainsSamePointsAsGreedySearch() {
    Random random = new Random(3591);
    for (int trial = 0; trial < 100; trial++) {
      Coordinate[] points = new Coordinate[100];
      for (int i = 0; i < points.length; i++) {
        points[i] = new Coordinate(random.nextInt(30) / 4.0, random.nextInt(30) / 4.0);
      }
      Coordinate[] sorted = Arrays.stream(points).distinct().sorted().toArray(Coordinate[]::new);
      List<Coordinate> expected = new ArrayList<>();
      for (Coordinate point : sorted) {
        boolean duplicate = false;
        for (Coordinate previous : expected) {
          if (previous.distance(point) < 1) {
            duplicate = true;
            break;
          }
        }
        if (!duplicate) expected.add(point);
      }
      GeometryDuplicateCoordinateRemover.process(FACTORY.createMultiPoint(), 1);
      assertArrayEquals(
          expected.toArray(new Coordinate[0]),
          GeometryDuplicateCoordinateRemover.removeDuplicatePointsMultiPoint(points, false));
    }
  }
}
