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

import static org.junit.Assert.fail;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

public class TestBase {
  public Coordinate[] coordArray(double... coordValues) {
    Coordinate[] coords = new Coordinate[(int) (coordValues.length / 2)];
    for (int i = 0; i < coordValues.length; i += 2) {
      coords[(int) (i / 2)] = new Coordinate(coordValues[i], coordValues[i + 1]);
    }
    return coords;
  }

  public Coordinate[] coordArray3d(double... coordValues) {
    Coordinate[] coords = new Coordinate[(int) (coordValues.length / 3)];
    for (int i = 0; i < coordValues.length; i += 3) {
      coords[(int) (i / 3)] =
          new Coordinate(coordValues[i], coordValues[i + 1], coordValues[i + 2]);
    }
    return coords;
  }

  public void assertGeometryEquals(String expectedWkt, String actualWkt, double tolerance) {
    WKTReader reader = new WKTReader();
    try {
      Geometry expectedGeom = reader.read(expectedWkt);
      Geometry actualGeom = reader.read(actualWkt);
      if (!expectedGeom.equals(actualGeom)) {
        if (!expectedGeom.buffer(tolerance).contains(actualGeom)
            || !actualGeom.buffer(tolerance).contains(expectedGeom)) {
          fail(String.format("Geometry %s should equal to %s", actualWkt, expectedWkt));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void assertGeometryEquals(String expectedWkt, String actualWkt) {
    assertGeometryEquals(expectedWkt, actualWkt, 1e-6);
  }
}
