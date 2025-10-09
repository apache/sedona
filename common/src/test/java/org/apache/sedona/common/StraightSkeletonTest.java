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

import static org.junit.Assert.*;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class StraightSkeletonTest {

  private static String repeat(String str, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  @Test
  public void testLShapedPolygonComparison() throws Exception {
    String wkt =
        "POLYGON ((190 190, 10 190, 10 10, 190 10, 190 20, 160 30, 60 30, 60 130, 190 140, 190 190))";

    Geometry polygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.approximateMedialAxis(polygon);

    int numSegments = medialAxis.getNumGeometries();

    assertNotNull("Medial axis should not be null", medialAxis);
    assertEquals("Should have at least one segment", 10, numSegments);
  }

  @Test
  public void testSimpleSquare() throws Exception {
    String wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";

    Geometry polygon = Constructors.geomFromWKT(wkt, 0);
    Geometry medialAxis = Functions.approximateMedialAxis(polygon);

    int numSegments = medialAxis.getNumGeometries();

    assertNotNull("Medial axis should not be null", medialAxis);
    assertEquals("Should have at least one segment", 4, numSegments);
  }
}
