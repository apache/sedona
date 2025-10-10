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

import org.apache.sedona.common.approximate.StraightSkeleton;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

public class VisualizationTest {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final WKTReader WKT_READER = new WKTReader(GEOMETRY_FACTORY);

  @Test
  public void generateVisualizationData() throws Exception {
    StraightSkeleton skeleton = new StraightSkeleton();

    // Test cases - using actual test polygons from StraightSkeletonTest
    String[] testCases = {
      "Square",
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
      "Rectangle",
      "POLYGON ((0 0, 20 0, 20 10, 0 10, 0 0))",
      "Triangle",
      "POLYGON ((0 0, 10 0, 5 8.66, 0 0))",
      "T-Shape",
      "POLYGON ((4 0, 6 0, 6 8, 10 8, 10 10, 0 10, 0 8, 4 8, 4 0))",
      "L-Shape",
      "POLYGON ((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0))",
      "C-Shape",
      "POLYGON ((0 0, 10 0, 10 5, 5 5, 5 10, 10 10, 10 15, 0 15, 0 0))",
      "Star",
      "POLYGON ((5 0, 6 4, 10 5, 6 6, 5 10, 4 6, 0 5, 4 4, 5 0))",
      "T-Junction",
      "POLYGON ((45 0, 55 0, 55 40, 70 40, 70 50, 30 50, 30 40, 45 40, 45 0))"
    };

    System.out.println("=== VISUALIZATION DATA ===");

    for (int i = 0; i < testCases.length; i += 2) {
      String name = testCases[i];
      String wkt = testCases[i + 1];

      Polygon polygon = (Polygon) WKT_READER.read(wkt);
      Geometry result = skeleton.computeSkeleton(polygon);

      System.out.println("\n" + name + ":");
      System.out.println("  Input WKT: " + wkt);
      System.out.println("  Output WKT: " + result.toText());
      System.out.println("  Segment count: " + result.getNumGeometries());
    }
  }
}
