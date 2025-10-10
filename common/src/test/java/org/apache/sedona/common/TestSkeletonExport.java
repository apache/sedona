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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.WKTWriter;

public class TestSkeletonExport {
  public static void main(String[] args) throws Exception {
    String[][] tests = {
      {"T-Junction", "POLYGON ((45 0, 55 0, 55 40, 70 40, 70 50, 30 50, 30 40, 45 40, 45 0))"},
      {
        "4-Way Intersection",
        "POLYGON ((45 100, 55 100, 55 70, 70 70, 70 55, 100 55, 100 45, 70 45, 70 30, 55 30, 55 0, 45 0, 45 30, 30 30, 30 45, 0 45, 0 55, 30 55, 30 70, 45 70, 45 100))"
      },
      {
        "Complex Branching",
        "POLYGON ((47 0, 53 0, 53 15, 55 16, 65 14, 66 17, 56 19, 54 18, 54 30, 56 31, 70 33, 71 36, 57 34, 55 33, 55 45, 57 46, 72 50, 73 53, 58 49, 56 48, 56 60, 44 60, 44 48, 42 49, 27 53, 28 50, 43 46, 45 45, 45 33, 43 34, 29 36, 30 33, 44 31, 46 30, 46 18, 44 19, 34 17, 35 14, 45 16, 47 15, 47 0))"
      }
    };

    WKTWriter writer = new WKTWriter();
    writer.setFormatted(false);

    for (String[] test : tests) {
      String name = test[0];
      String wkt = test[1];

      System.out.println("### " + name + " ###");

      Geometry polygon = Constructors.geomFromWKT(wkt, 0);
      Geometry skeleton = Functions.straightSkeleton(polygon);

      System.out.println("SEGMENTS:" + skeleton.getNumGeometries());

      // Print each line segment
      for (int i = 0; i < skeleton.getNumGeometries(); i++) {
        LineString line = (LineString) skeleton.getGeometryN(i);
        System.out.println("LINE:" + writer.write(line));
      }

      System.out.println();
    }
  }
}
