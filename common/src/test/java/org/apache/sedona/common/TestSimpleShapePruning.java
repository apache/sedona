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

import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

public class TestSimpleShapePruning {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final WKTReader WKT_READER = new WKTReader(GEOMETRY_FACTORY);

  @Test
  public void testSimpleShapes() throws Exception {
    // Test Rectangle
    Geometry rectangle = WKT_READER.read("POLYGON ((0 0, 20 0, 20 5, 0 5, 0 0))");
    Geometry rectSkeleton = Functions.straightSkeleton(rectangle);
    Geometry rectPruned = Functions.approximateMedialAxis(rectangle);

    System.out.println("\n=== Rectangle ===");
    System.out.println("Straight Skeleton: " + rectSkeleton.toText());
    System.out.println("Segments: " + rectSkeleton.getNumGeometries());
    System.out.println("Pruned: " + rectPruned.toText());
    System.out.println("Segments: " + rectPruned.getNumGeometries());
    System.out.println("PostGIS Expected: MULTILINESTRING ((2.5 2.5, 17.5 2.5))");

    // Test Square
    Geometry square = WKT_READER.read("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    Geometry squareSkeleton = Functions.straightSkeleton(square);
    Geometry squarePruned = Functions.approximateMedialAxis(square);

    System.out.println("\n=== Square ===");
    System.out.println("Straight Skeleton: " + squareSkeleton.toText());
    System.out.println("Segments: " + squareSkeleton.getNumGeometries());
    System.out.println("Pruned: " + squarePruned.toText());
    System.out.println("Segments: " + squarePruned.getNumGeometries());
    System.out.println("PostGIS Expected: MULTILINESTRING ((5 5, 5 5))");
  }
}
