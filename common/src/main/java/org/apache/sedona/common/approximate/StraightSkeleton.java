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
package org.apache.sedona.common.approximate;

import java.util.*;
import javax.vecmath.Point3d;
import org.locationtech.jts.geom.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twak.camp.Corner;
import org.twak.camp.Edge;
import org.twak.camp.Machine;
import org.twak.camp.Output;
import org.twak.camp.Skeleton;
import org.twak.utils.collections.Loop;
import org.twak.utils.collections.LoopL;

/**
 * Straight skeleton computation for polygons using the campskeleton library.
 *
 * <p>The straight skeleton is a method of representing a polygon by a topological skeleton. It is
 * defined by a continuous shrinking process in which each edge of the polygon is moved inward in a
 * parallel manner. This implementation uses the campskeleton library which implements the weighted
 * straight skeleton algorithm based on Felkel's approach.
 *
 * <p>References: - Tom Kelly and Peter Wonka (2011). Interactive Architectural Modeling with
 * Procedural Extrusions - Felkel, P., & Obdržálek, Š. (1998). Straight skeleton implementation
 */
public class StraightSkeleton {
  private static final Logger log = LoggerFactory.getLogger(StraightSkeleton.class);

  public StraightSkeleton() {}

  /**
   * Compute the straight skeleton for a polygon.
   *
   * @param polygon Input polygon (must be simple, non-self-intersecting)
   * @return MultiLineString representing the straight skeleton edges
   */
  public Geometry computeSkeleton(Polygon polygon) {
    if (polygon == null || polygon.isEmpty()) {
      return null;
    }

    GeometryFactory factory = polygon.getFactory();

    try {
      // Convert JTS polygon to campskeleton format
      LoopL<Edge> input = convertPolygonToEdges(polygon);

      // Compute straight skeleton
      Skeleton skeleton = new Skeleton(input, true);
      skeleton.skeleton();

      // Check if skeleton computation succeeded
      if (skeleton.output == null || skeleton.output.edges == null) {
        log.warn("Campskeleton failed to produce output for polygon: {}", polygon.toText());
        return factory.createMultiLineString(new LineString[0]);
      }

      // Convert skeleton output to JTS geometry
      List<LineString> skeletonEdges = extractSkeletonEdges(skeleton, factory);

      if (skeletonEdges.isEmpty()) {
        log.warn("No skeleton edges extracted for polygon: {}", polygon.toText());
        return factory.createMultiLineString(new LineString[0]);
      }

      return factory.createMultiLineString(skeletonEdges.toArray(new LineString[0]));

    } catch (Exception e) {
      log.error("Failed to compute straight skeleton for polygon: {}", polygon.toText(), e);
      throw new RuntimeException("Failed to compute straight skeleton: " + e.getMessage(), e);
    }
  }

  /**
   * Convert JTS Polygon to campskeleton LoopL<Edge> format.
   *
   * <p>Creates Corner objects for each vertex and connects them with Edge objects. Each edge is
   * assigned a default Machine (speed) for uniform shrinking.
   */
  private LoopL<Edge> convertPolygonToEdges(Polygon polygon) {
    LoopL<Edge> input = new LoopL<>();
    Loop<Edge> loop = new Loop<>();

    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();

    // Create corners first - share corners between consecutive edges
    List<Corner> corners = new ArrayList<>();
    for (int i = 0; i < coords.length - 1; i++) { // -1 because last coord = first coord
      Coordinate c = coords[i];
      corners.add(new Corner(c.x, c.y));
    }

    // Create a default machine (uniform speed for all edges)
    // The angle parameter (Math.PI / 4 = 45 degrees) represents the roof slope
    // For a pure straight skeleton (not weighted), this value doesn't matter much
    Machine defaultMachine = new Machine(Math.PI / 4);

    // Create edges connecting consecutive corners
    for (int i = 0; i < corners.size(); i++) {
      Corner c1 = corners.get(i);
      Corner c2 = corners.get((i + 1) % corners.size());

      Edge edge = new Edge(c1, c2);
      edge.machine = defaultMachine; // Set machine for the edge
      loop.append(edge);
    }

    input.add(loop);
    return input;
  }

  /**
   * Extract skeleton edges from campskeleton output.
   *
   * <p>The campskeleton output contains SharedEdge objects representing the skeleton edges. We
   * project these from 3D to 2D by discarding the z coordinate.
   */
  private List<LineString> extractSkeletonEdges(Skeleton skeleton, GeometryFactory factory) {
    List<LineString> edges = new ArrayList<>();

    // Use the edges from the skeleton output
    // skeleton.output.edges is an IdentityLookup with a map field
    for (Output.SharedEdge edge : skeleton.output.edges.map.values()) {
      Point3d start = edge.start;
      Point3d end = edge.end;

      // Filter out boundary edges (those with z=0 on both endpoints)
      // The straight skeleton edges are "raised" above the polygon plane
      double epsilon = 1e-10;
      boolean isBoundaryEdge = (Math.abs(start.z) < epsilon && Math.abs(end.z) < epsilon);

      if (isBoundaryEdge) {
        continue; // Skip boundary edges
      }

      // Project from 3D to 2D (discard z coordinate)
      Coordinate c1 = new Coordinate(start.x, start.y);
      Coordinate c2 = new Coordinate(end.x, end.y);

      // Only create edge if points are different in 2D
      if (!c1.equals2D(c2)) {
        LineString lineString = factory.createLineString(new Coordinate[] {c1, c2});
        edges.add(lineString);
      }
    }

    return edges;
  }
}
