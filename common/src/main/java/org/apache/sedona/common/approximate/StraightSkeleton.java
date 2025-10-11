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
import org.locationtech.jts.algorithm.Orientation;
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

  /**
   * Target size for normalized geometry. The campskeleton library works best with geometries of a
   * reasonable size (around 100 units). Geometries are scaled to have their maximum dimension equal
   * to this value.
   */
  private static final double NORMALIZED_SIZE = 100.0;

  /**
   * Default roof slope angle (in radians) for the skeleton machine. This represents the angle at
   * which edges shrink. For a pure straight skeleton (not weighted), this value doesn't
   * significantly affect the output.
   */
  private static final double DEFAULT_MACHINE_ANGLE = Math.PI / 4; // 45 degrees

  /**
   * Epsilon value for detecting boundary edges. Edges with z-coordinates below this threshold on
   * both endpoints are considered boundary edges and filtered out from the skeleton output.
   */
  private static final double BOUNDARY_EDGE_EPSILON = 1e-10;

  public StraightSkeleton() {}

  /**
   * Compute the straight skeleton for a polygon.
   *
   * <p>The campskeleton library has numerical stability issues with certain geometries. To improve
   * robustness, we preprocess the polygon by: 1. Centering it at the origin (0,0) 2. Scaling it to
   * a reasonable size 3. Ensuring counter-clockwise orientation
   *
   * <p>After computing the skeleton, we transform it back to the original coordinate system.
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
      // Step 1: Calculate centroid and envelope for normalization
      Point centroid = polygon.getCentroid();
      double offsetX = centroid.getX();
      double offsetY = centroid.getY();

      Envelope envelope = polygon.getEnvelopeInternal();
      double maxDim = Math.max(envelope.getWidth(), envelope.getHeight());

      // Use a scale factor to normalize the geometry to a reasonable size
      // This improves numerical stability in campskeleton
      double scaleFactor = (maxDim > 0) ? (NORMALIZED_SIZE / maxDim) : 1.0;

      // Step 2: Normalize the polygon (center and scale)
      Polygon normalizedPolygon = normalizePolygon(polygon, offsetX, offsetY, scaleFactor);

      // Step 3: Convert JTS polygon to campskeleton format
      LoopL<Edge> input = convertPolygonToEdges(normalizedPolygon);

      // Step 4: Compute straight skeleton
      Skeleton skeleton = new Skeleton(input, true);
      skeleton.skeleton();

      // Check if skeleton computation succeeded
      if (skeleton.output == null || skeleton.output.edges == null) {
        log.warn("Campskeleton failed to produce output for polygon: {}", polygon.toText());
        return factory.createMultiLineString(new LineString[0]);
      }

      // Step 5: Extract skeleton edges from normalized coordinate system
      List<LineString> normalizedEdges = extractSkeletonEdges(skeleton, factory);

      if (normalizedEdges.isEmpty()) {
        log.warn("No skeleton edges extracted for polygon: {}", polygon.toText());
        return factory.createMultiLineString(new LineString[0]);
      }

      // Step 6: Transform skeleton edges back to original coordinate system
      List<LineString> transformedEdges = new ArrayList<>();
      for (LineString edge : normalizedEdges) {
        LineString transformed = transformLineStringBack(edge, offsetX, offsetY, scaleFactor);
        transformedEdges.add(transformed);
      }

      return factory.createMultiLineString(transformedEdges.toArray(new LineString[0]));

    } catch (Exception e) {
      log.error("Failed to compute straight skeleton for polygon: {}", polygon.toText(), e);
      throw new RuntimeException("Failed to compute straight skeleton: " + e.getMessage(), e);
    }
  }

  /**
   * Normalize polygon by centering at origin and scaling.
   *
   * @param polygon Original polygon
   * @param offsetX X offset to subtract (centroid x)
   * @param offsetY Y offset to subtract (centroid y)
   * @param scaleFactor Scale factor to apply
   * @return Normalized polygon
   */
  private Polygon normalizePolygon(
      Polygon polygon, double offsetX, double offsetY, double scaleFactor) {
    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();
    Coordinate[] normalizedCoords = new Coordinate[coords.length];

    for (int i = 0; i < coords.length; i++) {
      double x = (coords[i].x - offsetX) * scaleFactor;
      double y = (coords[i].y - offsetY) * scaleFactor;
      normalizedCoords[i] = new Coordinate(x, y);
    }

    GeometryFactory factory = polygon.getFactory();
    LinearRing shell = factory.createLinearRing(normalizedCoords);
    return factory.createPolygon(shell);
  }

  /**
   * Transform a LineString back from normalized coordinates to original coordinate system.
   *
   * @param lineString LineString in normalized coordinates
   * @param offsetX Original X offset to add back
   * @param offsetY Original Y offset to add back
   * @param scaleFactor Scale factor to reverse
   * @return LineString in original coordinate system
   */
  private LineString transformLineStringBack(
      LineString lineString, double offsetX, double offsetY, double scaleFactor) {
    Coordinate[] coords = lineString.getCoordinates();
    Coordinate[] transformedCoords = new Coordinate[coords.length];

    for (int i = 0; i < coords.length; i++) {
      double x = (coords[i].x / scaleFactor) + offsetX;
      double y = (coords[i].y / scaleFactor) + offsetY;
      transformedCoords[i] = new Coordinate(x, y);
    }

    return lineString.getFactory().createLineString(transformedCoords);
  }

  /**
   * Convert JTS Polygon to campskeleton LoopL<Edge> format.
   *
   * <p>Creates Corner objects for each vertex and connects them with Edge objects. Each edge is
   * assigned a default Machine (speed) for uniform shrinking.
   *
   * <p>The campskeleton library requires polygons to be in counter-clockwise (CCW) orientation.
   * This method ensures the polygon is properly oriented before conversion.
   */
  private LoopL<Edge> convertPolygonToEdges(Polygon polygon) {
    LoopL<Edge> input = new LoopL<>();
    Loop<Edge> loop = new Loop<>();

    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();

    // Ensure counter-clockwise orientation (required by campskeleton)
    // If the ring is clockwise, reverse the coordinate order
    if (!Orientation.isCCW(coords)) {
      // Reverse the coordinates (excluding the closing point)
      Coordinate[] reversed = new Coordinate[coords.length];
      for (int i = 0; i < coords.length - 1; i++) {
        reversed[i] = coords[coords.length - 2 - i];
      }
      // Add the closing point
      reversed[coords.length - 1] = reversed[0];
      coords = reversed;
    }

    // Create corners first - share corners between consecutive edges
    List<Corner> corners = new ArrayList<>();
    for (int i = 0; i < coords.length - 1; i++) { // -1 because last coord = first coord
      Coordinate c = coords[i];
      corners.add(new Corner(c.x, c.y));
    }

    // Create a default machine (uniform speed for all edges)
    Machine defaultMachine = new Machine(DEFAULT_MACHINE_ANGLE);

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
      boolean isBoundaryEdge =
          (Math.abs(start.z) < BOUNDARY_EDGE_EPSILON && Math.abs(end.z) < BOUNDARY_EDGE_EPSILON);

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
