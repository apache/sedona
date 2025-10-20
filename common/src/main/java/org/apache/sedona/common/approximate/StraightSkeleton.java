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
  private static final double NORMALIZED_SIZE = 1000.0;

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
   * a reasonable size 3. Ensuring counter-clockwise orientation 4. Optionally reducing vertex count
   * by merging shortest edges
   *
   * <p>After computing the skeleton, we transform it back to the original coordinate system.
   *
   * @param polygon Input polygon (must be simple, non-self-intersecting)
   * @param maxVertices Maximum number of vertices to keep (0 to disable simplification). If the
   *     polygon has more vertices than this limit, the shortest edges will be merged until the
   *     vertex count is reduced. Recommended: 100-500 for performance
   * @return MultiLineString representing the straight skeleton edges
   */
  public Geometry computeSkeleton(Polygon polygon, int maxVertices) {
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

      // Step 2: Normalize the polygon (center, scale, and optionally simplify by merging short
      // edges)
      Polygon normalizedPolygon =
          normalizePolygon(polygon, offsetX, offsetY, scaleFactor, maxVertices);

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
   * Normalize polygon by centering at origin and scaling. If the polygon has too many vertices, it
   * will merge the shortest edges to reduce vertex count. Handles both exterior ring and interior
   * rings (holes).
   *
   * @param polygon Original polygon
   * @param offsetX X offset to subtract (centroid x)
   * @param offsetY Y offset to subtract (centroid y)
   * @param scaleFactor Scale factor to apply
   * @param maxVertices Maximum number of vertices to keep (0 to disable simplification)
   * @return Normalized polygon
   */
  private Polygon normalizePolygon(
      Polygon polygon, double offsetX, double offsetY, double scaleFactor, int maxVertices) {
    GeometryFactory factory = polygon.getFactory();

    // Normalize exterior ring
    LinearRing normalizedShell =
        normalizeRing(
            polygon.getExteriorRing(), offsetX, offsetY, scaleFactor, maxVertices, factory);

    // Normalize interior rings (holes)
    LinearRing[] normalizedHoles = new LinearRing[polygon.getNumInteriorRing()];
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      normalizedHoles[i] =
          normalizeRing(
              polygon.getInteriorRingN(i), offsetX, offsetY, scaleFactor, maxVertices, factory);
    }

    return factory.createPolygon(normalizedShell, normalizedHoles);
  }

  /**
   * Normalize a single ring by centering at origin and scaling.
   *
   * @param ring Original ring
   * @param offsetX X offset to subtract
   * @param offsetY Y offset to subtract
   * @param scaleFactor Scale factor to apply
   * @param maxVertices Maximum number of vertices to keep (0 to disable simplification)
   * @param factory GeometryFactory for creating the result
   * @return Normalized ring
   */
  private LinearRing normalizeRing(
      LineString ring,
      double offsetX,
      double offsetY,
      double scaleFactor,
      int maxVertices,
      GeometryFactory factory) {
    Coordinate[] coords = ring.getCoordinates();

    // First pass: normalize all coordinates
    List<Coordinate> normalizedCoords = new ArrayList<>();
    for (int i = 0; i < coords.length - 1; i++) { // Skip last coordinate (duplicate of first)
      double x = (coords[i].x - offsetX) * scaleFactor;
      double y = (coords[i].y - offsetY) * scaleFactor;
      normalizedCoords.add(new Coordinate(x, y));
    }

    // Second pass: merge shortest edges if we exceed maxVertices
    if (maxVertices > 0 && normalizedCoords.size() > maxVertices) {
      normalizedCoords = simplifyByMergingShortestEdges(normalizedCoords, maxVertices);
    }

    // Close the ring
    normalizedCoords.add(new Coordinate(normalizedCoords.get(0)));

    Coordinate[] coordArray = normalizedCoords.toArray(new Coordinate[0]);
    return factory.createLinearRing(coordArray);
  }

  /**
   * Simplify a polygon by repeatedly removing the vertex that creates the shortest edge.
   *
   * @param coords List of coordinates (without closing coordinate)
   * @param targetVertexCount Target number of vertices
   * @return Simplified list of coordinates
   */
  private List<Coordinate> simplifyByMergingShortestEdges(
      List<Coordinate> coords, int targetVertexCount) {
    // Ensure we keep at least 3 vertices for a valid polygon
    targetVertexCount = Math.max(3, targetVertexCount);

    // Create a working copy
    List<Coordinate> result = new ArrayList<>(coords);

    // Keep removing the shortest edge until we reach target
    while (result.size() > targetVertexCount) {
      double minLength = Double.MAX_VALUE;
      int shortestEdgeIdx = -1;

      // Find the shortest edge
      for (int i = 0; i < result.size(); i++) {
        Coordinate curr = result.get(i);
        Coordinate next = result.get((i + 1) % result.size());
        double length = curr.distance(next);

        if (length < minLength) {
          minLength = length;
          shortestEdgeIdx = i;
        }
      }

      // Remove the vertex at the end of the shortest edge
      // This effectively merges the two edges adjacent to this vertex
      int vertexToRemove = (shortestEdgeIdx + 1) % result.size();
      result.remove(vertexToRemove);
    }

    return result;
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
   * <p>The campskeleton library requires the exterior ring to be in counter-clockwise (CCW)
   * orientation and interior rings (holes) to be in clockwise (CW) orientation. This method ensures
   * proper orientation before conversion.
   *
   * @param polygon Input polygon with potential holes
   * @return LoopL containing the exterior ring and all interior rings as separate loops
   */
  private LoopL<Edge> convertPolygonToEdges(Polygon polygon) {
    LoopL<Edge> input = new LoopL<>();
    Machine defaultMachine = new Machine(DEFAULT_MACHINE_ANGLE);

    // Add exterior ring (must be CCW)
    Loop<Edge> exteriorLoop = convertRingToLoop(polygon.getExteriorRing(), true, defaultMachine);
    input.add(exteriorLoop);

    // Add interior rings / holes (must be CW)
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      Loop<Edge> holeLoop = convertRingToLoop(polygon.getInteriorRingN(i), false, defaultMachine);
      input.add(holeLoop);
    }

    return input;
  }

  /**
   * Convert a single ring (exterior or interior) to a campskeleton Loop.
   *
   * @param ring The linear ring to convert
   * @param isExterior true if this is the exterior ring (CCW), false for holes (CW)
   * @param machine The Machine to assign to all edges
   * @return Loop of edges representing the ring
   */
  private Loop<Edge> convertRingToLoop(LineString ring, boolean isExterior, Machine machine) {
    Loop<Edge> loop = new Loop<>();
    Coordinate[] coords = ring.getCoordinates();

    // Check orientation and reverse if needed
    boolean isCCW = Orientation.isCCW(coords);

    // Exterior rings should be CCW, holes should be CW
    boolean needsReverse = (isExterior && !isCCW) || (!isExterior && isCCW);

    if (needsReverse) {
      // Reverse the coordinates (excluding the closing point)
      Coordinate[] reversed = new Coordinate[coords.length];
      for (int i = 0; i < coords.length - 1; i++) {
        reversed[i] = coords[coords.length - 2 - i];
      }
      // Add the closing point
      reversed[coords.length - 1] = reversed[0];
      coords = reversed;
    }

    // Create corners for each vertex
    List<Corner> corners = new ArrayList<>();
    for (int i = 0; i < coords.length - 1; i++) { // -1 because last coord = first coord
      Coordinate c = coords[i];
      corners.add(new Corner(c.x, c.y));
    }

    // Create edges connecting consecutive corners
    for (int i = 0; i < corners.size(); i++) {
      Corner c1 = corners.get(i);
      Corner c2 = corners.get((i + 1) % corners.size());

      Edge edge = new Edge(c1, c2);
      edge.machine = machine; // Set machine for the edge
      loop.append(edge);
    }

    return loop;
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
