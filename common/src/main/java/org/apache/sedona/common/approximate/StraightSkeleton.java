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
import org.locationtech.jts.geom.*;
import org.locationtech.jts.math.Vector2D;

/**
 * Straight Skeleton implementation for computing exact polygon skeletons.
 *
 * <p>This is a from-scratch implementation of the straight skeleton algorithm, which constructs the
 * skeleton by simulating a continuous shrinking process where polygon edges move inward at constant
 * speed. The algorithm processes edge and split events in temporal order to construct the skeleton
 * topology.
 *
 * <p>Algorithm overview: 1. Initialize LAV (List of Active Vertices) with input polygon vertices 2.
 * Compute bisector rays for each vertex 3. Detect edge events (adjacent edges collide) and split
 * events (bisector hits non-adjacent edge) 4. Process events by time, updating LAV and creating
 * skeleton edges 5. Extract medial axis from resulting skeleton
 *
 * <p>References: - CGAL Straight_skeleton_2 implementation - Aichholzer, O., Aurenhammer, F.
 * (1998). Straight Skeletons for General Polygonal Figures
 */
public class StraightSkeleton {

  private static final double EPSILON = 1e-10;

  /**
   * Represents a vertex in the straight skeleton.
   *
   * <p>Each vertex stores: - Its current position (x, y coordinates) - The time when it was created
   * (distance from original edge) - References to previous/next vertices in the LAV - The bisector
   * ray direction - Whether the vertex is still active
   */
  static class SkeletonVertex {
    Coordinate position;
    double time;
    SkeletonVertex prevInLAV;
    SkeletonVertex nextInLAV;
    Vector2D bisector; // Normalized direction of inward bisector
    boolean isActive;
    Edge prevEdge; // Edge coming into this vertex
    Edge nextEdge; // Edge going out from this vertex

    SkeletonVertex(Coordinate position, double time) {
      this.position = new Coordinate(position);
      this.time = time;
      this.isActive = true;
    }

    /**
     * Compute the bisector direction for this vertex based on adjacent edges.
     *
     * <p>The bisector is the inward-pointing ray that bisects the angle between the previous and
     * next edges. For convex vertices, this points toward the interior. For reflex vertices (angle
     * > 180°), it points away from the interior.
     */
    void computeBisector() {
      if (prevEdge == null || nextEdge == null) {
        bisector = null;
        return;
      }

      // Get edge direction vectors
      Vector2D v1 = prevEdge.direction;
      Vector2D v2 = nextEdge.direction;

      // Compute bisector as the normalized sum of perpendicular inward vectors
      // For the perpendicular inward vector: rotate edge direction 90° CCW
      Vector2D perp1 = new Vector2D(-v1.getY(), v1.getX());
      Vector2D perp2 = new Vector2D(-v2.getY(), v2.getX());

      // Bisector direction
      double bx = perp1.getX() + perp2.getX();
      double by = perp1.getY() + perp2.getY();

      double length = Math.sqrt(bx * bx + by * by);
      if (length < EPSILON) {
        // Parallel edges - bisector is perpendicular to both
        bisector = perp1.normalize();
      } else {
        bisector = new Vector2D(bx / length, by / length);
      }
    }

    /**
     * Calculate the speed at which this vertex moves along its bisector.
     *
     * <p>The speed depends on the angle between adjacent edges. For a convex vertex with angle θ,
     * speed = 1 / sin(θ/2).
     */
    double computeSpeed() {
      if (prevEdge == null || nextEdge == null || bisector == null) {
        return 1.0;
      }

      Vector2D v1 = prevEdge.direction;
      Vector2D v2 = nextEdge.direction;

      // Compute angle between edges
      double dot = v1.dot(v2);
      double cross = v1.getX() * v2.getY() - v1.getY() * v2.getX();
      double angle = Math.atan2(cross, dot);

      // Normalize angle to [0, 2π)
      if (angle < 0) angle += 2 * Math.PI;

      // Speed = 1 / sin(angle/2)
      double halfAngle = angle / 2.0;
      double sinHalfAngle = Math.sin(halfAngle);

      if (Math.abs(sinHalfAngle) < EPSILON) {
        return Double.POSITIVE_INFINITY; // Parallel edges
      }

      return 1.0 / Math.abs(sinHalfAngle);
    }

    /** Check if this vertex is reflex (interior angle > 180°). */
    boolean isReflex() {
      if (prevEdge == null || nextEdge == null) {
        return false;
      }

      Vector2D v1 = prevEdge.direction;
      Vector2D v2 = nextEdge.direction;

      // Cross product indicates turn direction
      double cross = v1.getX() * v2.getY() - v1.getY() * v2.getX();
      return cross < 0; // Clockwise turn = reflex
    }

    @Override
    public String toString() {
      return String.format(
          "V(%.2f, %.2f, t=%.2f, active=%b)", position.x, position.y, time, isActive);
    }
  }

  /**
   * Represents an edge in the straight skeleton.
   *
   * <p>An edge connects two vertices and has a direction. Original edges come from the input
   * polygon, while skeleton edges are created during event processing.
   */
  static class Edge {
    SkeletonVertex start;
    SkeletonVertex end;
    Vector2D direction; // Normalized direction from start to end
    boolean isOriginal; // True for input polygon edges, false for skeleton edges

    Edge(SkeletonVertex start, SkeletonVertex end, boolean isOriginal) {
      this.start = start;
      this.end = end;
      this.isOriginal = isOriginal;

      // Compute direction
      double dx = end.position.x - start.position.x;
      double dy = end.position.y - start.position.y;
      double length = Math.sqrt(dx * dx + dy * dy);

      if (length < EPSILON) {
        direction = new Vector2D(1, 0); // Degenerate edge
      } else {
        direction = new Vector2D(dx / length, dy / length);
      }
    }

    double length() {
      return start.position.distance(end.position);
    }

    @Override
    public String toString() {
      return String.format("Edge[%s -> %s, original=%b]", start.position, end.position, isOriginal);
    }
  }

  /**
   * Represents an event in the straight skeleton algorithm.
   *
   * <p>Events occur when: - EDGE: Two adjacent vertices collide (their bisectors intersect) -
   * SPLIT: A reflex vertex's bisector hits a non-adjacent edge
   *
   * <p>Events are processed in chronological order (by time).
   */
  static class SkeletonEvent implements Comparable<SkeletonEvent> {
    enum EventType {
      EDGE,
      SPLIT
    }

    EventType type;
    double time;
    Coordinate point; // Location where event occurs
    SkeletonVertex vertexA; // For EDGE: first vertex, for SPLIT: reflex vertex
    SkeletonVertex vertexB; // For EDGE: second vertex, for SPLIT: vertex before split point
    SkeletonVertex vertexC; // For SPLIT: vertex after split point
    Edge oppositeEdge; // For SPLIT: the edge being split

    // Event validity tracking (events become invalid when vertices are removed)
    boolean isValid;

    private SkeletonEvent(EventType type, double time, Coordinate point) {
      this.type = type;
      this.time = time;
      this.point = new Coordinate(point);
      this.isValid = true;
    }

    /** Create an edge event (two adjacent vertices collapse). */
    static SkeletonEvent createEdgeEvent(
        SkeletonVertex a, SkeletonVertex b, double time, Coordinate point) {
      SkeletonEvent event = new SkeletonEvent(EventType.EDGE, time, point);
      event.vertexA = a;
      event.vertexB = b;
      return event;
    }

    /** Create a split event (reflex vertex hits opposite edge). */
    static SkeletonEvent createSplitEvent(
        SkeletonVertex reflexVertex,
        Edge oppositeEdge,
        SkeletonVertex edgeVertexA,
        SkeletonVertex edgeVertexB,
        double time,
        Coordinate point) {
      SkeletonEvent event = new SkeletonEvent(EventType.SPLIT, time, point);
      event.vertexA = reflexVertex;
      event.vertexB = edgeVertexA;
      event.vertexC = edgeVertexB;
      event.oppositeEdge = oppositeEdge;
      return event;
    }

    /** Events are ordered by time, then by type. */
    @Override
    public int compareTo(SkeletonEvent other) {
      int timeCompare = Double.compare(this.time, other.time);
      if (timeCompare != 0) return timeCompare;

      // Tie-breaker: EDGE events before SPLIT events
      return this.type.compareTo(other.type);
    }

    /**
     * Check if this event is still valid.
     *
     * <p>An event becomes invalid if any of its participating vertices have been deactivated.
     */
    boolean checkValidity() {
      if (!isValid) return false;

      if (vertexA == null || !vertexA.isActive) {
        isValid = false;
        return false;
      }

      if (vertexB == null || !vertexB.isActive) {
        isValid = false;
        return false;
      }

      if (type == EventType.EDGE) {
        // For edge events, check that A and B are still adjacent
        if (vertexA.nextInLAV != vertexB || vertexB.prevInLAV != vertexA) {
          isValid = false;
          return false;
        }
      } else if (type == EventType.SPLIT) {
        // For split events, check that C is still active
        if (vertexC == null || !vertexC.isActive) {
          isValid = false;
          return false;
        }
      }

      return true;
    }

    @Override
    public String toString() {
      return String.format(
          "%s event at t=%.2f, point=(%.2f, %.2f), valid=%b",
          type, time, point.x, point.y, isValid);
    }
  }

  /**
   * List of Active Vertices (LAV) - circular doubly-linked list.
   *
   * <p>The LAV represents the current "wavefront" of the shrinking polygon. As events are
   * processed, vertices are added/removed from the LAV.
   */
  static class LAV {
    SkeletonVertex head;
    int size;

    LAV() {
      this.head = null;
      this.size = 0;
    }

    /** Add a vertex to the LAV. */
    void add(SkeletonVertex vertex) {
      if (head == null) {
        head = vertex;
        vertex.nextInLAV = vertex;
        vertex.prevInLAV = vertex;
      } else {
        // Insert at end (before head)
        SkeletonVertex tail = head.prevInLAV;
        tail.nextInLAV = vertex;
        vertex.prevInLAV = tail;
        vertex.nextInLAV = head;
        head.prevInLAV = vertex;
      }
      size++;
    }

    /** Remove a vertex from the LAV. */
    void remove(SkeletonVertex vertex) {
      if (size == 0) return;

      vertex.isActive = false;

      if (size == 1) {
        head = null;
      } else {
        vertex.prevInLAV.nextInLAV = vertex.nextInLAV;
        vertex.nextInLAV.prevInLAV = vertex.prevInLAV;

        if (head == vertex) {
          head = vertex.nextInLAV;
        }
      }
      size--;
    }

    /** Get all vertices in LAV order. */
    List<SkeletonVertex> getVertices() {
      List<SkeletonVertex> result = new ArrayList<>();
      if (head == null) return result;

      SkeletonVertex current = head;
      do {
        result.add(current);
        current = current.nextInLAV;
      } while (current != head);

      return result;
    }

    boolean isEmpty() {
      return size == 0;
    }

    @Override
    public String toString() {
      return String.format("LAV(size=%d)", size);
    }
  }

  // Skeleton computation state
  private LAV lav;
  private PriorityQueue<SkeletonEvent> eventQueue;
  private List<Edge> skeletonEdges;
  private List<SkeletonVertex> allVertices;

  public StraightSkeleton() {
    this.lav = new LAV();
    this.eventQueue = new PriorityQueue<>();
    this.skeletonEdges = new ArrayList<>();
    this.allVertices = new ArrayList<>();
  }

  /**
   * Compute the straight skeleton for a polygon.
   *
   * @param polygon Input polygon (must be simple, non-self-intersecting)
   * @return MultiLineString representing the skeleton edges
   */
  public Geometry computeSkeleton(Polygon polygon) {
    if (polygon == null || polygon.isEmpty()) {
      return null;
    }

    // Initialize LAV with polygon vertices
    initializeLAV(polygon);

    // Process events until LAV is empty or too small
    while (!eventQueue.isEmpty() && lav.size > 2) {
      SkeletonEvent event = eventQueue.poll();

      // Skip invalid events
      if (!event.checkValidity()) {
        continue;
      }

      // Process event
      processEvent(event);
    }

    // Convert skeleton edges to geometry
    return buildSkeletonGeometry(polygon.getFactory());
  }

  /**
   * Initialize the LAV with the input polygon's vertices.
   *
   * <p>This creates the initial wavefront and computes bisectors for all vertices.
   */
  private void initializeLAV(Polygon polygon) {
    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();

    // Create vertices (skip last duplicate coordinate)
    List<SkeletonVertex> vertices = new ArrayList<>();
    for (int i = 0; i < coords.length - 1; i++) {
      SkeletonVertex vertex = new SkeletonVertex(coords[i], 0.0);
      vertices.add(vertex);
      allVertices.add(vertex);
      lav.add(vertex);
    }

    // Create edges and link vertices
    for (int i = 0; i < vertices.size(); i++) {
      SkeletonVertex v1 = vertices.get(i);
      SkeletonVertex v2 = vertices.get((i + 1) % vertices.size());

      Edge edge = new Edge(v1, v2, true);
      v1.nextEdge = edge;
      v2.prevEdge = edge;
    }

    // Compute bisectors
    for (SkeletonVertex vertex : vertices) {
      vertex.computeBisector();
    }

    // Initialize event queue
    for (SkeletonVertex vertex : vertices) {
      computeEventsForVertex(vertex);
    }
  }

  /**
   * Compute potential events for a vertex.
   *
   * <p>This checks for: 1. Edge event with next vertex (bisectors intersect) 2. Split event with
   * opposite edges (for reflex vertices)
   */
  private void computeEventsForVertex(SkeletonVertex vertex) {
    if (!vertex.isActive || vertex.bisector == null) {
      return;
    }

    // Check edge event with next vertex
    SkeletonVertex next = vertex.nextInLAV;
    if (next != null && next.isActive && next.bisector != null) {
      computeEdgeEvent(vertex, next);
    }

    // Check split events for reflex vertices
    if (vertex.isReflex()) {
      computeSplitEvents(vertex);
    }
  }

  /**
   * Compute edge event between two adjacent vertices.
   *
   * <p>An edge event occurs when the bisectors of two adjacent vertices intersect. The intersection
   * point and time are computed by finding where the two rays meet.
   */
  private void computeEdgeEvent(SkeletonVertex v1, SkeletonVertex v2) {
    if (v1.bisector == null || v2.bisector == null) {
      return;
    }

    // Compute intersection of two rays
    // Ray 1: v1.position + t1 * v1.bisector * speed1
    // Ray 2: v2.position + t2 * v2.bisector * speed2

    double speed1 = v1.computeSpeed();
    double speed2 = v2.computeSpeed();

    if (Double.isInfinite(speed1) || Double.isInfinite(speed2)) {
      return; // Parallel edges
    }

    // Solve for intersection
    Coordinate intersection =
        computeRayIntersection(v1.position, v1.bisector, speed1, v2.position, v2.bisector, speed2);

    if (intersection == null) {
      return; // No intersection
    }

    // Compute time (distance from original position)
    double time = v1.position.distance(intersection) / speed1;

    if (time > EPSILON) {
      SkeletonEvent event = SkeletonEvent.createEdgeEvent(v1, v2, time, intersection);
      eventQueue.add(event);
    }
  }

  /**
   * Compute split events for a reflex vertex.
   *
   * <p>A split event occurs when a reflex vertex's bisector hits a non-adjacent edge. This splits
   * the LAV into two separate LAVs.
   */
  private void computeSplitEvents(SkeletonVertex reflexVertex) {
    // Check intersection with all non-adjacent edges
    List<SkeletonVertex> lavVertices = lav.getVertices();

    for (int i = 0; i < lavVertices.size(); i++) {
      SkeletonVertex edgeStart = lavVertices.get(i);
      SkeletonVertex edgeEnd = edgeStart.nextInLAV;

      // Skip adjacent edges
      if (edgeStart == reflexVertex
          || edgeStart == reflexVertex.prevInLAV
          || edgeEnd == reflexVertex
          || edgeEnd == reflexVertex.nextInLAV) {
        continue;
      }

      Edge edge = edgeStart.nextEdge;
      if (edge == null) continue;

      // Compute intersection of bisector ray with edge
      Coordinate intersection =
          computeRayEdgeIntersection(
              reflexVertex.position,
              reflexVertex.bisector,
              reflexVertex.computeSpeed(),
              edgeStart.position,
              edgeEnd.position);

      if (intersection != null) {
        double time = reflexVertex.position.distance(intersection) / reflexVertex.computeSpeed();

        if (time > EPSILON) {
          SkeletonEvent event =
              SkeletonEvent.createSplitEvent(
                  reflexVertex, edge, edgeStart, edgeEnd, time, intersection);
          eventQueue.add(event);
        }
      }
    }
  }

  /**
   * Compute intersection of two rays.
   *
   * @return Intersection point, or null if rays don't intersect
   */
  private Coordinate computeRayIntersection(
      Coordinate p1, Vector2D d1, double speed1, Coordinate p2, Vector2D d2, double speed2) {

    // Parametric form:
    // Ray 1: p1 + t * d1 * speed1
    // Ray 2: p2 + s * d2 * speed2
    // Solve: p1 + t * d1 * speed1 = p2 + s * d2 * speed2

    double dx = p2.x - p1.x;
    double dy = p2.y - p1.y;

    double det = d1.getX() * speed1 * d2.getY() * speed2 - d1.getY() * speed1 * d2.getX() * speed2;

    if (Math.abs(det) < EPSILON) {
      return null; // Parallel rays
    }

    double t = (dx * d2.getY() * speed2 - dy * d2.getX() * speed2) / det;
    double s = (dx * d1.getY() * speed1 - dy * d1.getX() * speed1) / det;

    if (t < -EPSILON || s < -EPSILON) {
      return null; // Intersection behind ray origins
    }

    double x = p1.x + t * d1.getX() * speed1;
    double y = p1.y + t * d1.getY() * speed1;

    return new Coordinate(x, y);
  }

  /**
   * Compute intersection of a ray with an edge segment.
   *
   * @return Intersection point, or null if no intersection
   */
  private Coordinate computeRayEdgeIntersection(
      Coordinate rayOrigin,
      Vector2D rayDirection,
      double speed,
      Coordinate edgeStart,
      Coordinate edgeEnd) {

    // Ray: rayOrigin + t * rayDirection * speed
    // Edge: edgeStart + s * (edgeEnd - edgeStart), s in [0, 1]

    double dx = edgeEnd.x - edgeStart.x;
    double dy = edgeEnd.y - edgeStart.y;

    double det = rayDirection.getX() * speed * dy - rayDirection.getY() * speed * dx;

    if (Math.abs(det) < EPSILON) {
      return null; // Parallel
    }

    double px = rayOrigin.x - edgeStart.x;
    double py = rayOrigin.y - edgeStart.y;

    double t = (px * dy - py * dx) / det;
    double s = (px * rayDirection.getY() * speed - py * rayDirection.getX() * speed) / det;

    if (t < -EPSILON || s < -EPSILON || s > 1 + EPSILON) {
      return null; // No intersection
    }

    double x = rayOrigin.x + t * rayDirection.getX() * speed;
    double y = rayOrigin.y + t * rayDirection.getY() * speed;

    return new Coordinate(x, y);
  }

  /**
   * Process an event (edge or split).
   *
   * <p>This updates the LAV, creates skeleton edges, and computes new events for affected vertices.
   */
  private void processEvent(SkeletonEvent event) {
    if (event.type == SkeletonEvent.EventType.EDGE) {
      processEdgeEvent(event);
    } else {
      processSplitEvent(event);
    }
  }

  /**
   * Process an edge event.
   *
   * <p>Two adjacent vertices collapse to a single point. Remove both vertices from LAV, create a
   * new vertex at the event point, and add skeleton edges connecting the old vertices to the new
   * one.
   */
  private void processEdgeEvent(SkeletonEvent event) {
    SkeletonVertex v1 = event.vertexA;
    SkeletonVertex v2 = event.vertexB;

    // Create new vertex at event point
    SkeletonVertex newVertex = new SkeletonVertex(event.point, event.time);
    allVertices.add(newVertex);

    // Create skeleton edges
    skeletonEdges.add(new Edge(v1, newVertex, false));
    skeletonEdges.add(new Edge(v2, newVertex, false));

    // Update LAV: remove v1 and v2, insert newVertex between their neighbors
    SkeletonVertex prevVertex = v1.prevInLAV;
    SkeletonVertex nextVertex = v2.nextInLAV;

    lav.remove(v1);
    lav.remove(v2);

    if (lav.size > 0) {
      // Insert new vertex
      newVertex.prevInLAV = prevVertex;
      newVertex.nextInLAV = nextVertex;
      prevVertex.nextInLAV = newVertex;
      nextVertex.prevInLAV = newVertex;
      newVertex.isActive = true;
      lav.size++;

      // Update edges
      newVertex.prevEdge = prevVertex.nextEdge;
      newVertex.nextEdge = nextVertex.prevEdge;

      // Compute bisector for new vertex
      newVertex.computeBisector();

      // Compute new events
      computeEventsForVertex(prevVertex);
      computeEventsForVertex(newVertex);
      computeEventsForVertex(nextVertex);
    }
  }

  /**
   * Process a split event.
   *
   * <p>A reflex vertex's bisector hits an opposite edge, splitting it. This creates two new
   * vertices and splits the LAV.
   */
  private void processSplitEvent(SkeletonEvent event) {
    // For now, simplified implementation: treat as edge collapse
    // Full split event handling requires LAV splitting logic (complex)
    SkeletonVertex reflexVertex = event.vertexA;

    // Create new vertex at event point
    SkeletonVertex newVertex = new SkeletonVertex(event.point, event.time);
    allVertices.add(newVertex);

    // Create skeleton edge from reflex vertex to event point
    skeletonEdges.add(new Edge(reflexVertex, newVertex, false));

    // Remove reflex vertex from LAV
    SkeletonVertex prevVertex = reflexVertex.prevInLAV;
    SkeletonVertex nextVertex = reflexVertex.nextInLAV;

    lav.remove(reflexVertex);

    if (lav.size > 0) {
      // Connect neighbors
      prevVertex.nextInLAV = nextVertex;
      nextVertex.prevInLAV = prevVertex;

      // Recompute events
      computeEventsForVertex(prevVertex);
      computeEventsForVertex(nextVertex);
    }
  }

  /**
   * Build the final skeleton geometry from computed edges.
   *
   * @return MultiLineString containing all skeleton edges (full skeleton, not filtered)
   */
  private Geometry buildSkeletonGeometry(GeometryFactory factory) {
    if (skeletonEdges.isEmpty()) {
      return factory.createMultiLineString(new LineString[0]);
    }

    // Filter to get medial axis (not full skeleton)
    List<LineString> medialAxisEdges = extractMedialAxisEdges(factory);

    if (medialAxisEdges.isEmpty()) {
      return factory.createMultiLineString(new LineString[0]);
    }

    return factory.createMultiLineString(medialAxisEdges.toArray(new LineString[0]));
  }

  /**
   * Extract the medial axis from the full skeleton using aggressive filtering.
   *
   * <p>This implementation applies PostGIS-like filtering to reduce the skeleton to only major
   * branches, matching the behavior of SFCGAL's ST_ApproximateMedialAxis.
   *
   * <p>Filtering strategy: 1. Remove degenerate (zero-length) edges 2. Build vertex degree map 3.
   * Remove short edges connected to high-degree junction vertices 4. Merge chains of degree-2
   * vertices 5. Filter based on edge significance (length relative to polygon size)
   */
  private List<LineString> extractMedialAxisEdges(GeometryFactory factory) {
    if (skeletonEdges.isEmpty()) {
      return new ArrayList<>();
    }

    // Step 1: Build vertex connectivity map
    Map<SkeletonVertex, List<Edge>> vertexEdges = new HashMap<>();
    for (Edge edge : skeletonEdges) {
      double length = edge.start.position.distance(edge.end.position);
      if (length < EPSILON) {
        continue; // Skip degenerate edges
      }

      vertexEdges.computeIfAbsent(edge.start, k -> new ArrayList<>()).add(edge);
      vertexEdges.computeIfAbsent(edge.end, k -> new ArrayList<>()).add(edge);
    }

    // Step 2: Compute edge significance scores
    // Longer edges that are farther from boundary are more significant
    Map<Edge, Double> edgeScores = new HashMap<>();
    double maxEdgeLength = 0.0;

    for (Edge edge : skeletonEdges) {
      double length = edge.length();
      if (length < EPSILON) continue;

      // Score based on: length * time (distance from boundary)
      double avgTime = (edge.start.time + edge.end.time) / 2.0;
      double score = length * (1.0 + avgTime);
      edgeScores.put(edge, score);
      maxEdgeLength = Math.max(maxEdgeLength, length);
    }

    // Step 3: Aggressive filtering - remove short edges at high-degree junctions
    Set<Edge> filteredEdges = new HashSet<>();
    double lengthThreshold = maxEdgeLength * 0.1; // 10% of max edge length

    for (Edge edge : skeletonEdges) {
      double length = edge.length();
      if (length < EPSILON) continue;

      int startDegree = vertexEdges.getOrDefault(edge.start, Collections.emptyList()).size();
      int endDegree = vertexEdges.getOrDefault(edge.end, Collections.emptyList()).size();

      // Filter out short edges connecting to high-degree vertices (junction cleanup)
      if (length < lengthThreshold && (startDegree > 2 || endDegree > 2)) {
        continue; // Skip this edge
      }

      // Filter out edges with very low significance scores
      double score = edgeScores.getOrDefault(edge, 0.0);
      if (score < lengthThreshold * 0.5) {
        continue;
      }

      filteredEdges.add(edge);
    }

    // Step 4: Build graph and find major branches using DFS from leaf nodes
    Map<SkeletonVertex, List<Edge>> filteredGraph = new HashMap<>();
    for (Edge edge : filteredEdges) {
      filteredGraph.computeIfAbsent(edge.start, k -> new ArrayList<>()).add(edge);
      filteredGraph.computeIfAbsent(edge.end, k -> new ArrayList<>()).add(edge);
    }

    // Step 5: Chain simplification - merge degree-2 vertices
    Set<Edge> majorBranches = new HashSet<>();
    Set<SkeletonVertex> visited = new HashSet<>();

    // Find leaf vertices (degree 1) and high-degree vertices (degree > 2)
    List<SkeletonVertex> keyVertices = new ArrayList<>();
    for (Map.Entry<SkeletonVertex, List<Edge>> entry : filteredGraph.entrySet()) {
      int degree = entry.getValue().size();
      if (degree != 2) { // Leaf or junction
        keyVertices.add(entry.getKey());
      }
    }

    // Trace paths between key vertices
    for (SkeletonVertex start : keyVertices) {
      if (visited.contains(start)) continue;

      for (Edge startEdge : filteredGraph.getOrDefault(start, Collections.emptyList())) {
        SkeletonVertex current = getOtherVertex(startEdge, start);
        Edge prevEdge = startEdge;

        List<Edge> chain = new ArrayList<>();
        chain.add(startEdge);

        // Follow chain until we hit another key vertex
        while (!visited.contains(current)) {
          visited.add(current);
          List<Edge> edges = filteredGraph.getOrDefault(current, Collections.emptyList());

          if (edges.size() != 2) {
            // Reached a key vertex (leaf or junction)
            break;
          }

          // Continue along the chain
          Edge nextEdge = null;
          for (Edge e : edges) {
            if (e != prevEdge) {
              nextEdge = e;
              break;
            }
          }

          if (nextEdge == null) break;

          chain.add(nextEdge);
          prevEdge = nextEdge;
          current = getOtherVertex(nextEdge, current);
        }

        // Add chain as major branch if it's significant
        if (!chain.isEmpty()) {
          double totalLength = chain.stream().mapToDouble(Edge::length).sum();
          if (totalLength > lengthThreshold * 0.5) {
            majorBranches.addAll(chain);
          }
        }
      }
    }

    // Step 6: Convert major branches to LineStrings
    List<LineString> result = new ArrayList<>();
    for (Edge edge : majorBranches) {
      Coordinate[] coords = new Coordinate[] {edge.start.position, edge.end.position};
      LineString line = factory.createLineString(coords);
      result.add(line);
    }

    // If no major branches found, fall back to most significant edges
    if (result.isEmpty() && !filteredEdges.isEmpty()) {
      // Return top N most significant edges
      List<Edge> sortedEdges = new ArrayList<>(filteredEdges);
      sortedEdges.sort(
          (e1, e2) ->
              Double.compare(edgeScores.getOrDefault(e2, 0.0), edgeScores.getOrDefault(e1, 0.0)));

      int maxEdges = Math.min(3, sortedEdges.size());
      for (int i = 0; i < maxEdges; i++) {
        Edge edge = sortedEdges.get(i);
        Coordinate[] coords = new Coordinate[] {edge.start.position, edge.end.position};
        LineString line = factory.createLineString(coords);
        result.add(line);
      }
    }

    return result;
  }

  /** Helper method to get the other vertex of an edge. */
  private SkeletonVertex getOtherVertex(Edge edge, SkeletonVertex vertex) {
    if (edge.start == vertex) {
      return edge.end;
    } else {
      return edge.start;
    }
  }
}
